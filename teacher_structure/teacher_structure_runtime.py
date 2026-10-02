"""Teacher-only staging and publication helpers; parent project supplies Doris/config."""
import os
from pathlib import Path
import uuid
from dataclasses import dataclass
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
import project_config as c
from doris_io import connect, healthy, query, stream_file
from .teacher_structure_history import baseline, record_history, ident, table

SILVER_DB = os.getenv('UDISE_SILVER_DB', 'udise_silver')
PROJECT_DIR = Path(getattr(c, 'PROJECT_DIR', Path(__file__).resolve().parents[1])).resolve()
SILVER_STAGE_ROOT = Path(os.getenv('TEACHER_STRUCTURE_SILVER_STAGE_ROOT',
    str(PROJECT_DIR / 'udise_data/teacher_structure/silver_stage')))
REPLICATION_NUM = int(getattr(c, 'DORIS_REPLICATION_NUM', os.getenv('DORIS_REPLICATION_NUM','1')))

def make_spark(name):
    spark = (SparkSession.builder.appName(name)
        .config('spark.driver.memory',os.getenv('SPARK_DRIVER_MEMORY','4g'))
        .config('spark.driver.maxResultSize','2g')
        .config('spark.sql.shuffle.partitions',os.getenv('SPARK_SHUFFLE_PARTITIONS','8'))
        .config('spark.sql.files.maxPartitionBytes','64m').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    return spark

@dataclass(frozen=True)
class TableSpec:
    table: str
    columns: tuple
    keys: tuple
    distribution: tuple
    partitions: int = 8
    @property
    def column_names(self):
        return tuple(n for n,_,_ in self.columns)
    @property
    def local_path(self):
        return SILVER_STAGE_ROOT / self.table

def files(path):
    result = sorted(path.rglob('*.parquet'))
    if not result: raise RuntimeError(f'No Parquet under {path}')
    return result

def write_stage(df,spec):
    spec.local_path.parent.mkdir(parents=True,exist_ok=True)
    df.select(*spec.column_names).repartition(spec.partitions).write.mode('overwrite').parquet(str(spec.local_path))
    rows = sum(pq.ParquetFile(p).metadata.num_rows for p in files(spec.local_path))
    print(f'STAGE {spec.table}: {rows:,} rows')
    return rows

def exists(conn,db,name):
    return bool(query(conn,f'SHOW TABLES FROM {ident(db)} LIKE %s',(name,)))

def count(conn,db,name):
    return int(query(conn,f'SELECT COUNT(*) n FROM {table(db,name)}')[0]['n'])

def ddl(spec,name):
    columns = ','.join(f'{ident(n)} {kind} '+('NULL' if nullable else 'NOT NULL') for n,kind,nullable in spec.columns)
    return f'''CREATE TABLE {table(SILVER_DB,name)} ({columns})
    DUPLICATE KEY ({','.join(map(ident,spec.keys))})
    DISTRIBUTED BY HASH({','.join(map(ident,spec.distribution))}) BUCKETS {spec.partitions}
    PROPERTIES ("replication_num"="{REPLICATION_NUM}")'''

def publish(spec,expected):
    conn = connect()
    stage = spec.table+'__stage_'+uuid.uuid4().hex[:12]
    try:
        healthy(conn)
        query(conn,f'CREATE DATABASE IF NOT EXISTS {ident(SILVER_DB)}')
        if not exists(conn,SILVER_DB,spec.table): query(conn,ddl(spec,spec.table))
        query(conn,ddl(spec,stage))
        for i,p in enumerate(files(spec.local_path)):
            stream_file(SILVER_DB,stage,p,f'teacher_{uuid.uuid4().hex}_{i}',
                pq.ParquetFile(p).metadata.num_rows,spec.column_names,file_format='parquet')
        if count(conn,SILVER_DB,stage)!=expected: raise RuntimeError('Teacher load count mismatch')
        baseline(conn,SILVER_DB,spec.table,spec.table+'_history',spec.keys)
        record_history(conn,SILVER_DB,stage,spec.table+'_history',spec.keys)
        query(conn,f'ALTER TABLE {table(SILVER_DB,spec.table)} REPLACE WITH TABLE {ident(stage)} PROPERTIES ("swap"="true")')
        if count(conn,SILVER_DB,spec.table)!=expected: raise RuntimeError('Teacher final count mismatch')
        print(f'PUBLISHED {SILVER_DB}.{spec.table}: {expected:,}')
    finally:
        try: query(conn,f'DROP TABLE IF EXISTS {table(SILVER_DB,stage)}')
        finally: conn.close()
