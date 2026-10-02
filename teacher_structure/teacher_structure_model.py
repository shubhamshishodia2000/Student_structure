"""Teacher facts with observation-time SCD2; two current Gold reports."""
import os
import uuid
from doris_io import connect, healthy, query
from .teacher_structure_history import baseline, record_history, ident, table, columns, exists

SILVER_DB = os.getenv('UDISE_SILVER_DB','udise_silver')
GOLD_DB = os.getenv('UDISE_GOLD_DB','udise_gold')
REPLICATION_NUM = int(os.getenv('DORIS_REPLICATION_NUM','1'))
MANAGEMENT_GROUPS = ('Government','Government Aided','Private Unaided Recognized','Others')
GRADES = {1:'grades_1_5',2:'grades_1_8',4:'grades_6_8',6:'grades_1_10',
          7:'grades_6_10',8:'grades_9_10',3:'grades_1_12',5:'grades_6_12',
          10:'grades_9_12',11:'grades_11_12'}
FACT_COLUMNS = [
    ('academic_year','VARCHAR(7) NOT NULL'),('udise_sch_code','VARCHAR(32) NOT NULL'),
    ('state_cd','VARCHAR(10) NOT NULL'),('state_name','VARCHAR(160) NOT NULL'),
    ('management_center_id','INT NOT NULL'),('management_group','VARCHAR(64) NOT NULL'),
    ('sch_category_id','INT NOT NULL'),('category','VARCHAR(128) NOT NULL'),
    ('male_tch','BIGINT NOT NULL'),('female_tch','BIGINT NOT NULL'),
    ('transgen_tch','BIGINT NOT NULL'),('total_teachers','BIGINT NOT NULL'),
]
REPORT_KEYS = [('ac_year','VARCHAR(7) NOT NULL'),('india_state_ut','VARCHAR(160) NOT NULL')]
MANAGEMENT_COLUMNS = REPORT_KEYS + [('management','VARCHAR(64) NOT NULL')] + [
    (n,'BIGINT NOT NULL') for n in ('total','foundational_preparatory','middle','secondary',*GRADES.values())]
CATEGORY_COLUMNS = REPORT_KEYS + [('category','VARCHAR(128) NOT NULL')] + [
    (n,'BIGINT NOT NULL') for n in ('total','government','government_aided','private_unaided_recognized','others')]


def count(conn,sql): return int(query(conn,sql)[0]['n'])
def check(conn,sql,message):
    if count(conn,sql): raise RuntimeError(message)
def total_rows(conn,db,name): return count(conn,f'SELECT COUNT(*) n FROM {table(db,name)}')

def check_keys(conn,db,name,keys):
    cols=','.join(map(ident,keys))
    check(conn,f'SELECT COUNT(*) n FROM (SELECT {cols} FROM {table(db,name)} GROUP BY {cols} HAVING COUNT(*)>1) d',f'{db}.{name}: duplicate keys')


def source_preflight(conn):
    for name,keys in [('teacher_structure_snapshot',('academic_year','udise_sch_code')),
                      ('school_master_snapshot',('academic_year','udise_sch_code')),
                      ('state_master',('academic_year','state_cd')),
                      ('school_category_master',('academic_year','sch_category_id')),
                      ('dim_management',('management_sk',))]:
        if not exists(conn,SILVER_DB,name) or total_rows(conn,SILVER_DB,name)==0:
            raise RuntimeError(f'Missing/empty {SILVER_DB}.{name}; complete Student Silver and Teacher normalization')
        check_keys(conn,SILVER_DB,name,keys)
    check(conn,f'''SELECT COUNT(*) n FROM {table(SILVER_DB,'teacher_structure_snapshot')} t
    LEFT JOIN {table(SILVER_DB,'school_master_snapshot')} s
      ON s.academic_year=t.academic_year AND s.udise_sch_code=t.udise_sch_code
    LEFT JOIN {table(SILVER_DB,'state_master')} st
      ON st.academic_year=s.academic_year AND st.state_cd=s.state_cd
    LEFT JOIN {table(SILVER_DB,'school_category_master')} c
      ON c.academic_year=s.academic_year AND c.sch_category_id=s.sch_category_id
    LEFT JOIN {table(SILVER_DB,'dim_management')} m ON m.management_sk=s.management_center_id
    WHERE s.udise_sch_code IS NULL OR st.state_name IS NULL OR TRIM(st.state_name)=''
       OR c.sch_category_id IS NULL OR s.sch_category_id NOT IN (1,2,3,4,5,6,7,8,10,11,12)
       OR m.management_group IS NULL
       OR m.management_group NOT IN ('Government','Government Aided','Private Unaided Recognized','Others')
       OR t.male_tch IS NULL OR t.female_tch IS NULL OR t.transgen_tch IS NULL
       OR t.male_tch<0 OR t.female_tch<0 OR t.transgen_tch<0''',
       'Invalid teacher counts or missing school/state/category/management mappings')


def fact_select():
    return f'''SELECT t.academic_year,t.udise_sch_code,s.state_cd,st.state_name,
    s.management_center_id,m.management_group,s.sch_category_id,
    CASE WHEN s.sch_category_id IN (1,12) THEN 'Foundational + Preparatory School'
         WHEN s.sch_category_id IN (2,4) THEN 'Middle School'
         ELSE 'Secondary School' END category,
    t.male_tch,t.female_tch,t.transgen_tch,t.male_tch+t.female_tch+t.transgen_tch total_teachers
    FROM {table(SILVER_DB,'teacher_structure_snapshot')} t
    JOIN {table(SILVER_DB,'school_master_snapshot')} s
      ON s.academic_year=t.academic_year AND s.udise_sch_code=t.udise_sch_code
    JOIN {table(SILVER_DB,'state_master')} st
      ON st.academic_year=s.academic_year AND st.state_cd=s.state_cd
    JOIN {table(SILVER_DB,'school_category_master')} c
      ON c.academic_year=s.academic_year AND c.sch_category_id=s.sch_category_id
    JOIN {table(SILVER_DB,'dim_management')} m ON m.management_sk=s.management_center_id'''


def create(conn,db,name,spec,keys):
    if exists(conn,db,name):
        if [n for n,_,_ in columns(conn,db,name)] != [n for n,_ in spec]:
            raise RuntimeError(f'{db}.{name}: existing columns differ; review before migration')
        return
    query(conn,f'''CREATE TABLE {table(db,name)}
        ({','.join(ident(n)+' '+kind for n,kind in spec)})
        DUPLICATE KEY ({','.join(map(ident,keys))})
        DISTRIBUTED BY HASH({ident(keys[0])}) BUCKETS 8
        PROPERTIES ("replication_num"="{REPLICATION_NUM}")''')


def stage_publish(conn,db,target,spec,keys,select,validator,keep_history=False):
    create(conn,db,target,spec,keys)
    stage=target+'__stage_'+uuid.uuid4().hex[:12]
    query(conn,f'CREATE TABLE {table(db,stage)} LIKE {table(db,target)}')
    try:
        query(conn,f'INSERT INTO {table(db,stage)} ({",".join(ident(n) for n,_ in spec)}) {select}')
        check_keys(conn,db,stage,keys)
        validator(stage)
        if keep_history:
            baseline(conn,db,target,target+'_history',keys)
            record_history(conn,db,stage,target+'_history',keys)
        query(conn,f'ALTER TABLE {table(db,target)} REPLACE WITH TABLE {ident(stage)} PROPERTIES ("swap"="true")')
        print(f'PUBLISHED {db}.{target}: {total_rows(conn,db,target):,} rows')
    finally:
        query(conn,f'DROP TABLE IF EXISTS {table(db,stage)}')


def build_fact():
    conn=connect()
    try:
        healthy(conn)
        source_preflight(conn)
        def validate(name):
            if total_rows(conn,SILVER_DB,name)!=total_rows(conn,SILVER_DB,'teacher_structure_snapshot'):
                raise RuntimeError('Teacher fact/source count mismatch')
            check(conn,f'SELECT COUNT(*) n FROM {table(SILVER_DB,name)} WHERE total_teachers<>male_tch+female_tch+transgen_tch','Teacher fact totals do not reconcile')
        stage_publish(conn,SILVER_DB,'fact_teacher_structure',FACT_COLUMNS,
                      ('academic_year','udise_sch_code'),fact_select(),validate,True)
    finally: conn.close()


def report_cte():
    return f'''WITH mapped AS (
      SELECT academic_year ac_year,state_name india_state_ut,management_group management,
             category,sch_category_id,total_teachers
      FROM {table(SILVER_DB,'fact_teacher_structure')}
    ), geography AS (
      SELECT * FROM mapped
      UNION ALL SELECT ac_year,'Available Source Total',management,category,sch_category_id,total_teachers FROM mapped
    )'''


def management_select():
    expressions=['SUM(total_teachers)',
        'SUM(CASE WHEN sch_category_id IN (1,12) THEN total_teachers ELSE 0 END)',
        'SUM(CASE WHEN sch_category_id IN (2,4) THEN total_teachers ELSE 0 END)',
        'SUM(CASE WHEN sch_category_id IN (3,5,6,7,8,10,11) THEN total_teachers ELSE 0 END)']
    # Report primary column includes pre-primary-only teachers, matching supplied HP/Maharashtra examples.
    expressions += [f'SUM(CASE WHEN sch_category_id {"IN (1,12)" if code==1 else "="+str(code)} THEN total_teachers ELSE 0 END)' for code in GRADES]
    return report_cte()+f''', report AS (
      SELECT * FROM geography
      UNION ALL SELECT ac_year,india_state_ut,'All Management',category,sch_category_id,total_teachers FROM geography
    ) SELECT ac_year,india_state_ut,management,{','.join(expressions)}
    FROM report GROUP BY ac_year,india_state_ut,management'''


def category_select():
    metrics = ['SUM(total_teachers)']+[f"SUM(CASE WHEN management='{group}' THEN total_teachers ELSE 0 END)" for group in MANAGEMENT_GROUPS]
    return report_cte()+f''' SELECT ac_year,india_state_ut,category,{','.join(metrics)}
    FROM geography GROUP BY ac_year,india_state_ut,category'''


def validate_management(conn,name):
    if not total_rows(conn,GOLD_DB,name): raise RuntimeError('Empty teacher management report')
    check(conn,f'''SELECT COUNT(*) n FROM {table(GOLD_DB,name)}
       WHERE total<>foundational_preparatory+middle+secondary
          OR total<>({'+'.join(GRADES.values())})''','Teacher report categories do not reconcile')
    check(conn,f'''SELECT COUNT(*) n FROM (
      SELECT ac_year,india_state_ut,
       MAX(CASE WHEN management='All Management' THEN total END) a,
       SUM(CASE WHEN management<>'All Management' THEN total ELSE 0 END) b
      FROM {table(GOLD_DB,name)} GROUP BY ac_year,india_state_ut
    ) x WHERE a<>b OR a IS NULL''','Teacher management groups do not reconcile')
    check(conn,f'''SELECT COUNT(*) n FROM (
       SELECT f.academic_year FROM {table(SILVER_DB,'fact_teacher_structure')} f
       LEFT JOIN {table(GOLD_DB,name)} g ON g.ac_year=f.academic_year
       AND g.india_state_ut='Available Source Total' AND g.management='All Management'
       GROUP BY f.academic_year HAVING MAX(g.total) IS NULL OR MAX(g.total)<>SUM(f.total_teachers)
    ) x''','Teacher report differs from source fact totals')


def validate_category(conn,name):
    if not total_rows(conn,GOLD_DB,name): raise RuntimeError('Empty teacher category report')
    check(conn,f'SELECT COUNT(*) n FROM {table(GOLD_DB,name)} WHERE total<>government+government_aided+private_unaided_recognized+others','Category management split does not reconcile')
    check(conn,f'''SELECT COUNT(*) n FROM (
      SELECT c.ac_year,c.india_state_ut FROM {table(GOLD_DB,name)} c
      LEFT JOIN {table(GOLD_DB,'teacher_structure_management')} m
       ON m.ac_year=c.ac_year AND m.india_state_ut=c.india_state_ut AND m.management='All Management'
      GROUP BY c.ac_year,c.india_state_ut HAVING MAX(m.total) IS NULL OR SUM(c.total)<>MAX(m.total)
    ) x''','Category report differs from management report')


def build_gold(conn):
    if SILVER_DB==GOLD_DB: raise ValueError('Silver and Gold must differ')
    if not exists(conn,SILVER_DB,'fact_teacher_structure'): raise RuntimeError('Run Teacher Silver first')
    check_keys(conn,SILVER_DB,'fact_teacher_structure',('academic_year','udise_sch_code'))
    query(conn,f'CREATE DATABASE IF NOT EXISTS {ident(GOLD_DB)}')
    stage_publish(conn,GOLD_DB,'teacher_structure_management',MANAGEMENT_COLUMNS,
        ('ac_year','india_state_ut','management'),management_select(),lambda n: validate_management(conn,n))
    stage_publish(conn,GOLD_DB,'teacher_structure_category',CATEGORY_COLUMNS,
        ('ac_year','india_state_ut','category'),category_select(),lambda n: validate_category(conn,n))
    print('GOLD: PASS')


