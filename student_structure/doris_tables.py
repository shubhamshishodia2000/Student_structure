"""Doris 4.x schema contract. Business mappings are not inferred from names."""
import re
from dataclasses import dataclass


def ident(value):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', value):
        raise ValueError(f'Unsafe SQL identifier: {value!r}')
    return f'`{value}`'


@dataclass(frozen=True)
class Table:
    database: str  # logical database role: silver / audit
    name: str
    keys: tuple
    columns: tuple  # (name, SQL type, nullable)

    def ddl(self, databases, replicas=1, buckets=2):
        if replicas < 1 or buckets < 1:
            raise ValueError('Replication and bucket counts must be positive')
        if tuple(c[0] for c in self.columns[:len(self.keys)]) != self.keys:
            raise ValueError(f'{self.name}: key columns must be first')
        if len(set(c[0] for c in self.columns)) != len(self.columns):
            raise ValueError(f'{self.name}: repeated column')
        cols = ',\n    '.join(f'{ident(n)} {t} {"NULL" if null else "NOT NULL"}' for n,t,null in self.columns)
        keys = ', '.join(map(ident, self.keys))
        return (f'CREATE TABLE IF NOT EXISTS {ident(databases[self.database])}.{ident(self.name)} (\n    {cols}\n)\n'
                f'ENGINE=OLAP\nUNIQUE KEY ({keys})\nDISTRIBUTED BY HASH ({ident(self.keys[0])}) BUCKETS {buckets}\n'
                f'PROPERTIES ("replication_num"="{replicas}", "enable_unique_key_merge_on_write"="true");')


def cols(*specs):
    return tuple((name, typ, nullable) for name,typ,nullable in specs)


LINEAGE = cols(
    ('_academic_year','VARCHAR(7)',False), ('_source_database','VARCHAR(128)',False),
    ('_source_schema','VARCHAR(128)',False), ('_source_table','VARCHAR(128)',False),
    ('_batch_id','CHAR(36)',False), ('_extracted_at','DATETIME(6)',True),
    ('_row_hash','CHAR(64)',False),
)
HISTORY = cols(
    ('_attribute_hash','CHAR(64)',False), ('_effective_from_year_key','INT',False),
    ('_effective_to_year_key','INT',True), ('_is_current','BOOLEAN',False),
    ('_version_number','INT',False), ('_created_batch_id','CHAR(36)',False),
    ('_closed_batch_id','CHAR(36)',True),
)
SCHOOL_ATTRIBUTES = tuple((x, 'VARCHAR(255)' if x=='school_name' else 'VARCHAR(32)', True) for x in (
    'school_name','state_cd','district_cd','block_cd','vill_ward_cd','cluster_cd','inityear')) + tuple(
    (x,'BIGINT',True) for x in ('sch_loc_r_u','sch_category_id','sch_type','sch_mgmt_id','class_frm',
    'class_to','sch_mgmt_center_id','school_status','ppsec_yn','ppsec_cls_frm','strm_arts',
    'strm_science','strm_commerce','strm_vocational','strm_other','is_nsqf'))
FACT_KEY = cols(('academic_year_key','INT',False), ('udise_sch_code','VARCHAR(11)',False),
                ('item_group','BIGINT',False), ('item_id','BIGINT',False))
FACT_KEYS = tuple(x[0] for x in FACT_KEY)
FACT_BASE = FACT_KEY + cols(('school_sk','CHAR(64)',False))
STUDENT_MEASURES = tuple((f'c{i}_{g}','BIGINT',True) for i in range(1,13) for g in ('b','g')) + tuple(
    (f'pp{i}_{g}','BIGINT',True) for i in (3,2,1) for g in ('b','g','t')) + tuple(
    (f'c{i}_t','BIGINT',True) for i in range(1,13))
TABLES = [
    Table('silver','dim_academic_year',('academic_year_key',), cols(
        ('academic_year_key','INT',False),('academic_year','VARCHAR(7)',False),
        ('start_year','INT',False),('end_year','INT',False))),
    Table('silver','school_history',('school_sk',), cols(
        ('school_sk','CHAR(64)',False),('school_bk','VARCHAR(64)',False),
        ('udise_sch_code','VARCHAR(11)',False)) + SCHOOL_ATTRIBUTES + HISTORY + LINEAGE + cols(
        ('_valid_from','DATETIME(6)',False),('_valid_to','DATETIME(6)',True))),
    Table('silver','block_history',('block_sk',), cols(
        ('block_sk','CHAR(64)',False),('block_bk','VARCHAR(128)',False),
        ('udise_block_code','VARCHAR(32)',True),('block_name','VARCHAR(255)',True),
        ('udise_dist_code','VARCHAR(32)',True),('udise_state_code','VARCHAR(32)',True),
        ('inityear','VARCHAR(32)',True)) + HISTORY + LINEAGE),
    Table('silver','student_fact',FACT_KEYS,FACT_BASE + STUDENT_MEASURES + LINEAGE),
    # These two are declared target contracts; source-to-item mappings must be supplied.
    Table('silver','teacher_fact',FACT_KEYS,FACT_BASE + cols(
        ('male_count','BIGINT',True),('female_count','BIGINT',True),
        ('transgender_count','BIGINT',True),('teacher_count','BIGINT',True)) + LINEAGE),
    Table('silver','school_fact',FACT_KEYS,FACT_BASE + cols(
        ('numeric_value','DECIMAL(38,10)',True),('text_value','STRING',True),
        ('boolean_value','BOOLEAN',True)) + LINEAGE),
    Table('silver','dim_item',('fact','item_group','item_id'),cols(
        ('fact','VARCHAR(100)',False),('item_group','BIGINT',False),('item_id','BIGINT',False),
        ('item_group_name','VARCHAR(255)',False),('item_name','VARCHAR(255)',False),
        ('unit','VARCHAR(64)',False),('aggregation_rule','VARCHAR(255)',False),
        ('definition_source','VARCHAR(500)',False))),
]
for entity, code, parents in (
    ('state','udise_state_code',()),
    ('district','udise_dist_code',('udise_state_code',)),
    ('cluster','udise_cluster_code',('udise_block_code','udise_dist_code','udise_state_code')),
):
    TABLES.append(Table('silver',f'{entity}_history',(f'{entity}_sk',), cols(
        (f'{entity}_sk','CHAR(64)',False),(f'{entity}_bk','VARCHAR(128)',False),
        (code,'VARCHAR(32)',True),(f'{entity}_name','VARCHAR(255)',True)) + tuple(
        (x,'VARCHAR(32)',True) for x in parents) + HISTORY + LINEAGE))

ROW_COUNTS = cols(('source_rows','BIGINT',False),('accepted_rows','BIGINT',False),('rejected_rows','BIGINT',False))
TABLES += [
    Table('audit','bronze_dataset',('bronze_dataset_id',),cols(
        ('bronze_dataset_id','CHAR(36)',False),('academic_year_key','INT',False),
        ('source_database','VARCHAR(128)',False),('source_schema','VARCHAR(128)',False),
        ('source_table','VARCHAR(128)',False),('source_revision','VARCHAR(128)',False),
        ('source_rows','BIGINT',False),('path','VARCHAR(2048)',False),('signature','CHAR(64)',False),
        ('batch_id','CHAR(36)',False),('registered_at','DATETIME(6)',False))),
    Table('audit','pipeline_batch',('batch_id',),cols(
        ('batch_id','CHAR(36)',False),('stage','VARCHAR(100)',False),('status','VARCHAR(32)',False),
        ('source_signature','CHAR(64)',True),('started_at','DATETIME(6)',False),
        ('completed_at','DATETIME(6)',True),('message','STRING',True))),
    Table('audit','batch_input',('batch_id','bronze_dataset_id'),cols(
        ('batch_id','CHAR(36)',False),('bronze_dataset_id','CHAR(36)',False),('input_role','VARCHAR(100)',False))),
    Table('audit','rejected_record',('batch_id','entity','record_id'),cols(
        ('batch_id','CHAR(36)',False),('entity','VARCHAR(100)',False),('record_id','CHAR(64)',False),
        ('academic_year_key','INT',False),('reason','STRING',False),('source_record','JSON',False),
        ('rejected_at','DATETIME(6)',False))),
    Table('audit','fact_batch_event',('event_id',),cols(
        ('event_id','CHAR(36)',False),('fact','VARCHAR(100)',False),('academic_year_key','INT',False),
        ('batch_id','CHAR(36)',False),('bronze_dataset_id','CHAR(36)',False)) + ROW_COUNTS + cols(
        ('status','VARCHAR(32)',False),('applied_at','DATETIME(6)',False))),
    Table('audit','fact_year_batch',('fact','academic_year_key'),cols(
        ('fact','VARCHAR(100)',False),('academic_year_key','INT',False),('batch_id','CHAR(36)',False),
        ('bronze_dataset_id','CHAR(36)',False)) + ROW_COUNTS + cols(('applied_at','DATETIME(6)',False))),
    Table('audit','scd2_year_batch',('entity','academic_year_key'),cols(
        ('entity','VARCHAR(100)',False),('academic_year_key','INT',False),('batch_id','CHAR(36)',False),
        ('bronze_dataset_id','CHAR(36)',False)) + ROW_COUNTS + cols(('applied_at','DATETIME(6)',False))),
    Table('audit','scd2_snapshot',('entity','academic_year_key','business_key','batch_id'),cols(
        ('entity','VARCHAR(100)',False),('academic_year_key','INT',False),('business_key','VARCHAR(128)',False),
        ('batch_id','CHAR(36)',False),('academic_year','VARCHAR(7)',False),('attributes','JSON',False),
        ('lineage','JSON',False),('explicitly_closed','BOOLEAN',False),('attribute_hash','CHAR(64)',False),
        ('version_key','CHAR(64)',True))),
    Table('audit','scd2_reconciliation',('entity','academic_year_key','batch_id'),cols(
        ('entity','VARCHAR(100)',False),('academic_year_key','INT',False),('batch_id','CHAR(36)',False)) + tuple(
        (x,'BIGINT',False) for x in ('observed_count','opened_count','changed_count','closed_count','version_count','current_count')) + cols(
        ('reconciled_at','DATETIME(6)',False))),
    Table('audit','gold_batch',('batch_id',),cols(
        ('batch_id','CHAR(36)',False),('academic_year_key','INT',False),('source_database','VARCHAR(128)',False),
        ('coverage','VARCHAR(20)',False),('status','VARCHAR(20)',False),('source_signature','CHAR(64)',False),
        ('school_rows','BIGINT',False),('medium_quarantined_rows','BIGINT',False),('built_at','DATETIME(6)',False),
        ('evaluated_at','DATETIME(6)',True),('certified_at','DATETIME(6)',True))),
    Table('audit','gold_check',('batch_id','model','metric'),cols(
        ('batch_id','CHAR(36)',False),('model','VARCHAR(100)',False),('metric','VARCHAR(100)',False),
        ('baseline_source','VARCHAR(200)',False),('owner_ref','VARCHAR(200)',False),
        ('expected_total','DECIMAL(38,10)',True),('observed_total','DECIMAL(38,10)',True),
        ('absolute_tolerance','DECIMAL(38,10)',False),('state','VARCHAR(20)',False),
        ('exception_ref','VARCHAR(200)',True),('exception_approved','BOOLEAN',False),('checked_at','DATETIME(6)',False))),
    Table('audit','table_load',('load_id',),cols(
        ('load_id','CHAR(36)',False),('target_database','VARCHAR(128)',False),('target_table','VARCHAR(128)',False),
        ('staging_table','VARCHAR(128)',False),('signature','CHAR(64)',False),('expected_rows','BIGINT',False),
        ('status','VARCHAR(32)',False),('started_at','DATETIME(6)',False),('completed_at','DATETIME(6)',True))),
]
BY_NAME = {t.name:t for t in TABLES}
