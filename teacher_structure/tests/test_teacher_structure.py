"""Offline SQL compatibility tests; no live Doris or source data is changed."""
import hashlib
import importlib
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import types
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))

class Harness:
    def __init__(self):
        self.conn=sqlite3.connect(':memory:')
        self.conn.row_factory=sqlite3.Row
        for db in ('udise_silver','udise_gold'): self.conn.execute(f"ATTACH ':memory:' AS {db}")
        self.conn.create_function('MD5',1,lambda s:hashlib.md5(str(s).encode()).hexdigest())
        self.conn.create_function('CONCAT',-1,lambda *args:''.join(str(a) for a in args))
        self.conn.create_function('IF',3,lambda c,a,b:a if c else b)
    def query(self,conn,sql,params=()):
        sql=sql.strip()
        if sql.startswith('SHOW TABLES FROM'):
            db=re.search(r'FROM `([^`]+)`',sql)[1]
            return [dict(r) for r in conn.execute(f'SELECT name FROM {db}.sqlite_master WHERE type="table" AND name LIKE ?',params)]
        if sql.startswith('DESC '):
            db,name=re.findall(r'`([^`]+)`',sql)
            return [dict(Field=r['name'],Type=r['type'],Null='NO' if r['notnull'] else 'YES')
                    for r in conn.execute(f'PRAGMA {db}.table_info(`{name}`)')]
        if sql.startswith('CREATE DATABASE'): return []
        if sql.startswith('CREATE TABLE') and ' LIKE ' in sql:
            db,name,sdb,source=re.findall(r'`([^`]+)`',sql)
            ddl=conn.execute(f'SELECT sql FROM {sdb}.sqlite_master WHERE name=?',(source,)).fetchone()[0]
            sql=f'CREATE TABLE `{db}`.`{name}` '+ddl[ddl.index('('):]
        elif sql.startswith('CREATE TABLE'):
            sql=re.split(r'\s+DUPLICATE KEY\s*\(',sql,flags=re.I)[0]
        elif sql.startswith('ALTER TABLE'):
            names=re.findall(r'`([^`]+)`',sql)
            if 'REPLACE WITH TABLE' in sql:
                db,target,stage=names
                conn.execute(f'ALTER TABLE {db}.`{target}` RENAME TO swap_temp')
                conn.execute(f'ALTER TABLE {db}.`{stage}` RENAME TO `{target}`')
                conn.execute(f'ALTER TABLE {db}.swap_temp RENAME TO `{stage}`')
                return []
            db,target,new=names
            conn.execute(f'ALTER TABLE {db}.`{target}` RENAME TO `{new}`'); return []
        sql=re.sub(r"CAST\(('.*?') AS DATETIMEV2\(6\)\)",r'\1',sql)
        sql=re.sub(r'AS STRING\b','AS TEXT',sql)
        cursor=conn.execute(sql,params)
        return [dict(r) for r in cursor.fetchall()] if cursor.description else []

h=Harness()
fake=types.ModuleType('doris_io');fake.connect=lambda:h.conn;fake.healthy=lambda c:None;fake.query=h.query
sys.modules['doris_io']=fake
model=importlib.import_module('teacher_structure.teacher_structure_model')
history=importlib.import_module('teacher_structure.teacher_structure_history')

class TeacherTests(unittest.TestCase):
    def setUp(self):
        global h
        h=Harness(); fake.query=h.query; model.query=h.query; history.query=h.query
        self.c=h.conn
        self.c.executescript('''
        CREATE TABLE udise_silver.teacher_structure_snapshot(academic_year VARCHAR(7) NOT NULL,udise_sch_code VARCHAR(32) NOT NULL,male_tch BIGINT NOT NULL,female_tch BIGINT NOT NULL,transgen_tch BIGINT NOT NULL);
        CREATE TABLE udise_silver.school_master_snapshot(academic_year,udise_sch_code,state_cd,management_center_id,sch_category_id);
        CREATE TABLE udise_silver.state_master(academic_year,state_cd,state_name);
        CREATE TABLE udise_silver.school_category_master(academic_year,sch_category_id);
        CREATE TABLE udise_silver.dim_management(management_sk,management_group);
        INSERT INTO udise_silver.state_master VALUES('2025-26','27','Maharashtra'),('2024-25','27','Maharashtra');
        ''')
        for i,group in enumerate(model.MANAGEMENT_GROUPS,1): self.c.execute('INSERT INTO udise_silver.dim_management VALUES(?,?)',(i,group))
        for code in (*model.GRADES,12): self.c.execute('INSERT INTO udise_silver.school_category_master VALUES(?,?)',('2025-26',code))
    def add(self,code,total,group=1,school=None):
        school=school or str(code)
        self.c.execute('INSERT INTO udise_silver.teacher_structure_snapshot VALUES(?,?,?,?,?)',('2025-26',school,total-1,0,1))
        self.c.execute('INSERT INTO udise_silver.school_master_snapshot VALUES(?,?,?,?,?)',('2025-26',school,'27',group,code))
    def fact(self):
        model.source_preflight(self.c)
        model.stage_publish(self.c,model.SILVER_DB,'fact_teacher_structure',model.FACT_COLUMNS,
            ('academic_year','udise_sch_code'),model.fact_select(),lambda n:None,True)
    def test_maharashtra_and_report_reconciliation(self):
        numbers=[136154,193891,272,154013,29421,4060,188668,14851,1651,27255]
        for i,(code,total) in enumerate(zip(model.GRADES,numbers)): self.add(code,total,i%4+1)
        self.add(12,36)
        self.fact();model.build_gold(self.c)
        row=self.c.execute("SELECT * FROM udise_gold.teacher_structure_management WHERE india_state_ut='Maharashtra' AND management='All Management'").fetchone()
        self.assertEqual(row['total'],750272)
        self.assertEqual([row[n] for n in model.GRADES.values()],[136190,*numbers[1:]])
        self.assertEqual([row[n] for n in ('foundational_preparatory','middle','secondary')],[136190,194163,419919])
        rows=self.c.execute("SELECT * FROM udise_gold.teacher_structure_category WHERE india_state_ut='Maharashtra'").fetchall()
        self.assertEqual(len(rows),3)
        self.assertEqual(sum(r['total'] for r in rows),750272)
        self.assertTrue(all(r['total']==sum(r[k] for k in ('government','government_aided','private_unaided_recognized','others')) for r in rows))
        self.assertFalse(model.exists(self.c,model.GOLD_DB,'teacher_structure_ptr'))
        self.assertFalse(model.exists(self.c,model.GOLD_DB,'teacher_structure_summary'))
        self.assertFalse(model.exists(self.c,model.GOLD_DB,'teacher_structure_management_history'))
    def test_historical_join_and_mapping_failure(self):
        self.add(1,10)
        self.c.execute("INSERT INTO udise_silver.school_master_snapshot VALUES('2024-25','1','27',2,4)")
        self.fact()
        self.assertEqual(model.total_rows(self.c,model.SILVER_DB,'fact_teacher_structure'),1)
        self.c.execute('DELETE FROM udise_silver.dim_management WHERE management_sk=1')
        with self.assertRaises(RuntimeError): model.source_preflight(self.c)
    def test_persistent_scd2_cycle_and_mapped_changes(self):
        self.add(1,10)
        self.fact()
        name='udise_silver.fact_teacher_structure_history'
        self.fact()
        self.assertEqual(self.c.execute(f'SELECT COUNT(*) FROM {name}').fetchone()[0],1)
        self.c.execute('UPDATE udise_silver.teacher_structure_snapshot SET male_tch=male_tch+10')
        self.fact()
        rows=self.c.execute(f'SELECT * FROM {name} ORDER BY version_no').fetchall()
        self.assertEqual([r['total_teachers'] for r in rows],[10,20])
        self.assertEqual(rows[0]['valid_to'],rows[1]['valid_from'])
        self.assertEqual([r['is_current'] for r in rows],[0,1])
        self.c.execute('UPDATE udise_silver.school_master_snapshot SET management_center_id=2')
        self.fact()
        self.assertEqual(self.c.execute(f'SELECT management_group FROM {name} WHERE is_current=1').fetchone()[0],'Government Aided')
        # Restoration records another revision rather than overwriting history.
        self.c.execute('UPDATE udise_silver.teacher_structure_snapshot SET male_tch=male_tch-10')
        self.fact()
        self.assertEqual(self.c.execute(f'SELECT total_teachers FROM {name} WHERE is_current=1').fetchone()[0],10)
        self.add(12,1)
        self.c.execute("DELETE FROM udise_silver.teacher_structure_snapshot WHERE udise_sch_code='1'")
        self.fact()
        r=self.c.execute(f"SELECT change_type,is_deleted FROM {name} WHERE is_current=1 AND udise_sch_code='1'").fetchone()
        self.assertEqual(tuple(r),('DELETE',1))
        self.c.execute("INSERT INTO udise_silver.teacher_structure_snapshot VALUES('2025-26','1',9,0,1)")
        self.fact()
        r=self.c.execute(f"SELECT change_type,is_deleted,version_no FROM {name} WHERE is_current=1 AND udise_sch_code='1'").fetchone()
        self.assertEqual(tuple(r),('REACTIVATED',0,6))
    def test_snapshot_scd2(self):
        self.add(1,10)
        for _ in range(2): history.record_history(self.c,'udise_silver','teacher_structure_snapshot','teacher_structure_snapshot_history',('academic_year','udise_sch_code'))
        self.c.execute('UPDATE udise_silver.teacher_structure_snapshot SET male_tch=male_tch+10')
        history.record_history(self.c,'udise_silver','teacher_structure_snapshot','teacher_structure_snapshot_history',('academic_year','udise_sch_code'))
        self.assertEqual(self.c.execute('SELECT COUNT(*) FROM udise_silver.teacher_structure_snapshot_history').fetchone()[0],2)
    def test_real_parquet_apply_restore_and_guard(self):
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError: self.skipTest('pyarrow not installed')
        config=types.ModuleType('project_config');config.PROJECT_DIR=Path(tempfile.gettempdir());sys.modules['project_config']=config
        helper=importlib.import_module('teacher_structure.test_real_teacher_row')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'bronze';root.mkdir()
            path=root/'part.parquet'
            original=pa.table({'udise_sch_code':['02010100501','02010100502'],'male_tch':['6','7'],'female_tch':[4,5],'transgen_tch':[0,0]})
            pq.write_table(original,path)
            sha=helper.digest(path)
            crc=path.with_name('.'+path.name+'.crc');crc.write_bytes(b'original checksum')
            backup=Path(tmp)/'backup'
            helper.apply(root,'02010100501','male_tch',10,backup,'2025-26')
            changed=pq.ParquetFile(path).read()
            self.assertEqual(changed['male_tch'].to_pylist(),['16','7'])
            self.assertFalse(crc.exists())
            helper.restore(backup/'state.json')
            self.assertEqual(helper.digest(path),sha)
            self.assertEqual(crc.read_bytes(),b'original checksum')
            helper.restore(backup/'state.json')
            path.write_bytes(b'unrelated edit')
            with self.assertRaises(RuntimeError): helper.restore(backup/'state.json')

if __name__=='__main__': unittest.main()
