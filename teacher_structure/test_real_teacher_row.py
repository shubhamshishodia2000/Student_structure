#!/usr/bin/env python3
"""Inspect, back up, change and restore one Bronze teacher count; show persistent SCD2."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import re
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from . import teacher_structure_bronze as bronze


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''): h.update(chunk)
    return h.hexdigest()


def locate(root,code,column):
    matches=[]
    for path in sorted(root.rglob('*.parquet')):
        pf=pq.ParquetFile(path)
        names={n.lower():n for n in pf.schema_arrow.names}
        if 'udise_sch_code' not in names or column not in names:
            raise RuntimeError(f'{path}: missing requested column')
        data=pf.read(columns=[names['udise_sch_code'],names[column]])
        mask=pc.equal(pc.utf8_trim_whitespace(pc.cast(data[names['udise_sch_code']],pa.string())),code)
        for i in pc.indices_nonzero(pc.fill_null(mask,False)).to_pylist():
            matches.append((path,i,names[column],data[names[column]][i].as_py()))
    if len(matches)!=1: raise RuntimeError(f'Expected exactly one school row; found {len(matches)}. Nothing changed.')
    return matches[0]


def history(year,code):
    from doris_io import connect,query
    from .teacher_structure_history import table,exists
    db=os.getenv('UDISE_SILVER_DB','udise_silver')
    conn=connect()
    try:
        for name in ('teacher_structure_snapshot','fact_teacher_structure',
                     'teacher_structure_snapshot_history','fact_teacher_structure_history'):
            print('\n'+db+'.'+name)
            if not exists(conn,db,name):
                print('Not built yet'); continue
            order='version_no' if name.endswith('_history') else 'academic_year'
            for row in query(conn,f'SELECT * FROM {table(db,name)} WHERE academic_year=%s AND udise_sch_code=%s ORDER BY {order}',(year,code)):
                print(row)
    finally: conn.close()


def restore(state_path):
    state=json.loads(state_path.read_text())
    target,backup=Path(state['target']),Path(state['backup'])
    if digest(backup)!=state['original_sha']: raise RuntimeError('Backup checksum mismatch')
    current=digest(target)
    checksum=target.with_name('.'+target.name+'.crc')
    saved=state_path.parent/'original.crc'
    if current==state['original_sha']:
        if saved.exists(): shutil.copy2(saved,checksum)
        print('Already restored'); return
    if current!=state['modified_sha']: raise RuntimeError('Bronze changed after test; refusing to overwrite unrelated edits')
    temp=target.with_name(target.name+'.teacher_restore.tmp')
    shutil.copy2(backup,temp)
    os.replace(temp,target)
    if saved.exists(): shutil.copy2(saved,checksum)
    print(f"RESTORED: {state['column']} {state['new_value']} -> {state['old_value']}. Rerun Silver and Gold.")


def apply(root,code,column,delta,backup_dir,year):
    target,index,actual,value=locate(root,code,column)
    if value is None: raise RuntimeError('Teacher count is NULL; resolve source before testing')
    try: old=int(value)
    except (TypeError,ValueError): raise RuntimeError('Invalid original teacher count')
    if str(value).strip()!=str(old): raise RuntimeError('Expected an integral teacher count')
    new=old+delta
    if delta==0 or new<0: raise RuntimeError('Delta must change the count and keep it nonnegative')
    state_path=backup_dir/'state.json'
    if backup_dir.exists(): raise RuntimeError('Backup directory already exists; use restore or a new --backup-dir')
    original_sha=digest(target)
    data=pq.ParquetFile(target).read()
    values=data[actual].to_pylist()
    values[index]=str(new) if pa.types.is_string(data[actual].type) or pa.types.is_large_string(data[actual].type) else type(value)(new)
    changed=data.set_column(data.schema.get_field_index(actual),data.schema.field(actual),pa.array(values,type=data[actual].type))
    backup_dir.mkdir(parents=True)
    backup=backup_dir/'original.parquet'
    shutil.copy2(target,backup)
    if digest(backup)!=original_sha: raise RuntimeError('Source changed while backing up')
    fd,path=tempfile.mkstemp(prefix='teacher_test_',suffix='.tmp',dir=target.parent)
    os.close(fd)
    temp=Path(path)
    try:
        pq.write_table(changed,temp,compression='snappy')
        verified=pq.ParquetFile(temp).read()
        if verified.num_rows!=data.num_rows: raise RuntimeError('Row count changed')
        for n in data.column_names:
            if not verified[n].equals(changed[n]): raise RuntimeError(f'Unexpected difference: {n}')
        if digest(target)!=original_sha: raise RuntimeError('Concurrent Bronze modification detected')
        state=dict(target=str(target.resolve()),backup=str(backup.resolve()),
                   original_sha=original_sha,modified_sha=digest(temp),academic_year=year,
                   school_code=code,column=actual,old_value=old,new_value=new)
        state_path.write_text(json.dumps(state,indent=2))
        checksum=target.with_name('.'+target.name+'.crc')
        if checksum.exists(): shutil.move(str(checksum),str(backup_dir/'original.crc'))
        os.replace(temp,target)
    finally: temp.unlink(missing_ok=True)
    print(f'APPLIED: {code} {actual} {old} -> {new}; original bytes backed up.')
    print('Rerun Silver then Gold, then use history. Use restore and rerun to record restoration.')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('inspect','apply','restore','history'))
    parser.add_argument('--year',default='2025-26')
    parser.add_argument('--school-code',default='02010100501')
    parser.add_argument('--column',choices=('male_tch','female_tch','transgen_tch'),default='male_tch')
    parser.add_argument('--delta',type=int,default=10)
    parser.add_argument('--bronze-dir',type=Path)
    parser.add_argument('--backup-dir',type=Path)
    args=parser.parse_args()
    if not re.fullmatch(r'\d{4}-\d{2}',args.year): parser.error('Use academic year YYYY-YY')
    if not re.fullmatch(r'\d{1,32}',args.school_code): parser.error('School code must contain digits')
    backup=args.backup_dir or bronze.PROJECT_DIR/'.pipeline_state'/f'teacher_row_test_{args.year}_{args.school_code}_{args.column}'
    if args.action=='history': history(args.year,args.school_code); return
    if args.action=='restore': restore(backup/'state.json'); return
    root=args.bronze_dir or bronze.bronze_year_root(args.year)/'tch_summary'
    if args.action=='apply': apply(root,args.school_code,args.column,args.delta,backup,args.year)
    else:
        path,index,column,value=locate(root,args.school_code,args.column)
        print(f'{args.year} | school={args.school_code} | {column}={value} | {path} row={index}')

if __name__=='__main__': main()
