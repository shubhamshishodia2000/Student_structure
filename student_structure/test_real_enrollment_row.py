"""Back up, change, and restore one specified Bronze enrollment cell."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

BASE = Path('/home/shubham/udise_pyspark_updated/airflow_testing/udise_pipeline_to_silver')
CODE = '02010100501'


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def locate(root):
    matches = []
    for path in sorted(root.rglob('*.parquet')):
        pf = pq.ParquetFile(path)
        names = pf.schema_arrow.names
        def choose(options):
            return next(n for n in options if n in names)
        code = choose(('udise_sch_code', 'school_code'))
        group = choose(('item_group', 'item_group_id'))
        item = choose(('item_id', 'itemid'))
        data = pf.read(columns=[code, group, item, 'c1_b'])
        mask = pc.and_(pc.equal(pc.utf8_trim_whitespace(pc.cast(data[code], pa.string())), CODE),
                       pc.and_(pc.equal(pc.cast(data[group], pa.int64()), 1),
                               pc.equal(pc.cast(data[item], pa.int64()), 3)))
        for index in pc.indices_nonzero(pc.fill_null(mask, False)).to_pylist():
            matches.append((path, index, data['c1_b'][index].as_py()))
    if len(matches) != 1:
        raise RuntimeError(f'Expected exactly one Bronze row; found {len(matches)}. Nothing changed.')
    return matches[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('inspect', 'apply', 'restore', 'repair-checksum'))
    parser.add_argument('--bronze-dir', type=Path, default=BASE / 'udise_data/bronze/2025-26/sch_enr_fresh')
    parser.add_argument('--backup-dir', type=Path, default=BASE / '.pipeline_state/enrollment_row_test_02010100501_1_3')
    args = parser.parse_args()
    state_file = args.backup_dir / 'state.json'
    if args.action in ('restore', 'repair-checksum'):
        state = json.loads(state_file.read_text())
        target, backup = Path(state['target']), Path(state['backup'])
        if digest(backup) != state['original_sha']:
            raise RuntimeError('Backup checksum mismatch')
        current = digest(target)
        checksum = target.with_name('.' + target.name + '.crc')
        saved_crc = args.backup_dir / 'original.crc'
        if args.action == 'repair-checksum':
            if current not in (state['original_sha'], state['modified_sha']):
                raise RuntimeError('Bronze file does not match either test snapshot')
            if current == state['modified_sha'] and checksum.exists():
                if saved_crc.exists():
                    raise RuntimeError('Saved checksum already exists; inspect before replacing it')
                shutil.move(str(checksum), str(saved_crc))
                print('Stale checksum moved to backup; modified Parquet retained.')
            elif current == state['original_sha'] and saved_crc.exists():
                shutil.copy2(saved_crc, checksum)
                print('Original checksum restored.')
            else:
                print('No checksum repair needed.')
            return
        if current == state['original_sha']:
            if saved_crc.exists():
                shutil.copy2(saved_crc, checksum)
            print('Already restored; Bronze c1_b=1.')
            return
        if current != state['modified_sha']:
            raise RuntimeError('Bronze file changed after this test; refusing to overwrite unrelated edits')
        temp = target.with_name(target.name + '.restore.tmp')
        shutil.copy2(backup, temp)
        os.replace(temp, target)
        if saved_crc.exists():
            shutil.copy2(saved_crc, checksum)
        print('RESTORED: Bronze c1_b=1. Rerun Silver and Gold to record the restoration.')
        return
    target, index, value = locate(args.bronze_dir)
    print(f'2025-26 | school={CODE} | item_group=1 | item_id=3 | c1_b={value}')
    print(f'Parquet file: {target}')
    if args.action == 'inspect':
        return
    if value is None or int(value) != 1:
        raise RuntimeError('Expected the original c1_b=1; nothing changed')
    if state_file.exists():
        raise RuntimeError('A backup for this test already exists; keep it and use restore if needed')
    data = pq.ParquetFile(target).read()
    changed_values = data['c1_b'].to_pylist()
    changed_values[index] = '11' if pa.types.is_string(data['c1_b'].type) or pa.types.is_large_string(data['c1_b'].type) else type(value)(11)
    changed = data.set_column(data.schema.get_field_index('c1_b'), data.schema.field('c1_b'),
                              pa.array(changed_values, type=data['c1_b'].type))
    args.backup_dir.mkdir(parents=True, exist_ok=True)
    backup = args.backup_dir / 'original.parquet'
    shutil.copy2(target, backup)
    fd, temporary = tempfile.mkstemp(prefix='row_test_', suffix='.tmp', dir=target.parent)
    os.close(fd)
    temp = Path(temporary)
    try:
        pq.write_table(changed, temp, compression='snappy')
        verified = pq.ParquetFile(temp).read()
        if verified.num_rows != data.num_rows:
            raise RuntimeError('Row count changed')
        for column in data.column_names:
            expected = changed[column] if column == 'c1_b' else data[column]
            if not verified[column].equals(expected):
                raise RuntimeError(f'Unexpected difference in {column}')
        state = dict(target=str(target.resolve()), backup=str(backup.resolve()),
                     original_sha=digest(backup), modified_sha=digest(temp))
        if digest(target) != state['original_sha']:
            raise RuntimeError('Concurrent Bronze modification detected')
        state_file.write_text(json.dumps(state, indent=2))
        # Hadoop's checksum is for the original bytes; retain it outside Bronze.
        checksum = target.with_name('.' + target.name + '.crc')
        if checksum.exists():
            shutil.move(str(checksum), str(args.backup_dir / 'original.crc'))
        os.replace(temp, target)
    finally:
        temp.unlink(missing_ok=True)
    print('APPLIED: only this row c1_b changed 1 -> 11. Original Parquet backed up.')
    print('Rerun Silver and Gold to record the change. Keep the backup for restore.')


if __name__ == '__main__':
    main()
