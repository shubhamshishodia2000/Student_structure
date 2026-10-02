"""Doris SQL and bounded file transfers; credentials never go in command arguments."""
import json
import subprocess
import time
from pathlib import Path
from urllib.parse import urlparse
from doris_tables import ident


def connect():
    import pymysql
    import project_config as c
    conn = pymysql.connect(host=c.DORIS_HOST, port=c.DORIS_SQL_PORT, user=c.DORIS_USER,
        password=c.DORIS_PASSWORD, autocommit=True, connect_timeout=15, read_timeout=300,
        write_timeout=300, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    with conn.cursor() as cur:
        cur.execute("SET time_zone = '+00:00'")
    return conn


def query(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall()) if cur.description else []


def database_names():
    import project_config as c
    return {'silver':c.DORIS_SILVER_DB, 'audit':c.DORIS_AUDIT_DB, 'gold':c.DORIS_GOLD_DB}


def healthy(conn):
    import project_config as c
    fes, bes = query(conn,'SHOW FRONTENDS'), query(conn,'SHOW BACKENDS')
    truth = lambda v: str(v).lower() in ('true','1')
    if not any(truth(x.get('Alive')) for x in fes):
        raise RuntimeError('No live Doris frontend')
    live = [x for x in bes if truth(x.get('Alive')) and not truth(x.get('SystemDecommissioned'))]
    if len(live) < c.DORIS_REPLICATION_NUM:
        raise RuntimeError(f'{len(live)} live backends; replication={c.DORIS_REPLICATION_NUM}')
    print('Doris FE/BE health: PASS')
    return live


def insert_rows(conn, database, table, rows):
    if not rows:
        return
    names = tuple(rows[0])
    if any(tuple(r) != names for r in rows):
        raise ValueError('Inconsistent insert columns')
    sql = (f'INSERT INTO {ident(database)}.{ident(table)} (' + ','.join(map(ident,names)) + ') VALUES ('
           + ','.join(['%s']*len(names)) + ')')
    # Audit/reference rows only; bulk facts use Stream Load.
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(sql, tuple(json.dumps(v) if isinstance(v,(dict,list)) else v for v in row.values()))


def assert_schema(conn, database, table):
    """Fail on an existing incompatible schema; CREATE IF NOT EXISTS is not migration."""
    actual = query(conn, f'DESCRIBE {ident(database)}.{ident(table.name)}')
    if [r['Field'] for r in actual] != [x[0] for x in table.columns]:
        raise RuntimeError(f'{database}.{table.name}: column contract differs; no automatic ALTER/DROP was attempted')
    def normalize(s):
        s=s.lower().replace(' ','').replace('datetimev2','datetime').replace('decimalv3','decimal').replace('jsonb','json')
        if s in ('boolean','bool'): return 'tinyint'
        if s == 'text': return 'string'
        return s
    for got,(name,typ,nullable) in zip(actual,table.columns):
        gt, et = normalize(got['Type']), normalize(typ)
        if gt != et or (str(got.get('Null')).upper()=='YES') != nullable:
            raise RuntimeError(f'{database}.{table.name}.{name}: expected {typ}, nullable={nullable}; found {got}')
        if (str(got.get('Key')).lower()=='true') != (name in table.keys):
            raise RuntimeError(f'{database}.{table.name}: key contract differs')
    create = query(conn,f'SHOW CREATE TABLE {ident(database)}.{ident(table.name)}')[0]
    text=' '.join(str(v) for v in create.values()).upper()
    if 'UNIQUE KEY' not in text:
        raise RuntimeError(f'{database}.{table.name}: expected UNIQUE KEY model')


def load_result(result, expected_rows):
    status=result.get('Status')
    if status in ('Success','Publish Timeout'):
        for field, expected in [('NumberTotalRows',expected_rows),('NumberLoadedRows',expected_rows),
                                 ('NumberFilteredRows',0),('NumberUnselectedRows',0)]:
            if int(result.get(field,-1)) != expected:
                raise RuntimeError(f'Stream Load {field}: {result.get(field)} != {expected}')
        return 'visible' if status=='Success' else 'pending'
    if status=='Label Already Exists' and result.get('ExistingJobStatus')=='FINISHED':
        return 'visible'  # target staging contents are subsequently validated in SQL
    raise RuntimeError(f'Stream Load did not complete: {status}; {result.get("Message","")}')


def stream_file(database, table, path, label, expected_rows, columns, file_format="parquet"):
    if file_format not in ("parquet", "json"):
        raise ValueError("Unsupported Stream Load format")
    import project_config as c
    parsed=urlparse(c.DORIS_FE_URL)
    if parsed.scheme not in ('http','https') or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError('DORIS_FE_URL must be an HTTP(S) URL without credentials')
    ident(database); ident(table)
    if '\n' in c.DORIS_USER+c.DORIS_PASSWORD or '\r' in c.DORIS_USER+c.DORIS_PASSWORD:
        raise ValueError('Newlines are not allowed in credentials')
    def cq(s): return '"'+s.replace('\\','\\\\').replace('"','\\"')+'"'
    config='user = '+cq(c.DORIS_USER+':'+c.DORIS_PASSWORD)+'\n'
    args=['curl','--silent','--show-error','--fail-with-body','--location-trusted','--max-time','1900',
          '--config','-','-X','PUT','-H','Expect: 100-continue','-H',f'format: {file_format}',
          '-H','strict_mode: true','-H','max_filter_ratio: 0','-H','timezone: UTC',
          '-H','timeout: 1800','-H','exec_mem_limit: 536870912','-H',f'label: {label}',
          '-H','columns: '+','.join(columns),'-T',str(Path(path)),
          f'{c.DORIS_FE_URL}/api/{database}/{table}/_stream_load']
    if file_format == 'json':
        args[1:1] = ['-H', 'read_json_by_line: true', '-H', 'strip_outer_array: false',
            '-H', 'jsonpaths: '+json.dumps(['$.'+name for name in columns],separators=(',',':'))]
    run=subprocess.run(args,input=config,text=True,capture_output=True)
    if run.returncode:
        raise RuntimeError(f'HTTP transfer failed (curl {run.returncode}); retry with the same load ID')
    try: result=json.loads(run.stdout)
    except ValueError as exc: raise RuntimeError('Non-JSON Stream Load response') from exc
    return load_result(result,expected_rows)
