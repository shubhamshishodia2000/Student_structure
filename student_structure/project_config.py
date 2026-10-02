"""Shared configuration. No database connections occur during import."""
import json
import os
from pathlib import Path
from dotenv import load_dotenv

DEFAULT_PROJECT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = Path(os.getenv('UDISE_PROJECT_DIR', str(DEFAULT_PROJECT_DIR))).expanduser().resolve()
load_dotenv(PROJECT_DIR / '.env', override=False)
load_dotenv(PROJECT_DIR / '.env.doris', override=False)
STATE_ROOT = PROJECT_DIR / '.pipeline_state'


def _env(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f'Missing environment setting: {name}')
    return value


DATA_HOST = _env('NEON_DATA_HOST', '')
META_HOST = _env('NEON_META_HOST', '')
NEON_USER = _env('NEON_USER', 'neondb_owner')
NEON_DATA_PASSWORD = _env('NEON_DATA_PASSWORD', '')
NEON_META_PASSWORD = _env('NEON_META_PASSWORD', '')
METADATA_DB = _env('METADATA_DB', 'udise_metadata')
LOG_DB = _env('LOG_DB', 'udise_logs')
JDBC_JAR_PATH = Path(_env('JDBC_JAR_PATH', '/home/shubham/spark-jars/postgresql-42.7.4.jar')).expanduser()
JDBC_DRIVER = 'org.postgresql.Driver'
BRONZE_ROOT = Path(_env('UDISE_BRONZE_ROOT', str(PROJECT_DIR / 'udise_data/bronze'))).expanduser()
SILVER_ROOT = PROJECT_DIR / 'udise_data/silver'
PROFILE_ROOT = PROJECT_DIR / 'udise_data/profiling'
PROFILE_COMPARE_ROOT = PROJECT_DIR / 'udise_data/profiling_comparison'
SPARK_DRIVER_MEMORY = _env('SPARK_DRIVER_MEMORY', '2g')
SPARK_DRIVER_MAX_RESULT_SIZE = _env('SPARK_DRIVER_MAX_RESULT_SIZE', '512m')
SPARK_SHUFFLE_PARTITIONS = _env('SPARK_SHUFFLE_PARTITIONS', '4')
SPARK_MASTER = _env('SPARK_MASTER', 'local[2]')

# Names confirmed by the user; unknown schemas are discovered, never guessed.
_DEFAULTS = {
    '2020-21': ('2021', 'udise_2021', None),
    '2021-22': ('2122', 'udise_2122', None),
    '2022-23': ('2223', 'udise_2223', None),
    '2023-24': ('2324', 'udise_2324', None),
    '2024-25': ('2425', 'udise_2425', 'udiseschema_np_2425'),
    '2025-26': ('2526', 'udise_2526', 'udiseschema_np'),
}
ACADEMIC_SOURCES = {}
for _year, (_suffix, _db, _schema) in _DEFAULTS.items():
    ACADEMIC_SOURCES[_year] = {
        'database': _env(f'SOURCE_{_suffix}_DB', _db),
        'schema': _env(f'SOURCE_{_suffix}_SCHEMA', _schema) or None,
        'revision': _env(f'SOURCE_{_suffix}_REVISION', 'initial-restore'),
        'year_key': int(_year[:4]),
    }

# Runtime discovery files are keyed by database to avoid using stale resolutions.
for _year, _cfg in ACADEMIC_SOURCES.items():
    _path = STATE_ROOT / 'sources' / f'{_year}.json'
    if not _cfg['schema'] and _path.exists():
        _resolved = json.loads(_path.read_text())
        if _resolved.get('database') == _cfg['database'] and _resolved.get('host') == DATA_HOST:
            _cfg['schema'] = _resolved['schema']

YEARS = tuple(ACADEMIC_SOURCES)
SOURCE_2425_DB = ACADEMIC_SOURCES['2024-25']['database']
SOURCE_2425_SCHEMA = ACADEMIC_SOURCES['2024-25']['schema']
SOURCE_2526_DB = ACADEMIC_SOURCES['2025-26']['database']
SOURCE_2526_SCHEMA = ACADEMIC_SOURCES['2025-26']['schema']
DORIS_HOST = _env('DORIS_HOST', '127.0.0.1')
DORIS_SQL_PORT = int(_env('DORIS_SQL_PORT', '9030'))
DORIS_FE_URL = _env('DORIS_FE_URL', 'http://127.0.0.1:8030').rstrip('/')
DORIS_USER = _env('DORIS_USER', 'root')
# An explicitly empty password is supported for the user's local instance.
DORIS_PASSWORD = _env('DORIS_PASSWORD', '')
DORIS_SILVER_DB = _env('DORIS_SILVER_DB', 'udise_silver')
DORIS_AUDIT_DB = _env('DORIS_AUDIT_DB', 'udise_audit')
DORIS_GOLD_DB = _env('DORIS_GOLD_DB', 'udise_gold')
DORIS_REPLICATION_NUM = int(_env('DORIS_REPLICATION_NUM', '1'))
DORIS_BUCKETS = int(_env('DORIS_BUCKETS', '2'))


def require_neon():
    for name in ('NEON_DATA_HOST', 'NEON_META_HOST', 'NEON_DATA_PASSWORD', 'NEON_META_PASSWORD'):
        _env(name, required=True)


def jdbc_url(database_name, host):
    return f'jdbc:postgresql://{host}:5432/{database_name}?sslmode=require&stringtype=unspecified'


def jdbc_properties(password, fetchsize=None):
    props = {'user': NEON_USER, 'password': password, 'driver': JDBC_DRIVER}
    if fetchsize:
        props['fetchsize'] = str(fetchsize)
    return props
