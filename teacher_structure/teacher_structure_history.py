"""Persistent SCD2 revisions for complete snapshots, using Doris atomic table swaps.

Run one pipeline at a time (Airflow max_active_runs=1). valid_from/valid_to are
UTC observation times, with exclusive valid_to. They are not academic dates.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
import re
import uuid

from doris_io import query


META = ("version_no", "valid_from", "valid_to", "is_current", "row_hash",
        "is_deleted", "change_type")
IGNORED = {"processed_at", "updated_at", "created_at", "ingestion_timestamp"}


def ident(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return f"`{name}`"


def table(db, name):
    return f"{ident(db)}.{ident(name)}"


def columns(conn, db, name):
    rows = query(conn, f"DESC {table(db, name)}")
    result = []
    for row in rows:
        lower = {str(k).lower(): v for k, v in row.items()}
        kind = str(lower["type"])
        if not re.fullmatch(r"[A-Za-z0-9_(), ]+", kind):
            raise ValueError(f"Unsupported SQL type: {kind}")
        if re.fullmatch(r"datetime\(\d+\)", kind, re.I):
            kind = "DATETIMEV2" + kind[kind.index("("):]
        result.append((str(lower["field"]), kind, str(lower["null"]).upper() == "YES"))
    return result


def exists(conn, db, name):
    return bool(query(conn, f"SHOW TABLES FROM {ident(db)} LIKE %s", (name,)))


def count(conn, sql):
    return int(query(conn, sql)[0]["n"])


def hash_expr(alias, names):
    # Length prefixes distinguish NULL, empty strings, delimiters and literal NULL.
    parts = []
    for name in names:
        c = f"{alias}.{ident(name)}"
        value = f"CAST({c} AS STRING)"
        parts.append(f"IF({c} IS NULL, 'N;', CONCAT('V', CAST(LENGTH({value}) AS STRING), ':', {value}, ';'))")
    return "MD5(CONCAT(" + ", ".join(parts) + "))"


def ensure_history(conn, db, source, history, keys):
    spec = columns(conn, db, source)
    names = [n for n, _, _ in spec]
    if set(names) & set(META):
        raise ValueError("Source contains reserved history columns")
    if not keys or not set(keys).issubset(names):
        raise ValueError("History business keys must exist in the source")
    lookup = {n: (kind, nullable) for n, kind, nullable in spec}
    if any(lookup[k][1] for k in keys):
        raise ValueError("History business keys must be NOT NULL")
    ordered = list(keys) + [n for n in names if n not in keys]
    ddl = [f"{ident(n)} {lookup[n][0]} {'NULL' if lookup[n][1] else 'NOT NULL'}" for n in ordered]
    ddl += ["version_no BIGINT NOT NULL", "valid_from DATETIMEV2(6) NOT NULL",
            "valid_to DATETIMEV2(6) NULL", "is_current TINYINT NOT NULL",
            "row_hash VARCHAR(32) NOT NULL", "is_deleted TINYINT NOT NULL",
            "change_type VARCHAR(16) NOT NULL"]
    query(conn, f"CREATE TABLE IF NOT EXISTS {table(db, history)} ({', '.join(ddl)}) "
          f"DUPLICATE KEY ({', '.join(map(ident, keys))}) "
          f"DISTRIBUTED BY HASH({ident(keys[0])}) BUCKETS 16 "
          f"PROPERTIES ('replication_num'='{int(os.getenv('DORIS_REPLICATION_NUM', '1'))}')")
    if [n for n, _, _ in columns(conn, db, history)] != ordered + list(META):
        raise RuntimeError(f"{db}.{history}: schema changed; migrate history before proceeding")
    return ordered


def record_history(conn, db, source, history, keys):
    """Compare a FULL source snapshot to persistent history, then publish revisions.

    A missing key becomes a tombstone; reappearance gets another active version.
    Processing timestamps are retained but excluded from change detection.
    """
    names = ensure_history(conn, db, source, history, keys)
    src, hist = table(db, source), table(db, history)
    keycols = ', '.join(map(ident, keys))
    if count(conn, f"SELECT COUNT(*) AS n FROM (SELECT {keycols} FROM {src} "
             f"GROUP BY {keycols} HAVING COUNT(*) > 1) x"):
        raise RuntimeError(f"{db}.{source}: duplicate business keys; history not modified")
    if count(conn, f"SELECT COUNT(*) AS n FROM (SELECT {keycols} FROM {hist} "
             f"WHERE is_current=1 GROUP BY {keycols} HAVING COUNT(*) > 1) x"):
        raise RuntimeError(f"{db}.{history}: multiple current versions")
    hashed = hash_expr('s', [n for n in names if n not in IGNORED])
    join = ' AND '.join(f"s.{ident(k)} = h.{ident(k)}" for k in keys)
    first = ident(keys[0])
    source_cte = f"SELECT s.*, {hashed} AS _new_hash FROM {src} s"
    narrow_cte = f"SELECT {', '.join('s.' + ident(k) for k in keys)}, {hashed} AS _new_hash FROM {src} s"
    changed = f"h.{first} IS NULL OR h.is_deleted=1 OR h.row_hash<>s._new_hash"
    new_rows = count(conn, f"WITH incoming AS ({narrow_cte}) SELECT COUNT(*) AS n "
                     f"FROM incoming s LEFT JOIN {hist} h ON {join} AND h.is_current=1 WHERE {changed}")
    deleted_rows = count(conn, f"SELECT COUNT(*) AS n FROM {hist} h LEFT JOIN {src} s "
                         f"ON {join} WHERE h.is_current=1 AND h.is_deleted=0 AND s.{first} IS NULL")
    if not new_rows and not deleted_rows:
        print(f"HISTORY {db}.{history}: unchanged; no new versions")
        return
    clock = datetime.now(timezone.utc).replace(tzinfo=None)
    previous = query(conn, f"SELECT MAX(valid_from) AS last_time FROM {hist}")[0]['last_time']
    if previous:
        if isinstance(previous, str):
            previous = datetime.fromisoformat(previous)
        clock = max(clock, previous + timedelta(microseconds=1))
    stamp = "CAST('" + clock.strftime('%Y-%m-%d %H:%M:%S.%f') + "' AS DATETIMEV2(6))"
    stage_name = history + '__stage_' + uuid.uuid4().hex[:10]
    stage = table(db, stage_name)
    allcols = ', '.join(map(ident, names + list(META)))
    old_count = count(conn, f"SELECT COUNT(*) AS n FROM {hist}")
    query(conn, f"CREATE TABLE {stage} LIKE {hist}")
    try:
        closing = f"h.is_current=1 AND ((s.{first} IS NOT NULL AND "
        closing += "(h.is_deleted=1 OR h.row_hash<>s._new_hash)) OR "
        closing += f"(s.{first} IS NULL AND h.is_deleted=0))"
        payload = ', '.join(f"h.{ident(n)}" for n in names)
        query(conn, f"INSERT INTO {stage} ({allcols}) WITH incoming AS ({narrow_cte}) "
              f"SELECT {payload}, h.version_no, h.valid_from, "
              f"IF({closing}, {stamp}, h.valid_to), "
              f"IF({closing}, 0, h.is_current), h.row_hash, h.is_deleted, h.change_type "
              f"FROM {hist} h LEFT JOIN incoming s ON {join}")
        payload = ', '.join(f"s.{ident(n)}" for n in names)
        query(conn, f"INSERT INTO {stage} ({allcols}) WITH incoming AS ({source_cte}) "
              f"SELECT {payload}, COALESCE(h.version_no,0)+1, {stamp}, NULL, 1, "
              f"s._new_hash, 0, CASE WHEN h.{first} IS NULL THEN 'INITIAL' "
              f"WHEN h.is_deleted=1 THEN 'REACTIVATED' ELSE 'CHANGE' END "
              f"FROM incoming s LEFT JOIN {hist} h ON {join} AND h.is_current=1 WHERE {changed}")
        payload = ', '.join(f"h.{ident(n)}" for n in names)
        query(conn, f"INSERT INTO {stage} ({allcols}) SELECT {payload}, h.version_no+1, "
              f"{stamp}, NULL, 1, h.row_hash, 1, 'DELETE' FROM {hist} h "
              f"LEFT JOIN {src} s ON {join} WHERE h.is_current=1 AND h.is_deleted=0 AND s.{first} IS NULL")
        if count(conn, f"SELECT COUNT(*) AS n FROM {stage}") != old_count + new_rows + deleted_rows:
            raise RuntimeError("History staging row count mismatch")
        if count(conn, f"SELECT COUNT(*) AS n FROM (SELECT {keycols} FROM {stage} "
                 f"WHERE is_current=1 GROUP BY {keycols} HAVING COUNT(*)<>1) x"):
            raise RuntimeError("History staging has duplicate current keys")
        active = count(conn, f"SELECT COUNT(*) AS n FROM {stage} WHERE is_current=1 AND is_deleted=0")
        if active != count(conn, f"SELECT COUNT(*) AS n FROM {src}"):
            raise RuntimeError("History active rows do not reconcile to source")
        query(conn, f'ALTER TABLE {hist} REPLACE WITH TABLE {ident(stage_name)} PROPERTIES ("swap"="true")')
        print(f"HISTORY {db}.{history}: {new_rows} new/changed, {deleted_rows} deleted")
    finally:
        query(conn, f"DROP TABLE IF EXISTS {stage}")


def baseline(conn, db, source, history, keys):
    """Capture existing values before the first replacement; never reset history."""
    if not exists(conn, db, history) or count(conn, f"SELECT COUNT(*) AS n FROM {table(db, history)}") == 0:
        record_history(conn, db, source, history, keys)
