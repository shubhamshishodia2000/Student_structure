"""Shared Doris SQL builders. Silver owns dimensions, SCD2 and facts; Gold owns summaries."""
from __future__ import annotations

import argparse
import os
import uuid
from student_structure_history import baseline, record_history
from typing import Iterable

from doris_io import connect, healthy, query

try:
    from doris_tables import ident
except Exception:
    def ident(value: str) -> str:
        if not value or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for ch in value):
            raise ValueError(f"Unsafe SQL identifier: {value!r}")
        return f"`{value}`"

SILVER_DB = os.getenv("UDISE_SILVER_DB", "udise_silver")
DW_DB = SILVER_DB
GOLD_DB = os.getenv("UDISE_GOLD_DB", "udise_gold")

MANAGEMENT_SOURCE_DB = os.getenv("UDISE_MANAGEMENT_SOURCE_DB", "udise_gold")
MANAGEMENT_SOURCE_TABLE = os.getenv("UDISE_MANAGEMENT_SOURCE_TABLE", "dim_management")
FORCE_REFRESH_MANAGEMENT = os.getenv("UDISE_REFRESH_MANAGEMENT_DIM", "0").strip().lower() in {
    "1", "true", "yes", "y"
}

REPLICATION_NUM = int(os.getenv("DORIS_REPLICATION_NUM", "1"))


def banner(text: str) -> None:
    print("\n" + "=" * 100)
    print(text)
    print("=" * 100)


def qname(database: str, table: str) -> str:
    return f"{ident(database)}.{ident(table)}"


def table_exists(conn, database: str, table: str) -> bool:
    return bool(query(conn, f"SHOW TABLES FROM {ident(database)} LIKE %s", (table,)))


def count_rows(conn, database: str, table: str) -> int:
    return int(query(conn, f"SELECT COUNT(*) AS n FROM {qname(database, table)}")[0]["n"])


def table_columns(conn, database: str, table: str) -> set[str]:
    rows = query(conn, f"DESC {qname(database, table)}")
    result: set[str] = set()
    for row in rows:
        candidate = None
        for key, value in row.items():
            if str(key).lower() in {"field", "column", "column_name", "name"}:
                candidate = value
                break
        if candidate is None and row:
            candidate = next(iter(row.values()))
        if candidate is not None:
            result.add(str(candidate))
    return result


def first_column(columns: Iterable[str], candidates: Iterable[str], *, required: bool = True) -> str | None:
    lower = {name.lower(): name for name in columns}
    for candidate in candidates:
        if candidate.lower() in lower:
            return lower[candidate.lower()]
    if required:
        raise RuntimeError(f"Expected one of {tuple(candidates)}; available={sorted(columns)}")
    return None


def execute(conn, sql: str) -> None:
    query(conn, sql)


def ensure_databases(conn) -> None:
    if SILVER_DB == GOLD_DB:
        raise ValueError("Silver and Gold must use different databases")
    execute(conn, f"CREATE DATABASE IF NOT EXISTS {ident(DW_DB)}")
    execute(conn, f"CREATE DATABASE IF NOT EXISTS {ident(GOLD_DB)}")


def migrate_management_report(conn):
    """Rename the old report once; never guess if both names already exist."""
    old = table_exists(conn, GOLD_DB, "student_structure_summary")
    new = table_exists(conn, GOLD_DB, "student_structure_management")
    if old and new:
        raise RuntimeError("Both student_structure_summary and student_structure_management exist. "
                           "Rename the retired summary table to a backup name, then rerun.")
    if old:
        execute(conn, f"ALTER TABLE {qname(GOLD_DB, 'student_structure_summary')} "
                      f"RENAME {ident('student_structure_management')}")


def ensure_tables(conn, *, layer: str) -> None:
    ddl = [
        f"""
        CREATE TABLE IF NOT EXISTS {qname(DW_DB, 'dim_academic_year')} (
            academic_year_sk INT NOT NULL,
            academic_year VARCHAR(7) NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            is_latest TINYINT NOT NULL,
            updated_at DATETIMEV2(6) NOT NULL
        )
        DUPLICATE KEY (academic_year_sk)
        DISTRIBUTED BY HASH(academic_year_sk) BUCKETS 1
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qname(DW_DB, 'dim_state')} (
            state_sk VARCHAR(32) NOT NULL,
            state_cd VARCHAR(10) NOT NULL,
            state_name VARCHAR(160) NULL,
            updated_at DATETIMEV2(6) NOT NULL
        )
        DUPLICATE KEY (state_sk)
        DISTRIBUTED BY HASH(state_sk) BUCKETS 1
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qname(DW_DB, 'dim_district')} (
            district_sk VARCHAR(32) NOT NULL,
            state_sk VARCHAR(32) NOT NULL,
            state_cd VARCHAR(10) NOT NULL,
            district_cd VARCHAR(20) NOT NULL,
            district_name VARCHAR(180) NULL,
            updated_at DATETIMEV2(6) NOT NULL
        )
        DUPLICATE KEY (district_sk)
        DISTRIBUTED BY HASH(district_sk) BUCKETS 2
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qname(DW_DB, 'dim_category')} (
            category_sk INT NOT NULL,
            sch_category_id INT NOT NULL,
            category_name VARCHAR(255) NULL,
            education_detailed VARCHAR(64) NULL,
            category VARCHAR(128) NULL,
            category_detailed VARCHAR(255) NULL,
            updated_at DATETIMEV2(6) NOT NULL
        )
        DUPLICATE KEY (category_sk)
        DISTRIBUTED BY HASH(category_sk) BUCKETS 1
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qname(DW_DB, 'dim_management')} (
            management_sk INT NOT NULL,
            management_center_id INT NOT NULL,
            management VARCHAR(128) NOT NULL,
            management_detailed VARCHAR(255) NULL,
            management_group VARCHAR(64) NOT NULL,
            updated_at DATETIMEV2(6) NOT NULL
        )
        DUPLICATE KEY (management_sk)
        DISTRIBUTED BY HASH(management_sk) BUCKETS 1
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qname(DW_DB, 'dim_social_category')} (
            social_category_sk INT NOT NULL,
            item_group INT NOT NULL,
            item_id INT NOT NULL,
            social_category VARCHAR(32) NOT NULL,
            updated_at DATETIMEV2(6) NOT NULL
        )
        DUPLICATE KEY (social_category_sk)
        DISTRIBUTED BY HASH(social_category_sk) BUCKETS 1
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qname(DW_DB, 'dim_school_scd2')} (
            udise_sch_code VARCHAR(32) NOT NULL,
            valid_from DATE NOT NULL,
            school_sk VARCHAR(32) NOT NULL,
            version_no INT NOT NULL,
            school_name VARCHAR(255) NULL,
            state_sk VARCHAR(32) NOT NULL,
            district_sk VARCHAR(32) NOT NULL,
            category_sk INT NULL,
            management_sk INT NOT NULL,
            state_cd VARCHAR(10) NOT NULL,
            district_cd VARCHAR(20) NOT NULL,
            sch_category_id INT NULL,
            management_center_id INT NOT NULL,
            school_status INT NULL,
            valid_to DATE NOT NULL,
            is_current TINYINT NOT NULL,
            row_hash VARCHAR(32) NOT NULL,
            change_type VARCHAR(32) NOT NULL,
            created_at DATETIMEV2(6) NOT NULL
        )
        DUPLICATE KEY (udise_sch_code, valid_from)
        DISTRIBUTED BY HASH(udise_sch_code) BUCKETS 16
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qname(DW_DB, 'school_scd2_change_audit')} (
            udise_sch_code VARCHAR(32) NOT NULL,
            version_no INT NOT NULL,
            previous_valid_from DATE NULL,
            current_valid_from DATE NOT NULL,
            previous_school_name VARCHAR(255) NULL,
            current_school_name VARCHAR(255) NULL,
            previous_state_cd VARCHAR(10) NULL,
            current_state_cd VARCHAR(10) NOT NULL,
            previous_district_cd VARCHAR(20) NULL,
            current_district_cd VARCHAR(20) NOT NULL,
            previous_category_id INT NULL,
            current_category_id INT NULL,
            previous_management_id INT NULL,
            current_management_id INT NOT NULL,
            previous_school_status INT NULL,
            current_school_status INT NULL,
            changed_columns VARCHAR(500) NOT NULL,
            detected_at DATETIMEV2(6) NOT NULL
        )
        DUPLICATE KEY (udise_sch_code, version_no)
        DISTRIBUTED BY HASH(udise_sch_code) BUCKETS 8
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qname(DW_DB, 'fact_student_structure')} (
            academic_year_sk INT NOT NULL,
            school_sk VARCHAR(32) NOT NULL,
            social_category_sk INT NOT NULL,
            state_sk VARCHAR(32) NOT NULL,
            district_sk VARCHAR(32) NOT NULL,
            category_sk INT NULL,
            management_sk INT NOT NULL,
            academic_year VARCHAR(7) NOT NULL,
            udise_sch_code VARCHAR(32) NOT NULL,
            item_group INT NOT NULL,
            item_id INT NOT NULL,
            foundational_boys BIGINT NOT NULL,
            foundational_girls BIGINT NOT NULL,
            foundational_transgender BIGINT NOT NULL,
            foundational BIGINT NOT NULL,
            preparatory_boys BIGINT NOT NULL,
            preparatory_girls BIGINT NOT NULL,
            preparatory_transgender BIGINT NOT NULL,
            preparatory BIGINT NOT NULL,
            middle_boys BIGINT NOT NULL,
            middle_girls BIGINT NOT NULL,
            middle_transgender BIGINT NOT NULL,
            middle BIGINT NOT NULL,
            secondary_boys BIGINT NOT NULL,
            secondary_girls BIGINT NOT NULL,
            secondary_transgender BIGINT NOT NULL,
            secondary BIGINT NOT NULL,
            total_boys BIGINT NOT NULL,
            total_girls BIGINT NOT NULL,
            total_transgender BIGINT NOT NULL,
            total_enrollment BIGINT NOT NULL,
            pre_primary_available TINYINT NOT NULL,
            transgender_available TINYINT NOT NULL,
            processed_at DATETIMEV2(6) NOT NULL
        )
        DUPLICATE KEY (academic_year_sk, school_sk, social_category_sk)
        DISTRIBUTED BY HASH(academic_year_sk, school_sk) BUCKETS 16
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {qname(GOLD_DB, 'student_structure_management')} (
            ac_year VARCHAR(7) NOT NULL,
            india_state_ut VARCHAR(160) NOT NULL,
            management VARCHAR(64) NOT NULL,
            total BIGINT NOT NULL,
            foundational BIGINT NOT NULL,
            preparatory BIGINT NOT NULL,
            middle BIGINT NOT NULL,
            secondary BIGINT NOT NULL
        )
        DUPLICATE KEY (ac_year, india_state_ut, management)
        DISTRIBUTED BY HASH(ac_year) BUCKETS 2
        PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
        """,
    ]
    if layer not in {"silver", "gold"}:
        raise ValueError("layer must be silver or gold")
    selected = ddl[:-1] if layer == "silver" else ddl[-1:]
    if layer == "gold":
        migrate_management_report(conn)
        selected.append(f"""
            CREATE TABLE IF NOT EXISTS {qname(GOLD_DB, 'student_structure_category')} (
                ac_year VARCHAR(7) NOT NULL,
                india_state_ut VARCHAR(160) NOT NULL,
                category VARCHAR(128) NOT NULL,
                total BIGINT NOT NULL,
                government BIGINT NOT NULL,
                government_aided BIGINT NOT NULL,
                private_unaided_recognized BIGINT NOT NULL,
                others BIGINT NOT NULL
            )
            DUPLICATE KEY (ac_year, india_state_ut, category)
            DISTRIBUTED BY HASH(ac_year) BUCKETS 2
            PROPERTIES ("replication_num"="{REPLICATION_NUM}")
        """)
    for statement in selected:
        execute(conn, statement)


def ensure_silver_sources(conn) -> None:
    required = (
        "state_master",
        "district_master",
        "school_category_master",
        "school_master_snapshot",
        "enrollment_social_category",
    )
    missing = [table for table in required if not table_exists(conn, SILVER_DB, table)]
    if missing:
        raise RuntimeError(
            f"Missing Doris Silver tables in {SILVER_DB}: {missing}. Run student_structure_silver.py first."
        )


def seed_management_dimension(conn) -> None:
    target_rows = count_rows(conn, DW_DB, "dim_management")
    if target_rows > 0 and not FORCE_REFRESH_MANAGEMENT:
        print(f"Management dimension already populated: {target_rows} rows; keeping Doris source of truth")
        return

    if not table_exists(conn, MANAGEMENT_SOURCE_DB, MANAGEMENT_SOURCE_TABLE):
        raise RuntimeError(
            f"{DW_DB}.dim_management is empty and bootstrap source "
            f"{MANAGEMENT_SOURCE_DB}.{MANAGEMENT_SOURCE_TABLE} does not exist. "
            "Populate the Doris management dimension once, or point UDISE_MANAGEMENT_SOURCE_DB / "
            "UDISE_MANAGEMENT_SOURCE_TABLE to an existing Doris management dimension."
        )

    source_columns = table_columns(conn, MANAGEMENT_SOURCE_DB, MANAGEMENT_SOURCE_TABLE)
    id_col = first_column(source_columns, ("management_center_id", "sch_mgmt_center_id"))
    management_col = first_column(source_columns, ("management", "management_name", "management_group"))
    detail_col = first_column(
        source_columns,
        ("management_detailed", "management_detail", "management_description"),
        required=False,
    )

    detail_expr = f"CAST({ident(detail_col)} AS STRING)" if detail_col else "NULL"
    mgmt_expr = f"TRIM(CAST({ident(management_col)} AS STRING))"
    lower_expr = f"LOWER({mgmt_expr})"

    execute(conn, f"TRUNCATE TABLE {qname(DW_DB, 'dim_management')}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(DW_DB, 'dim_management')}
        (
            management_sk, management_center_id, management, management_detailed,
            management_group, updated_at
        )
        SELECT
            CAST({ident(id_col)} AS INT) AS management_sk,
            CAST({ident(id_col)} AS INT) AS management_center_id,
            {mgmt_expr} AS management,
            {detail_expr} AS management_detailed,
            CASE
                WHEN {lower_expr} LIKE '%private%' AND {lower_expr} LIKE '%unaided%'
                    THEN 'Private Unaided Recognized'
                WHEN {lower_expr} LIKE '%aided%'
                    THEN 'Government Aided'
                WHEN {lower_expr} LIKE '%government%' OR {lower_expr} LIKE '%govt%'
                    THEN 'Government'
                ELSE 'Others'
            END AS management_group,
            CURRENT_TIMESTAMP(6) AS updated_at
        FROM {qname(MANAGEMENT_SOURCE_DB, MANAGEMENT_SOURCE_TABLE)}
        """,
    )
    rows = count_rows(conn, DW_DB, "dim_management")
    if rows <= 0:
        raise RuntimeError("Management dimension bootstrap produced zero rows")
    print(f"Bootstrapped {DW_DB}.dim_management from Doris -> {rows} rows")


def build_category_dimension(conn) -> None:
    """Add report labels while retaining category_sk = source category ID."""
    banner("BUILD dim_category with report mappings")
    source = query(conn, f"SELECT DISTINCT sch_category_id FROM {qname(SILVER_DB, 'school_category_master')}")
    allowed = {1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12}
    unexpected = [row["sch_category_id"] for row in source if row["sch_category_id"] not in allowed]
    if not source or unexpected:
        raise RuntimeError(f"Missing or unmapped category IDs: {unexpected}; review source before refresh")
    missing_names = int(query(conn, f"""
        SELECT COUNT(*) AS n FROM (
            SELECT sch_category_id FROM {qname(SILVER_DB, 'school_category_master')}
            GROUP BY sch_category_id
            HAVING MAX(NULLIF(TRIM(category_name), '')) IS NULL
        ) missing
    """)[0]["n"])
    if missing_names:
        raise RuntimeError("Category names missing; run corrected student_structure_silver.py first")
    existing = table_columns(conn, DW_DB, "dim_category")
    additions = {"education_detailed": "VARCHAR(64)", "category": "VARCHAR(128)", "category_detailed": "VARCHAR(255)"}
    missing = [f"{ident(name)} {kind} NULL" for name, kind in additions.items() if name not in existing]
    if missing:
        execute(conn, f"ALTER TABLE {qname(DW_DB, 'dim_category')} ADD COLUMN (" + ", ".join(missing) + ")")
    if not set(additions).issubset(table_columns(conn, DW_DB, "dim_category")):
        raise RuntimeError("Category schema change pending; wait for SHOW ALTER TABLE COLUMN FROM udise_silver, then retry")
    execute(conn, f"TRUNCATE TABLE {qname(DW_DB, 'dim_category')}")
    execute(conn, f"""
        INSERT INTO {qname(DW_DB, 'dim_category')}
        (category_sk, sch_category_id, category_name, education_detailed, category, category_detailed, updated_at)
        SELECT sch_category_id, sch_category_id,
            MAX(NULLIF(TRIM(category_name), '')),
            CASE sch_category_id
                WHEN 1 THEN 'Preparatory'
                WHEN 2 THEN 'Middle'
                WHEN 3 THEN 'Secondary'
                WHEN 4 THEN 'Middle'
                WHEN 5 THEN 'Secondary'
                WHEN 6 THEN 'Secondary'
                WHEN 7 THEN 'Secondary'
                WHEN 8 THEN 'Secondary'
                WHEN 10 THEN 'Secondary'
                WHEN 11 THEN 'Secondary'
                WHEN 12 THEN 'Foundational'
                END,
            CASE sch_category_id
                WHEN 1 THEN 'Foundational + Preparatory School'
                WHEN 2 THEN 'Middle School'
                WHEN 3 THEN 'Secondary School'
                WHEN 4 THEN 'Middle School'
                WHEN 5 THEN 'Secondary School'
                WHEN 6 THEN 'Secondary School'
                WHEN 7 THEN 'Secondary School'
                WHEN 8 THEN 'Secondary School'
                WHEN 10 THEN 'Secondary School'
                WHEN 11 THEN 'Secondary School'
                WHEN 12 THEN 'Pre-Primary School'
                END,
            CASE sch_category_id
                WHEN 1 THEN 'Grades 1 to 5'
                WHEN 2 THEN 'Grades 1 to 8'
                WHEN 3 THEN 'Grades 1 to 12'
                WHEN 4 THEN 'Grades 6 to 8'
                WHEN 5 THEN 'Grades 6 to 12'
                WHEN 6 THEN 'Grades 1 to 10'
                WHEN 7 THEN 'Grades 6 to 10'
                WHEN 8 THEN 'Grades 9 & 10'
                WHEN 10 THEN 'Grades 9 to 12'
                WHEN 11 THEN 'Grades 11 & 12'
                WHEN 12 THEN 'Pre-Primary Only'
                END,
            CURRENT_TIMESTAMP(6)
        FROM {qname(SILVER_DB, 'school_category_master')}
        GROUP BY sch_category_id
    """)
    print(f"dim_category: {count_rows(conn, DW_DB, 'dim_category'):,} rows")


def build_type1_dimensions(conn) -> None:
    banner("BUILD CONFORMED TYPE-1 DIMENSIONS")

    execute(conn, f"TRUNCATE TABLE {qname(DW_DB, 'dim_academic_year')}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(DW_DB, 'dim_academic_year')}
        SELECT
            CAST(CONCAT(SUBSTR(academic_year, 1, 4), SUBSTR(academic_year, 6, 2)) AS INT),
            academic_year,
            CAST(CONCAT(SUBSTR(academic_year, 1, 4), '-04-01') AS DATE),
            DATE_SUB(
                DATE_ADD(CAST(CONCAT(SUBSTR(academic_year, 1, 4), '-04-01') AS DATE), INTERVAL 1 YEAR),
                INTERVAL 1 DAY
            ),
            CASE WHEN academic_year = (SELECT MAX(academic_year) FROM {qname(SILVER_DB, 'school_master_snapshot')}) THEN 1 ELSE 0 END,
            CURRENT_TIMESTAMP(6)
        FROM (
            SELECT DISTINCT academic_year
            FROM {qname(SILVER_DB, 'school_master_snapshot')}
        ) y
        """,
    )

    execute(conn, f"TRUNCATE TABLE {qname(DW_DB, 'dim_state')}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(DW_DB, 'dim_state')}
        SELECT
            MD5(state_cd),
            state_cd,
            MAX(state_name),
            CURRENT_TIMESTAMP(6)
        FROM {qname(SILVER_DB, 'state_master')}
        GROUP BY state_cd
        """,
    )

    execute(conn, f"TRUNCATE TABLE {qname(DW_DB, 'dim_district')}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(DW_DB, 'dim_district')}
        SELECT
            MD5(CONCAT(state_cd, '|', district_cd)),
            MD5(state_cd),
            state_cd,
            district_cd,
            MAX(district_name),
            CURRENT_TIMESTAMP(6)
        FROM {qname(SILVER_DB, 'district_master')}
        GROUP BY state_cd, district_cd
        """,
    )

    build_category_dimension(conn)

    seed_management_dimension(conn)

    execute(conn, f"TRUNCATE TABLE {qname(DW_DB, 'dim_social_category')}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(DW_DB, 'dim_social_category')}
        SELECT
            item_id,
            item_group,
            item_id,
            MAX(social_category),
            CURRENT_TIMESTAMP(6)
        FROM {qname(SILVER_DB, 'enrollment_social_category')}
        GROUP BY item_group, item_id
        """,
    )

    print("Type-1 dimension row counts:")
    for table in (
        "dim_academic_year", "dim_state", "dim_district", "dim_category",
        "dim_management", "dim_social_category",
    ):
        print(f"  {table}: {count_rows(conn, DW_DB, table):,}")


def build_school_scd2(conn) -> None:
    banner("BUILD dim_school_scd2")

    missing_mgmt = int(
        query(
            conn,
            f"""
            SELECT COUNT(*) AS n
            FROM (
                SELECT DISTINCT s.management_center_id
                FROM {qname(SILVER_DB, 'school_master_snapshot')} s
                LEFT JOIN {qname(DW_DB, 'dim_management')} m
                  ON m.management_center_id = s.management_center_id
                WHERE m.management_center_id IS NULL
            ) x
            """,
        )[0]["n"]
    )
    if missing_mgmt:
        raise RuntimeError(
            f"Management dimension is incomplete: {missing_mgmt} management IDs used by school snapshots are missing"
        )

    execute(conn, f"TRUNCATE TABLE {qname(DW_DB, 'dim_school_scd2')}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(DW_DB, 'dim_school_scd2')}
        WITH base AS (
            SELECT
                academic_year,
                CAST(CONCAT(SUBSTR(academic_year, 1, 4), '-04-01') AS DATE) AS snapshot_date,
                udise_sch_code,
                school_name,
                state_cd,
                district_cd,
                sch_category_id,
                management_center_id,
                school_status,
                MD5(CONCAT_WS('|',
                    COALESCE(school_name, '<NULL>'),
                    COALESCE(state_cd, '<NULL>'),
                    COALESCE(district_cd, '<NULL>'),
                    COALESCE(CAST(sch_category_id AS STRING), '<NULL>'),
                    COALESCE(CAST(management_center_id AS STRING), '<NULL>'),
                    COALESCE(CAST(school_status AS STRING), '<NULL>')
                )) AS row_hash
            FROM {qname(SILVER_DB, 'school_master_snapshot')}
        ),
        marked AS (
            SELECT
                b.*,
                CASE
                    WHEN LAG(row_hash) OVER (
                        PARTITION BY udise_sch_code ORDER BY snapshot_date
                    ) = row_hash THEN 0
                    ELSE 1
                END AS new_version
            FROM base b
        ),
        grouped AS (
            SELECT
                m.*,
                SUM(new_version) OVER (
                    PARTITION BY udise_sch_code
                    ORDER BY snapshot_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS version_group
            FROM marked m
        ),
        collapsed AS (
            SELECT
                udise_sch_code,
                version_group,
                MIN(snapshot_date) AS valid_from,
                MAX(school_name) AS school_name,
                MAX(state_cd) AS state_cd,
                MAX(district_cd) AS district_cd,
                MAX(sch_category_id) AS sch_category_id,
                MAX(management_center_id) AS management_center_id,
                MAX(school_status) AS school_status,
                MAX(row_hash) AS row_hash
            FROM grouped
            GROUP BY udise_sch_code, version_group
        ),
        ranged AS (
            SELECT
                c.*,
                COALESCE(
                    DATE_SUB(
                        LEAD(valid_from) OVER (PARTITION BY udise_sch_code ORDER BY valid_from),
                        INTERVAL 1 DAY
                    ),
                    CAST('9999-12-31' AS DATE)
                ) AS valid_to,
                ROW_NUMBER() OVER (
                    PARTITION BY udise_sch_code ORDER BY valid_from
                ) AS version_no
            FROM collapsed c
        )
        SELECT
            r.udise_sch_code,
            r.valid_from,
            MD5(CONCAT(r.udise_sch_code, '|', CAST(r.valid_from AS STRING))) AS school_sk,
            r.version_no,
            r.school_name,
            MD5(r.state_cd) AS state_sk,
            MD5(CONCAT(r.state_cd, '|', r.district_cd)) AS district_sk,
            r.sch_category_id AS category_sk,
            r.management_center_id AS management_sk,
            r.state_cd,
            r.district_cd,
            r.sch_category_id,
            r.management_center_id,
            r.school_status,
            r.valid_to,
            CASE WHEN r.valid_to = CAST('9999-12-31' AS DATE) THEN 1 ELSE 0 END AS is_current,
            r.row_hash,
            CASE WHEN r.version_no = 1 THEN 'INITIAL' ELSE 'ATTRIBUTE_CHANGE' END AS change_type,
            CURRENT_TIMESTAMP(6)
        FROM ranged r
        """,
    )

    rows = count_rows(conn, DW_DB, "dim_school_scd2")
    if rows <= 0:
        raise RuntimeError("dim_school_scd2 produced zero rows")

    bad_current = int(
        query(
            conn,
            f"""
            SELECT COUNT(*) AS n
            FROM (
                SELECT udise_sch_code, SUM(is_current) AS current_versions
                FROM {qname(DW_DB, 'dim_school_scd2')}
                GROUP BY udise_sch_code
                HAVING current_versions <> 1
            ) x
            """,
        )[0]["n"]
    )
    if bad_current:
        raise RuntimeError(f"SCD2 validation failed: {bad_current} schools do not have exactly one current version")

    execute(conn, f"TRUNCATE TABLE {qname(DW_DB, 'school_scd2_change_audit')}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(DW_DB, 'school_scd2_change_audit')}
        WITH versions AS (
            SELECT
                d.*,
                LAG(valid_from) OVER (PARTITION BY udise_sch_code ORDER BY valid_from) AS previous_valid_from,
                LAG(school_name) OVER (PARTITION BY udise_sch_code ORDER BY valid_from) AS previous_school_name,
                LAG(state_cd) OVER (PARTITION BY udise_sch_code ORDER BY valid_from) AS previous_state_cd,
                LAG(district_cd) OVER (PARTITION BY udise_sch_code ORDER BY valid_from) AS previous_district_cd,
                LAG(sch_category_id) OVER (PARTITION BY udise_sch_code ORDER BY valid_from) AS previous_category_id,
                LAG(management_center_id) OVER (PARTITION BY udise_sch_code ORDER BY valid_from) AS previous_management_id,
                LAG(school_status) OVER (PARTITION BY udise_sch_code ORDER BY valid_from) AS previous_school_status
            FROM {qname(DW_DB, 'dim_school_scd2')} d
        )
        SELECT
            udise_sch_code,
            version_no,
            previous_valid_from,
            valid_from,
            previous_school_name,
            school_name,
            previous_state_cd,
            state_cd,
            previous_district_cd,
            district_cd,
            previous_category_id,
            sch_category_id,
            previous_management_id,
            management_center_id,
            previous_school_status,
            school_status,
            CONCAT_WS(', ',
                CASE WHEN COALESCE(previous_school_name, '<NULL>') <> COALESCE(school_name, '<NULL>') THEN 'school_name' END,
                CASE WHEN COALESCE(previous_state_cd, '<NULL>') <> COALESCE(state_cd, '<NULL>') THEN 'state' END,
                CASE WHEN COALESCE(previous_district_cd, '<NULL>') <> COALESCE(district_cd, '<NULL>') THEN 'district' END,
                CASE WHEN COALESCE(CAST(previous_category_id AS STRING), '<NULL>') <> COALESCE(CAST(sch_category_id AS STRING), '<NULL>') THEN 'category' END,
                CASE WHEN COALESCE(CAST(previous_management_id AS STRING), '<NULL>') <> COALESCE(CAST(management_center_id AS STRING), '<NULL>') THEN 'management' END,
                CASE WHEN COALESCE(CAST(previous_school_status AS STRING), '<NULL>') <> COALESCE(CAST(school_status AS STRING), '<NULL>') THEN 'school_status' END
            ) AS changed_columns,
            CURRENT_TIMESTAMP(6)
        FROM versions
        WHERE version_no > 1
        """,
    )

    print(f"dim_school_scd2: {rows:,} rows")
    print(f"school_scd2_change_audit: {count_rows(conn, DW_DB, 'school_scd2_change_audit'):,} change rows")


def metric_sum(alias: str, columns: Iterable[str]) -> str:
    return " + ".join(f"COALESCE({alias}.{name}, 0)" for name in columns)


def stage_gender(alias: str, levels: Iterable[str], suffix: str) -> str:
    return metric_sum(alias, [f"{level}_{suffix}" for level in levels])


def stage_total(alias: str, levels: Iterable[str]) -> str:
    return " + ".join(
        f"({stage_gender(alias, levels, suffix)})" for suffix in ("b", "g", "t")
    )


def _build_fact(conn, target_table) -> None:
    banner("BUILD fact_student_structure")
    stages = {
        "foundational": ("pp3", "pp2", "pp1", "c1", "c2"),
        "preparatory": ("c3", "c4", "c5"),
        "middle": ("c6", "c7", "c8"),
        "secondary": ("c9", "c10", "c11", "c12"),
    }

    expressions: dict[str, str] = {}
    for stage, levels in stages.items():
        expressions[f"{stage}_boys"] = stage_gender("e", levels, "b")
        expressions[f"{stage}_girls"] = stage_gender("e", levels, "g")
        expressions[f"{stage}_transgender"] = stage_gender("e", levels, "t")
        expressions[stage] = stage_total("e", levels)

    expressions["total_boys"] = " + ".join(f"({expressions[f'{stage}_boys']})" for stage in stages)
    expressions["total_girls"] = " + ".join(f"({expressions[f'{stage}_girls']})" for stage in stages)
    expressions["total_transgender"] = " + ".join(f"({expressions[f'{stage}_transgender']})" for stage in stages)
    expressions["total_enrollment"] = " + ".join(f"({expressions[stage]})" for stage in stages)

    execute(conn, f"TRUNCATE TABLE {qname(DW_DB, target_table)}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(DW_DB, target_table)}
        SELECT
            ay.academic_year_sk,
            s.school_sk,
            sc.social_category_sk,
            s.state_sk,
            s.district_sk,
            s.category_sk,
            s.management_sk,
            e.academic_year,
            e.udise_sch_code,
            e.item_group,
            e.item_id,
            {expressions['foundational_boys']} AS foundational_boys,
            {expressions['foundational_girls']} AS foundational_girls,
            {expressions['foundational_transgender']} AS foundational_transgender,
            {expressions['foundational']} AS foundational,
            {expressions['preparatory_boys']} AS preparatory_boys,
            {expressions['preparatory_girls']} AS preparatory_girls,
            {expressions['preparatory_transgender']} AS preparatory_transgender,
            {expressions['preparatory']} AS preparatory,
            {expressions['middle_boys']} AS middle_boys,
            {expressions['middle_girls']} AS middle_girls,
            {expressions['middle_transgender']} AS middle_transgender,
            {expressions['middle']} AS middle,
            {expressions['secondary_boys']} AS secondary_boys,
            {expressions['secondary_girls']} AS secondary_girls,
            {expressions['secondary_transgender']} AS secondary_transgender,
            {expressions['secondary']} AS secondary,
            {expressions['total_boys']} AS total_boys,
            {expressions['total_girls']} AS total_girls,
            {expressions['total_transgender']} AS total_transgender,
            {expressions['total_enrollment']} AS total_enrollment,
            e.pre_primary_available,
            e.transgender_available,
            CURRENT_TIMESTAMP(6)
        FROM {qname(SILVER_DB, 'enrollment_social_category')} e
        JOIN {qname(DW_DB, 'dim_academic_year')} ay
          ON ay.academic_year = e.academic_year
        JOIN {qname(DW_DB, 'dim_social_category')} sc
          ON sc.item_group = e.item_group AND sc.item_id = e.item_id
        JOIN {qname(DW_DB, 'dim_school_scd2')} s
          ON s.udise_sch_code = e.udise_sch_code
         AND CAST(CONCAT(SUBSTR(e.academic_year, 1, 4), '-04-01') AS DATE)
             BETWEEN s.valid_from AND s.valid_to
        """,
    )

    source_rows = count_rows(conn, SILVER_DB, "enrollment_social_category")
    fact_rows = count_rows(conn, DW_DB, target_table)
    if fact_rows != source_rows:
        raise RuntimeError(
            f"Fact row-count mismatch: Silver enrollment={source_rows:,}, fact={fact_rows:,}. "
            "This normally indicates an unresolved or overlapping SCD2 school version."
        )

    bad_math = int(
        query(
            conn,
            f"""
            SELECT COUNT(*) AS n
            FROM {qname(DW_DB, target_table)}
            WHERE total_enrollment <> foundational + preparatory + middle + secondary
            """,
        )[0]["n"]
    )
    if bad_math:
        raise RuntimeError(f"Fact validation failed: {bad_math} rows do not reconcile")
    print(f"fact_student_structure: {fact_rows:,} rows")


def _build_summary(conn, target_table) -> None:
    banner("BUILD presentation-ready Gold summary")
    validate_gold_inputs(conn)
    execute(conn, f"TRUNCATE TABLE {qname(GOLD_DB, target_table)}")
    execute(
        conn,
        f"""
        INSERT INTO {qname(GOLD_DB, target_table)}
        SELECT
            f.academic_year AS ac_year,
            s.state_name AS india_state_ut,
            'All Management' AS management,
            SUM(f.total_enrollment) AS total,
            SUM(f.foundational) AS foundational,
            SUM(f.preparatory) AS preparatory,
            SUM(f.middle) AS middle,
            SUM(f.secondary) AS secondary
        FROM {qname(DW_DB, 'fact_student_structure')} f
        JOIN {qname(DW_DB, 'dim_state')} s ON s.state_sk = f.state_sk
        GROUP BY f.academic_year, s.state_name

        UNION ALL

        SELECT
            f.academic_year,
            s.state_name,
            m.management_group,
            SUM(f.total_enrollment),
            SUM(f.foundational),
            SUM(f.preparatory),
            SUM(f.middle),
            SUM(f.secondary)
        FROM {qname(DW_DB, 'fact_student_structure')} f
        JOIN {qname(DW_DB, 'dim_state')} s ON s.state_sk = f.state_sk
        JOIN {qname(DW_DB, 'dim_management')} m ON m.management_sk = f.management_sk
        GROUP BY f.academic_year, s.state_name, m.management_group

        UNION ALL

        SELECT
            f.academic_year,
            'Available Source Total',
            'All Management',
            SUM(f.total_enrollment),
            SUM(f.foundational),
            SUM(f.preparatory),
            SUM(f.middle),
            SUM(f.secondary)
        FROM {qname(DW_DB, 'fact_student_structure')} f
        GROUP BY f.academic_year

        UNION ALL

        SELECT
            f.academic_year,
            'Available Source Total',
            m.management_group,
            SUM(f.total_enrollment),
            SUM(f.foundational),
            SUM(f.preparatory),
            SUM(f.middle),
            SUM(f.secondary)
        FROM {qname(DW_DB, 'fact_student_structure')} f
        JOIN {qname(DW_DB, 'dim_management')} m ON m.management_sk = f.management_sk
        GROUP BY f.academic_year, m.management_group
        """,
    )

    rows = count_rows(conn, GOLD_DB, target_table)
    if rows <= 0:
        raise RuntimeError("student_structure_management produced zero rows")

    bad_math = int(
        query(
            conn,
            f"""
            SELECT COUNT(*) AS n
            FROM {qname(GOLD_DB, target_table)}
            WHERE total <> foundational + preparatory + middle + secondary
            """,
        )[0]["n"]
    )
    if bad_math:
        raise RuntimeError(f"Summary validation failed: {bad_math} rows do not reconcile")

    reconciliation = query(
        conn,
        f"""
        SELECT
            ac_year,
            MAX(CASE WHEN management = 'All Management' THEN total END) AS all_management,
            SUM(CASE WHEN management <> 'All Management' THEN total ELSE 0 END) AS bucket_total
        FROM {qname(GOLD_DB, target_table)}
        WHERE india_state_ut = 'Available Source Total'
        GROUP BY ac_year
        ORDER BY ac_year
        """,
    )
    for row in reconciliation:
        if int(row["all_management"]) != int(row["bucket_total"]):
            raise RuntimeError(f"Management reconciliation failed: {row}")

    print(f"student_structure_management: {rows:,} rows")
    print("Example user query:")
    print(
        f"  SELECT * FROM {GOLD_DB}.student_structure_management "
        "WHERE ac_year='2025-26' AND management='All Management';"
    )


def validate(conn) -> None:
    banner("VALIDATE STUDENT STRUCTURE STAR SCHEMA")
    required = [
        (DW_DB, "dim_academic_year"),
        (DW_DB, "dim_state"),
        (DW_DB, "dim_district"),
        (DW_DB, "dim_category"),
        (DW_DB, "dim_management"),
        (DW_DB, "dim_social_category"),
        (DW_DB, "dim_school_scd2"),
        (DW_DB, "fact_student_structure"),
        (GOLD_DB, "student_structure_management"),
        (GOLD_DB, "student_structure_category"),
    ]
    for database, table in required:
        if not table_exists(conn, database, table):
            raise RuntimeError(f"Missing {database}.{table}")
        rows = count_rows(conn, database, table)
        if rows <= 0:
            raise RuntimeError(f"{database}.{table} has zero rows")
        print(f"{database}.{table}: {rows:,}")

    latest = query(
        conn,
        f"""
        SELECT ac_year, india_state_ut, management, total, foundational, preparatory, middle, secondary
        FROM {qname(GOLD_DB, 'student_structure_management')}
        WHERE ac_year = (SELECT MAX(ac_year) FROM {qname(GOLD_DB, 'student_structure_management')})
          AND management = 'All Management'
        ORDER BY india_state_ut
        """,
    )
    print("\nLatest-year All Management rows:")
    for row in latest:
        print(row)
    validate_gold_inputs(conn)
    validate_category_report(conn, "student_structure_category")
    print("VALIDATION: PASS")




def build_versioned_current(conn, database, target, keys, builder, *, keep_history=True):
    """Validate new current data, persist history, then atomically replace current."""
    history = target + "_history"
    stage = target + "__stage_" + uuid.uuid4().hex[:10]
    execute(conn, f"CREATE TABLE {qname(database, stage)} LIKE {qname(database, target)}")
    try:
        builder(conn, stage)
        if keep_history:
            if database == GOLD_DB:
                raise ValueError("Gold reports must not create history tables")
            baseline(conn, database, target, history, keys)
            record_history(conn, database, stage, history, keys)
        execute(conn, f'ALTER TABLE {qname(database, target)} REPLACE WITH TABLE {ident(stage)} PROPERTIES ("swap"="true")')
    finally:
        execute(conn, f"DROP TABLE IF EXISTS {qname(database, stage)}")


def build_fact(conn):
    build_versioned_current(conn, DW_DB, "fact_student_structure",
                            ("academic_year", "udise_sch_code", "item_group", "item_id"), _build_fact)


def build_summary(conn):
    build_versioned_current(conn, GOLD_DB, "student_structure_management",
                            ("ac_year", "india_state_ut", "management"), _build_summary, keep_history=False)


def validate_gold_inputs(conn):
    """Prevent missing mappings or repeated dimensions from corrupting totals."""
    for name, key in (("dim_state", "state_sk"), ("dim_management", "management_sk"),
                      ("dim_category", "category_sk")):
        bad = query(conn, f"SELECT COUNT(*) AS n FROM (SELECT {ident(key)} "
                    f"FROM {qname(DW_DB, name)} GROUP BY {ident(key)} HAVING COUNT(*)>1) d")[0]["n"]
        if int(bad):
            raise RuntimeError(f"{name} has repeated dimension keys")
    bad = query(conn, f"""
        SELECT COUNT(*) AS n
        FROM {qname(DW_DB, 'fact_student_structure')} f
        LEFT JOIN {qname(DW_DB, 'dim_state')} s ON s.state_sk=f.state_sk
        LEFT JOIN {qname(DW_DB, 'dim_management')} m ON m.management_sk=f.management_sk
        LEFT JOIN {qname(DW_DB, 'dim_category')} c ON c.category_sk=f.category_sk
        WHERE s.state_sk IS NULL OR s.state_name IS NULL OR TRIM(s.state_name)=''
           OR m.management_sk IS NULL OR m.management_group IS NULL
           OR m.management_group NOT IN ('Government','Government Aided','Private Unaided Recognized','Others')
           OR (f.category_sk IS NOT NULL AND (c.category_sk IS NULL OR c.category IS NULL OR TRIM(c.category)=''))
    """)[0]["n"]
    if int(bad):
        raise RuntimeError(f"Gold mapping validation failed for {bad} facts; correct Silver mappings")


def category_insert_sql(target_table):
    return f"""
        INSERT INTO {qname(GOLD_DB, target_table)}
        (ac_year, india_state_ut, category, total,
         government, government_aided, private_unaided_recognized, others)
        WITH mapped AS (
            SELECT f.academic_year, s.state_name,
                   COALESCE(c.category, 'Unknown') AS category,
                   m.management_group, f.total_enrollment
            FROM {qname(DW_DB, 'fact_student_structure')} f
            JOIN {qname(DW_DB, 'dim_state')} s ON s.state_sk=f.state_sk
            JOIN {qname(DW_DB, 'dim_management')} m ON m.management_sk=f.management_sk
            LEFT JOIN {qname(DW_DB, 'dim_category')} c ON c.category_sk=f.category_sk
        ), geographies AS (
            SELECT academic_year, state_name AS geography, category,
                   management_group, total_enrollment FROM mapped
            UNION ALL
            SELECT academic_year, 'Available Source Total' AS geography, category,
                   management_group, total_enrollment FROM mapped
        )
        SELECT academic_year, geography, category, SUM(total_enrollment),
               SUM(CASE WHEN management_group='Government' THEN total_enrollment ELSE 0 END),
               SUM(CASE WHEN management_group='Government Aided' THEN total_enrollment ELSE 0 END),
               SUM(CASE WHEN management_group='Private Unaided Recognized' THEN total_enrollment ELSE 0 END),
               SUM(CASE WHEN management_group='Others' THEN total_enrollment ELSE 0 END)
        FROM geographies
        GROUP BY academic_year, geography, category
    """


def validate_category_report(conn, target_table):
    target = qname(GOLD_DB, target_table)
    if count_rows(conn, GOLD_DB, target_table) <= 0:
        raise RuntimeError("student_structure_category produced zero rows")
    bad = query(conn, f"SELECT COUNT(*) AS n FROM {target} "
                "WHERE total<>government+government_aided+private_unaided_recognized+others")[0]["n"]
    if int(bad):
        raise RuntimeError("Category management columns do not reconcile to total")
    mismatch = query(conn, f"""
        WITH actual AS (
            SELECT ac_year, india_state_ut, SUM(total) AS total
            FROM {target} GROUP BY ac_year, india_state_ut
        ), expected AS (
            SELECT ac_year, india_state_ut, total
            FROM {qname(GOLD_DB, 'student_structure_management')}
            WHERE management='All Management'
        )
        SELECT COUNT(*) AS n
        FROM actual a FULL OUTER JOIN expected e
          ON a.ac_year=e.ac_year AND a.india_state_ut=e.india_state_ut
        WHERE a.ac_year IS NULL OR e.ac_year IS NULL OR a.total<>e.total
    """)[0]["n"]
    if int(mismatch):
        raise RuntimeError("Category totals differ from the final management report")


def _build_category_report(conn, target_table):
    validate_gold_inputs(conn)
    execute(conn, category_insert_sql(target_table))
    validate_category_report(conn, target_table)


def build_category_report(conn):
    build_versioned_current(conn, GOLD_DB, "student_structure_category",
                            ("ac_year", "india_state_ut", "category"),
                            _build_category_report, keep_history=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Silver model without starting Spark")
    parser.add_argument("--stage", choices=("silver", "fact", "category"), default="silver")
    args = parser.parse_args()
    conn = connect()
    try:
        healthy(conn)
        ensure_databases(conn)
        ensure_silver_sources(conn)
        ensure_tables(conn, layer="silver")
        if args.stage == "category":
            build_category_dimension(conn)
            return
        if args.stage == "silver":
            build_type1_dimensions(conn)
            build_school_scd2(conn)
        build_fact(conn)
        print("SILVER MODEL: PASS")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
