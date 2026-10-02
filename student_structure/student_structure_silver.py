#!/usr/bin/env python3
"""Build udise_silver: normalized sources, shared dimensions, school SCD2 and detailed student facts.
Annual snapshots remain available to rebuild school history. Local Parquet is staging only.
"""
from __future__ import annotations

from student_structure_history import baseline, record_history

import argparse
import subprocess
import sys
import hashlib
import os
import shutil
import uuid
from dataclasses import dataclass
from functools import reduce
from pathlib import Path
from typing import Iterable, Sequence

import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import project_config as c
from doris_io import connect, healthy, query, stream_file

try:
    from doris_tables import ident
except Exception:
    def ident(value: str) -> str:
        if not value or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for ch in value):
            raise ValueError(f"Unsafe SQL identifier: {value!r}")
        return f"`{value}`"

YEARS = (
    "2020-21",
    "2021-22",
    "2022-23",
    "2023-24",
    "2024-25",
    "2025-26",
)

SOURCE_DATABASES = {
    "2020-21": "udise_2021",
    "2021-22": "udise_2122",
    "2022-23": "udise_2223",
    "2023-24": "udise_2324",
    "2024-25": "udise_2425",
    "2025-26": "udise_2526",
}

SOURCE_SCHEMAS = {
    "2020-21": "udiseschema_np_2021",
    "2021-22": "udiseschema_np_2122",
    "2022-23": "udiseschema_np_2223",
    "2023-24": "udiseschema_np_2324",
    "2024-25": "udiseschema_np_2425",
    "2025-26": "udiseschema_np",
}

SCHOOL_MASTER_CANDIDATES = (
    "school_master",
    "school_master_local",
    "sch_master",
    "sch_master_local",
)

LEVELS = (
    "pp3", "pp2", "pp1",
    "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12",
)
SUFFIXES = ("b", "g", "t")
METRIC_COLUMNS = tuple(f"{level}_{suffix}" for level in LEVELS for suffix in SUFFIXES)

PROJECT_DIR = Path(getattr(c, "PROJECT_DIR", Path(__file__).resolve().parent)).resolve()
BRONZE_ROOT = Path(
    getattr(c, "BRONZE_ROOT", PROJECT_DIR / "udise_data" / "bronze")
).resolve()
SILVER_STAGE_ROOT = Path(
    os.getenv(
        "STUDENT_STRUCTURE_SILVER_STAGE_ROOT",
        str(PROJECT_DIR / "udise_data" / "student_structure" / "silver_stage"),
    )
).resolve()
SILVER_DB = os.getenv("UDISE_SILVER_DB", "udise_silver")
REPLICATION_NUM = int(
    getattr(c, "DORIS_REPLICATION_NUM", os.getenv("DORIS_REPLICATION_NUM", "1"))
)


def banner(text: str) -> None:
    print("\n" + "=" * 100)
    print(text)
    print("=" * 100)


def make_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.driver.maxResultSize", os.getenv("SPARK_DRIVER_MAX_RESULT_SIZE", "2g"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.sql.files.maxPartitionBytes", "64m")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def union_all(frames: Sequence[DataFrame]) -> DataFrame:
    if not frames:
        raise RuntimeError("No DataFrames to union")
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)


def first_existing(df: DataFrame, candidates: Iterable[str], *, required: bool = True) -> str | None:
    lower_map = {name.lower(): name for name in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_map:
            return lower_map[candidate.lower()]
    if required:
        raise RuntimeError(f"None of required columns {list(candidates)} exist. Available={df.columns}")
    return None


def safe_string(column: F.Column) -> F.Column:
    return F.trim(column.cast("string"))


def bronze_year_root(year: str) -> Path:
    canonical = BRONZE_ROOT / year
    if canonical.is_dir():
        return canonical.resolve()

    runs_root = PROJECT_DIR / "udise_data" / "bronze_runs"
    candidates: list[Path] = []
    if runs_root.is_dir():
        for candidate in sorted(runs_root.glob(f"*/{year}")):
            if candidate.is_dir():
                candidates.append(candidate.resolve())

    required = ("mst_state", "mst_district", "mst_sch_category", "sch_enr_fresh")
    complete: list[Path] = []
    for candidate in candidates:
        has_required = all((candidate / table).is_dir() for table in required)
        schools = [name for name in SCHOOL_MASTER_CANDIDATES if (candidate / name).is_dir()]
        if has_required and len(schools) == 1:
            complete.append(candidate)

    if len(complete) == 1:
        return complete[0]
    if len(complete) > 1:
        raise RuntimeError(
            f"{year}: multiple complete historical Bronze runs found:\n  - "
            + "\n  - ".join(str(x) for x in complete)
        )
    raise RuntimeError(f"{year}: Bronze directory not found")


def find_school_master_table(year: str) -> str:
    root = bronze_year_root(year)
    found = [name for name in SCHOOL_MASTER_CANDIDATES if (root / name).is_dir()]
    if len(found) != 1:
        raise RuntimeError(f"{year}: expected one school-master table, found={found}")
    return found[0]


def read_bronze(year: str, table: str) -> DataFrame:
    path = bronze_year_root(year) / table
    if not path.is_dir():
        raise RuntimeError(f"Missing Bronze Parquet: {path}")
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession")
    return spark.read.parquet(path.as_posix())


@dataclass(frozen=True)
class TableSpec:
    table: str
    columns: tuple[tuple[str, str, bool], ...]
    keys: tuple[str, ...]
    distribution: tuple[str, ...]
    partitions: int = 4

    @property
    def local_path(self) -> Path:
        return SILVER_STAGE_ROOT / self.table

    @property
    def ordered_columns(self) -> tuple[tuple[str, str, bool], ...]:
        by_name = {name: (name, sql_type, nullable) for name, sql_type, nullable in self.columns}
        missing = [name for name in self.keys if name not in by_name]
        if missing:
            raise RuntimeError(f"{self.table}: missing key columns {missing}")
        key_set = set(self.keys)
        return tuple(by_name[name] for name in self.keys) + tuple(
            column for column in self.columns if column[0] not in key_set
        )

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(name for name, _, _ in self.ordered_columns)


STATE_COLUMNS = (
    ("academic_year", "VARCHAR(7)", False),
    ("state_cd", "VARCHAR(10)", False),
    ("state_name", "VARCHAR(160)", True),
    ("source_db", "VARCHAR(64)", False),
    ("source_schema", "VARCHAR(96)", False),
    ("processed_at", "DATETIMEV2(6)", False),
)

DISTRICT_COLUMNS = (
    ("academic_year", "VARCHAR(7)", False),
    ("district_cd", "VARCHAR(20)", False),
    ("state_cd", "VARCHAR(10)", False),
    ("district_name", "VARCHAR(180)", True),
    ("source_db", "VARCHAR(64)", False),
    ("source_schema", "VARCHAR(96)", False),
    ("processed_at", "DATETIMEV2(6)", False),
)

CATEGORY_COLUMNS = (
    ("academic_year", "VARCHAR(7)", False),
    ("sch_category_id", "INT", False),
    ("category_name", "VARCHAR(255)", True),
    ("source_db", "VARCHAR(64)", False),
    ("source_schema", "VARCHAR(96)", False),
    ("processed_at", "DATETIMEV2(6)", False),
)

SCHOOL_COLUMNS = (
    ("academic_year", "VARCHAR(7)", False),
    ("udise_sch_code", "VARCHAR(32)", False),
    ("school_name", "VARCHAR(255)", True),
    ("state_cd", "VARCHAR(10)", False),
    ("district_cd", "VARCHAR(20)", False),
    ("sch_category_id", "INT", True),
    ("management_center_id", "INT", False),
    ("school_status", "INT", True),
    ("source_db", "VARCHAR(64)", False),
    ("source_schema", "VARCHAR(96)", False),
    ("source_table", "VARCHAR(64)", False),
    ("processed_at", "DATETIMEV2(6)", False),
)

ENROLLMENT_COLUMNS = (
    ("academic_year", "VARCHAR(7)", False),
    ("udise_sch_code", "VARCHAR(32)", False),
    ("item_group", "INT", False),
    ("item_id", "INT", False),
    ("social_category", "VARCHAR(32)", False),
    *((name, "BIGINT", True) for name in METRIC_COLUMNS),
    ("pre_primary_available", "TINYINT", False),
    ("transgender_available", "TINYINT", False),
    ("source_db", "VARCHAR(64)", False),
    ("source_schema", "VARCHAR(96)", False),
    ("processed_at", "DATETIMEV2(6)", False),
)

SPECS = {
    "state_master": TableSpec(
        "state_master", STATE_COLUMNS,
        ("academic_year", "state_cd"), ("academic_year", "state_cd"), 1,
    ),
    "district_master": TableSpec(
        "district_master", DISTRICT_COLUMNS,
        ("academic_year", "district_cd"), ("academic_year", "district_cd"), 1,
    ),
    "school_category_master": TableSpec(
        "school_category_master", CATEGORY_COLUMNS,
        ("academic_year", "sch_category_id"), ("academic_year",), 1,
    ),
    "school_master_snapshot": TableSpec(
        "school_master_snapshot", SCHOOL_COLUMNS,
        ("academic_year", "udise_sch_code"), ("academic_year", "state_cd"), 8,
    ),
    "enrollment_social_category": TableSpec(
        "enrollment_social_category", ENROLLMENT_COLUMNS,
        ("academic_year", "udise_sch_code", "item_group", "item_id"),
        ("academic_year", "udise_sch_code"), 12,
    ),
}


def create_table_sql(spec: TableSpec, table_name: str) -> str:
    lines = []
    for name, sql_type, nullable in spec.ordered_columns:
        lines.append(f"        {ident(name)} {sql_type} {'NULL' if nullable else 'NOT NULL'}")
    keys = ", ".join(ident(x) for x in spec.keys)
    dist = ", ".join(ident(x) for x in spec.distribution)
    buckets = max(1, min(16, spec.partitions))
    return f"""
    CREATE TABLE {ident(SILVER_DB)}.{ident(table_name)}
    (
{',\n'.join(lines)}
    )
    DUPLICATE KEY ({keys})
    DISTRIBUTED BY HASH({dist}) BUCKETS {buckets}
    PROPERTIES ("replication_num" = "{REPLICATION_NUM}")
    """


def table_exists(conn, database: str, table: str) -> bool:
    return bool(query(conn, f"SHOW TABLES FROM {ident(database)} LIKE %s", (table,)))


def parquet_files(path: Path) -> list[Path]:
    files = sorted(p for p in path.rglob("*.parquet") if p.is_file())
    if not files:
        raise RuntimeError(f"No Parquet files under {path}")
    return files


def parquet_row_count(path: Path) -> int:
    return sum(int(pq.ParquetFile(file).metadata.num_rows) for file in parquet_files(path))


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_stage(df: DataFrame, spec: TableSpec) -> int:
    if spec.local_path.exists():
        shutil.rmtree(spec.local_path)
    spec.local_path.parent.mkdir(parents=True, exist_ok=True)
    (
        df.select(*spec.column_names)
        .repartition(max(1, spec.partitions))
        .write.mode("overwrite")
        .parquet(spec.local_path.as_posix())
    )
    rows = parquet_row_count(spec.local_path)
    print(f"STAGE PARQUET: {spec.local_path} -> {rows:,} rows")
    return rows


def publish(spec: TableSpec, expected_rows: int) -> int:
    run_token = uuid.uuid4().hex[:12]
    stage_table = f"{spec.table}__stage_{run_token}"
    conn = connect()
    try:
        healthy(conn)
        query(conn, f"CREATE DATABASE IF NOT EXISTS {ident(SILVER_DB)}")
        if not table_exists(conn, SILVER_DB, spec.table):
            query(conn, create_table_sql(spec, spec.table))

        query(conn, f"DROP TABLE IF EXISTS {ident(SILVER_DB)}.{ident(stage_table)}")
        query(conn, create_table_sql(spec, stage_table))

        loaded = 0
        for index, path in enumerate(parquet_files(spec.local_path)):
            rows = int(pq.ParquetFile(path).metadata.num_rows)
            label = f"ss_silver_{run_token}_{index}_{file_sha256(path)[:8]}"
            stream_file(
                SILVER_DB,
                stage_table,
                path,
                label,
                rows,
                spec.column_names,
                file_format="parquet",
            )
            loaded += rows

        stage_count = int(
            query(conn, f"SELECT COUNT(*) AS n FROM {ident(SILVER_DB)}.{ident(stage_table)}")[0]["n"]
        )
        if loaded != expected_rows or stage_count != expected_rows:
            raise RuntimeError(
                f"{spec.table}: load mismatch expected={expected_rows:,}, files={loaded:,}, stage={stage_count:,}"
            )

        # Capture the previous snapshot once, then compare every business field.
        history_table = spec.table + "_history"
        baseline(conn, SILVER_DB, spec.table, history_table, spec.keys)
        record_history(conn, SILVER_DB, stage_table, history_table, spec.keys)

        query(
            conn,
            f"ALTER TABLE {ident(SILVER_DB)}.{ident(spec.table)} "
            f"REPLACE WITH TABLE {ident(stage_table)} PROPERTIES (\"swap\"=\"true\")",
        )
        query(conn, f"DROP TABLE IF EXISTS {ident(SILVER_DB)}.{ident(stage_table)}")

        final_count = int(
            query(conn, f"SELECT COUNT(*) AS n FROM {ident(SILVER_DB)}.{ident(spec.table)}")[0]["n"]
        )
        if final_count != expected_rows:
            raise RuntimeError(f"{spec.table}: final={final_count:,}, expected={expected_rows:,}")
        print(f"DORIS PUBLISHED: {SILVER_DB}.{spec.table} -> {final_count:,} rows")
        return final_count
    except Exception:
        try:
            query(conn, f"DROP TABLE IF EXISTS {ident(SILVER_DB)}.{ident(stage_table)}")
        except Exception:
            pass
        raise
    finally:
        conn.close()


def duplicate_conflict_count(df: DataFrame, keys: Sequence[str], value_cols: Sequence[str]) -> int:
    signature = F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(name).cast("string"), F.lit("<NULL>")) for name in value_cols],
        ),
        256,
    )
    return (
        df.select(*keys, signature.alias("_sig"))
        .groupBy(*keys)
        .agg(F.countDistinct("_sig").alias("_variants"))
        .filter(F.col("_variants") > 1)
        .count()
    )


def normalize_state(year: str) -> DataFrame:
    df = read_bronze(year, "mst_state")
    code = first_existing(df, ("udise_state_code", "state_cd", "state_code"))
    name = first_existing(df, ("state_name", "state_name_english", "state_name_eng"), required=False)
    return df.select(
        F.lit(year).alias("academic_year"),
        safe_string(F.col(code)).alias("state_cd"),
        (safe_string(F.col(name)) if name else F.lit(None).cast("string")).alias("state_name"),
        F.lit(SOURCE_DATABASES[year]).alias("source_db"),
        F.lit(SOURCE_SCHEMAS[year]).alias("source_schema"),
        F.current_timestamp().alias("processed_at"),
    ).dropDuplicates(["academic_year", "state_cd"])


def normalize_district(year: str) -> DataFrame:
    df = read_bronze(year, "mst_district")
    state = first_existing(df, ("udise_state_code", "state_cd", "state_code"))
    code = first_existing(df, ("udise_district_code", "district_cd", "district_code", "udise_dist_code"))
    name = first_existing(df, ("district_name", "district_name_english", "district_name_eng"), required=False)
    return df.select(
        F.lit(year).alias("academic_year"),
        safe_string(F.col(code)).alias("district_cd"),
        safe_string(F.col(state)).alias("state_cd"),
        (safe_string(F.col(name)) if name else F.lit(None).cast("string")).alias("district_name"),
        F.lit(SOURCE_DATABASES[year]).alias("source_db"),
        F.lit(SOURCE_SCHEMAS[year]).alias("source_schema"),
        F.current_timestamp().alias("processed_at"),
    ).dropDuplicates(["academic_year", "district_cd"])


def normalize_category(year: str) -> DataFrame:
    df = read_bronze(year, "mst_sch_category")
    category_id = first_existing(df, ("sch_category_id", "school_category_id", "category_id"))
    name = first_existing(
        df,
        ("sch_category_type", "sch_category_name", "school_category_name", "category_name", "sch_category", "school_category", "category"),
        required=True,
    )
    return df.select(
        F.lit(year).alias("academic_year"),
        F.col(category_id).cast("int").alias("sch_category_id"),
        safe_string(F.col(name)).alias("category_name"),
        F.lit(SOURCE_DATABASES[year]).alias("source_db"),
        F.lit(SOURCE_SCHEMAS[year]).alias("source_schema"),
        F.current_timestamp().alias("processed_at"),
    ).dropDuplicates(["academic_year", "sch_category_id"])


def normalize_school(year: str) -> DataFrame:
    source_table = find_school_master_table(year)
    df = read_bronze(year, source_table)

    school_code = first_existing(df, ("udise_sch_code", "school_code"))
    school_name = first_existing(df, ("school_name", "sch_name"), required=False)
    state_cd = first_existing(df, ("state_cd", "udise_state_code", "state_code"))
    district_cd = first_existing(df, ("district_cd", "udise_district_code", "district_code", "udise_dist_code"))
    category_id = first_existing(df, ("sch_category_id", "school_category_id", "category_id"), required=False)
    management_center_id = first_existing(
        df,
        ("management_center_id", "sch_mgmt_center_id", "management_centre_id", "sch_mgmt_centre_id"),
    )
    school_status = first_existing(df, ("school_status", "sch_status"), required=False)

    result = df.select(
        F.lit(year).alias("academic_year"),
        safe_string(F.col(school_code)).alias("udise_sch_code"),
        (safe_string(F.col(school_name)) if school_name else F.lit(None).cast("string")).alias("school_name"),
        safe_string(F.col(state_cd)).alias("state_cd"),
        safe_string(F.col(district_cd)).alias("district_cd"),
        (F.col(category_id).cast("int") if category_id else F.lit(None).cast("int")).alias("sch_category_id"),
        F.col(management_center_id).cast("int").alias("management_center_id"),
        (F.col(school_status).cast("int") if school_status else F.lit(None).cast("int")).alias("school_status"),
        F.lit(SOURCE_DATABASES[year]).alias("source_db"),
        F.lit(SOURCE_SCHEMAS[year]).alias("source_schema"),
        F.lit(source_table).alias("source_table"),
        F.current_timestamp().alias("processed_at"),
    ).filter(F.col("udise_sch_code").isNotNull() & (F.length("udise_sch_code") > 0))

    required_nulls = {
        "state_cd": result.filter(F.col("state_cd").isNull() | (F.length("state_cd") == 0)).count(),
        "district_cd": result.filter(F.col("district_cd").isNull() | (F.length("district_cd") == 0)).count(),
        "management_center_id": result.filter(F.col("management_center_id").isNull()).count(),
    }
    bad = {name: count for name, count in required_nulls.items() if count}
    if bad:
        raise RuntimeError(f"{year}/{source_table}: required school values missing: {bad}")

    conflicts = duplicate_conflict_count(
        result,
        ["academic_year", "udise_sch_code"],
        [
            "school_name", "state_cd", "district_cd", "sch_category_id",
            "management_center_id", "school_status",
        ],
    )
    if conflicts:
        raise RuntimeError(f"{year}/{source_table}: {conflicts} school codes have conflicting attributes")

    return result.dropDuplicates(["academic_year", "udise_sch_code"])


def normalize_enrollment(year: str) -> DataFrame:
    df = read_bronze(year, "sch_enr_fresh")
    school_code = first_existing(df, ("udise_sch_code", "school_code"))
    item_group = first_existing(df, ("item_group", "item_group_id"))
    item_id = first_existing(df, ("item_id", "itemid"))

    has_pre_primary = any(name in df.columns for name in ("pp3_b", "pp3_g", "pp2_b", "pp2_g", "pp1_b", "pp1_g"))
    has_transgender = any(name.endswith("_t") and name in df.columns for name in METRIC_COLUMNS)

    result = (
        df.filter((F.col(item_group).cast("int") == 1) & F.col(item_id).cast("int").isin(1, 2, 3, 4))
        .select(
            F.lit(year).alias("academic_year"),
            safe_string(F.col(school_code)).alias("udise_sch_code"),
            F.col(item_group).cast("int").alias("item_group"),
            F.col(item_id).cast("int").alias("item_id"),
            *[
                (
                    F.col(name).cast("long") if name in df.columns else F.lit(0).cast("long")
                ).alias(name)
                for name in METRIC_COLUMNS
            ],
            F.lit(1 if has_pre_primary else 0).cast("tinyint").alias("pre_primary_available"),
            F.lit(1 if has_transgender else 0).cast("tinyint").alias("transgender_available"),
            F.lit(SOURCE_DATABASES[year]).alias("source_db"),
            F.lit(SOURCE_SCHEMAS[year]).alias("source_schema"),
            F.current_timestamp().alias("processed_at"),
        )
        .withColumn(
            "social_category",
            F.when(F.col("item_id") == 1, "General")
             .when(F.col("item_id") == 2, "SC")
             .when(F.col("item_id") == 3, "ST")
             .when(F.col("item_id") == 4, "OBC"),
        )
    )

    duplicates = (
        result.groupBy("academic_year", "udise_sch_code", "item_group", "item_id")
        .count().filter(F.col("count") > 1).count()
    )
    if duplicates:
        raise RuntimeError(f"{year}/sch_enr_fresh: {duplicates} duplicate school/social-category keys")
    return result


def preflight() -> None:
    banner("STUDENT STRUCTURE SILVER PREFLIGHT")
    errors: list[str] = []
    for year in YEARS:
        try:
            root = bronze_year_root(year)
            school = find_school_master_table(year)
            print(f"{year}: bronze={root} | school={school}")
        except Exception as exc:
            errors.append(str(exc))
    if errors:
        raise RuntimeError("Preflight failed:\n  - " + "\n  - ".join(errors))

    conn = connect()
    try:
        healthy(conn)
        print("Doris FE/BE health: PASS")
    finally:
        conn.close()


def prepare_silver() -> dict[str, int]:
    banner("BUILD STUDENT STRUCTURE SILVER")
    spark = make_spark("UDISE_Student_Structure_Silver")
    try:
        frames: dict[str, list[DataFrame]] = {name: [] for name in SPECS}
        for year in YEARS:
            print(f"Normalize {year}")
            frames["state_master"].append(normalize_state(year))
            frames["district_master"].append(normalize_district(year))
            frames["school_category_master"].append(normalize_category(year))
            frames["school_master_snapshot"].append(normalize_school(year))
            frames["enrollment_social_category"].append(normalize_enrollment(year))

        outputs = {name: union_all(parts) for name, parts in frames.items()}

        bad_category_names = outputs["school_category_master"].filter(
            F.col("category_name").isNull() | (F.length(F.trim(F.col("category_name"))) == 0)
        ).count()
        if bad_category_names:
            raise RuntimeError(
                f"Category validation: {bad_category_names} rows have missing names; "
                "check Bronze mst_sch_category before publishing Silver"
            )

        unmatched = (
            outputs["enrollment_social_category"]
            .select("academic_year", "udise_sch_code").distinct()
            .join(
                outputs["school_master_snapshot"].select("academic_year", "udise_sch_code"),
                ["academic_year", "udise_sch_code"],
                "left_anti",
            )
            .count()
        )
        if unmatched:
            raise RuntimeError(f"Silver validation: {unmatched} enrollment school keys are missing from school snapshot")

        published: dict[str, int] = {}
        for name, df in outputs.items():
            spec = SPECS[name]
            rows = write_stage(df, spec)
            if rows <= 0:
                raise RuntimeError(f"{name}: zero Silver rows")
            published[name] = rows

        print("SILVER PARQUET: PASS (Doris publication runs after this process exits)")
        return published
    finally:
        spark.stop()


def publish_silver() -> dict[str, int]:
    banner("PUBLISH SILVER AND HISTORY (SPARK PROCESS HAS EXITED)")
    published = {}
    for name, spec in SPECS.items():
        rows = parquet_row_count(spec.local_path)
        if rows <= 0:
            raise RuntimeError(f"{name}: missing or empty staged Parquet")
        published[name] = publish(spec, rows)
    print("SILVER: PASS")
    return published


def build_silver() -> dict[str, int]:
    preflight()
    subprocess.run([sys.executable, str(Path(__file__).resolve()), "--stage", "prepare"], check=True)
    return publish_silver()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("prepare", "publish", "normalize", "all"), default="all")
    args = parser.parse_args()
    if args.stage == "all":
        # Run Spark in a child process so its JVM exits before Doris SQL starts.
        subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--stage", "normalize"],
            check=True,
        )
        subprocess.run(
            [sys.executable, str(Path(__file__).with_name("student_structure_model.py")),
             "--stage", "silver"],
            check=True,
        )
    elif args.stage == "prepare":
        prepare_silver()
    elif args.stage == "publish":
        publish_silver()
    else:
        build_silver()


if __name__ == "__main__":
    main()
