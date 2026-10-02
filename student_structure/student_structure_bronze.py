#!/usr/bin/env python3
"""Student Structure - Bronze stage.

This stage does NOT re-ingest UDISE source databases. The project already has a
canonical Bronze layer. This script validates that the Bronze Parquet required by
Student Structure exists for all configured academic years and that exactly one
school-master table is available for each year.

Flow:
    existing UDISE Bronze Parquet -> validated Student Structure Bronze inputs

No mapping CSV is read by this stage.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Iterable

import pyarrow.parquet as pq

import project_config as c
from doris_io import connect, healthy

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

REQUIRED_TABLES = (
    "mst_state",
    "mst_district",
    "mst_sch_category",
    "sch_enr_fresh",
)

SCHOOL_MASTER_CANDIDATES = (
    "school_master",
    "school_master_local",
    "sch_master",
    "sch_master_local",
)

PROJECT_DIR = Path(getattr(c, "PROJECT_DIR", Path(__file__).resolve().parent)).resolve()
BRONZE_ROOT = Path(
    getattr(c, "BRONZE_ROOT", PROJECT_DIR / "udise_data" / "bronze")
).resolve()
MANIFEST_PATH = Path(
    os.getenv(
        "STUDENT_STRUCTURE_BRONZE_MANIFEST",
        str(PROJECT_DIR / ".pipeline_state" / "student_structure_bronze_manifest.json"),
    )
).resolve()


def banner(text: str) -> None:
    print("\n" + "=" * 100)
    print(text)
    print("=" * 100)


def parquet_files(path: Path) -> list[Path]:
    files = sorted(p for p in path.rglob("*.parquet") if p.is_file())
    if not files:
        raise RuntimeError(f"No Parquet files found under {path}")
    return files


def parquet_row_count(path: Path) -> int:
    return sum(int(pq.ParquetFile(file).metadata.num_rows) for file in parquet_files(path))


def parquet_columns(path: Path) -> set[str]:
    files = parquet_files(path)
    schema = pq.ParquetFile(files[0]).schema_arrow
    return set(schema.names)


def bronze_year_root(year: str) -> Path:
    """Resolve one unambiguous Bronze directory for an academic year."""
    canonical = BRONZE_ROOT / year
    if canonical.is_dir():
        return canonical.resolve()

    runs_root = PROJECT_DIR / "udise_data" / "bronze_runs"
    candidates: list[Path] = []
    if runs_root.is_dir():
        for candidate in sorted(runs_root.glob(f"*/{year}")):
            if candidate.is_dir():
                candidates.append(candidate.resolve())

    complete: list[Path] = []
    for candidate in candidates:
        has_required = all((candidate / table).is_dir() for table in REQUIRED_TABLES)
        school_matches = [
            name for name in SCHOOL_MASTER_CANDIDATES if (candidate / name).is_dir()
        ]
        if has_required and len(school_matches) == 1:
            complete.append(candidate)

    if len(complete) == 1:
        return complete[0]
    if len(complete) > 1:
        raise RuntimeError(
            f"{year}: multiple complete Bronze runs found; refusing to guess:\n  - "
            + "\n  - ".join(str(path) for path in complete)
        )
    raise RuntimeError(
        f"{year}: no complete Bronze directory found in {canonical} or {runs_root}/<run>/<year>"
    )


def find_school_master_table(year: str, year_root: Path | None = None) -> str:
    year_root = year_root or bronze_year_root(year)
    found = [name for name in SCHOOL_MASTER_CANDIDATES if (year_root / name).is_dir()]
    if len(found) != 1:
        raise RuntimeError(
            f"{year}: expected exactly one school-master table from "
            f"{SCHOOL_MASTER_CANDIDATES}; found={found}"
        )
    return found[0]


def require_any(columns: set[str], candidates: Iterable[str], label: str) -> str:
    lower = {name.lower(): name for name in columns}
    for candidate in candidates:
        if candidate.lower() in lower:
            return lower[candidate.lower()]
    raise RuntimeError(f"Missing {label}; expected one of {tuple(candidates)}, available={sorted(columns)}")


def validate_schema(year: str, year_root: Path, school_table: str) -> None:
    school_cols = parquet_columns(year_root / school_table)
    require_any(school_cols, ("udise_sch_code", "school_code"), "school code")
    require_any(school_cols, ("state_cd", "udise_state_code", "state_code"), "state code")
    require_any(
        school_cols,
        ("district_cd", "udise_district_code", "district_code", "udise_dist_code"),
        "district code",
    )
    require_any(
        school_cols,
        ("management_center_id", "sch_mgmt_center_id", "management_centre_id", "sch_mgmt_centre_id"),
        "CENTER management id",
    )

    enr_cols = parquet_columns(year_root / "sch_enr_fresh")
    require_any(enr_cols, ("udise_sch_code", "school_code"), "enrollment school code")
    require_any(enr_cols, ("item_group", "item_group_id"), "item_group")
    require_any(enr_cols, ("item_id", "itemid"), "item_id")
    for required_metric in ("c1_b", "c1_g", "c12_b", "c12_g"):
        if required_metric not in enr_cols:
            raise RuntimeError(f"{year}/sch_enr_fresh missing required metric {required_metric}")


def build_manifest() -> dict:
    banner("STUDENT STRUCTURE BRONZE VALIDATION")
    result: dict[str, dict] = {}

    for year in YEARS:
        year_root = bronze_year_root(year)
        school_table = find_school_master_table(year, year_root)

        for table in REQUIRED_TABLES:
            path = year_root / table
            if not path.is_dir():
                raise RuntimeError(f"{year}: missing Bronze table directory {path}")

        validate_schema(year, year_root, school_table)

        tables = list(REQUIRED_TABLES) + [school_table]
        counts = {table: parquet_row_count(year_root / table) for table in tables}

        result[year] = {
            "source_db": SOURCE_DATABASES[year],
            "source_schema": SOURCE_SCHEMAS[year],
            "bronze_root": str(year_root),
            "school_master_table": school_table,
            "row_counts": counts,
        }

        print(
            f"{year}: PASS | school={school_table} | "
            f"schools={counts[school_table]:,} | enrollment={counts['sch_enr_fresh']:,}"
        )

    conn = connect()
    try:
        healthy(conn)
        print("Doris FE/BE health: PASS")
    finally:
        conn.close()

    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"Manifest: {MANIFEST_PATH}")
    print("BRONZE: PASS")
    return result


def main() -> None:
    build_manifest()


if __name__ == "__main__":
    main()
