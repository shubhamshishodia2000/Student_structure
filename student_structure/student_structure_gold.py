#!/usr/bin/env python3
"""Build report summaries in udise_gold from the completed udise_silver model."""
import argparse
import student_structure_model as model


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("summary", "validate", "all"), default="all")
    args = parser.parse_args()
    conn = model.connect()
    try:
        model.healthy(conn)
        if args.stage != "validate":
            model.ensure_databases(conn)
            # Check the detailed model before changing any report data.
            for table in ("dim_state", "dim_management", "dim_category", "fact_student_structure"):
                if not model.table_exists(conn, model.SILVER_DB, table):
                    raise RuntimeError(f"Missing {model.SILVER_DB}.{table}; run Silver first")
            model.ensure_tables(conn, layer="gold")
            model.build_summary(conn)
            model.build_category_report(conn)
        model.validate(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
