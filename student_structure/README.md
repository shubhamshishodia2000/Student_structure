# Two-database student structure pipeline

Bronze Parquet -> udise_silver -> udise_gold -> SQL -> Superset / Power BI.

## udise_silver
- Normalized annual source tables: state_master, district_master, school_category_master, school_master_snapshot, enrollment_social_category.
- Reusable dimensions: dim_academic_year, dim_state, dim_district, dim_category, dim_management, dim_social_category.
- History: dim_school_scd2, school_scd2_change_audit.
- Detailed fact: fact_student_structure (academic year + school + social category).

Reuse dimensions across future facts. Repetition is a fact/measure, not a dimension; a future fact_student_repetition can reuse these dimensions. It is not implemented by these source scripts.

## udise_gold
student_structure_summary: enrollment and education-stage totals by academic year, state/available-source total, and management scope. Add future report tables here. report_queries.sql gives the final query.

## Install / run
Replace the three matching scripts and place student_structure_model.py beside Silver and Gold inside the existing student_structure directory. Bronze is unchanged. Replace the Airflow DAG in your dags directory. Existing project_config.py, doris_io.py, doris_tables.py and environment are still required.

Run the existing DAG, or run in order:
python student_structure_bronze.py
python student_structure_silver.py
python student_structure_gold.py --stage all

Defaults: UDISE_SILVER_DB=udise_silver and UDISE_GOLD_DB=udise_gold. Old STUDENT_STRUCTURE_*_DB and UDISE_DW_DB settings no longer choose databases.

Management names must be seeded once. By default the existing udise_gold.dim_management is read, never modified. Alternatively point UDISE_MANAGEMENT_SOURCE_DB and UDISE_MANAGEMENT_SOURCE_TABLE at an existing mapped dimension. After seeding, udise_silver.dim_management is reused. Thus an existing legacy Gold dimension can remain during migration; do not delete it before the first successful run.

Only two database names are targeted by default. Existing legacy databases are not deleted. Any unrelated existing tables in udise_gold remain. Normalized sources in Silver are deliberate annual snapshots, not duplicate report dimensions.

SCD2 is rebuilt from all six configured annual snapshots, with April 1 used as a modeled year boundary. It records observed annual attribute changes, not intra-year events. Keep all historical Bronze snapshots. Absence in a later year does not itself close a school's last version.

The inherited SQL refreshes dimensions, school history, facts and reports with TRUNCATE/INSERT. Run during a refresh window: it is not an atomic multi-table publish or incremental SCD2 merge. Existing Silver source-table publication retains its original staging mechanism.

Validation: Python syntax and stage/database routing checked locally. Live Doris, Spark and Airflow were unavailable here; run the DAG to validate counts and SQL compatibility in your environment.
