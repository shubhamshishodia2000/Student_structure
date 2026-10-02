"""Airflow DAG for the segregated UDISE+ Student Structure pipeline.

Validated Bronze Parquet -> Silver Doris -> Silver dimensions / SCD2 / facts -> Gold report summary
"""

import os
import shlex
from datetime import datetime, timedelta, timezone

from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator


PROJECT_DIR = os.getenv(
    "UDISE_PROJECT_DIR",
    "/home/shubham/udise_pyspark_updated/airflow_testing/udise_pipeline_to_silver",
)
PYTHON_BIN = os.getenv("UDISE_PYTHON_BIN", "/home/shubham/udise_env/bin/python")
JAVA_HOME = os.getenv("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")

BRONZE_SCRIPT = os.getenv(
    "STUDENT_STRUCTURE_BRONZE_SCRIPT",
    os.path.join(PROJECT_DIR, "student_structure", "student_structure_bronze.py"),
)
SILVER_SCRIPT = os.getenv(
    "STUDENT_STRUCTURE_SILVER_SCRIPT",
    os.path.join(PROJECT_DIR, "student_structure", "student_structure_silver.py"),
)
GOLD_SCRIPT = os.getenv(
    "STUDENT_STRUCTURE_GOLD_SCRIPT",
    os.path.join(PROJECT_DIR, "student_structure", "student_structure_gold.py"),
)

MODEL_SCRIPT = os.getenv(
    "STUDENT_STRUCTURE_MODEL_SCRIPT",
    os.path.join(PROJECT_DIR, "student_structure", "student_structure_model.py"),
)


def python_task(task_id: str, script: str, args: str, timeout: timedelta) -> BashOperator:
    command = f"""
        set -euo pipefail
        cd {shlex.quote(PROJECT_DIR)}

        unset SPARK_HOME || true
        unset PYTHONPATH || true
        unset CLASSPATH || true

        export JAVA_HOME={shlex.quote(JAVA_HOME)}
        export PYSPARK_PYTHON={shlex.quote(PYTHON_BIN)}
        export PYSPARK_DRIVER_PYTHON={shlex.quote(PYTHON_BIN)}

        test -f {shlex.quote(script)}
        {shlex.quote(PYTHON_BIN)} {shlex.quote(script)} {args}
    """
    return BashOperator(
        task_id=task_id,
        bash_command=command,
        execution_timeout=timeout,
    )


with DAG(
    dag_id="udise_student_structure_to_gold",
    description=(
        "UDISE+ Student Structure: Bronze validation -> Silver Doris -> "
        "udise_silver dimensions + school SCD2 + fact -> udise_gold reports"
    ),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "owner": "shubham",
        "retries": 0,
    },
    tags=["udise", "student-structure", "bronze", "silver", "gold", "scd2", "doris"],
) as dag:

    bronze = python_task(
        "validate_student_structure_bronze",
        BRONZE_SCRIPT,
        "",
        timedelta(minutes=30),
    )

    silver = python_task(
        "build_student_structure_silver",
        SILVER_SCRIPT,
        "--stage normalize",
        timedelta(hours=4),
    )

    silver_model = python_task(
        "build_student_structure_silver_model",
        MODEL_SCRIPT,
        "--stage silver",
        timedelta(hours=3),
    )

    gold = python_task(
        "build_student_structure_gold_reports",
        GOLD_SCRIPT,
        "--stage all",
        timedelta(hours=3),
    )

    validate = python_task(
        "validate_student_structure_gold",
        GOLD_SCRIPT,
        "--stage validate",
        timedelta(minutes=30),
    )

    bronze >> silver >> silver_model >> gold >> validate
