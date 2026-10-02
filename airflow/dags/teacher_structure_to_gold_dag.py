"""Place in Airflow dags; Student Structure Silver must already be built."""
import os
import shlex
from datetime import datetime, timezone
from airflow import DAG
try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

PROJECT_DIR = os.getenv('UDISE_PROJECT_DIR','/home/shubham/udise_pyspark_updated/airflow_testing/udise_pipeline_to_silver')
PYTHON = os.getenv('UDISE_PYTHON','/home/shubham/udise_env/bin/python')
with DAG(dag_id='udise_teacher_structure_to_gold',schedule=None,catchup=False,
         start_date=datetime(2026,1,1,tzinfo=timezone.utc),max_active_runs=1) as dag:
    tasks = []
    for stage in ('bronze','silver','gold'):
        tasks.append(BashOperator(task_id='teacher_structure_'+stage,
            bash_command=f'''set -euo pipefail
cd {shlex.quote(PROJECT_DIR)}
unset SPARK_HOME
unset PYTHONPATH
{shlex.quote(PYTHON)} -m teacher_structure.teacher_structure_{stage}
'''))
    tasks[0] >> tasks[1] >> tasks[2]
