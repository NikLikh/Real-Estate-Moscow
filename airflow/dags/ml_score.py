import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="ml_score",
    schedule="0 21 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
) as dag:
    BashOperator(
        task_id="score",
        bash_command="cd /opt/airflow && PYTHONPATH=/opt/airflow /opt/airflow/ml-venv/bin/python -m pipeline.ml.score",
    )
