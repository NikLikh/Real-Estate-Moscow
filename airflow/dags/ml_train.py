import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="ml_train",
    schedule="0 21 * * 0",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    BashOperator(
        task_id="train",
        bash_command="cd /opt/airflow && PYTHONPATH=/opt/airflow /opt/airflow/ml-venv/bin/python -m pipeline.ml.train",
    )
