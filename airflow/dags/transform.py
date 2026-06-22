import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="transform",
    schedule="0 20 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    BashOperator(
        task_id="dbt_build",
        bash_command="/opt/airflow/dbt-venv/bin/dbt build --project-dir /opt/dbt --profiles-dir /opt/dbt",
    )
