import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

DBT = "/opt/airflow/dbt-venv/bin/dbt"
DIRS = "--project-dir /opt/dbt --profiles-dir /opt/dbt"
GUARD = "raw_observations_fresh raw_observations_daily_volume scrape_coverage"
QUALITY = "raw_field_sanity price_outlier_rate rent_price_sanity"
UNIT_GUARD = "unit_slot_overlap unit_seller_coverage"

with DAG(
    dag_id="transform",
    schedule="0 20 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
) as dag:
    build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build {DIRS} --exclude {GUARD} {UNIT_GUARD} {QUALITY}",
    )
    guard = BashOperator(
        task_id="raw_freshness_guard",
        bash_command=f"{DBT} test {DIRS} --select {GUARD}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    unit_guard = BashOperator(
        task_id="unit_guard",
        bash_command=f"{DBT} test {DIRS} --select {UNIT_GUARD}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    quality = BashOperator(
        task_id="data_quality",
        bash_command=f"{DBT} test {DIRS} --select {QUALITY}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    build >> [guard, unit_guard, quality]
