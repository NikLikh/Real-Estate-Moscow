FROM apache/airflow:2.10.5

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*
USER airflow

RUN python -m venv /opt/airflow/dbt-venv && \
    /opt/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-postgres==1.9.0
