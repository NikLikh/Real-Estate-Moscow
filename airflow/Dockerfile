FROM apache/airflow:2.10.5

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends git libgomp1 && \
    rm -rf /var/lib/apt/lists/*
USER airflow

RUN python -m venv /opt/airflow/dbt-venv && \
    /opt/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-postgres==1.9.0

RUN python -m venv /opt/airflow/ml-venv && \
    /opt/airflow/ml-venv/bin/pip install --no-cache-dir \
    scikit-learn==1.8.0 lightgbm==4.6.0 pandas==3.0.1 numpy==2.4.3 scipy==1.17.1 \
    geopandas==1.1.3 shapely==2.1.2 pyproj==3.7.2 sqlalchemy==2.0.51 psycopg2-binary==2.9.11 \
    joblib==1.5.3 python-dotenv==1.2.2 pyyaml==6.0.3 xlsxwriter==3.2.0
