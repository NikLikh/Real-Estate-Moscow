FROM python:3.13-slim
WORKDIR /app
RUN pip install --no-cache-dir \
    fastapi==0.138.0 "uvicorn[standard]==0.49.0" psycopg2-binary==2.9.11 pandas==3.0.1 \
    openpyxl==3.1.5 xlsxwriter==3.2.0 sqlalchemy==2.0.51 python-dotenv==1.2.2 pyyaml==6.0.3
COPY . /app
CMD ["uvicorn", "app.backend.app:app", "--host", "0.0.0.0", "--port", "8000"]
