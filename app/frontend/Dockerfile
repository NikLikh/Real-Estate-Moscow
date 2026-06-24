FROM python:3.13-slim
WORKDIR /app
RUN pip install --no-cache-dir \
    streamlit==1.58.0 plotly==6.6.0 pandas==3.0.1 numpy==2.4.3 scipy==1.17.1 requests==2.32.5
COPY . /app
CMD ["streamlit", "run", "app/frontend/Home.py", "--server.address=0.0.0.0", "--server.port=8501"]
