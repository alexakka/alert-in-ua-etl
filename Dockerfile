FROM apache/airflow:2.6.2
COPY .env /app/.env

ADD requirements.txt .
RUN pip install -r requirements.txt

