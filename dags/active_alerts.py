import json
import os
from datetime import datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.environ.get("API_TOKEN")

HEADERS = {
    "Accept": "application/json",
    "Authorization": f"Bearer {TOKEN}",
}
URL = "https://api.alerts.in.ua/v1/alerts/active.json"

POSTGRES_CONN_ID = "postgres_connect"
HTTP_CONN_ID = "alerts_api"


def _extract_active_alerts(ti):
    response = requests.get(URL, headers=HEADERS)
    data = response.json()

    ti.xcom_push(key="active_alerts", value=data)


def _transform_active_alerts(ti):
    data = ti.xcom_pull(key="active_alerts")

    processed_alerts = []
    for alert in data["alerts"]:
        processed_alert = pd.json_normalize({
            "alert_id": alert.get("id"),
            "location_title": alert.get("location_title"),
            "location_type": alert.get("location_type"),
            "alert_type": alert.get("alert_type"),
            "started_at": pd.Timestamp(alert.get("started_at")).strftime("%Y-%m-%d %H:%M:%S")
        })

        processed_alerts.append(processed_alert)

    df_combined = pd.concat(processed_alerts, ignore_index=True)
    df_combined.to_csv("/tmp/processed_alerts.csv", index=False)


def _load_active_alerts():
    hook = PostgresHook(POSTGRES_CONN_ID)

    sql ="COPY active_alerts FROM stdin WITH DELIMITER AS ',' CSV HEADER;"

    hook.copy_expert(
        sql=sql,
        filename="/tmp/processed_alerts.csv"
    )


with DAG(
    dag_id="active_alerts",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS active_alerts(
                alert_id BIGINT,
                location_title VARCHAR(255),
                location_type VARCHAR(255),
                alert_type VARCHAR(255),
                started_at TIMESTAMP
            );
    """
    )

    is_alerts_api_available = HttpSensor(
        task_id="is_alerts_api_available",
        http_conn_id=HTTP_CONN_ID,
        endpoint="alerts/active.json",
        headers=HEADERS
    )

    extract_active_alerts = PythonOperator(
        task_id="extract_active_alerts",
        python_callable=_extract_active_alerts
    )

    transform_active_alerts = PythonOperator(
        task_id="transform_active_alerts",
        python_callable=_transform_active_alerts
    )

    load_active_alerts = PythonOperator(
        task_id="load_active_alerts",
        python_callable=_load_active_alerts
    )

    create_table >> is_alerts_api_available >> extract_active_alerts >> transform_active_alerts >> load_active_alerts
