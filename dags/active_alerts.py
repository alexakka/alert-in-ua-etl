from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv
import os
import requests
import json
import pandas as pd


load_dotenv()

TOKEN = os.environ.get("API_TOKEN")

HEADERS = {
    "Accept": "application/json",
    "Authorization": f"Bearer {TOKEN}",
}

URL = "https://api.alerts.in.ua/v1/alerts/active.json"

def get_active_alerts(ti):
    response = requests.get(URL, headers=HEADERS)
    data = response.json()

    ti.xcom_push(key="active_alerts", value=data)

def process_active_alerts(ti):
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




with DAG(
    dag_id="active_alerts",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    first_task = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_connect",
        sql="SELECT 2 / 1;"
    )

    is_alerts_api_available = HttpSensor(
        task_id="is_alerts_api_available",
        http_conn_id="alerts_api",
        endpoint="alerts/active.json",
        headers=HEADERS
    )

    extract_active_alerts = PythonOperator(
        task_id="extract_active_alerts",
        python_callable=get_active_alerts
    )

    processed_active_alerts = PythonOperator(
        task_id="processed_active_alerts",
        python_callable=process_active_alerts
    )



    first_task >> is_alerts_api_available >> extract_active_alerts >> processed_active_alerts
