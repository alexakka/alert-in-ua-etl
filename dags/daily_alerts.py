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
URL = "https://api.alerts.in.ua/v1/regions/:uid/alerts/:period.json"

POSTGRES_CONN_ID = "postgres_connect"
HTTP_CONN_ID = "alerts_api"

LOCATIONS = {
    3: "Хмельницька область",
    4: "Вінницька область",
    5: "Рівненська область",
    8: "Волинська область",
    9: "Дніпропетровська область",
    10: "Житомирська область",
    11: "Закарпатська область",
    12: "Запорізька область",
    13: "Івано-Франківська область",
    14: "Київська область",
    15: "Кіровоградська область",
    16: "Луганська область",
    17: "Миколаївська область",
    18: "Одеська область",
    19: "Полтавська область",
    20: "Сумська область",
    21: "Тернопільська область",
    22: "Харківська область",
    23: "Херсонська область",
    24: "Черкаська область",
    25: "Чернігівська область",
    26: "Чернівецька область",
    27: "Львівська область",
    28: "Донецька область",
    29: "Автономна Республіка Крим",
    30: "м. Севастополь",
    31: "м. Київ"
}


def _extract_daily_alerts(ti):
    alerts = []
    for location_id in LOCATIONS.keys():
        url = f"https://api.alerts.in.ua/v1/regions/{location_id}/alerts/month_ago.json"

        response = requests.get(url, headers=HEADERS)
        alert = response.json()
        alerts.extend(alert.get("alerts", []))


    data = {"alerts": alerts}

    ti.xcom_push(key="daily_alerts", value=data)


def _transform_alerts(ti):
    alerts = ti.xcom_pull(key="daily_alerts")

    transformed_alerts = []
    for alert in alerts.get("alerts"):
        transformed_alert = pd.json_normalize({
            "id": alert.get("id"),
            "location_title": alert.get("location_title"),
            "location_type": alert.get("location_type"),
            "started_at": pd.Timestamp(alert.get("started_at")).strftime("%Y-%m-%d %H:%M:%S"),
            "finished_at": (
                pd.Timestamp(alert["finished_at"]).strftime("%Y-%m-%d %H:%M:%S")
                if alert.get("finished_at") else None
            ),
            "updated_at": pd.Timestamp(alert.get("updated_at")).strftime("%Y-%m-%d %H:%M:%S"),
            "alert_type": alert.get("alert_type"),
            "location_oblast": alert.get("location_oblast"),
            "location_uid": alert.get("location_uid"),
            "notes": alert.get("notes"),
            "country": alert.get("country"),
            "deleted_at": alert.get("deleted_at"),
            "calculated": alert.get("calculated"),
            "location_oblast_uid": alert.get("location_oblast_uid")
        })

        transformed_alerts.append(transformed_alert)

    transformed_alerts_combined = pd.concat(transformed_alerts, ignore_index=True)
    transformed_alerts_combined.to_csv("/tmp/transformed_alerts.csv", index=False)


with DAG(
    dag_id="daily_alerts",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract_daily_alerts = PythonOperator(
        task_id="extract_daily_alerts",
        python_callable=_extract_daily_alerts
    )

    transform_alerts = PythonOperator(
        task_id="transform_alerts",
        python_callable=_transform_alerts
    )


    extract_daily_alerts >> transform_alerts
