import json
import os
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import requests
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

DBT_DIR = "/opt/airflow/dbt"


@dag(
    schedule="@daily",
    start_date=datetime(2026, 7, 20),  # 過去にするとbackfill練習ができる
    catchup=True,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    max_active_runs=1,
)
def weather_daily():
    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )
    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        retries=0,
    )

    @task
    def fetch_weather(ds=None) -> str:
        # 戻り値はファイルパス(これがXCom経由で次に渡る)

        url = "https://api.open-meteo.com/v1/forecast"

        response = requests.get(
            url,
            params={
                "latitude": "35",
                "longitude": "139",
                "hourly": "temperature_2m",
                "timezone": "Asia/Tokyo",
                "start_date": ds,
                "end_date": ds,
            },
        )
        response.raise_for_status()
        data = response.json()

        path = f"/opt/airflow/data/raw/{ds}.json"
        dirname = os.path.dirname(path)
        os.makedirs(dirname, exist_ok=True)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        print(f"Complete fetch for {ds}")
        return path

    @task
    def validate_raw(path: str) -> str:
        print(path)
        with open(path) as f:
            data = json.load(f)

        required_keys = ["latitude", "longitude", "hourly"]
        for key in required_keys:
            if key not in data:
                raise ValueError(f"missing key: {key}")

        if not isinstance(data["hourly"], dict):
            raise ValueError("hourly must be an object")

        return path

    @task
    def transform(path: str, ds=None) -> str:
        print(path)
        save_path = f"/opt/airflow/data/transformed/transformed_{ds}.csv"
        dirname = os.path.dirname(save_path)
        os.makedirs(dirname, exist_ok=True)

        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        df = pd.DataFrame(d["hourly"])
        # column1=time, type:string (ISO8601 format, e.g. 2026-07-24T00:00)
        # column2=temperature_2m, type:float
        df.to_csv(save_path, index=False)
        return save_path

    @task
    def load_to_db(path: str, ds=None) -> None:
        db_path = "/opt/airflow/data/warehouse/forecast.db"
        dirname = os.path.dirname(db_path)
        os.makedirs(dirname, exist_ok=True)

        conn = duckdb.connect(db_path)
        table = "TEMPERATURE"
        column1 = "date"
        column2 = "temperature"
        column3 = "ds"

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                {column1} VARCHAR PRIMARY KEY,
                {column2} DOUBLE,
                {column3} date,
                created_at TIMESTAMP DEFAULT current_timestamp
            )
        """)
        conn.execute(
            f"""
            DELETE FROM {table}
            WHERE ds = ?
            """,
            [ds],
        )

        df_new = pd.read_csv(path)
        conn.register("tmp_df", df_new)
        conn.execute(
            f"""
            INSERT INTO {table} (date, temperature, ds)
            SELECT 
                time AS date,
                temperature_2m AS temperature,
                ? AS ds
            FROM tmp_df
            """,
            [ds],
        )
        conn.unregister("tmp_df")

        print(f"loading {path} for {ds}")

        conn.close()

    loaded = load_to_db(transform(validate_raw(fetch_weather())))
    loaded >> dbt_run_task >> dbt_test_task


weather_daily()
