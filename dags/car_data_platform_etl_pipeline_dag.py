
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from src.ingestion.fred_macro_ingest import run as fred_ingest
from src.transform.transform_fred import transform_fred as fred_transform
from src.load.load_fred_staging import load_fred_staging as fred_load

from src.ingestion.nhtsa_ingest import run as nhtsa_ingest
from src.transform.transform_nhtsa import transform_nhtsa as nhtsa_transform
from src.load.load_nhtsa_staging import load_nhtsa_staging as load_nhtsa

from src.ingestion.sales_ingest import run as sales_ingest
from src.transform.transform_sales import transform_sales as sales_transform
from src.load.load_sales_staging import load_sales_staging as sales_load
# IMPORT FUNCTIONS
sources = {
    "fred": {
        "ingest": fred_ingest,
        "transform": fred_transform,
        "load": fred_load
    },
    "nhtsa": {
        "ingest": nhtsa_ingest,
        "transform": nhtsa_transform,
        "load": nhtsa_load
    },
    "sales": {
        "ingest": sales_ingest,
        "transform": sales_transform,
        "load": sales_load
    }
}

default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="car_data_platform_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "car-data"]
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    groups = []

    for source_name, funcs in sources.items():

        with TaskGroup(group_id=f"{source_name}_pipeline") as group:

            ingest = PythonOperator(
                task_id="ingest",
                python_callable=funcs["ingest"]
            )

            transform = PythonOperator(
                task_id="transform",
                python_callable=funcs["transform"]
            )

            load = PythonOperator(
                task_id="load",
                python_callable=funcs["load"]
            )

            ingest >> transform >> load

        groups.append(group)

    start >> groups >> end