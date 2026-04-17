from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import logging

# INGEST / TRANSFORM / LOAD
from src.ingestion.fred_macro_ingest import run as fred_ingest
from src.transform.transform_fred import main as fred_transform
from src.load.load_fred_staging import load_fred_staging as fred_load

from src.ingestion.nhtsa_ingest import run as nhtsa_ingest
from src.transform.transform_nhtsa import transform_nhtsa
from src.load.load_nhtsa_staging import load_nhtsa_staging as nhtsa_load

from src.ingestion.sales_ingest import run as sales_ingest
from src.transform.transform_sales import main as sales_transform
from src.load.load_sales_staging import load_sales_staging as sales_load

# VALIDATION
from src.validation.validate_fred import validate_fred
from src.validation.validate_nhtsa import validate_nhtsa
from src.validation.validate_sales import validate_sales

# QUALITY
from src.quality.check_fred_quality import check_fred_quality
from src.quality.check_nhtsa_quality import check_nhtsa_quality
from src.quality.check_sales_quality import check_sales_quality

# LOAD_TO_DW
from src.load.load_sales_to_dw import load_sales_to_dw
from src.load.load_fred_to_dw import load_fred_to_dw
from src.load.load_nhtsa_to_dw import load_nhtsa_to_dw


def failure_callback(context):
    logger = logging.getLogger("airflow.task")
    logger.error(f"Task FAILED: {context['task_instance'].task_id}", exc_info=True)


default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_callback
}


with DAG(
    dag_id="car_data_platform_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "production"]
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    sources = {
        "fred": {
            "ingest": fred_ingest,
            "validate": validate_fred,
            "transform": fred_transform,
            "quality": check_fred_quality,
            "load": fred_load
        },
        "nhtsa": {
            "ingest": nhtsa_ingest,
            "validate": validate_nhtsa,
            "transform": nhtsa_transform,
            "quality": check_nhtsa_quality,
            "load": nhtsa_load
        },
        "sales": {
            "ingest": sales_ingest,
            "validate": validate_sales,
            "transform": sales_transform,
            "quality": check_sales_quality,
            "load": sales_load
        }
    }

    groups = []

    for name, funcs in sources.items():

        with TaskGroup(group_id=f"{name}_pipeline") as group:

            ingest = PythonOperator(
                task_id="ingest",
                python_callable=funcs["ingest"]
            )

            validate = PythonOperator(
                task_id="validate",
                python_callable=funcs["validate"]
            )

            transform = PythonOperator(
                task_id="transform",
                python_callable=funcs["transform"]
            )

            quality = PythonOperator(
                task_id="quality",
                python_callable=funcs["quality"]
            )

            load = PythonOperator(
                task_id="load",
                python_callable=funcs["load"]
            )

            warehouse = PythonOperator(
               task_id="warehouse_load",
               python_callable=funcs["warehouse_load"]
            )

            ingest >> validate >> transform >> quality >> load >> warehouse

        groups.append(group)

    start >> groups >> end