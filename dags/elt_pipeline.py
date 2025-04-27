from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from cosmos.airflow.task_group import DbtTaskGroup
from datetime import datetime, timedelta
from cosmos.config import RenderConfig
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from include.ingest_data import download_data

dag_owner = 'user'

default_args = {
    'owner': dag_owner,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'nyx-taxi-pipeline',
    default_args=default_args,
    description='A Pipeline to Handle NYC taxi report automation',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nyc", "etl", "spark", "dbt"]
) as dag:

    @task
    def download_data_task():
        year = int(Variable.get("year", default_var=2022))
        service = Variable.get("service_type", default_var="green")
        download_data(service, year)
        return {"year": year, "service": service}

    @task.branch
    def choose_transform_service(download_result):
        service = download_result["service"]
        return f"transform_{service}"

    transform_green = SparkSubmitOperator(
        task_id='transform_green',
        conn_id='spark_default',
        application='./include/spark_warehouse/transform_load_green.py',
        verbose=True
    )

    transform_yellow = SparkSubmitOperator(
        task_id='transform_yellow',  
        conn_id='spark_default',
        application='./include/spark_warehouse/transform_load_yellow.py',
        verbose=True
    )

    # DBT TaskGroup for transformation using Cosmos
    @task
    def dbt_transformation_task():
        return DbtTaskGroup(
            group_id='transform_taxi_data',
            project_config=DBT_PROJECT_CONFIG,
            profile_config=DBT_CONFIG,
            render_config=RenderConfig(
                load_method='dbt_ls',  # Load DBT models dynamically
                select=['path:models/core/combined_taxi_data']  # Specify the model(s) to run
            )
        )

    # Task dependencies
    download_result = download_data_task()
    branch = choose_transform_service(download_result)

    # Set application_args after getting download result
    transform_green.application_args = [str(download_result['year']), download_result['service']]
    transform_yellow.application_args = [str(download_result['year']), download_result['service']]

    dbt_transform = dbt_transformation_task()
    # Branching logic
    download_result >> branch >> [transform_green, transform_yellow] >> dbt_transform
