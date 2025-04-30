from airflow import DAG
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta
# from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from include.ingest_data import download_data
from airflow.utils.trigger_rule import TriggerRule

dag_owner = 'user'

default_args = {
    'owner': dag_owner,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nyx-taxi-pipeline',
    default_args=default_args,
    description='A Pipeline to Handle NYC taxi report automation',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nyc", "etl", "spark", "bigquery"],
) as dag:

    @task
    def download_data_task():
        year = int(Variable.get("year", default_var=2022))
        service = Variable.get("service", default_var="green")
        download_data(service, year)
        return {"year": year, "service": service}

    @task.branch
    def choose_transform_service(download_result):
        service = download_result["service"].strip()
        return f"truncate_{service}"

    downloaded_data = download_data_task()
    chosen_service = choose_transform_service(downloaded_data)

    # spark_green = SparkSubmitOperator(
    #     task_id='transform_green',
    #     conn_id='spark_default',
    #     verbose=True,
    #     conf={
    #         "spark.jars": "/usr/local/airflow/include/lib/gcs-connector-3.0.6-shaded.jar",
    #         "spark.jars.packages": "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.42.1",
    #         "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    #         "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    #         "spark.hadoop.google.cloud.auth.service.account.enable": "true",
    #         "spark.hadoop.google.cloud.auth.type": "SERVICE_ACCOUNT_JSON_KEYFILE",
    #         "spark.hadoop.google.cloud.auth.service.account.json.keyfile": "/usr/local/airflow/include/credentials/cred_file.json",
    #         "temporaryGcsBucket": "project_clean_data",
    #         "parentProject": "indigo-muse-452811-u7",
    #         "spark.hadoop.fs.gs.project.id": "indigo-muse-452811-u7",
    #         "spark.executor.memory": "4g",
    #         "spark.executor.cores": "4",
    #         "spark.driver.memory": "4g",
    #         "spark.master": "local[*]"
    #     },
    #     application="/usr/local/airflow/include/spark_warehouse/transform_load_green.py",
    #     application_args=[
    #         "--year", "{{ ti.xcom_pull(task_ids='download_data_task')['year'] }}",
    #         "--service", "{{ ti.xcom_pull(task_ids='download_data_task')['service'] }}"
    #     ],
    # )

    # spark_yellow = SparkSubmitOperator(
    #     task_id='transform_yellow',
    #     conn_id='spark_default',
    #     verbose=True,
    #     conf={
    #         "spark.jars": "/usr/local/airflow/include/lib/gcs-connector-3.0.6-shaded.jar",
    #         "spark.jars.packages": "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.42.1",
    #         "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    #         "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    #         "spark.hadoop.google.cloud.auth.service.account.enable": "true",
    #         "spark.hadoop.google.cloud.auth.type": "SERVICE_ACCOUNT_JSON_KEYFILE",
    #         "spark.hadoop.google.cloud.auth.service.account.json.keyfile": "/usr/local/airflow/include/credentials/cred_file.json",
    #         "temporaryGcsBucket": "project_clean_data",
    #         "parentProject": "indigo-muse-452811-u7",
    #         "spark.hadoop.fs.gs.project.id": "indigo-muse-452811-u7",
    #         "spark.executor.memory": "4g",
    #         "spark.executor.cores": "4",
    #         "spark.driver.memory": "4g",
    #         "spark.master": "local[*]"
    #     },
    #     application="/usr/local/airflow/include/spark_warehouse/transform_load_yellow.py",
    #     application_args=[
    #         "--year", "{{ ti.xcom_pull(task_ids='download_data_task')['year'] }}",
    #         "--service", "{{ ti.xcom_pull(task_ids='download_data_task')['service'] }}"
    #     ],
    # )
    
    truncate_green = BashOperator(
        task_id='truncate_green',
        bash_command="""
        cd /usr/local/airflow/include/dbt &&
        dbt deps --profiles-dir .&& 
        dbt run-operation truncate_table --args '{"table_name": "indigo-muse-452811-u7.project_dataset.staging_table_green"}' --profiles-dir .
        """
    )

    truncate_yellow = BashOperator(
        task_id='truncate_yellow',
        bash_command="""
        cd /usr/local/airflow/include/dbt &&
        dbt deps --profiles-dir .&&
        dbt run-operation truncate_table --args '{"table_name": "indigo-muse-452811-u7.project_dataset.staging_table_yellow"}' --profiles-dir .
        """
    )
    
    spark_green = BashOperator(
        task_id='transform_green',
        bash_command="""  
        spark-submit \
        --master local[*] \
        --jars /usr/local/airflow/include/lib/gcs-connector-3.0.6-shaded.jar,/usr/local/airflow/include/lib/spark-bigquery-with-dependencies_2.12.0.42.1.jar \
        --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.42.1 \
        --conf "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" \
        --conf "spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS" \
        --conf "spark.hadoop.google.cloud.auth.service.account.enable=true" \
        --conf "spark.hadoop.google.cloud.auth.type=SERVICE_ACCOUNT_JSON_KEYFILE" \
        --conf "spark.hadoop.google.cloud.auth.service.account.json.keyfile=/usr/local/airflow/include/credentials/cred_file.json" \
        --conf "spark.hadoop.fs.gs.project.id=indigo-muse-452811-u7" \
        --conf "temporaryGcsBucket=project_clean_data" \
        --conf "parentProject=indigo-muse-452811-u7" \
        /usr/local/airflow/include/spark_warehouse/transform_load_green.py \
        --year "{{ ti.xcom_pull(task_ids='download_data_task')['year'] }}" \
        --service "{{ ti.xcom_pull(task_ids='download_data_task')['service'] }}"
        """,
    )
    
    spark_yellow = BashOperator(
        task_id='transform_yellow',
        bash_command=""" 
        spark-submit \
        --master local[*] \
        --jars /usr/local/airflow/include/lib/gcs-connector-3.0.6-shaded.jar,/usr/local/airflow/include/lib/spark-bigquery-with-dependencies_2.12.0.42.1.jar \
        --conf "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" \
        --conf "spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS" \
        --conf "spark.hadoop.google.cloud.auth.service.account.enable=true" \
        --conf "spark.hadoop.google.cloud.auth.type=SERVICE_ACCOUNT_JSON_KEYFILE" \
        --conf "spark.hadoop.google.cloud.auth.service.account.json.keyfile=/usr/local/airflow/include/credentials/cred_file.json" \
        --conf "spark.hadoop.fs.gs.project.id=indigo-muse-452811-u7" \
        --conf "temporaryGcsBucket=project_clean_data" \
        --conf "parentProject=indigo-muse-452811-u7" \
        /usr/local/airflow/include/spark_warehouse/transform_load_yellow.py \
        --year "{{ ti.xcom_pull(task_ids='download_data_task')['year'] }}" \
        --service "{{ ti.xcom_pull(task_ids='download_data_task')['service'] }}"
        """,
    )
    
    dim_dbt = BashOperator(
        trigger_rule = TriggerRule.ONE_SUCCESS,
        task_id="dim_dbt",
        bash_command="""
        cd /usr/local/airflow/include/dbt &&
        dbt run --model  core.dim_location \
            core.dim_payment \
            core.dim_rate_code \
            core.dim_store_fwd_flag dim_vendor \
            core.dim_vendor --profiles-dir .
        """
    )
    
    fact_dbt = BashOperator(
        task_id="fact_dbt",
        bash_command= """
        cd /usr/local/airflow/include/dbt &&
        dbt run --model core.production_table --profiles-dir .
        """
    )

    # Set dependencies
    downloaded_data >> chosen_service
    chosen_service >> [truncate_green, truncate_yellow]
    truncate_yellow >> spark_yellow
    truncate_green >> spark_green
    [spark_green, spark_yellow] >> dim_dbt >> fact_dbt
