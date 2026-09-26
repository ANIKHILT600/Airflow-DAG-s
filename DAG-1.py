from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
)

PROJECT_ID = "my-prod-project"
REGION = "asia-south1"
BUCKET = "my-prod-data"

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="customer_daily_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["production", "pyspark", "gcp"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    wait_for_input = GCSObjectExistenceSensor(
        task_id="wait_for_input",
        bucket=BUCKET,
        object="raw/customers/{{ ds }}/customers.json",
        poke_interval=60,
        timeout=60 * 30,
        mode="reschedule",
    )

    spark_job = DataprocSubmitJobOperator(
        task_id="customer_transformation",
        project_id=PROJECT_ID,
        region=REGION,

        job={
            "reference": {
                "project_id": PROJECT_ID,
            },
            "placement": {
                "cluster_name": "prod-dataproc-cluster",
            },
            "pyspark_job": {
                "main_python_file_uri":
                    f"gs://{BUCKET}/pyspark/customer_transformation.py",

                "args": [
                    "--input",
                    "gs://my-prod-data/raw/customers/{{ ds }}/",
                    "--output",
                    "gs://my-prod-data/processed/customers/{{ ds }}/",
                ],
            },
        },

        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    load_bigquery = BigQueryInsertJobOperator(
        task_id="load_bigquery",

        configuration={
            "query": {
                "query": """
                    MERGE `my-prod-project.analytics.customers` T
                    USING `my-prod-project.staging.customers` S
                    ON T.customer_id = S.customer_id

                    WHEN MATCHED THEN
                      UPDATE SET
                        name = S.name,
                        email = S.email

                    WHEN NOT MATCHED THEN
                      INSERT (
                        customer_id,
                        name,
                        email
                      )
                      VALUES (
                        S.customer_id,
                        S.name,
                        S.email
                      )
                """,
                "useLegacySql": False,
            }
        },

        location="asia-south1",
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> wait_for_input >> spark_job >> load_bigquery >> end
