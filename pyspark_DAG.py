"""
Production-style ETL pipeline for Google Cloud Composer.

Architecture:

    GCS RAW
       |
       v
    Dataproc / PySpark
       |
       v
    GCS REFINED / Parquet
       |
       v
    BigQuery STAGING
       |
       v
    Data Quality Checks
       |
       v
    BigQuery PRODUCTION
       |
       v
    Final Validation

Airflow's responsibility:
    - Scheduling
    - Dependency management
    - Retries
    - Monitoring
    - Orchestration
    - Data-quality coordination

Dataproc's responsibility:
    - Distributed processing
    - Transformation
    - Deduplication
    - Data cleansing

BigQuery's responsibility:
    - Analytical storage
    - SQL transformations
    - Serving/querying
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
)
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensor,
)
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


# =============================================================================
# 1. CONFIGURATION
# =============================================================================
#
# In production, avoid hardcoding environment-specific configuration.
#
# These values could alternatively come from:
#
#   - Airflow Variables
#   - environment-specific configuration
#   - Secret Manager
#   - deployment-time configuration
#
# Never store passwords/API secrets directly in the DAG.
# =============================================================================

PROJECT_ID = Variable.get(
    "gcp_project_id",
    default_var="my-production-project",
)

REGION = Variable.get(
    "gcp_region",
    default_var="us-central1",
)

RAW_BUCKET = Variable.get(
    "raw_bucket",
    default_var="company-data-raw",
)

REFINED_BUCKET = Variable.get(
    "refined_bucket",
    default_var="company-data-refined",
)

BQ_DATASET = Variable.get(
    "bigquery_dataset",
    default_var="analytics",
)

BQ_STAGING_DATASET = Variable.get(
    "bigquery_staging_dataset",
    default_var="analytics_staging",
)

RAW_PREFIX = "customer_events/raw"
REFINED_PREFIX = "customer_events/refined"

BQ_STAGING_TABLE = "customer_events_staging"
BQ_PRODUCTION_TABLE = "customer_events"


# =============================================================================
# 2. DAG DEFAULT ARGUMENTS
# =============================================================================

default_args = {
    # Team/person responsible for this pipeline.
    "owner": "data-platform",

    # Don't automatically depend on the previous DAG run.
    #
    # This is generally safer for pipelines where each execution should
    # independently process its own logical date.
    "depends_on_past": False,

    # Transient GCP/network/API failures should normally be retried.
    "retries": 3,

    # Wait five minutes before retrying.
    "retry_delay": timedelta(minutes=5),

    # Prevent a task from running indefinitely.
    "execution_timeout": timedelta(hours=2),

}


# =============================================================================
# 3. FAILURE CALLBACK
# =============================================================================

def task_failure_callback(context):
    """
    Centralized failure handling.

    In a real production environment this could publish to:

        - Google Chat
        - Slack
        - PagerDuty
        - email
        - incident-management system

    The callback receives the Airflow task execution context.
    """

    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["logical_date"]

    # Keep this callback lightweight.
    #
    # Do not perform expensive processing here because callbacks execute
    # as part of Airflow task handling.
    print(
        f"""
        PIPELINE FAILURE

        DAG: {dag_id}
        TASK: {task_id}
        EXECUTION DATE: {execution_date}
        """
    )


# =============================================================================
# 4. DAG DEFINITION
# =============================================================================

with DAG(
    dag_id="customer_events_production_etl",

    # The logical start date of this DAG.
    start_date=datetime(2026, 1, 1),

    # Run every day at 02:00.
    schedule="0 2 * * *",

    # Don't automatically backfill historical dates.
    #
    # Historical processing should normally be an explicit operational
    # decision rather than something that happens automatically.
    catchup=False,

    default_args=default_args,

    # Prevent overlapping production runs.
    #
    # This is especially important when the pipeline modifies the same
    # BigQuery production tables.
    max_active_runs=1,

    # Protect the Composer environment from too many simultaneously
    # running tasks.
    max_active_tasks=20,

    # Hard upper bound for the entire DAG run.
    dagrun_timeout=timedelta(hours=4),

    # Tags make DAGs easier to find in the Airflow UI.
    tags=[
        "production",
        "etl",
        "gcs",
        "dataproc",
        "pyspark",
        "bigquery",
    ],

    # Called whenever a task fails.
    on_failure_callback=task_failure_callback,

) as dag:

    # =========================================================================
    # 5. START
    # =========================================================================

    start = EmptyOperator(
        task_id="start",
    )


    # =========================================================================
    # 6. CREATE RUNTIME PATHS
    # =========================================================================
    #
    # Airflow templates such as {{ ds }} are evaluated at task execution time.
    #
    # For example:
    #
    #   2026-09-26
    #
    # becomes:
    #
    #   customer_events/raw/2026-09-26/
    #
    # Using the logical date rather than "today" makes reruns deterministic.
    # =========================================================================

    @task
    def generate_paths(**context):
        """
        Generate paths used by downstream tasks.

        TaskFlow API automatically pushes the return value to XCom.

        This is a good example of lightweight XCom usage.

        IMPORTANT:
            Don't put large datasets/files into XCom.
            XCom should contain metadata, paths, IDs, etc.
        """

        ds = context["ds"]

        return {
            "raw_path": f"gs://{RAW_BUCKET}/{RAW_PREFIX}/{ds}/",
            "refined_path": f"gs://{REFINED_BUCKET}/{REFINED_PREFIX}/{ds}/",
            "raw_object": f"{RAW_PREFIX}/{ds}/_SUCCESS",
        }


    paths = generate_paths()


    # =========================================================================
    # 7. CHECK INPUT FILE
    # =========================================================================
    #
    # A sensor waits for an external condition.
    #
    # Here we assume the upstream ingestion process writes a _SUCCESS marker
    # after all raw files for the partition have arrived.
    #
    # This is preferable to checking for an arbitrary data file because the
    # presence of one file doesn't necessarily mean the upstream process
    # has finished writing all files.
    # =========================================================================

    wait_for_raw_data = GCSObjectExistenceSensor(
        task_id="wait_for_raw_data",

        bucket=RAW_BUCKET,

        object=f"{RAW_PREFIX}/{{{{ ds }}}}/_SUCCESS",

        # Don't wait forever.
        timeout=60 * 60 * 2,

        # Check every five minutes.
        poke_interval=300,

        # Reschedule frees the Airflow worker while waiting.
        #
        # This is particularly useful for long-running sensors.
        mode="reschedule",

    )


    # =========================================================================
    # 8. INPUT VALIDATION
    # =========================================================================

    def validate_input_metadata(**context):
        """
        Lightweight Python validation.

        We intentionally don't read the entire dataset here.

        Airflow workers should not be used for large-scale data processing.
        """

        ds = context["ds"]

        print(f"Validating input partition: {ds}")

        # Additional metadata validation could happen here:
        #
        #   - expected file count
        #   - file naming convention
        #   - object size
        #   - upstream completion metadata
        #
        # For actual dataset/schema validation, use Spark or BigQuery.

        if not ds:
            raise AirflowException("Execution date is missing")


    validate_input = PythonOperator(
        task_id="validate_input_metadata",
        python_callable=validate_input_metadata,
    )


    # =========================================================================
    # 9. DATAPROC PROCESSING
    # =========================================================================
    #
    # There are two common production patterns:
    #
    # A) Use an existing Dataproc cluster
    #
    # B) Create an ephemeral cluster/job for each workload
    #
    # Dataproc Serverless batches are particularly useful because Airflow
    # doesn't have to manage cluster lifecycle.
    #
    # This example uses Dataproc Serverless.
    # =========================================================================

    dataproc_batch = DataprocCreateBatchOperator(
        task_id="run_pyspark_transformation",

        project_id=PROJECT_ID,
        region=REGION,

        batch_id="customer-events-{{ ds_nodash }}",

        batch={
            "pyspark_batch": {
                # Your PySpark application stored in GCS.
                "main_python_file_uri": (
                    f"gs://{REFINED_BUCKET}/jobs/customer_events.py"
                ),

                # Arguments passed to the PySpark application.
                "args": [
                    "--input",
                    f"gs://{RAW_BUCKET}/{RAW_PREFIX}/{{{{ ds }}}}/",

                    "--output",
                    f"gs://{REFINED_BUCKET}/{REFINED_PREFIX}/{{{{ ds }}}}/",

                    "--execution-date",
                    "{{ ds }}",
                ],
            },

            # Optional runtime properties.
            "runtime_config": {
                "version": "2.2",

                "properties": {
                    # Example Spark tuning.
                    "spark.executor.instances": "4",
                    "spark.executor.memory": "4g",
                    "spark.executor.cores": "2",

                    # Avoid excessive small output files.
                    "spark.sql.shuffle.partitions": "100",
                },
            },
        },

        # Don't wait forever.
        execution_timeout=timedelta(hours=2),

        # Retry transient Dataproc/API failures.
        retries=2,

        retry_delay=timedelta(minutes=10),
    )


    # =========================================================================
    # 10. VERIFY REFINED OUTPUT
    # =========================================================================
    #
    # The PySpark job should ideally write a _SUCCESS marker after the output
    # has been successfully committed.
    # =========================================================================

    wait_for_refined_data = GCSObjectExistenceSensor(
        task_id="wait_for_refined_data",

        bucket=REFINED_BUCKET,

        object=f"{REFINED_PREFIX}/{{{{ ds }}}}/_SUCCESS",

        timeout=60 * 60,

        poke_interval=120,

        mode="reschedule",
    )


    # =========================================================================
    # 11. BIGQUERY LOAD
    # =========================================================================
    #
    # Load refined Parquet files into a staging table first.
    #
    # Never directly overwrite the production table before validation.
    #
    # Pipeline:
    #
    #     refined GCS
    #          |
    #          v
    #     staging table
    #          |
    #          v
    #     validation
    #          |
    #          v
    #     production
    #
    # This creates a safer promotion model.
    # =========================================================================

    load_bigquery_staging = BigQueryInsertJobOperator(
        task_id="load_bigquery_staging",

        project_id=PROJECT_ID,

        configuration={
            "load": {
                "sourceUris": [
                    f"gs://{REFINED_BUCKET}/{REFINED_PREFIX}/{{{{ ds }}}}/*.parquet"
                ],

                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": BQ_STAGING_DATASET,
                    "tableId": BQ_STAGING_TABLE,
                },

                # Parquet schema is automatically detected.
                "sourceFormat": "PARQUET",

                # Replace the staging partition/table for this run.
                #
                # The staging layer is disposable.
                "writeDisposition": "WRITE_TRUNCATE",

                "createDisposition": "CREATE_IF_NEEDED",
            }
        },

        # BigQuery API calls are retryable.
        retries=3,

        retry_delay=timedelta(minutes=5),
    )


    # =========================================================================
    # 12. VERIFY BIGQUERY TABLE
    # =========================================================================

    staging_table_exists = BigQueryTableExistenceSensor(
        task_id="verify_staging_table",

        project_id=PROJECT_ID,

        dataset_id=BQ_STAGING_DATASET,

        table_id=BQ_STAGING_TABLE,

        timeout=30 * 60,

        poke_interval=60,

        mode="reschedule",
    )


    # =========================================================================
    # 13. DATA QUALITY VALIDATION
    # =========================================================================
    #
    # BigQueryCheckOperator expects the SQL query to return a row where all
    # returned values evaluate to True.
    # =========================================================================

    validate_row_count = BigQueryCheckOperator(
        task_id="validate_row_count",

        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{PROJECT_ID}.{BQ_STAGING_DATASET}.{BQ_STAGING_TABLE}`
        """,

        use_legacy_sql=False,

        retries=2,
    )


    validate_null_customer_ids = BigQueryCheckOperator(
        task_id="validate_customer_ids",

        sql=f"""
            SELECT COUNT(*) = 0
            FROM `{PROJECT_ID}.{BQ_STAGING_DATASET}.{BQ_STAGING_TABLE}`
            WHERE customer_id IS NULL
        """,

        use_legacy_sql=False,

        retries=2,
    )


    validate_duplicate_events = BigQueryCheckOperator(
        task_id="validate_duplicate_events",

        sql=f"""
            SELECT COUNT(*) = 0
            FROM (
                SELECT
                    event_id
                FROM `{PROJECT_ID}.{BQ_STAGING_DATASET}.{BQ_STAGING_TABLE}`
                GROUP BY event_id
                HAVING COUNT(*) > 1
            )
        """,

        use_legacy_sql=False,

        retries=2,
    )


    # =========================================================================
    # 14. DATA QUALITY TASK GROUP
    # =========================================================================
    #
    # TaskGroup is useful for organizing complex DAGs.
    #
    # The tasks above are intentionally defined individually because it makes
    # the example easier to understand.
    #
    # In a larger project, these validations could be grouped:
    #
    #     data_quality
    #       |- row_count
    #       |- null_check
    #       |- duplicate_check
    #
    # Airflow UI then shows this as a single logical group.
    # =========================================================================

    with TaskGroup(
        group_id="additional_quality_checks",
        tooltip="Additional production data quality checks",
    ) as additional_quality_checks:

        check_event_dates = BigQueryCheckOperator(
            task_id="check_event_dates",

            sql=f"""
                SELECT COUNT(*) = 0
                FROM `{PROJECT_ID}.{BQ_STAGING_DATASET}.{BQ_STAGING_TABLE}`
                WHERE event_timestamp > CURRENT_TIMESTAMP()
            """,

            use_legacy_sql=False,
        )

        check_event_type = BigQueryCheckOperator(
            task_id="check_event_type",

            sql=f"""
                SELECT COUNT(*) = 0
                FROM `{PROJECT_ID}.{BQ_STAGING_DATASET}.{BQ_STAGING_TABLE}`
                WHERE event_type IS NULL
            """,

            use_legacy_sql=False,
        )


    # =========================================================================
    # 15. PROMOTE STAGING -> PRODUCTION
    # =========================================================================
    #
    # Only execute this after every validation succeeds.
    #
    # MERGE is used instead of blindly INSERTing rows.
    #
    # This provides idempotency and makes reruns much safer.
    # =========================================================================

    promote_to_production = BigQueryInsertJobOperator(
        task_id="promote_to_production",

        project_id=PROJECT_ID,

        configuration={
            "query": {
                "query": f"""
                    MERGE `{PROJECT_ID}.{BQ_DATASET}.{BQ_PRODUCTION_TABLE}` AS target

                    USING (
                        SELECT
                            event_id,
                            customer_id,
                            event_type,
                            event_timestamp
                        FROM `{PROJECT_ID}.{BQ_STAGING_DATASET}.{BQ_STAGING_TABLE}`
                    ) AS source

                    ON target.event_id = source.event_id

                    WHEN MATCHED THEN
                        UPDATE SET
                            customer_id = source.customer_id,
                            event_type = source.event_type,
                            event_timestamp = source.event_timestamp

                    WHEN NOT MATCHED THEN
                        INSERT (
                            event_id,
                            customer_id,
                            event_type,
                            event_timestamp
                        )
                        VALUES (
                            source.event_id,
                            source.customer_id,
                            source.event_type,
                            source.event_timestamp
                        )
                """,

                "useLegacySql": False,
            }
        },

        retries=3,

        retry_delay=timedelta(minutes=5),
    )


    # =========================================================================
    # 16. POST-LOAD VALIDATION
    # =========================================================================

    validate_production = BigQueryCheckOperator(
        task_id="validate_production",

        sql=f"""
            SELECT COUNT(*) > 0
            FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_PRODUCTION_TABLE}`
        """,

        use_legacy_sql=False,
    )


    # =========================================================================
    # 17. PIPELINE SUCCESS
    # =========================================================================

    success = EmptyOperator(
        task_id="success",
    )


    # =========================================================================
    # 18. FAILURE / CLEANUP PATH
    # =========================================================================
    #
    # ALL_DONE means this task executes regardless of whether upstream tasks
    # succeeded or failed.
    #
    # This is useful for cleanup operations.
    #
    # Be careful:
    #
    #     TriggerRule.ALL_DONE
    #
    # should NOT normally be used for business-critical downstream tasks.
    # Otherwise a failed upstream task could still allow the pipeline to
    # continue.
    # =========================================================================

    cleanup = EmptyOperator(
        task_id="cleanup",

        trigger_rule=TriggerRule.ALL_DONE,
    )


    # =========================================================================
    # 19. DEPENDENCIES
    # =========================================================================

    start >> paths

    paths >> wait_for_raw_data

    wait_for_raw_data >> validate_input

    validate_input >> dataproc_batch

    dataproc_batch >> wait_for_refined_data

    wait_for_refined_data >> load_bigquery_staging

    load_bigquery_staging >> staging_table_exists

    staging_table_exists >> [
        validate_row_count,
        validate_null_customer_ids,
        validate_duplicate_events,
        additional_quality_checks,
    ]

    [
        validate_row_count,
        validate_null_customer_ids,
        validate_duplicate_events,
        additional_quality_checks,
    ] >> promote_to_production

    promote_to_production >> validate_production

    validate_production >> success

    # Cleanup should happen regardless of success/failure.
    [
        wait_for_raw_data,
        validate_input,
        dataproc_batch,
        wait_for_refined_data,
        load_bigquery_staging,
        staging_table_exists,
        promote_to_production,
        validate_production,
    ] >> cleanup
