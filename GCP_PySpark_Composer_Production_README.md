# Production GCP PySpark + Cloud Composer (Airflow) Data Pipeline

A production-oriented reference implementation for building, deploying, operating, and troubleshooting a GCP data pipeline using:

- **Cloud Composer (Apache Airflow)** for orchestration
- **Dataproc Serverless Spark** for PySpark processing
- **Cloud Storage (GCS)** for raw and processed data
- **BigQuery** for staging and curated analytical data
- **Secret Manager / IAM** for security
- **Cloud Logging / Monitoring** for observability
- **GitHub Actions** for CI/CD
- **Terraform** for infrastructure as code

The implementation intentionally demonstrates important production concepts such as retries, idempotency, data validation, dynamic task mapping, TaskGroups, sensors, XCom, Jinja templating, parameterization, data quality checks, structured logging, failure handling, and Spark performance tuning.

---

## 1. Architecture

```text
                         GitHub
                           |
                           v
                    GitHub Actions
                     CI / CD Pipeline
                           |
            +--------------+--------------+
            |                             |
        DAG tests                      PySpark tests
            |                             |
            +--------------+--------------+
                           |
                           v
                    Cloud Composer
                    Airflow Environment
                           |
             +-------------+-------------+
             |             |             |
             v             v             v
          GCS Sensor    Validation    Configuration
             |             |
             +------+------+
                    |
                    v
             Dataproc Serverless
                 Apache Spark
                    |
          +---------+----------+
          |                    |
          v                    v
       GCS Raw             BigQuery
          |                 Staging
          |                    |
          +---------+----------+
                    |
                    v
              Data Quality
                    |
                    v
             BigQuery MERGE
                    |
                    v
             Curated Tables
                    |
                    v
          Cloud Monitoring /
          Cloud Logging /
             Alerting
```

---

# 2. Recommended Repository Structure

```text
gcp-data-platform/
|
+-- dags/
|   +-- customer_pipeline.py
|
+-- pyspark/
|   +-- customer_transformation.py
|
+-- tests/
|   +-- test_dag.py
|   +-- test_customer_transformation.py
|
+-- sql/
|   +-- merge_customer.sql
|
+-- config/
|   +-- dev.yaml
|   +-- qa.yaml
|   +-- prod.yaml
|
+-- terraform/
|   +-- main.tf
|   +-- variables.tf
|   +-- outputs.tf
|   +-- iam.tf
|   +-- gcs.tf
|   +-- bigquery.tf
|   +-- composer.tf
|
+-- requirements/
|   +-- composer.txt
|   +-- spark.txt
|
+-- .github/
|   +-- workflows/
|       +-- ci.yml
|       +-- deploy.yml
|
+-- README.md
```

---

# 3. Example Data Flow

The pipeline processes customer files arriving in GCS.

```text
gs://company-prod-raw/customers/YYYY-MM-DD/
                    |
                    v
              Input validation
                    |
                    v
          Dataproc Serverless Spark
                    |
                    +--> Schema validation
                    |
                    +--> Null validation
                    |
                    +--> Deduplication
                    |
                    +--> Transformation
                    |
                    +--> Data quality metrics
                    |
                    v
        gs://company-prod-processed/
                    |
                    v
             BigQuery staging
                    |
                    v
                MERGE
                    |
                    v
          BigQuery curated table
```

---

# 4. Important Production Principles

## 4.1 Idempotency

Airflow tasks can retry.

A production pipeline must therefore be safe to execute more than once.

Bad pattern:

```text
Spark
  |
  v
INSERT INTO production
```

If Airflow retries, duplicate records may be created.

Better pattern:

```text
Spark
  |
  v
Staging
  |
  v
MERGE
  |
  v
Production
```

Example:

```sql
MERGE `project.analytics.customers` T
USING `project.staging.customers` S
ON T.customer_id = S.customer_id

WHEN MATCHED THEN
  UPDATE SET
    name = S.name,
    email = S.email,
    updated_at = S.updated_at

WHEN NOT MATCHED THEN
  INSERT (
    customer_id,
    name,
    email,
    updated_at
  )
  VALUES (
    S.customer_id,
    S.name,
    S.email,
    S.updated_at
  );
```

---

# 5. Airflow / Cloud Composer Concepts

## DAG

A DAG defines the workflow and dependencies.

```python
with DAG(
    dag_id="customer_pipeline",
    schedule="0 2 * * *",
    catchup=False,
):
    ...
```

---

## Operators

Common operators:

```text
EmptyOperator
PythonOperator
BashOperator
BigQueryInsertJobOperator
BigQueryCheckOperator
DataprocSubmitJobOperator
DataprocCreateBatchOperator
GCSObjectExistenceSensor
PubSubPublishMessageOperator
```

Use provider operators instead of shelling out to `gcloud` whenever a supported Airflow operator exists.

---

## Task Dependencies

```python
start >> validate >> spark >> quality >> load >> end
```

---

## Retries

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}
```

Retries are appropriate for transient failures such as:

- temporary GCP API errors
- network failures
- transient BigQuery errors
- temporary Dataproc failures

Retries should not hide permanent data-quality errors.

---

## Sensors

A sensor waits for an external condition.

Example:

```python
GCSObjectExistenceSensor(
    task_id="wait_for_input",
    bucket="company-prod-raw",
    object="customers/{{ ds }}/customers.json",
    poke_interval=60,
    timeout=1800,
    mode="reschedule",
)
```

### Sensor modes

`poke`:

```text
Worker slot
   |
   +-- sensor waits
   |
   +-- sensor waits
```

`reschedule`:

```text
Worker slot
   |
   +-- check
   |
   +-- release slot
   |
   +-- check later
```

For long waits, `reschedule` or a supported deferrable sensor is generally preferable.

---

# 6. Jinja Templating

Airflow provides runtime context.

Example:

```python
"gs://bucket/raw/customers/{{ ds }}/"
```

Common values:

```text
{{ ds }}
{{ ds_nodash }}
{{ logical_date }}
{{ data_interval_start }}
{{ data_interval_end }}
{{ run_id }}
```

Do not confuse Airflow logical date with the wall-clock time when a task actually executes.

---

# 7. XCom

XCom is designed for small task metadata.

Good:

```text
record_count = 125000
```

Bad:

```text
2 GB dataframe
```

Example:

```python
from airflow.decorators import task

@task
def get_file():
    return "customers.json"

@task
def process_file(file_name):
    print(file_name)

process_file(get_file())
```

Never use XCom as a data lake.

Use GCS / BigQuery for actual data.

---

# 8. TaskGroups

TaskGroups improve DAG organization.

```text
customer_pipeline
|
+-- ingestion
|   +-- validate
|   +-- ingest
|
+-- transformation
|   +-- spark
|
+-- quality
|   +-- null_check
|   +-- duplicate_check
|
+-- publishing
    +-- merge
```

---

# 9. Dynamic Task Mapping

Useful when the number of tasks is determined at runtime.

```python
@task
def get_files():
    return [
        "customer_1.json",
        "customer_2.json",
        "customer_3.json",
    ]

@task
def process_file(file_name):
    print(file_name)

process_file.expand(
    file_name=get_files()
)
```

Airflow creates:

```text
process_file[0]
process_file[1]
process_file[2]
```

This is useful for processing:

- multiple files
- multiple partitions
- multiple tables
- multiple independent datasets

Avoid creating thousands of mapped tasks unless the Airflow environment is sized for it.

---

# 10. Trigger Rules

Default behavior:

```text
all_success
```

Other useful rules:

```text
all_done
one_success
one_failed
none_failed
none_skipped
```

Example cleanup task:

```python
cleanup = EmptyOperator(
    task_id="cleanup",
    trigger_rule="all_done",
)
```

Use trigger rules intentionally. A cleanup task should not accidentally make a failed pipeline look successful.

---

# 11. Pools and Concurrency

Production Composer environments must be protected from overload.

Important controls include:

```text
DAG max_active_runs
task concurrency / task limits
Airflow pools
environment worker capacity
Dataproc concurrency
BigQuery quotas
```

Example:

```python
with DAG(
    dag_id="customer_pipeline",
    max_active_runs=1,
):
    ...
```

If only one run should process a partition at a time, `max_active_runs=1` is useful.

---

# 12. Production DAG

File:

```text
dags/customer_pipeline.py
```

```python
"""
Production-oriented Cloud Composer DAG.

Flow:
    1. Wait for GCS input
    2. Validate runtime parameters
    3. Submit PySpark to Dataproc Serverless
    4. Run BigQuery data quality checks
    5. MERGE staging data into curated table
    6. Publish success/failure notifications

Important:
- Keep secrets out of DAG source code.
- Use Composer environment configuration / Secret Manager / Airflow connections.
- Keep large data out of XCom.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
)
from airflow.utils.task_group import TaskGroup


PROJECT_ID = "my-prod-project"
REGION = "asia-south1"

RAW_BUCKET = "company-prod-raw"
PROCESSED_BUCKET = "company-prod-processed"

BQ_PROJECT = PROJECT_ID
BQ_DATASET = "analytics"

PYSPARK_URI = (
    f"gs://{PROCESSED_BUCKET}/jobs/"
    "customer_transformation.py"
)


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,

    # Retry transient infrastructure/API failures.
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,

    # Prevent a task from retrying forever.
    "execution_timeout": timedelta(hours=2),
}


def task_failure_callback(context):
    """
    Production implementation can publish to:
      - Google Chat
      - Slack
      - PagerDuty
      - Pub/Sub
      - Email

    Keep notification logic lightweight.
    """
    task_instance = context.get("task_instance")

    print(
        "Task failed: "
        f"{task_instance.dag_id}.{task_instance.task_id}"
    )


with DAG(
    dag_id="customer_daily_pipeline",

    description=(
        "Production customer ingestion and "
        "transformation pipeline"
    ),

    start_date=datetime(2026, 1, 1),

    schedule="0 2 * * *",

    catchup=False,

    max_active_runs=1,

    default_args=default_args,

    on_failure_callback=task_failure_callback,

    render_template_as_native_obj=True,

    tags=[
        "production",
        "gcp",
        "pyspark",
        "customer",
    ],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    # ----------------------------------------------------------
    # INPUT
    # ----------------------------------------------------------

    wait_for_input = GCSObjectExistenceSensor(
        task_id="wait_for_input",

        bucket=RAW_BUCKET,

        object=(
            "customers/"
            "{{ ds }}/"
            "customers.json"
        ),

        # Check every minute.
        poke_interval=60,

        # Fail after 30 minutes.
        timeout=60 * 30,

        # Do not occupy a worker while waiting.
        mode="reschedule",
    )

    # ----------------------------------------------------------
    # RUNTIME VALIDATION
    # ----------------------------------------------------------

    @task
    def validate_runtime():
        """
        Validate configuration before expensive Spark work.
        """
        if not PROJECT_ID:
            raise ValueError(
                "PROJECT_ID is not configured"
            )

        if not REGION:
            raise ValueError(
                "REGION is not configured"
            )

        return {
            "project": PROJECT_ID,
            "region": REGION,
        }

    runtime_validation = validate_runtime()

    # ----------------------------------------------------------
    # SPARK
    # ----------------------------------------------------------

    spark_batch = {
        "pyspark_batch": {
            "main_python_file_uri": PYSPARK_URI,

            "args": [
                "--input",
                (
                    f"gs://{RAW_BUCKET}/customers/"
                    "{{ ds }}/customers.json"
                ),
                "--output",
                (
                    f"gs://{PROCESSED_BUCKET}/customers/"
                    "{{ ds }}/"
                ),
                "--run-date",
                "{{ ds }}",
            ],

            "jar_file_uris": [],
        },

        "runtime_config": {
            "version": "2.2",
        },

        "environment_config": {
            "execution_config": {
                "service_account": (
                    "data-pipeline@"
                    f"{PROJECT_ID}.iam.gserviceaccount.com"
                ),
                "subnetwork_uri": (
                    f"projects/{PROJECT_ID}/"
                    "regions/asia-south1/"
                    "subnetworks/data-subnet"
                ),
            }
        },
    }

    submit_spark = DataprocCreateBatchOperator(
        task_id="submit_spark",

        project_id=PROJECT_ID,

        region=REGION,

        batch=spark_batch,

        batch_id=(
            "customer-"
            "{{ ds_nodash }}"
            "-{{ run_id | replace(':', '-') }}"
        ),

        deferrable=True,

    )

    # ----------------------------------------------------------
    # DATA QUALITY
    # ----------------------------------------------------------

    with TaskGroup(
        group_id="data_quality"
    ) as data_quality:

        row_count_check = BigQueryInsertJobOperator(
            task_id="row_count_check",

            project_id=BQ_PROJECT,

            location=REGION,

            configuration={
                "query": {
                    "query": f"""
                    DECLARE row_count INT64;

                    SET row_count = (
                      SELECT COUNT(*)
                      FROM `{BQ_PROJECT}.{BQ_DATASET}.staging_customers`
                      WHERE ingestion_date = DATE('{{{{ ds }}}}')
                    );

                    ASSERT row_count > 0
                    AS 'No customer records found';
                    """,

                    "useLegacySql": False,
                }
            },
        )

        null_check = BigQueryInsertJobOperator(
            task_id="null_customer_id_check",

            project_id=BQ_PROJECT,

            location=REGION,

            configuration={
                "query": {
                    "query": f"""
                    ASSERT NOT EXISTS (
                      SELECT 1
                      FROM `{BQ_PROJECT}.{BQ_DATASET}.staging_customers`
                      WHERE ingestion_date = DATE('{{{{ ds }}}}')
                        AND customer_id IS NULL
                    )
                    AS 'customer_id contains NULL values';
                    """,

                    "useLegacySql": False,
                }
            },
        )

        duplicate_check = BigQueryInsertJobOperator(
            task_id="duplicate_customer_check",

            project_id=BQ_PROJECT,

            location=REGION,

            configuration={
                "query": {
                    "query": f"""
                    ASSERT NOT EXISTS (
                      SELECT customer_id
                      FROM `{BQ_PROJECT}.{BQ_DATASET}.staging_customers`
                      WHERE ingestion_date = DATE('{{{{ ds }}}}')
                      GROUP BY customer_id
                      HAVING COUNT(*) > 1
                    )
                    AS 'Duplicate customer_id detected';
                    """,

                    "useLegacySql": False,
                }
            },
        )

        [
            row_count_check,
            null_check,
            duplicate_check,
        ]

    # ----------------------------------------------------------
    # PUBLISH
    # ----------------------------------------------------------

    merge_to_curated = BigQueryInsertJobOperator(
        task_id="merge_to_curated",

        project_id=BQ_PROJECT,

        location=REGION,

        configuration={
            "query": {
                "query": f"""
                MERGE `{BQ_PROJECT}.{BQ_DATASET}.customers` T
                USING (
                  SELECT
                    customer_id,
                    name,
                    email,
                    updated_at
                  FROM
                    `{BQ_PROJECT}.{BQ_DATASET}.staging_customers`
                  WHERE
                    ingestion_date = DATE('{{{{ ds }}}}')
                ) S
                ON T.customer_id = S.customer_id

                WHEN MATCHED THEN
                  UPDATE SET
                    name = S.name,
                    email = S.email,
                    updated_at = S.updated_at

                WHEN NOT MATCHED THEN
                  INSERT (
                    customer_id,
                    name,
                    email,
                    updated_at
                  )
                  VALUES (
                    S.customer_id,
                    S.name,
                    S.email,
                    S.updated_at
                  )
                """,

                "useLegacySql": False,
            }
        },
    )

    end = EmptyOperator(
        task_id="end"
    )

    (
        start
        >> wait_for_input
        >> runtime_validation
        >> submit_spark
        >> data_quality
        >> merge_to_curated
        >> end
    )
```

---

# 13. Production PySpark Job

File:

```text
pyspark/customer_transformation.py
```

```python
"""
Production-oriented PySpark customer transformation.

Input:
    GCS JSON

Output:
    GCS Parquet

Responsibilities:
    - Explicit schema
    - Input validation
    - Data cleansing
    - Deduplication
    - Audit columns
    - Partitioned output
    - Structured logging
    - Graceful Spark shutdown

This job is designed to be called by Cloud Composer.
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s "
        "%(levelname)s "
        "%(name)s "
        "%(message)s"
    ),
)

LOGGER = logging.getLogger("customer_transformation")


CUSTOMER_SCHEMA = StructType(
    [
        StructField(
            "customer_id",
            StringType(),
            False,
        ),
        StructField(
            "name",
            StringType(),
            True,
        ),
        StructField(
            "email",
            StringType(),
            True,
        ),
        StructField(
            "country",
            StringType(),
            True,
        ),
        StructField(
            "updated_at",
            TimestampType(),
            True,
        ),
    ]
)


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
    )

    parser.add_argument(
        "--output",
        required=True,
    )

    parser.add_argument(
        "--run-date",
        required=True,
    )

    return parser.parse_args()


def validate_columns(df):
    """
    Validate that mandatory columns exist.
    """

    required_columns = {
        "customer_id",
        "name",
        "email",
        "country",
        "updated_at",
    }

    actual_columns = set(df.columns)

    missing = (
        required_columns - actual_columns
    )

    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )


def clean_data(df):
    """
    Apply deterministic transformations.
    """

    df = (
        df
        .withColumn(
            "customer_id",
            F.trim(F.col("customer_id")),
        )
        .withColumn(
            "name",
            F.trim(F.col("name")),
        )
        .withColumn(
            "email",
            F.lower(
                F.trim(F.col("email"))
            ),
        )
        .withColumn(
            "country",
            F.upper(
                F.trim(F.col("country"))
            ),
        )
    )

    # Reject records without a business key.
    df = df.filter(
        F.col("customer_id").isNotNull()
        & (F.col("customer_id") != "")
    )

    return df


def deduplicate(df):
    """
    Keep the latest version of each customer.

    Window ordering must be deterministic enough for the
    source system. In production, consider adding an event
    sequence number if timestamps can tie.
    """

    window = (
        Window
        .partitionBy("customer_id")
        .orderBy(
            F.col("updated_at").desc_nulls_last()
        )
    )

    return (
        df
        .withColumn(
            "_row_number",
            F.row_number().over(window),
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop("_row_number")
    )


def add_audit_columns(df, run_date):
    return (
        df
        .withColumn(
            "ingestion_date",
            F.to_date(
                F.lit(run_date)
            ),
        )
        .withColumn(
            "processed_at",
            F.current_timestamp(),
        )
    )


def write_output(df, output_path):
    """
    Write partitioned Parquet.

    For very large datasets, tune partition count based on
    actual data volume instead of using a hard-coded value.
    """

    (
        df
        .write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("country")
        .save(output_path)
    )


def main():

    args = parse_arguments()

    spark = (
        SparkSession.builder
        .appName("customer-transformation")
        .getOrCreate()
    )

    try:

        LOGGER.info(
            "Starting customer transformation"
        )

        LOGGER.info(
            "Input: %s",
            args.input,
        )

        LOGGER.info(
            "Output: %s",
            args.output,
        )

        # ------------------------------------------------------
        # READ
        # ------------------------------------------------------

        df = (
            spark.read
            .schema(CUSTOMER_SCHEMA)
            .json(args.input)
        )

        input_count = df.count()

        LOGGER.info(
            "Input record count: %s",
            input_count,
        )

        if input_count == 0:
            raise ValueError(
                "Input dataset is empty"
            )

        # ------------------------------------------------------
        # VALIDATION
        # ------------------------------------------------------

        validate_columns(df)

        # ------------------------------------------------------
        # TRANSFORMATION
        # ------------------------------------------------------

        df = clean_data(df)

        df = deduplicate(df)

        df = add_audit_columns(
            df,
            args.run_date,
        )

        output_count = df.count()

        LOGGER.info(
            "Output record count: %s",
            output_count,
        )

        if output_count == 0:
            raise ValueError(
                "Transformation produced zero records"
            )

        # ------------------------------------------------------
        # WRITE
        # ------------------------------------------------------

        write_output(
            df,
            args.output,
        )

        LOGGER.info(
            "Customer transformation completed successfully"
        )

    except Exception:

        LOGGER.exception(
            "Customer transformation failed"
        )

        # Important:
        # Re-raise so Dataproc/Airflow sees the task as failed.
        raise

    finally:

        spark.stop()

        LOGGER.info(
            "Spark session stopped"
        )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)
```

---

# 14. Spark Performance Concepts

## Partitioning

Spark divides data into partitions.

```text
Input
 |
 +-- Partition 1
 +-- Partition 2
 +-- Partition 3
 +-- Partition 4
```

Parallelism occurs at partition level.

---

## `repartition`

```python
df = df.repartition(
    200,
    "customer_id",
)
```

Causes a shuffle.

Use when you actually need to redistribute data.

---

## `coalesce`

```python
df = df.coalesce(20)
```

Generally reduces the number of partitions without a full shuffle.

Useful before writing when you have too many tiny output files.

---

## Small File Problem

Bad:

```text
customers/
    part-0001.parquet
    part-0002.parquet
    ...
    part-900000.parquet
```

Millions of small files can hurt:

- metadata operations
- listing
- query planning
- BigQuery external access
- downstream processing

Prefer appropriately sized files.

---

# 15. Shuffle

Operations that commonly cause shuffle:

```python
groupBy()
join()
distinct()
orderBy()
repartition()
window functions
```

Shuffle can be expensive because data moves between executors.

---

# 16. Broadcast Join

If one dataset is small:

```python
from pyspark.sql.functions import broadcast

result = large_df.join(
    broadcast(small_df),
    "customer_id",
)
```

This can avoid a large shuffle.

Do not broadcast a dataset that is too large for executor memory.

---

# 17. Adaptive Query Execution

AQE can dynamically optimize Spark SQL execution.

Relevant configuration:

```text
spark.sql.adaptive.enabled=true
```

AQE can help with:

- skew
- shuffle partition coalescing
- join strategy optimization

Validate behavior using the Spark UI rather than assuming a configuration automatically fixes performance.

---

# 18. Data Skew

Example:

```text
customer_id=A -> 10 records
customer_id=B -> 20 records
customer_id=C -> 2 billion records
```

One partition becomes huge.

Symptoms:

```text
Most executors finish
One executor remains running
```

Possible approaches:

```text
Salting
Better partition keys
Broadcasting small tables
AQE skew handling
Pre-aggregation
Data model changes
```

---

# 19. Caching

Use cache only when a dataframe is reused.

```python
df = df.cache()

df.count()
df.groupBy("country").count()
```

Do not cache every dataframe.

Caching consumes memory and can cause pressure/spilling.

---

# 20. Explicit Schemas

Avoid relying on schema inference for important production pipelines.

Prefer:

```python
spark.read.schema(
    CUSTOMER_SCHEMA
).json(path)
```

instead of:

```python
spark.read.json(path)
```

Benefits:

- predictable types
- earlier failures
- better performance
- easier schema governance

---

# 21. Data Quality

Production pipelines should validate:

```text
Row count
Nulls
Duplicates
Primary/business key
Allowed values
Referential integrity
Freshness
Schema
Data distribution
```

Example:

```sql
SELECT COUNT(*)
FROM staging_customers
WHERE customer_id IS NULL;
```

Expected:

```text
0
```

---

# 22. Bad Data / Quarantine

Do not necessarily fail the entire pipeline because of a few malformed records.

A common pattern:

```text
                Raw
                 |
        +--------+--------+
        |                 |
      Valid             Invalid
        |                 |
        v                 v
    Processing         Quarantine
                            |
                            v
                    GCS bad-records/
```

Quarantine records should retain enough information for investigation.

---

# 23. Security

Use IAM instead of embedding credentials.

Recommended model:

```text
Cloud Composer
       |
       v
Composer service account
       |
       +--> GCS access
       |
       +--> Dataproc Serverless
       |
       +--> BigQuery
       |
       +--> Secret Manager
```

Use a dedicated runtime service account for Spark.

Avoid:

```python
PASSWORD = "mypassword"
```

Avoid service account JSON keys in Git.

Prefer:

```text
IAM
Workload identity / service account identity
Secret Manager
Airflow Connections
```

---

# 24. IAM Principle of Least Privilege

Example conceptual permissions:

```text
Composer service account:
    orchestrate workloads

Spark service account:
    read raw GCS
    write processed GCS
    write staging BigQuery

Analyst:
    read curated BigQuery

Developer:
    deploy DAGs/jobs
```

Do not give the Spark service account project-wide Owner permissions.

---

# 25. Observability

Monitor at least:

### Airflow

```text
DAG failures
Task duration
Task retries
Queued tasks
Running tasks
Scheduler health
Worker utilization
DAG run duration
```

### Spark

```text
Job duration
Stage duration
Executor failures
Executor memory
Shuffle read
Shuffle write
Spill
Input records
Output records
```

### Data

```text
Freshness
Row counts
Null percentages
Duplicate counts
Data quality failures
```

### GCP

```text
API errors
Quota errors
BigQuery jobs
GCS errors
Dataproc failures
Service account permission failures
```

---

# 26. Logging

Use Python logging.

Good:

```python
LOGGER.info(
    "Processed records=%s",
    count,
)
```

Avoid:

```python
print("something happened")
```

for important production events.

Never log:

```text
Passwords
Access tokens
Secrets
PII
Credentials
```

---

# 27. Failure Handling

Pipeline:

```text
Input
  |
  v
Spark
  |
  +---- failure
  |       |
  |       v
  |     retry
  |       |
  |       v
  |     success
  |
  v
Quality
  |
  +---- failure --> stop
  |
  v
MERGE
```

Important distinction:

### Transient failure

Examples:

```text
Network failure
Temporary GCP API failure
Temporary quota issue
```

Retry may be appropriate.

### Permanent failure

Examples:

```text
Missing required column
Invalid business data
Zero records
Invalid schema
```

Do not blindly retry forever.

---

# 28. Airflow Backfill

Suppose:

```text
2026-09-01
2026-09-02
2026-09-03
```

failed.

Backfill should reprocess those logical data intervals.

Before backfill:

```text
Is pipeline idempotent?
Is target partition isolated?
Will MERGE be safe?
Will downstream systems receive duplicates?
Are dependencies available?
```

Never blindly backfill production.

---

# 29. Catchup

```python
catchup=False
```

means Airflow does not automatically create all historical scheduled runs after `start_date`.

For pipelines requiring historical processing, use explicit controlled backfills.

---

# 30. Data Interval

Airflow's logical scheduling model is based around data intervals.

For a daily pipeline:

```text
Run for 2026-09-25
```

usually represents:

```text
2026-09-25 00:00
        to
2026-09-26 00:00
```

Use:

```text
data_interval_start
data_interval_end
```

when the processing logic depends on the interval rather than simply the wall-clock execution time.

---

# 31. Dataproc Serverless vs Dataproc Cluster

## Serverless

Good when:

```text
Jobs are batch oriented
Cluster lifecycle management should be minimized
Workload is variable
You don't need a long-lived cluster
```

Concept:

```text
Airflow
  |
  v
Serverless Spark Batch
  |
  v
Compute
  |
  v
Job completes
```

## Managed Dataproc Cluster

Useful when:

```text
Long-running workloads
Many jobs share a cluster
Special cluster configuration
Persistent environment requirements
```

Architecture:

```text
Composer
   |
   v
Dataproc Cluster
   |
   +-- Driver
   +-- Workers
   +-- Spark
```

---

# 32. BigQuery Loading Patterns

Common architecture:

```text
Spark
 |
 v
GCS Parquet
 |
 v
BigQuery staging
 |
 v
MERGE
 |
 v
Curated
```

For some workloads, Spark can write directly using the BigQuery connector.

However, separating processing from publishing can provide better control over:

- retries
- validation
- lineage
- reprocessing
- idempotency

Choose based on workload requirements.

---

# 33. Partitioned BigQuery Tables

Example:

```sql
CREATE TABLE analytics.customers
(
    customer_id STRING,
    name STRING,
    email STRING,
    updated_at TIMESTAMP
)
PARTITION BY DATE(updated_at);
```

Partitioning can reduce scanned data.

---

# 34. Clustering

Example:

```sql
CREATE TABLE analytics.customers
(
    customer_id STRING,
    name STRING,
    country STRING,
    updated_at TIMESTAMP
)
PARTITION BY DATE(updated_at)
CLUSTER BY customer_id, country;
```

Use clustering based on actual query patterns.

---

# 35. Testing

## DAG test

```python
from airflow.models import DagBag


def test_dag_loaded():

    dagbag = DagBag(
        dag_folder="dags/",
        include_examples=False,
    )

    dag = dagbag.get_dag(
        "customer_daily_pipeline"
    )

    assert dag is not None
    assert len(dag.tasks) > 0
```

---

## PySpark unit test

Conceptually:

```python
def test_clean_data(spark):

    data = [
        (
            " C001 ",
            " John ",
            "JOHN@EXAMPLE.COM ",
            "us",
            None,
        )
    ]

    df = spark.createDataFrame(
        data,
        [
            "customer_id",
            "name",
            "email",
            "country",
            "updated_at",
        ],
    )

    result = clean_data(df)

    row = result.first()

    assert row.customer_id == "C001"
    assert row.email == "john@example.com"
    assert row.country == "US"
```

---

# 36. CI/CD

Recommended flow:

```text
Developer
   |
   v
Feature branch
   |
   v
Pull Request
   |
   +--> Python lint
   |
   +--> DAG import test
   |
   +--> PySpark unit tests
   |
   +--> Security scan
   |
   +--> Terraform validate
   |
   v
Merge
   |
   v
Build / package
   |
   v
Deploy to Composer / GCS
```

Never make production deployment depend solely on a developer manually copying files.

---

# 37. Environment Separation

Use:

```text
DEV
QA
PROD
```

with separate:

```text
GCP projects
GCS buckets
BigQuery datasets
service accounts
Composer environments
```

Example:

```text
company-dev
company-qa
company-prod
```

Avoid putting environment-specific values directly throughout DAG code.

---

# 38. Configuration

Prefer configuration:

```yaml
project_id: my-prod-project
region: asia-south1

raw_bucket: company-prod-raw
processed_bucket: company-prod-processed

bigquery:
  dataset: analytics
  staging_table: staging_customers
  target_table: customers
```

rather than hard-coding dozens of values in Python.

---

# 39. Secrets

Do not store:

```text
API keys
Passwords
Private keys
Service account JSON
Database passwords
```

in:

```text
Git
DAG source
PySpark source
README
```

Use:

```text
Secret Manager
Airflow Connections
IAM
```

---

# 40. Production Checklist

## Airflow

- [ ] `catchup` intentionally configured
- [ ] retries configured
- [ ] retry backoff configured
- [ ] execution timeout configured
- [ ] task dependencies reviewed
- [ ] sensors use appropriate mode
- [ ] DAG has tags
- [ ] DAG import tested
- [ ] concurrency limits reviewed
- [ ] pools configured if necessary
- [ ] alerting configured
- [ ] XCom not used for large data

## PySpark

- [ ] Explicit schema
- [ ] Input validation
- [ ] Data quality checks
- [ ] Idempotent output
- [ ] Correct partition strategy
- [ ] Shuffle analyzed
- [ ] Join strategy reviewed
- [ ] Skew checked
- [ ] Small files controlled
- [ ] Logging enabled
- [ ] Exception re-raised
- [ ] Spark session closed

## GCP

- [ ] Dedicated service accounts
- [ ] Least privilege IAM
- [ ] Secret Manager
- [ ] GCS lifecycle policies
- [ ] BigQuery partitioning
- [ ] BigQuery clustering where useful
- [ ] Monitoring
- [ ] Logging
- [ ] Budget alerts
- [ ] Quota awareness

## CI/CD

- [ ] Unit tests
- [ ] DAG validation
- [ ] Linting
- [ ] Security scanning
- [ ] Terraform validation
- [ ] Environment separation
- [ ] PR review
- [ ] Controlled production deployment

---

# 41. Troubleshooting Guide

## DAG not appearing

Check:

```text
DAG import errors
Python syntax
Provider versions
Missing dependencies
Composer logs
```

---

## Spark job fails immediately

Check:

```text
Service account
GCS permissions
Dataproc API
Region
Subnetwork
PySpark URI
Dependency versions
```

---

## Spark job is slow

Check:

```text
Input size
Number of partitions
Shuffle
Data skew
Join strategy
Executor memory
Executor cores
Spill
Small files
AQE
```

---

## One executor is much slower

Likely possibilities:

```text
Data skew
Large partition
Uneven data distribution
Expensive task
GC pressure
```

Inspect the Spark UI before changing random Spark configurations.

---

## BigQuery load fails

Check:

```text
Schema mismatch
Permissions
Dataset location
Quota
Bad records
Partition configuration
Table existence
```

---

# 42. Production Incident Example

Problem:

```text
Daily DAG normally finishes in 45 minutes.

Today:
DAG = 4 hours
```

Investigation:

```text
1. Check Composer task duration
2. Check Spark job duration
3. Open Spark UI
4. Check stages
5. Check shuffle
6. Check executor duration
7. Check partition sizes
8. Check data volume
9. Check skew
10. Check BigQuery load duration
```

Suppose:

```text
Input:
200 GB

Yesterday:
200 GB

Today:
205 GB

One Spark task:
2 hours

Other tasks:
10 minutes
```

Likely area to investigate:

```text
Partition/data skew
```

Possible remediation:

```text
Broadcast small dimension
Salting
Better partition key
AQE
Pre-aggregation
```

Do not immediately increase executor count without identifying the bottleneck.

---

# 43. Interview Questions You Should Be Able to Answer

## Airflow

### 1. Why use Airflow?

Because Airflow provides workflow orchestration, scheduling, dependency management, retries, monitoring, and operational control.

### 2. What is XCom?

A mechanism for passing small metadata between Airflow tasks.

### 3. Why not use XCom for datasets?

XCom is metadata-oriented, not a distributed data-transfer mechanism.

### 4. What is idempotency?

Executing the same logical operation multiple times produces the same intended final state.

### 5. How do you make a DAG idempotent?

Use deterministic partitions, staging tables, MERGE/upsert semantics, controlled overwrite behavior, and unique run/data identifiers.

### 6. Poke vs reschedule sensor?

Poke keeps a worker occupied while waiting. Reschedule releases the worker slot between checks.

### 7. What is dynamic task mapping?

Runtime creation of task instances based on dynamically generated inputs.

### 8. What is catchup?

Automatic creation of missed scheduled DAG runs after the DAG's start date.

### 9. How do retries work?

Airflow reruns failed tasks according to retry configuration. Retry behavior should be used mainly for transient failures.

### 10. What is a TaskGroup?

A logical UI/organizational grouping of related tasks. It does not create a separate DAG.

---

# 44. PySpark Interview Questions

### 1. What is a shuffle?

Redistribution of data between executors, often caused by joins, groupBy, distinct, orderBy, windows, and repartition.

### 2. `repartition()` vs `coalesce()`?

`repartition()` generally causes a shuffle and can increase/decrease partitions.

`coalesce()` generally reduces partitions with less movement.

### 3. What is a broadcast join?

A small dataset is replicated to executors so the large dataset does not require a full shuffle join.

### 4. What is data skew?

Uneven distribution of records across partitions causing some tasks to process significantly more data than others.

### 5. What is lazy evaluation?

Spark transformations build an execution plan. Computation happens when an action requires a result.

### 6. Transformation vs action?

Transformation:

```text
select
filter
join
withColumn
```

Action:

```text
count
collect
write
show
```

### 7. Why avoid `collect()`?

It moves all data to the driver and can cause driver memory exhaustion.

### 8. Why use Parquet?

Columnar format with compression and efficient predicate/column pushdown.

### 9. Why use explicit schema?

Predictability, validation, performance, and schema governance.

### 10. What causes an executor OOM?

Possible causes include:

```text
Large partitions
Huge broadcast
Data skew
Excessive caching
Large aggregations
Poor partitioning
```

---

# 45. End-to-End Production Flow

The complete workflow should look like:

```text
                GCS Raw
                   |
                   v
          Composer Sensor
                   |
                   v
          Runtime Validation
                   |
                   v
       Dataproc Serverless Spark
                   |
        +----------+----------+
        |          |          |
        v          v          v
      Schema    Transform   Dedupe
        |          |          |
        +----------+----------+
                   |
                   v
             GCS Parquet
                   |
                   v
           BigQuery Staging
                   |
                   v
             Data Quality
             /    |    \
            /     |     \
         null   dupes   count
            \     |     /
             \    |    /
                  v
                MERGE
                  |
                  v
          BigQuery Curated
                  |
                  v
       Monitoring / Alerting
```

---

# 46. Recommended Production Enhancements

For a real enterprise implementation, add:

```text
Data Catalog / Dataplex
Data lineage
Great Expectations / equivalent DQ framework
Pub/Sub event-driven ingestion
Cloud Logging structured logs
Cloud Monitoring dashboards
PagerDuty / incident integration
Terraform modules
Artifact Registry
Cloud Build / GitHub Actions
VPC Service Controls where required
CMEK where required
Private networking
Cloud NAT where required
Audit logging
Cost monitoring
Data retention policies
GCS lifecycle rules
BigQuery partition expiration
```

---

# 47. Key Mental Model

Think of the platform as five layers:

```text
1. ORCHESTRATION
   Cloud Composer / Airflow

2. COMPUTE
   Dataproc / Spark

3. STORAGE
   GCS

4. ANALYTICS
   BigQuery

5. OPERATIONS
   IAM + Logging + Monitoring + Alerting
```

Airflow should **orchestrate**.

Spark should **process data**.

GCS should **store files**.

BigQuery should **serve analytical workloads**.

IAM should **control access**.

Monitoring should **tell you when something is wrong**.

This separation makes the platform easier to scale, operate, troubleshoot, and secure.

---

# 48. Final Production Rule

Do not judge a data pipeline only by:

```text
"Does the DAG run successfully?"
```

A production pipeline should answer:

```text
Can it recover from failure?
Can it safely retry?
Can it be backfilled?
Can it process late data?
Can it detect bad data?
Can we trace what happened?
Can we monitor it?
Can we secure it?
Can we control its cost?
Can we deploy it repeatedly?
Can we reproduce a historical result?
Can we operate it at 2 AM?
```

That is the difference between a demo pipeline and a production data platform.

---

## 49. Suggested Next Implementation

Build the following project end-to-end:

```text
Customer Data Platform
|
+-- Cloud Composer
|      |
|      +-- GCS sensor
|      +-- Dynamic task mapping
|      +-- TaskGroups
|      +-- Dataproc Serverless
|      +-- BigQuery DQ
|      +-- MERGE
|      +-- Notifications
|
+-- PySpark
|      |
|      +-- Schema validation
|      +-- Transformations
|      +-- Deduplication
|      +-- Joins
|      +-- Data skew handling
|      +-- Partitioning
|      +-- Performance tuning
|
+-- GCS
|      |
|      +-- Raw
|      +-- Processed
|      +-- Quarantine
|
+-- BigQuery
|      |
|      +-- Staging
|      +-- Curated
|      +-- Partitioning
|      +-- Clustering
|
+-- Terraform
|      |
|      +-- IAM
|      +-- GCS
|      +-- BigQuery
|      +-- Composer
|      +-- Networking
|
+-- CI/CD
       |
       +-- Unit tests
       +-- DAG validation
       +-- Security scan
       +-- Deployment
```

This project covers the majority of the concepts expected from a production-oriented **GCP Data Engineer / Cloud Data Platform Engineer** role.
