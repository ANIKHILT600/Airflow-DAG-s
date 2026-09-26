"""
PySpark transformation for customer events.

Input:
    GCS raw JSON/CSV/etc.

Output:
    GCS refined Parquet

Responsibilities:
    - Read raw data
    - Validate required columns
    - Clean data
    - Deduplicate
    - Add derived columns
    - Write partitioned Parquet
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_date,
)


def parse_args():
    """Parse arguments passed from the Airflow Dataproc task."""

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
        "--execution-date",
        required=True,
    )

    return parser.parse_args()


def validate_schema(df):
    """
    Validate the minimum schema required by the pipeline.

    Failing early here is preferable to producing bad refined data.
    """

    required_columns = {
        "event_id",
        "customer_id",
        "event_type",
        "event_timestamp",
    }

    actual_columns = set(df.columns)

    missing_columns = required_columns - actual_columns

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )


def main():

    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("customer-events-refinement")
        .getOrCreate()
    )

    try:

        # ================================================================
        # 1. READ RAW DATA
        # ================================================================

        raw_df = (
            spark.read
            .option("header", "true")
            .json(args.input)
        )

        # ================================================================
        # 2. VALIDATE SCHEMA
        # ================================================================

        validate_schema(raw_df)

        # ================================================================
        # 3. CLEAN DATA
        # ================================================================

        cleaned_df = (
            raw_df
            .filter(col("event_id").isNotNull())
            .filter(col("customer_id").isNotNull())
            .filter(col("event_timestamp").isNotNull())
        )

        # ================================================================
        # 4. DEDUPLICATION
        # ================================================================
        #
        # event_id is assumed to uniquely identify an event.
        #
        # Deduplication is important because upstream systems may resend
        # the same event.
        # ================================================================

        deduplicated_df = cleaned_df.dropDuplicates(
            ["event_id"]
        )

        # ================================================================
        # 5. DERIVED COLUMNS
        # ================================================================

        refined_df = (
            deduplicated_df

            # Convert timestamp into a proper Spark timestamp.
            .withColumn(
                "event_timestamp",
                col("event_timestamp").cast("timestamp"),
            )

            # Create a partitioning column.
            .withColumn(
                "event_date",
                to_date(col("event_timestamp")),
            )

            # Add processing timestamp for lineage.
            .withColumn(
                "processed_at",
                current_timestamp(),
            )
        )

        # ================================================================
        # 6. WRITE REFINED DATA
        # ================================================================
        #
        # Parquet is generally preferable to CSV/JSON for the refined
        # analytical layer because it is columnar and efficient.
        #
        # Partitioning by event_date makes downstream BigQuery/data-lake
        # operations more efficient.
        # ================================================================

        (
            refined_df
            .repartition("event_date")
            .write
            .mode("overwrite")
            .partitionBy("event_date")
            .parquet(args.output)
        )

    finally:

        # Always stop Spark cleanly.
        spark.stop()


if __name__ == "__main__":
    main()
