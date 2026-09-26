from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    row_number,
)
from pyspark.sql.window import Window


def main():

    spark = (
        SparkSession.builder
        .appName("customer-transformation")
        .getOrCreate()
    )

    input_path = "gs://my-prod-data/raw/customers/"
    output_path = "gs://my-prod-data/processed/customers/"

    try:

        df = (
            spark.read
            .option("header", "true")
            .json(input_path)
        )

        # Schema/data validation
        required_columns = {
            "customer_id",
            "name",
            "email",
        }

        missing_columns = (
            required_columns -
            set(df.columns)
        )

        if missing_columns:
            raise ValueError(
                f"Missing columns: {missing_columns}"
            )

        # Remove invalid records
        df = df.filter(
            col("customer_id").isNotNull()
        )

        # Deduplicate
        window = Window.partitionBy(
            "customer_id"
        ).orderBy(
            col("updated_at").desc()
        )

        df = (
            df.withColumn(
                "row_number",
                row_number().over(window)
            )
            .filter(
                col("row_number") == 1
            )
            .drop("row_number")
        )

        # Audit column
        df = df.withColumn(
            "processed_at",
            current_timestamp()
        )

        # Write
        (
            df.write
            .mode("overwrite")
            .partitionBy("country")
            .parquet(output_path)
        )

    except Exception:
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
