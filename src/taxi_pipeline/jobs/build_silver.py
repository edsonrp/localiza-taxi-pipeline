from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxi_pipeline.settings import load_config, project_path
from taxi_pipeline.spark import get_spark
from taxi_pipeline.utils.filesystem import ensure_dir


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _add_record_hash(df: DataFrame) -> DataFrame:
    """
    Creates a technical hash to support traceability and deduplication analysis.
    Metadata columns are ignored in the hash.
    """
    business_columns = [column for column in df.columns if not column.startswith("_")]

    hash_expr = F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(column).cast("string"), F.lit("__NULL__"))
                for column in business_columns
            ],
        ),
        256,
    )

    return df.withColumn("_record_hash", hash_expr)


def _apply_quality_rules(df: DataFrame) -> DataFrame:
    """
    Applies minimum data quality rules required for the challenge.

    A record can have more than one error.
    The column _dq_errors stores all detected errors.
    """
    df = _add_record_hash(df)

    error_rules = [
        F.when(
            F.col("lpep_pickup_datetime").isNull(),
            F.lit("NULL_PICKUP_DATETIME"),
        ),
        F.when(
            F.col("lpep_dropoff_datetime").isNull(),
            F.lit("NULL_DROPOFF_DATETIME"),
        ),
        F.when(
            F.col("lpep_pickup_datetime") >= F.col("lpep_dropoff_datetime"),
            F.lit("INVALID_TRIP_DURATION"),
        ),
        F.when(
            F.col("trip_distance").isNull(),
            F.lit("NULL_TRIP_DISTANCE"),
        ),
        F.when(
            F.col("trip_distance") < 0,
            F.lit("NEGATIVE_TRIP_DISTANCE"),
        ),
        F.when(
            F.col("total_amount").isNull(),
            F.lit("NULL_TOTAL_AMOUNT"),
        ),
        F.when(
            F.col("total_amount") <= 0,
            F.lit("INVALID_TOTAL_AMOUNT"),
        ),
        F.when(
            F.col("PULocationID").isNull(),
            F.lit("NULL_PICKUP_LOCATION_ID"),
        ),
        F.when(
            F.col("DOLocationID").isNull(),
            F.lit("NULL_DROPOFF_LOCATION_ID"),
        ),
    ]

    return (
        df
        .withColumn("_dq_errors", F.array(*error_rules))
        .withColumn("_dq_errors", F.expr("filter(_dq_errors, x -> x is not null)"))
    )


def _deduplicate_valid_records(df: DataFrame) -> DataFrame:
    """
    Deduplicates valid records using a natural business key.

    Green Taxi data does not provide a unique trip id.
    Therefore, the deduplication key uses the main trip attributes.
    """
    dedup_key = [
        "VendorID",
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
        "total_amount",
        "payment_type",
    ]

    existing_columns = [column for column in dedup_key if column in df.columns]

    return df.dropDuplicates(existing_columns)


def _build_quality_audit(
    spark: SparkSession,
    df_with_rules: DataFrame,
    valid_df: DataFrame,
    invalid_base_df: DataFrame,
    invalid_by_error_df: DataFrame,
    quality_run_id: str,
) -> DataFrame:
    total_records = df_with_rules.count()
    valid_candidates = df_with_rules.filter(F.size(F.col("_dq_errors")) == 0).count()
    valid_records = valid_df.count()
    invalid_records = invalid_base_df.count()
    duplicates_removed = valid_candidates - valid_records

    summary_rows = [
        ("green_taxi_trips", "silver", "TOTAL_RECORDS", None, total_records, quality_run_id),
        ("green_taxi_trips", "silver", "VALID_RECORDS", None, valid_records, quality_run_id),
        ("green_taxi_trips", "silver", "INVALID_RECORDS", None, invalid_records, quality_run_id),
        ("green_taxi_trips", "silver", "DUPLICATES_REMOVED", None, duplicates_removed, quality_run_id),
    ]

    summary_df = spark.createDataFrame(
        summary_rows,
        ["dataset", "layer", "metric_name", "error_type", "metric_value", "quality_run_id"],
    )

    error_counts_df = (
        invalid_by_error_df
        .groupBy("error_type")
        .agg(F.count(F.lit(1)).alias("metric_value"))
        .withColumn("dataset", F.lit("green_taxi_trips"))
        .withColumn("layer", F.lit("silver"))
        .withColumn("metric_name", F.lit("ERROR_RECORDS_BY_TYPE"))
        .withColumn("quality_run_id", F.lit(quality_run_id))
        .select(
            "dataset",
            "layer",
            "metric_name",
            "error_type",
            "metric_value",
            "quality_run_id",
        )
    )

    return (
        summary_df
        .unionByName(error_counts_df)
        .withColumn("audited_at_utc", F.current_timestamp())
    )


def build_silver_layer(spark: SparkSession, cfg: dict, quality_run_id: str) -> None:
    bronze_path = project_path(cfg["paths"]["bronze_green_taxi"])
    valid_output_path = ensure_dir(project_path(cfg["paths"]["silver_green_taxi_valid"]))
    invalid_output_path = ensure_dir(project_path(cfg["paths"]["silver_green_taxi_invalid"]))

    audit_quality_path = ensure_dir(
        project_path("data/audit/quality_counts")
    )

    bronze_df = spark.read.parquet(str(bronze_path))

    df_with_rules = _apply_quality_rules(bronze_df)

    valid_candidates_df = (
        df_with_rules
        .filter(F.size(F.col("_dq_errors")) == 0)
    )

    valid_df = (
        _deduplicate_valid_records(valid_candidates_df)
        .withColumn("_quality_run_id", F.lit(quality_run_id))
        .withColumn("_processed_at_utc", F.current_timestamp())
    )

    invalid_base_df = (
        df_with_rules
        .filter(F.size(F.col("_dq_errors")) > 0)
        .withColumn("_quality_run_id", F.lit(quality_run_id))
        .withColumn("_processed_at_utc", F.current_timestamp())
    )

    invalid_by_error_df = (
        invalid_base_df
        .withColumn("error_type", F.explode(F.col("_dq_errors")))
    )

    (
        valid_df
        .write
        .mode("overwrite")
        .partitionBy("_source_year_month")
        .parquet(str(valid_output_path))
    )

    (
        invalid_by_error_df
        .write
        .mode("overwrite")
        .partitionBy("error_type")
        .parquet(str(invalid_output_path))
    )

    audit_df = _build_quality_audit(
        spark=spark,
        df_with_rules=df_with_rules,
        valid_df=valid_df,
        invalid_base_df=invalid_base_df,
        invalid_by_error_df=invalid_by_error_df,
        quality_run_id=quality_run_id,
    )

    (
        audit_df
        .write
        .mode("append")
        .parquet(str(audit_quality_path))
    )

    print("\n[QUALITY AUDIT]")
    audit_df.orderBy("metric_name", "error_type").show(truncate=False)

    print("\n[OK] Silver layer completed.")
    print(f"Valid records written to: {valid_output_path}")
    print(f"Invalid records written to: {invalid_output_path}")
    print(f"Quality audit written to: {audit_quality_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build silver layer with data quality rules.")
    parser.add_argument("--config", default="configs/local.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    quality_run_id = _run_id()

    spark = get_spark(cfg["app"]["name"] + "_build_silver")

    try:
        build_silver_layer(spark, cfg, quality_run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
