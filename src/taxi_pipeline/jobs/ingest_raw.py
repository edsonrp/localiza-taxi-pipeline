from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxi_pipeline.settings import load_config, project_path
from taxi_pipeline.spark import get_spark
from taxi_pipeline.utils.filesystem import assert_file_exists, ensure_dir


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _green_taxi_input_paths(cfg: dict) -> list[Path]:
    landing_dir = project_path(cfg["paths"]["landing_green_taxi"])
    paths = [landing_dir / f"green_tripdata_{month}.parquet" for month in cfg["sources"]["months"]]
    return [assert_file_exists(path) for path in paths]


def _add_common_metadata(df: DataFrame, dataset_name: str, run_id: str) -> DataFrame:
    return (
        df
        .withColumn("_source_file", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
        .withColumn("_dataset", F.lit(dataset_name))
        .withColumn("_ingestion_run_id", F.lit(run_id))
        .withColumn("_ingested_at_utc", F.current_timestamp())
    )


def _build_audit(df: DataFrame, dataset_name: str, run_id: str) -> DataFrame:
    return (
        df
        .groupBy("_dataset", "_source_file")
        .agg(F.count(F.lit(1)).alias("records_read"))
        .withColumn("layer", F.lit("bronze"))
        .withColumn("ingestion_run_id", F.lit(run_id))
        .withColumn("audited_at_utc", F.current_timestamp())
        .withColumn("source_year_month", F.regexp_extract(F.col("_source_file"), r"green_tripdata_(\d{4}-\d{2})\.parquet", 1))
        .withColumn("source_year_month", F.when(F.col("source_year_month") == "", F.lit(None)).otherwise(F.col("source_year_month")))
        .select(
            F.col("_dataset").alias("dataset"),
            "_source_file",
            "source_year_month",
            "records_read",
            "layer",
            "ingestion_run_id",
            "audited_at_utc",
        )
    )


def ingest_green_taxi_trips(spark: SparkSession, cfg: dict, run_id: str) -> DataFrame:
    input_paths = _green_taxi_input_paths(cfg)
    output_path = ensure_dir(project_path(cfg["paths"]["bronze_green_taxi"]))

    df = spark.read.parquet(*[str(path) for path in input_paths])
    df = _add_common_metadata(df, dataset_name="green_taxi_trips", run_id=run_id)
    df = df.withColumn(
        "_source_year_month",
        F.regexp_extract(F.col("_source_file"), r"green_tripdata_(\d{4}-\d{2})\.parquet", 1),
    )

    (
        df.write
        .mode("overwrite")
        .partitionBy("_source_year_month")
        .parquet(str(output_path))
    )

    return _build_audit(df, dataset_name="green_taxi_trips", run_id=run_id)


def ingest_taxi_zone_lookup(spark: SparkSession, cfg: dict, run_id: str) -> DataFrame:
    input_path = assert_file_exists(
        project_path(cfg["paths"]["landing_taxi_zone_lookup"]) / "taxi_zone_lookup.csv"
    )
    output_path = ensure_dir(project_path(cfg["paths"]["bronze_taxi_zone_lookup"]))

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(input_path))
    )
    df = _add_common_metadata(df, dataset_name="taxi_zone_lookup", run_id=run_id)

    df.write.mode("overwrite").parquet(str(output_path))

    return _build_audit(df, dataset_name="taxi_zone_lookup", run_id=run_id)


def write_ingestion_audit(audit_df: DataFrame, cfg: dict) -> None:
    audit_path = ensure_dir(project_path(cfg["paths"]["audit_ingestion_counts"]))
    audit_df.write.mode("append").parquet(str(audit_path))


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest raw NYC TLC files into bronze layer.")
    parser.add_argument("--config", default="configs/local.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    run_id = _run_id()
    spark = get_spark(cfg["app"]["name"] + "_ingest_raw")

    try:
        green_audit = ingest_green_taxi_trips(spark, cfg, run_id)
        zone_audit = ingest_taxi_zone_lookup(spark, cfg, run_id)
        audit_df = green_audit.unionByName(zone_audit)
        write_ingestion_audit(audit_df, cfg)

        print("\n[INGESTION AUDIT]")
        audit_df.orderBy("dataset", "_source_file").show(truncate=False)
        print("\n[OK] Bronze ingestion completed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
