from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from taxi_pipeline.settings import load_config, project_path
from taxi_pipeline.spark import get_spark
from taxi_pipeline.utils.filesystem import ensure_dir


def _payment_type_description() -> F.Column:
    return (
        F.when(F.col("payment_type") == 1, F.lit("Credit card"))
        .when(F.col("payment_type") == 2, F.lit("Cash"))
        .when(F.col("payment_type") == 3, F.lit("No charge"))
        .when(F.col("payment_type") == 4, F.lit("Dispute"))
        .when(F.col("payment_type") == 5, F.lit("Unknown"))
        .when(F.col("payment_type") == 6, F.lit("Voided trip"))
        .otherwise(F.lit("Unknown"))
    )


def _prepare_zones(zone_df: DataFrame, alias_prefix: str) -> DataFrame:
    return (
        zone_df
        .select(
            F.col("LocationID").alias(f"{alias_prefix}_location_id"),
            F.col("Borough").alias(f"{alias_prefix}_borough"),
            F.col("Zone").alias(f"{alias_prefix}_zone"),
            F.col("service_zone").alias(f"{alias_prefix}_service_zone"),
        )
    )


def build_revenue_by_pickup_zone(
    trips_df: DataFrame,
    zones_df: DataFrame,
    output_path: str,
) -> DataFrame:
    pickup_zones_df = _prepare_zones(zones_df, "pickup")

    result_df = (
        trips_df
        .join(
            pickup_zones_df,
            trips_df["PULocationID"] == pickup_zones_df["pickup_location_id"],
            "left",
        )
        .groupBy(
            F.col("pickup_zone"),
            F.col("pickup_borough"),
        )
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_ticket"),
        )
        .orderBy(F.col("total_revenue").desc())
        .limit(10)
        .select(
            F.col("pickup_zone").alias("zone"),
            F.col("pickup_borough").alias("borough"),
            "trip_count",
            "total_revenue",
            "avg_ticket",
        )
    )

    (
        result_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    return result_df


def build_top_expensive_trips(
    trips_df: DataFrame,
    zones_df: DataFrame,
    output_path: str,
) -> DataFrame:
    pickup_zones_df = _prepare_zones(zones_df, "pickup")
    dropoff_zones_df = _prepare_zones(zones_df, "dropoff")

    enriched_df = (
        trips_df
        .join(
            pickup_zones_df,
            trips_df["PULocationID"] == pickup_zones_df["pickup_location_id"],
            "left",
        )
        .join(
            dropoff_zones_df,
            trips_df["DOLocationID"] == dropoff_zones_df["dropoff_location_id"],
            "left",
        )
        .withColumn("trip_date", F.to_date("lpep_pickup_datetime"))
        .withColumn("pickup_time", F.date_format("lpep_pickup_datetime", "HH:mm:ss"))
        .withColumn("payment_type_description", _payment_type_description())
    )

    window_by_day = (
        Window
        .partitionBy("trip_date")
        .orderBy(F.col("total_amount").desc(), F.col("lpep_pickup_datetime").asc())
    )

    most_expensive_by_day_df = (
        enriched_df
        .withColumn("rn", F.row_number().over(window_by_day))
        .filter(F.col("rn") == 1)
    )

    result_df = (
        most_expensive_by_day_df
        .orderBy(F.col("total_amount").desc())
        .limit(5)
        .select(
            "trip_date",
            "pickup_time",
            F.col("pickup_zone"),
            F.col("dropoff_zone"),
            F.round(F.col("trip_distance"), 2).alias("trip_distance"),
            F.round(F.col("total_amount"), 2).alias("total_amount"),
            F.col("payment_type_description").alias("payment_type"),
        )
    )

    (
        result_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    return result_df


def build_avg_tip_by_dropoff_borough(
    trips_df: DataFrame,
    zones_df: DataFrame,
    output_path: str,
) -> DataFrame:
    dropoff_zones_df = _prepare_zones(zones_df, "dropoff")

    credit_card_payment_type = 1

    result_df = (
        trips_df
        .filter(F.col("payment_type") == credit_card_payment_type)
        .filter(F.col("fare_amount") > 0)
        .join(
            dropoff_zones_df,
            trips_df["DOLocationID"] == dropoff_zones_df["dropoff_location_id"],
            "left",
        )
        .withColumn("tip_rate", F.col("tip_amount") / F.col("fare_amount"))
        .groupBy(F.col("dropoff_borough"))
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip_amount"),
            F.round(F.avg("tip_rate") * 100, 2).alias("avg_tip_rate_pct"),
        )
        .orderBy(F.col("avg_tip_rate_pct").desc())
        .select(
            F.col("dropoff_borough").alias("borough"),
            "trip_count",
            "avg_tip_amount",
            "avg_tip_rate_pct",
        )
    )

    (
        result_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    return result_df


def build_gold_layer(spark: SparkSession, cfg: dict) -> None:
    silver_trips_path = project_path(cfg["paths"]["silver_green_taxi_valid"])
    bronze_zones_path = project_path(cfg["paths"]["bronze_taxi_zone_lookup"])

    revenue_output_path = ensure_dir(project_path(cfg["paths"]["gold_revenue_by_pickup_zone"]))
    expensive_trips_output_path = ensure_dir(project_path(cfg["paths"]["gold_top_expensive_trips"]))
    avg_tip_output_path = ensure_dir(project_path(cfg["paths"]["gold_avg_tip_by_dropoff_borough"]))

    trips_df = spark.read.parquet(str(silver_trips_path))
    zones_df = spark.read.parquet(str(bronze_zones_path))

    revenue_df = build_revenue_by_pickup_zone(
        trips_df=trips_df,
        zones_df=zones_df,
        output_path=str(revenue_output_path),
    )

    expensive_trips_df = build_top_expensive_trips(
        trips_df=trips_df,
        zones_df=zones_df,
        output_path=str(expensive_trips_output_path),
    )

    avg_tip_df = build_avg_tip_by_dropoff_borough(
        trips_df=trips_df,
        zones_df=zones_df,
        output_path=str(avg_tip_output_path),
    )

    print("\n[GOLD] Top 10 revenue by pickup zone")
    revenue_df.show(10, truncate=False)

    print("\n[GOLD] Top 5 most expensive trips")
    expensive_trips_df.show(5, truncate=False)

    print("\n[GOLD] Average tip rate by dropoff borough")
    avg_tip_df.show(50, truncate=False)

    print("\n[OK] Gold layer completed.")
    print(f"Revenue by pickup zone written to: {revenue_output_path}")
    print(f"Top expensive trips written to: {expensive_trips_output_path}")
    print(f"Average tip by dropoff borough written to: {avg_tip_output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build gold analytical outputs.")
    parser.add_argument("--config", default="configs/local.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    spark = get_spark(cfg["app"]["name"] + "_build_gold")

    try:
        build_gold_layer(spark, cfg)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
