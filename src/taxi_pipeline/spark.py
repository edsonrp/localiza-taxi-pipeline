from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "localiza_taxi_pipeline") -> SparkSession:
    """Create a local SparkSession suitable for the technical challenge."""
    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
