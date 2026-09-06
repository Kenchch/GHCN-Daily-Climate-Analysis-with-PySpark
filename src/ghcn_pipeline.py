"""Command-line GHCN-Daily analysis workflows implemented with PySpark."""

from __future__ import annotations

import argparse
import json
import time
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

COUNTRY_FIELDS = [("country_code", 0, 2), ("country_name", 3, 47)]
STATE_FIELDS = [("state_code", 0, 2), ("state_name", 3, 47)]
STATION_FIELDS = [
    ("station_id", 0, 11),
    ("latitude", 12, 8),
    ("longitude", 21, 9),
    ("elevation_m", 31, 6),
    ("state_code", 38, 2),
    ("station_name", 41, 30),
    ("gsn_flag", 72, 3),
    ("hcn_crn_flag", 76, 3),
    ("wmo_id", 80, 5),
]
INVENTORY_FIELDS = [
    ("station_id", 0, 11),
    ("latitude", 12, 8),
    ("longitude", 21, 9),
    ("element", 31, 4),
    ("first_year", 36, 4),
    ("last_year", 41, 4),
]

CORE_ELEMENTS = ("PRCP", "SNOW", "SNWD", "TMAX", "TMIN")
DAILY_SCHEMA = T.StructType(
    [
        T.StructField("station_id", T.StringType(), False),
        T.StructField("date_raw", T.StringType(), True),
        T.StructField("element", T.StringType(), True),
        T.StructField("value", T.IntegerType(), True),
        T.StructField("measurement_flag", T.StringType(), True),
        T.StructField("quality_flag", T.StringType(), True),
        T.StructField("source_flag", T.StringType(), True),
        T.StructField("observation_time", T.StringType(), True),
    ]
)


def spark_session() -> SparkSession:
    return SparkSession.builder.appName("ghcn-daily-analysis").getOrCreate()


def fixed_width(
    path: str, fields: Iterable[tuple[str, int, int]], spark: SparkSession
) -> DataFrame:
    """Read a fixed-width text file using 0-indexed (start, length) field definitions."""
    frame = spark.read.text(path)
    return frame.select(
        *[
            F.trim(F.substring("value", start + 1, length)).alias(name)
            for name, start, length in fields
        ]
    )


def read_metadata(
    spark: SparkSession, stations: str, countries: str, states: str, inventory: str
):
    station_df = fixed_width(
        stations,
        STATION_FIELDS,
        spark,
    ).select(
        "station_id",
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.when(
            F.col("elevation_m").cast("double") != -999.9,
            F.col("elevation_m").cast("double"),
        ).alias("elevation_m"),
        "state_code",
        "station_name",
        "gsn_flag",
        "hcn_crn_flag",
        "wmo_id",
    )
    country_df = fixed_width(countries, COUNTRY_FIELDS, spark)
    state_df = fixed_width(states, STATE_FIELDS, spark)
    inventory_df = fixed_width(
        inventory,
        INVENTORY_FIELDS,
        spark,
    ).select(
        "station_id",
        "element",
        F.col("first_year").cast("int"),
        F.col("last_year").cast("int"),
    )
    return station_df, country_df, state_df, inventory_df


def enrich_stations(
    station_df: DataFrame,
    country_df: DataFrame,
    state_df: DataFrame,
    inventory_df: DataFrame,
) -> DataFrame:
    inventory_summary = (
        inventory_df.groupBy("station_id")
        .agg(
            F.min("first_year").alias("station_first_year"),
            F.max("last_year").alias("station_last_year"),
            F.sort_array(F.collect_set("element")).alias("observed_elements"),
        )
        .withColumn("element_count", F.size("observed_elements"))
        .withColumn(
            "core_element_count",
            F.size(
                F.array_intersect(
                    "observed_elements", F.array(*[F.lit(x) for x in CORE_ELEMENTS])
                )
            ),
        )
    )
    return (
        station_df.withColumn("country_code", F.substring("station_id", 1, 2))
        .join(country_df, "country_code", "left")
        .join(state_df, "state_code", "left")
        .join(inventory_summary, "station_id", "left")
    )


def classify_daily(spark: SparkSession, path: str) -> DataFrame:
    """Parse observations and retain an explicit first rejection reason."""
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    frame = (
        spark.read.schema(DAILY_SCHEMA)
        .option("header", "false")
        .csv(path)
        .withColumn(
            "date", F.to_date(F.try_to_timestamp("date_raw", F.lit("yyyyMMdd")))
        )
    )
    return frame.withColumn(
        "reject_reason",
        F.when(F.col("date").isNull(), "invalid_date")
        .when(
            F.col("station_id").isNull() | (F.trim("station_id") == ""),
            "missing_station",
        )
        .when(F.col("value").isNull() | (F.col("value") == -9999), "missing_value")
        .when(
            F.col("quality_flag").isNotNull() & (F.trim("quality_flag") != ""),
            "quality_flag",
        ),
    )


def read_daily(spark: SparkSession, path: str) -> DataFrame:
    return (
        classify_daily(spark, path)
        .filter("reject_reason IS NULL")
        .drop("date_raw", "reject_reason")
    )


def haversine_km(left_lat: str, left_lon: str, right_lat: str, right_lon: str):
    radius_km = F.lit(6371.0088)
    d_lat = F.radians(F.col(right_lat) - F.col(left_lat))
    d_lon = F.radians(F.col(right_lon) - F.col(left_lon))
    a = F.pow(F.sin(d_lat / 2), 2) + F.cos(F.radians(F.col(left_lat))) * F.cos(
        F.radians(F.col(right_lat))
    ) * F.pow(F.sin(d_lon / 2), 2)
    return 2 * radius_km * F.asin(F.sqrt(a))


def write_nz_temperature(daily: DataFrame, stations: DataFrame, output: str) -> None:
    nz = daily.join(
        F.broadcast(
            stations.filter(F.col("country_code") == "NZ").select("station_id")
        ),
        "station_id",
    )
    monthly = (
        nz.filter(F.col("element").isin("TMIN", "TMAX"))
        .withColumn("temperature_c", F.col("value") / F.lit(10.0))
        .groupBy(
            "station_id",
            F.to_date(F.date_trunc("month", "date")).alias("month"),
            "element",
        )
        .agg(F.avg("temperature_c").alias("mean_temperature_c"))
    )
    monthly.write.mode("overwrite").parquet(f"{output}/monthly_parquet")
    monthly = daily.sparkSession.read.parquet(f"{output}/monthly_parquet")
    monthly.groupBy("month", "element").agg(
        F.avg("mean_temperature_c").alias("national_mean_temperature_c")
    ).orderBy("month").coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output}/national_monthly_csv"
    )


def write_country_precipitation(
    daily: DataFrame, stations: DataFrame, output: str
) -> None:
    yearly = (
        daily.filter((F.col("element") == "PRCP") & (F.col("value") >= 0))
        .join(
            F.broadcast(stations.select("station_id", "country_code", "country_name")),
            "station_id",
        )
        .withColumn("year", F.year("date"))
        .groupBy("country_code", "country_name", "year", "station_id")
        .agg(F.sum(F.col("value") / F.lit(10.0)).alias("station_precipitation_mm"))
        .groupBy("country_code", "country_name", "year")
        .agg(
            F.avg("station_precipitation_mm").alias("mean_station_precipitation_mm"),
            F.countDistinct("station_id").alias("station_count"),
        )
    )
    yearly.coalesce(1).write.mode("overwrite").option("header", True).csv(output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    enrich = sub.add_parser("enrich-stations")
    for name in ("stations", "countries", "states", "inventory", "output"):
        enrich.add_argument(f"--{name}", required=True)
    for command in ("nz-temperature", "country-precipitation"):
        job = sub.add_parser(command)
        job.add_argument("--daily", required=True)
        job.add_argument("--stations", required=True)
        job.add_argument("--output", required=True)
    nearest = sub.add_parser("nearest-stations")
    nearest.add_argument("--stations", required=True)
    nearest.add_argument("--latitude", type=float, required=True)
    nearest.add_argument("--longitude", type=float, required=True)
    nearest.add_argument("--limit", type=int, default=10)
    nearest.add_argument("--output", required=True)
    args = parser.parse_args()
    started = time.perf_counter()
    spark = spark_session()
    metrics = {
        "command": args.command,
        "spark_version": spark.version,
        "master": spark.sparkContext.master,
    }
    try:
        if args.command == "enrich-stations":
            tables = read_metadata(
                spark, args.stations, args.countries, args.states, args.inventory
            )
            metrics["input_rows"] = {
                name: frame.count()
                for name, frame in zip(
                    ["stations", "countries", "states", "inventory"], tables
                )
            }
            enrich_stations(*tables).write.mode("overwrite").parquet(args.output)
            metrics["output_rows"] = spark.read.parquet(args.output).count()
        elif args.command == "nearest-stations":
            if not (
                -90 <= args.latitude <= 90
                and -180 <= args.longitude <= 180
                and args.limit > 0
            ):
                raise ValueError("Invalid coordinates or limit")
            stations = (
                spark.read.parquet(args.stations)
                .withColumn("query_lat", F.lit(args.latitude))
                .withColumn("query_lon", F.lit(args.longitude))
            )
            nearest_rows = (
                stations.withColumn(
                    "distance_km",
                    haversine_km("latitude", "longitude", "query_lat", "query_lon"),
                )
                .filter("distance_km IS NOT NULL")
                .orderBy("distance_km", "station_id")
                .limit(args.limit)
                .drop("query_lat", "query_lon", "observed_elements")
            )
            nearest_rows.coalesce(1).write.mode("overwrite").option("header", True).csv(
                args.output
            )
            metrics["output_rows"] = (
                spark.read.option("header", True).csv(args.output).count()
            )
        else:
            classified = classify_daily(spark, args.daily)
            counts = {
                row.reject_reason or "accepted": row["count"]
                for row in classified.groupBy("reject_reason").count().collect()
            }
            metrics["input_rows"] = sum(counts.values())
            metrics["acceptance_counts"] = counts
            daily = classified.filter("reject_reason IS NULL").drop(
                "date_raw", "reject_reason"
            )
            stations = spark.read.parquet(args.stations)
            if args.command == "nz-temperature":
                write_nz_temperature(daily, stations, args.output)
                metrics["output_rows"] = {
                    "monthly": spark.read.parquet(
                        f"{args.output}/monthly_parquet"
                    ).count(),
                    "national_monthly": spark.read.option("header", True)
                    .csv(f"{args.output}/national_monthly_csv")
                    .count(),
                }
            else:
                write_country_precipitation(daily, stations, args.output)
                metrics["output_rows"] = (
                    spark.read.option("header", True).csv(args.output).count()
                )
        metrics["elapsed_seconds"] = round(time.perf_counter() - started, 3)
        payload = json.dumps(metrics, indent=2)
        output_path = spark._jvm.org.apache.hadoop.fs.Path(
            f"{args.output}/_metrics.json"
        )
        filesystem = output_path.getFileSystem(spark._jsc.hadoopConfiguration())
        stream = filesystem.create(output_path, True)
        try:
            stream.write(bytearray(payload.encode("utf-8")))
        finally:
            stream.close()
        print(payload)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
