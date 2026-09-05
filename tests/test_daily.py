"""Small real-Spark regression checks; no NOAA archive download required."""
import pytest
from pyspark.sql import SparkSession
from src.ghcn_pipeline import read_daily, write_nz_temperature, write_country_precipitation


def test_quality_flags_units_and_country_aggregation(tmp_path):
    spark = (SparkSession.builder.master("local[2]").appName("ghcn-test")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    try:
        observations = tmp_path / "daily.csv"
        observations.write_text(
            "NZ001,20240101,TMAX,200,,,S,\n"
            "NZ001,20240102,TMAX,100,,,S,\n"
            "NZ001,20240103,TMAX,999,,X,S,\n"
            "NZ001,20240101,PRCP,100,,,S,\n"
            "NZ001,20240102,PRCP,200,,,S,\n"
            "NZ002,20240101,PRCP,500,,,S,\n"
            "NZ002,20240102,PRCP,-9999,,,S,\n"
            "US001,20240101,TMAX,400,,,S,\n"
        )
        daily = read_daily(spark, str(observations))
        assert daily.count() == 7
        stations = spark.createDataFrame(
            [("NZ001", "NZ", "New Zealand"), ("NZ002", "NZ", "New Zealand"),
             ("US001", "US", "United States")],
            ["station_id", "country_code", "country_name"],
        )
        temperature = str(tmp_path / "temperature")
        write_nz_temperature(daily, stations, temperature)
        monthly = spark.read.parquet(f"{temperature}/monthly_parquet").collect()
        assert len(monthly) == 1
        assert monthly[0].mean_temperature_c == 15.0
        rainfall = str(tmp_path / "rainfall")
        write_country_precipitation(daily, stations, rainfall)
        rows = spark.read.option("header", True).csv(rainfall).collect()
        assert len(rows) == 1
        assert rows[0].country_code == "NZ"
        assert float(rows[0].mean_station_precipitation_mm) == 40.0
        assert int(rows[0].station_count) == 2
    finally:
        spark.stop()
