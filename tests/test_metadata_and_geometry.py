"""Tests for metadata parsing, station enrichment and the Haversine expression.

`test_daily.py` covers the observation workflows end to end. This file covers
the parts upstream of them: the fixed-width reader that builds the station
dimension, the joins that enrich it, and the distance expression. All of it
runs on synthetic records against a local Spark session, so no part of the
13+ GB archive is needed.
"""

from __future__ import annotations

import math

import pytest

from src.ghcn_pipeline import CORE_ELEMENTS, enrich_stations, fixed_width, haversine_km

# Field layout published in GHCN-Daily's readme, as (name, 0-indexed start, length).
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


def place(fields, values):
    """Build a fixed-width line by writing each value at its published offset."""
    line = [" "] * max(start + length for _, start, length in fields)
    for name, start, length in fields:
        value = values.get(name, "")
        assert len(value) <= length, f"{name} does not fit in {length} columns"
        line[start : start + len(value)] = value
    return "".join(line)


def write_lines(tmp_path, name, lines):
    path = tmp_path / name
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


class TestFixedWidth:
    """The field table is 0-indexed and Spark's `substring` is 1-indexed. An
    off-by-one in that conversion shifts every field by one character and raises
    nothing — it just quietly corrupts the station dimension."""

    def test_fields_are_read_from_their_published_offsets(self, spark, tmp_path):
        values = {
            "station_id": "NZM00093781",
            "latitude": "-43.4890",
            "longitude": "172.5320",
            "elevation_m": "  37.5",
            "station_name": "CHRISTCHURCH INTL",
            "gsn_flag": "GSN",
            "wmo_id": "93781",
        }
        path = write_lines(tmp_path, "stations.txt", [place(STATION_FIELDS, values)])

        row = fixed_width(path, STATION_FIELDS, spark).collect()[0]

        assert row["station_id"] == "NZM00093781"
        assert row["latitude"] == "-43.4890"
        assert row["longitude"] == "172.5320"
        assert row["elevation_m"] == "37.5"
        assert row["station_name"] == "CHRISTCHURCH INTL"
        assert row["gsn_flag"] == "GSN"
        assert row["wmo_id"] == "93781"

    def test_absent_optional_fields_come_back_empty_not_shifted(self, spark, tmp_path):
        """State and HCN/CRN are blank for most non-US stations. A shifted read
        would pull neighbouring characters into them instead of an empty string."""
        values = {
            "station_id": "NZM00093781",
            "latitude": "-43.4890",
            "longitude": "172.5320",
            "station_name": "CHRISTCHURCH INTL",
            "wmo_id": "93781",
        }
        path = write_lines(tmp_path, "stations.txt", [place(STATION_FIELDS, values)])

        row = fixed_width(path, STATION_FIELDS, spark).collect()[0]

        assert row["state_code"] == ""
        assert row["hcn_crn_flag"] == ""
        assert row["gsn_flag"] == ""


class TestEnrichStations:
    @pytest.fixture
    def tables(self, spark):
        stations = spark.createDataFrame(
            [
                ("NZM00093781", -43.4890, 172.5320, 37.5, "", "CHRISTCHURCH INTL"),
                ("USW00094728", 40.7789, -73.9692, 39.6, "NY", "NEW YORK CNTRL PK"),
                ("ZZX00000001", 0.0, 0.0, 0.0, "", "ORPHAN STATION"),
            ],
            "station_id string, latitude double, longitude double, "
            "elevation_m double, state_code string, station_name string",
        )
        countries = spark.createDataFrame(
            [("NZ", "New Zealand"), ("US", "United States")],
            "country_code string, country_name string",
        )
        states = spark.createDataFrame([("NY", "New York")], "state_code string, state_name string")
        inventory = spark.createDataFrame(
            [
                ("NZM00093781", "TMAX", 1950, 2024),
                ("NZM00093781", "TMIN", 1955, 2020),
                ("NZM00093781", "PRCP", 1943, 2024),
                ("NZM00093781", "ACMH", 1980, 1990),  # not a core element
                ("USW00094728", "PRCP", 1869, 2024),
            ],
            "station_id string, element string, first_year int, last_year int",
        )
        return stations, countries, states, inventory

    def test_station_stays_the_grain_with_no_country_or_inventory_match(self, tables):
        stations, _, _, _ = tables

        result = enrich_stations(*tables)

        assert result.count() == stations.count() == 3
        orphan = result.filter("station_id = 'ZZX00000001'").collect()
        assert len(orphan) == 1, "left joins must keep a station with no country or inventory"
        assert orphan[0]["country_name"] is None
        assert orphan[0]["station_first_year"] is None

    def test_country_code_is_derived_from_the_station_id_prefix(self, tables):
        rows = {r["station_id"]: r for r in enrich_stations(*tables).collect()}

        assert rows["NZM00093781"]["country_code"] == "NZ"
        assert rows["NZM00093781"]["country_name"] == "New Zealand"
        assert rows["USW00094728"]["country_name"] == "United States"

    def test_state_joins_only_where_a_state_code_exists(self, tables):
        rows = {r["station_id"]: r for r in enrich_stations(*tables).collect()}

        assert rows["USW00094728"]["state_name"] == "New York"
        assert rows["NZM00093781"]["state_name"] is None

    def test_core_element_count_ignores_non_core_elements(self, tables):
        chch = enrich_stations(*tables).filter("station_id = 'NZM00093781'").collect()[0]

        assert chch["element_count"] == 4, "TMAX, TMIN, PRCP and ACMH were all observed"
        assert chch["core_element_count"] == 3, "ACMH is not one of the five core elements"
        assert set(chch["observed_elements"]) - set(CORE_ELEMENTS) == {"ACMH"}

    def test_inventory_years_span_all_elements_at_a_station(self, tables):
        chch = enrich_stations(*tables).filter("station_id = 'NZM00093781'").collect()[0]

        assert chch["station_first_year"] == 1943, "earliest first_year across elements"
        assert chch["station_last_year"] == 2024, "latest last_year across elements"


class TestHaversine:
    def _distance(self, spark, lat1, lon1, lat2, lon2):
        frame = spark.createDataFrame(
            [(lat1, lon1, lat2, lon2)],
            "lat_a double, lon_a double, lat_b double, lon_b double",
        )
        return frame.select(
            haversine_km("lat_a", "lon_a", "lat_b", "lon_b").alias("km")
        ).collect()[0]["km"]

    def test_one_degree_of_latitude_is_one_radian_of_arc(self, spark):
        """Along a meridian the Haversine reduces to R * dlat, which is checkable
        by hand: 6371.0088 km * (pi / 180) = 111.195 km."""
        expected = 6371.0088 * math.radians(1.0)

        assert self._distance(spark, 0.0, 0.0, 1.0, 0.0) == pytest.approx(expected, rel=1e-9)

    def test_identical_points_are_zero_not_nan(self, spark):
        """Floating-point error inside sqrt/asin is the usual source of a NaN here."""
        assert self._distance(spark, -43.4890, 172.5320, -43.4890, 172.5320) == pytest.approx(
            0.0, abs=1e-6
        )

    def test_distance_is_symmetric(self, spark):
        there = self._distance(spark, -43.4890, 172.5320, -37.0082, 174.7850)
        back = self._distance(spark, -37.0082, 174.7850, -43.4890, 172.5320)

        assert there == pytest.approx(back, rel=1e-12)

    def test_a_known_domestic_leg_is_the_right_magnitude(self, spark):
        """Christchurch to Auckland is roughly 745 km great-circle. A degrees /
        radians mix-up or a wrong Earth radius misses this by orders of magnitude."""
        km = self._distance(spark, -43.4890, 172.5320, -37.0082, 174.7850)

        assert km == pytest.approx(745.0, abs=10.0)
