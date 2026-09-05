"""Shared fixtures: one local Spark session for the whole test session.

Starting a JVM costs a few seconds, so the session is created once and reused.
Tests must therefore never call ``stop()`` on it themselves — a stopped session
would be handed to every test that ran afterwards.
"""

from __future__ import annotations

import pathlib
import sys

import pytest

# Allow ``import src.ghcn_pipeline`` regardless of how pytest was invoked
# (``pytest`` does not put the repository root on sys.path; ``python -m pytest``
# does, and relying on the difference makes the suite invocation-dependent).
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("ghcn-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
