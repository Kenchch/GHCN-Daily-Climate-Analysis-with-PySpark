#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p output/evidence
run() {
  "${SPARK_SUBMIT:-spark-submit}" --master 'local[2]' --driver-memory 3g \
    --conf spark.sql.shuffle.partitions=8 src/ghcn_pipeline.py "$@"
}
run enrich-stations --stations data/raw/ghcnd-stations.txt \
  --countries data/raw/ghcnd-countries.txt --states data/raw/ghcnd-states.txt \
  --inventory data/raw/ghcnd-inventory.txt --output output/evidence/stations \
  > output/evidence/enrich-stations.log 2>&1
run nz-temperature --daily data/raw/2024.csv.gz --stations output/evidence/stations \
  --output output/evidence/nz-temperature > output/evidence/nz-temperature.log 2>&1
run country-precipitation --daily data/raw/2024.csv.gz --stations output/evidence/stations \
  --output output/evidence/country-precipitation > output/evidence/country-precipitation.log 2>&1
