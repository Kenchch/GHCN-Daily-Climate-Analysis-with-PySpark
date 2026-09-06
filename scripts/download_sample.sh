#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data/raw
fetch() {
  curl --fail --location --retry 3 "$1" --output "$2.part"
  mv -- "$2.part" "$2"
}
fetch https://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/by_year/2024.csv.gz data/raw/2024.csv.gz
for name in stations countries states inventory; do
  fetch "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-${name}.txt" "data/raw/ghcnd-${name}.txt"
done
sha256sum data/raw/2024.csv.gz data/raw/ghcnd-*.txt > data/raw/SHA256SUMS
