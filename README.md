# GHCN-Daily Climate Analysis with PySpark

Reproducible PySpark workflows for exploring the NOAA Global Historical Climatology Network Daily (GHCN-Daily) data described in the accompanying assignment report.

The project builds a station dimension from fixed-width metadata, enriches daily observations, analyses the five core weather elements (`PRCP`, `SNOW`, `SNWD`, `TMAX`, `TMIN`), and creates New Zealand temperature and country-level precipitation outputs.

## Project layout

```text
src/ghcn_pipeline.py     Command-line Spark workflow
config/example.env       Paths and output locations to customise
data/README.md           Input schema and data-access notes
420assignment_*.pdf      Original submitted report
```

## Setup

```bash
python -m venv .venv
. .venv/bin/activate                 # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp config/example.env .env
```

Set the paths in `.env` (or pass the equivalent command-line options). The source data is not included: the GHCN-Daily archive is large and has its own distribution terms.

## Run

```bash
# Build an enriched station table as Parquet
spark-submit src/ghcn_pipeline.py enrich-stations \
  --stations "$GHCN_STATIONS" --countries "$GHCN_COUNTRIES" \
  --states "$GHCN_STATES" --inventory "$GHCN_INVENTORY" \
  --output "$OUTPUT_DIR/enriched_stations"

# Produce New Zealand monthly temperature charts from daily observations
spark-submit src/ghcn_pipeline.py nz-temperature \
  --daily "$GHCN_DAILY_2024" --stations "$OUTPUT_DIR/enriched_stations" \
  --output "$OUTPUT_DIR/nz_temperature"

# Aggregate annual country precipitation and write a CSV for mapping
spark-submit src/ghcn_pipeline.py country-precipitation \
  --daily "$GHCN_DAILY_GLOB" --stations "$OUTPUT_DIR/enriched_stations" \
  --output "$OUTPUT_DIR/country_precipitation"
```

`daily` accepts a CSV/CSV.GZ glob. Values are stored in GHCN's tenths of a unit; the script converts `TMIN`/`TMAX` to degrees C and `PRCP` to millimetres in derived outputs.

## Reproducibility notes

- Metadata files are parsed by their published fixed-width positions rather than inferred as CSV.
- The station enrichment uses left joins so station records remain the primary grain.
- Distance calculations use a Spark SQL Haversine expression, avoiding Python-UDF serialisation overhead.
- The project intentionally does not ship the original 13+ GB data archive or cloud credentials.

The original PDF remains in this repository as the report that motivated the implementation.
