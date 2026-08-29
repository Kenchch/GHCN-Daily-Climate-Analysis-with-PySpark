# Data inputs

Download GHCN-Daily metadata and daily observation files from NOAA/NCEI, then point the commands in the top-level README to those locations. Do not commit the full archive or access credentials.

| File | Format | Key fields used |
| --- | --- | --- |
| `ghcnd-stations.txt` | fixed width | station ID, latitude, longitude, elevation, state, name, GSN, HCN/CRN |
| `ghcnd-countries.txt` | fixed width | two-letter country code, country name |
| `ghcnd-states.txt` | fixed width | state code, state name |
| `ghcnd-inventory.txt` | fixed width | station ID, element, first and last year |
| `daily/*.csv.gz` | comma separated, no header | station ID, date, element, value, measurement, quality, source, observation time |

The schema in `src/ghcn_pipeline.py` follows the fields used in the report. Consult the current NOAA readme for any source-format changes.
