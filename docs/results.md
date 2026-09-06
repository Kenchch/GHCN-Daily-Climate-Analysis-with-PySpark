# Validated 2024 run

Executed 2026-09-06 on WSL Ubuntu, Java 17.0.20, Python 3.12.14 and Spark 3.5.9.
Spark ran with `local[2]`, 3 GB driver memory and 8 shuffle partitions.
Times include session startup, validation counts and output writing; they are
single-run observations on this machine, not benchmark medians.

| Command | Seconds | Input rows | Output rows |
|---|---:|---|---|
| enrich-stations | 13.725 | `{'stations': 132501, 'countries': 219, 'states': 74, 'inventory': 782552}` | `132501` |
| nz-temperature | 124.578 | `37106497` | `{'monthly': 288, 'national_monthly': 24}` |
| country-precipitation | 140.259 | `37106497` | `180` |

Both daily commands read all 37,106,497 rows in the 2024 year file.
36,384 rows had a quality flag and were excluded; 37,070,113 were accepted.
No missing-value or invalid-date rejections occurred in this snapshot.
The full historical archive was not processed.

Reproduce:

```bash
bash scripts/download_sample.sh
bash scripts/run_evidence.sh
python scripts/plot.py
```

The plots use the committed small result tables; to refresh them after running
Spark, copy the `part-*.csv` from `output/evidence/nz-temperature/national_monthly_csv/`
and `output/evidence/country-precipitation/` to the corresponding `results/` CSVs.
[Input hashes](../results/SHA256SUMS) identify the downloaded snapshot.
[Machine-readable metrics](../results/run-metrics.json) preserve counts and times.
The year file is 171,213,483 compressed bytes; raw inputs are not redistributed.

Monthly temperatures average available station-month means. Annual precipitation
is the unweighted mean of observed station-year totals. Neither calculation
adjusts for incomplete coverage or spatial sampling. The precipitation plot
shows the 15 countries with most reporting stations, not the wettest countries.
These outputs do not reproduce the old full-archive coursework screenshots.
