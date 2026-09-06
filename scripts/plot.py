"""Rebuild the two README figures exclusively from committed result tables."""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def main():
    frame = pd.read_csv(
        ROOT / "results/nz-national-monthly-2024.csv", parse_dates=["month"]
    )
    fig, ax = plt.subplots(figsize=(9, 4.5), layout="constrained")
    for element, group in frame.groupby("element"):
        group = group.sort_values("month")
        ax.plot(
            group.month, group.national_mean_temperature_c, marker="o", label=element
        )
    ax.set(
        title="New Zealand station-network temperature · 2024",
        ylabel="Mean station temperature (°C)",
        xlabel="Month",
    )
    ax.legend()
    ax.grid(alpha=0.2)
    fig.savefig(ROOT / "assets/nz-temperature-2024.png", dpi=160)
    plt.close(fig)
    rain = pd.read_csv(ROOT / "results/country-precipitation-2024.csv")
    # A legible subset selected by station count, not precipitation magnitude.
    rain = rain.nlargest(15, "station_count").sort_values(
        "mean_station_precipitation_mm"
    )
    fig, ax = plt.subplots(figsize=(9, 6), layout="constrained")
    ax.barh(rain.country_name, rain.mean_station_precipitation_mm, color="#397ca3")
    ax.set(
        title="Observed annual precipitation · 2024\n15 countries with most reporting stations",
        xlabel="Unweighted mean of station totals (mm)",
    )
    ax.grid(axis="x", alpha=0.2)
    fig.savefig(ROOT / "assets/country-precipitation-2024.png", dpi=160)
    plt.close(fig)


if __name__ == "__main__":
    main()
