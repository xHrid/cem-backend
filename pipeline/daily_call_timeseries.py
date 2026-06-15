"""
09: Daily Call Frequency Time Series
======================================
Flow: Aggregate → 3-step filter → per-species daily call count plots +
      data availability heatmap → save PNGs
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

import config as cfg
from filter_utils import filter_detections


# =============================================================================
# ANALYSIS
# =============================================================================
def run_timeseries(df, output_dir, species_to_plot=cfg.SPECIES_TO_PLOT, max_species=cfg.MAX_TIMESERIES_SP):
    os.makedirs(output_dir, exist_ok=True)
    sns.set_style("whitegrid")

    global_dates_with_data = set(df["Date_Only"].unique())
    all_dates = pd.date_range(
        start=min(global_dates_with_data),
        end=max(global_dates_with_data),
    ).date

    print(f"Period: {min(all_dates)} to {max(all_dates)}")
    print(f"Days with data: {len(global_dates_with_data)} / {len(all_dates)}")

    if species_to_plot is None:
        unique_birds = df["common_name"].value_counts().head(max_species).index.tolist()
    else:
        unique_birds = species_to_plot

    print(f"Plotting {len(unique_birds)} species...")

    for idx, bird in enumerate(unique_birds):
        if idx % 10 == 0:
            print(f"  {idx+1}/{len(unique_birds)}: {bird}")

        bird_data = df[df["common_name"] == bird].groupby("Date_Only").size().reset_index(name="call_count")
        df_plot = pd.DataFrame({"Date_Only": all_dates})
        df_plot = df_plot.merge(bird_data, on="Date_Only", how="left").fillna(0)
        df_plot["is_gap"] = df_plot["Date_Only"].apply(lambda x: x not in global_dates_with_data)

        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(df_plot["Date_Only"], df_plot["call_count"], color="#e74c3c", lw=2.5, label="No Data", zorder=1)

        df_green = df_plot.copy()
        df_green.loc[df_green["is_gap"], "call_count"] = np.nan
        ax.plot(df_green["Date_Only"], df_green["call_count"], color="#2ecc71", lw=3, label="Recorder Active", zorder=2)

        ax.set_title(f"Daily Call Frequency: {bird}", fontsize=16, pad=15)
        ax.set_ylabel("Number of Calls")
        plt.xticks(rotation=45)
        plt.legend(frameon=True)
        plt.tight_layout()

        clean = bird.replace(" ", "_").lower()
        plt.savefig(os.path.join(output_dir, f"ts_{clean}.png"), dpi=150)
        plt.close(fig)

    # Data availability heatmap
    print("Generating data availability heatmap...")
    avail = df.groupby(["Spot", "Date_Only"]).size().reset_index(name="count")
    avail["Date_Only"] = pd.to_datetime(avail["Date_Only"])
    pivot = avail.pivot_table(index="Spot", columns="Date_Only", values="count", fill_value=0)
    binary = (pivot > 0).astype(int)

    plt.figure(figsize=(20, 4))
    sns.heatmap(binary, cmap="Greens", cbar=False, linewidths=0.1)
    plt.title("Data Availability Across Sites")
    plt.xlabel("Date")
    plt.ylabel("Monitoring Site")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "data_availability_heatmap.png"), dpi=150)
    plt.close()

    print(f"Done. {len(unique_birds)} plots saved to: {output_dir}")


# =============================================================================
# MAIN
# =============================================================================
def main():
    cfg.apply_overrides()
    df = filter_detections(
        cfg.AGGREGATE_FILE, cfg.EBIRD_FILE,
        cfg.DATE_START, cfg.DATE_END, cfg.SPOT_NAMES,
    )
    if df.empty:
        print("ERROR: No data after filtering.")
        return
    # Pass cfg values explicitly: run_timeseries defaults are bound at import
    # time, before apply_overrides(), so a --max-timeseries-species override
    # only takes effect when read here (post-override) and passed in.
    run_timeseries(df, cfg.OUTPUT_DIR_09_TIMESERIES,
                   species_to_plot=cfg.SPECIES_TO_PLOT, max_species=cfg.MAX_TIMESERIES_SP)


if __name__ == "__main__":
    main()
