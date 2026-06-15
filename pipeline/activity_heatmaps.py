"""
02: Species Activity Heatmaps (Normalized & Non-Normalized)
============================================================
Flow: Aggregate → 3-step filter → heatmap generation → save PNGs
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
def run_heatmaps(df, output_dir, top_n=cfg.TOP_N_SPECIES):
    os.makedirs(output_dir, exist_ok=True)

    df = df[~df["label"].str.contains("Engine|Siren", na=False)]
    print(f"Working with {len(df)} detections across {sorted(df['Spot'].unique())}")

    for spot in sorted(df["Spot"].unique()):
        spot_df = df[df["Spot"] == spot]
        num_days = spot_df["Date"].dt.date.nunique()
        if num_days == 0:
            continue

        top_species = spot_df["label"].value_counts().nlargest(top_n).index
        spot_top = spot_df[spot_df["label"].isin(top_species)]

        pivot = spot_top.pivot_table(
            index="label", columns="hour", values="filename",
            aggfunc="count", fill_value=0,
        )
        avg = pivot / num_days

        # Non-normalized
        plt.figure(figsize=(20, 10))
        sns.heatmap(avg, cmap="YlGnBu", linewidths=0.5, annot=True, fmt=".2f",
                    cbar_kws={"label": "Avg. Detections per Hour"})
        plt.title(f"Average Detections per Hour - {spot.replace('_', ' ').title()} "
                  f"(Averaged over {num_days} days)", fontsize=14)
        plt.xlabel("Hour of Day")
        plt.ylabel("Species")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"heatmap_non_normalized_{spot}.png"), dpi=300)
        plt.close()

        # Normalized
        norm = avg.div(avg.sum(axis=1), axis=0)
        plt.figure(figsize=(20, 10))
        sns.heatmap(norm, cmap="YlGnBu", linewidths=0.5, annot=True, fmt=".2f",
                    cbar_kws={"label": "Proportion of Daily Activity"})
        plt.title(f"Normalized Hourly Activity - {spot.replace('_', ' ').title()}", fontsize=14)
        plt.xlabel("Hour of Day")
        plt.ylabel("Species")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"heatmap_normalized_{spot}.png"), dpi=300)
        plt.close()

    print(f"Done. Heatmaps saved to: {output_dir}")


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
    # Pass cfg.TOP_N_SPECIES explicitly: the run_heatmaps default is bound at
    # import time, before apply_overrides(), so a --top-n-species override only
    # takes effect when read here (post-override) and passed in.
    run_heatmaps(df, cfg.OUTPUT_DIR_02_HEATMAPS, top_n=cfg.TOP_N_SPECIES)


if __name__ == "__main__":
    main()
