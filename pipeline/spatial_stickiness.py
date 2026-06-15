"""
04: Habitat Affinity (Spatial Stickiness)
==========================================
Flow: Aggregate → 3-step filter → Spearman correlation of consecutive-day
      spatial distribution vectors → bar chart + heatmap + CSV
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import spearmanr
import os

import config as cfg
from filter_utils import filter_detections


# =============================================================================
# ANALYSIS
# =============================================================================
def run_analysis(df, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    spot_list = sorted(df["Spot"].unique())
    date_list = sorted(df["Date"].unique())
    total_spots = len(spot_list)

    print(f"Spots: {total_spots}, Days: {len(date_list)}")

    if total_spots < 2:
        print("ERROR: Spatial stickiness requires >=2 spots.")
        return

    species_spot_counts = df.groupby("label")["Spot"].nunique()
    species_list = species_spot_counts[species_spot_counts == total_spots].index.tolist()
    print(f"Species at all {total_spots} spots: {len(species_list)}")

    if not species_list:
        print("ERROR: No species found at all spots.")
        return

    pivot = (
        df[df["label"].isin(species_list)]
        .groupby(["label", "Date", "Spot"]).size()
        .unstack(level="Spot", fill_value=0)
        .reindex(columns=spot_list, fill_value=0)
    )

    print("Calculating Habitat Affinity...")
    spatial_stickiness = {}
    for idx, species in enumerate(species_list):
        if idx % 10 == 0:
            print(f"  {idx+1}/{len(species_list)}...")
        if species not in pivot.index.get_level_values("label"):
            continue

        species_data = pivot.loc[species]
        available_dates = species_data.index
        day_corrs = []
        for i in range(len(date_list) - 1):
            d0, d1 = date_list[i], date_list[i + 1]
            if d0 not in available_dates or d1 not in available_dates:
                continue
            c0, c1 = species_data.loc[d0], species_data.loc[d1]
            if c0.nunique() > 1 and c1.nunique() > 1:
                corr, _ = spearmanr(c0.values, c1.values)
                if not np.isnan(corr):
                    day_corrs.append(corr)
        if day_corrs:
            spatial_stickiness[species] = np.mean(day_corrs)

    activity = df[df["label"].isin(species_list)].copy()
    daily_counts = activity.groupby(["label", "Spot", "Date"]).size().reset_index(name="daily_count")
    heatmap_data = daily_counts.groupby(["label", "Spot"])["daily_count"].mean().unstack(fill_value=0)

    spatial_df = pd.DataFrame(
        list(spatial_stickiness.items()), columns=["label", "Habitat_Affinity"],
    ).sort_values("Habitat_Affinity", ascending=False)

    combined = pd.merge(spatial_df, heatmap_data.reset_index(), on="label", how="outer")
    combined.sort_values("Habitat_Affinity", ascending=False, inplace=True)
    combined.to_csv(os.path.join(output_dir, "all_species_habitat_affinity.csv"), index=False)

    # Bar chart
    plt.figure(figsize=(10, max(12, len(spatial_df) * 0.3)))
    sns.barplot(x="Habitat_Affinity", y="label", data=spatial_df, palette="viridis")
    plt.title(f"Habitat Affinity ({len(spatial_df)} Species at All Sites)", fontsize=16)
    plt.xlabel("Average Spearman Correlation (rho)")
    plt.grid(axis="x", linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "spatial_stickiness_bar_chart.png"), dpi=300, bbox_inches="tight")
    plt.close()

    # Heatmap
    if not heatmap_data.empty:
        ordered = spatial_df["label"].tolist()
        hm = heatmap_data.reindex(ordered).fillna(0)
        plt.figure(figsize=(12, max(10, len(ordered) * 0.3)))
        sns.heatmap(hm, cmap="YlOrRd", annot=True, fmt=".1f",
                    cbar_kws={"label": "Avg Daily Detections"})
        plt.title("Per-Site Activity (aligned with Habitat Affinity ranking)")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "spatial_stickiness_heatmap.png"), dpi=300, bbox_inches="tight")
        plt.close()

    print(f"Done. Results saved to: {output_dir}")


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
    run_analysis(df, cfg.OUTPUT_DIR_04_SPATIAL)


if __name__ == "__main__":
    main()
