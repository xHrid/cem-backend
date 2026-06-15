"""
03: Activity Regularity (Temporal Stickiness)
==============================================
Flow: Aggregate → 3-step filter → Spearman correlation of consecutive-day
      hourly activity vectors → bar chart + CSV
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import spearmanr
import os

import config as cfg
from filter_utils import filter_detections

ACTIVITY_HOURS = range(0, 24)


# =============================================================================
# ANALYSIS
# =============================================================================
def run_analysis(df, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    activity_df = df[df["hour"].isin(ACTIVITY_HOURS)].copy()
    species_list = activity_df["label"].unique()
    spot_list = activity_df["Spot"].unique()
    date_list = sorted(activity_df["Date"].unique())
    num_days = activity_df["Date"].nunique()

    print(f"Species: {len(species_list)}, Spots: {len(spot_list)}, Days: {num_days}")

    hourly_counts = (
        activity_df
        .groupby(["label", "Spot", "Date", "hour"])
        .size()
        .unstack(level="hour", fill_value=0)
        .reindex(columns=list(ACTIVITY_HOURS), fill_value=0)
    )

    print("Calculating Activity Regularity...")
    temporal_stickiness = {}
    for idx, species in enumerate(species_list):
        if idx % 20 == 0:
            print(f"  {idx+1}/{len(species_list)}...")
        if species not in hourly_counts.index.get_level_values("label"):
            continue

        species_data = hourly_counts.loc[species]
        species_spot_corrs = []

        for spot in spot_list:
            if spot not in species_data.index.get_level_values("Spot"):
                continue
            spot_data = species_data.loc[spot]
            spot_dates = spot_data.index
            day_corrs = []
            for i in range(len(date_list) - 1):
                d0, d1 = date_list[i], date_list[i + 1]
                if d0 not in spot_dates or d1 not in spot_dates:
                    continue
                s0, s1 = spot_data.loc[d0], spot_data.loc[d1]
                if s0.sum() > 0 and s1.sum() > 0:
                    corr, _ = spearmanr(s0.values, s1.values)
                    if not np.isnan(corr):
                        day_corrs.append(corr)
            if day_corrs:
                species_spot_corrs.append(np.mean(day_corrs))

        if species_spot_corrs:
            temporal_stickiness[species] = np.mean(species_spot_corrs)

    avg_calls = activity_df.groupby("label").size().reset_index(name="total_calls")
    avg_calls["Avg_Calls_Per_Day"] = avg_calls["total_calls"] / num_days

    temporal_df = pd.DataFrame(
        list(temporal_stickiness.items()), columns=["label", "Activity_Regularity"],
    ).sort_values("Activity_Regularity", ascending=False)

    combined = pd.merge(temporal_df, avg_calls, on="label", how="left")
    combined.to_csv(os.path.join(output_dir, "all_species_activity_regularity.csv"), index=False)

    # Plot
    top = combined.head(cfg.TOP_N_TEMPORAL)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(22, 14))
    fig.suptitle(f"Activity Regularity: Top {cfg.TOP_N_TEMPORAL} Species", fontsize=16, fontweight="bold")

    sns.barplot(x="Activity_Regularity", y="label", data=top, palette="plasma", ax=ax1)
    ax1.set_title("Activity Regularity (Predictability)")
    ax1.set_xlabel("Average Spearman Correlation (rho)")
    ax1.set_xlim(-0.2, 1.0)
    ax1.grid(axis="x", linestyle="--", alpha=0.6)

    top_calls = combined.set_index("label").reindex(top["label"]).reset_index()
    sns.barplot(x="Avg_Calls_Per_Day", y="label", data=top_calls, palette="magma", ax=ax2)
    ax2.set_title("Average Daily Call Volume")
    ax2.set_ylabel("")
    ax2.grid(axis="x", linestyle="--", alpha=0.6)

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "temporal_stickiness_top_species.png"), dpi=300, bbox_inches="tight")
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
    run_analysis(df, cfg.OUTPUT_DIR_03_TEMPORAL)


if __name__ == "__main__":
    main()
