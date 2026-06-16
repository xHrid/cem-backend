"""
07: Migratory vs Resident Bird Classification
===============================================
Flow: Aggregate → 3-step filter → SCI + Kurtosis + PMR → classify → plots + CSV
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

import config as cfg
from filter_utils import filter_detections


# =============================================================================
# ANALYSIS
# =============================================================================
def run_classification(df, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    df["Date_Only_dt"] = pd.to_datetime(df["Date_Only"])
    daily_counts = df.groupby(["common_name", "Date_Only_dt"]).size().reset_index(name="daily_count")

    all_dates = pd.date_range(
        start=daily_counts["Date_Only_dt"].min(),
        end=daily_counts["Date_Only_dt"].max(),
    ).normalize()

    unique_birds = daily_counts["common_name"].unique()
    print(f"Analyzing {len(unique_birds)} species over {len(all_dates)} days")

    results = []
    for bird in unique_birds:
        bird_data = daily_counts[daily_counts["common_name"] == bird]
        full_ts = pd.DataFrame({"Date_Only_dt": all_dates})
        full_ts = full_ts.merge(bird_data[["Date_Only_dt", "daily_count"]], on="Date_Only_dt", how="left")
        full_ts["daily_count"] = full_ts["daily_count"].fillna(0)

        counts_arr = full_ts["daily_count"].values
        total = counts_arr.sum()
        if total == 0:
            continue

        # SCI
        if len(counts_arr) >= cfg.WINDOW_SIZE:
            rs_max = pd.Series(counts_arr).rolling(window=cfg.WINDOW_SIZE).sum().dropna().max()
        else:
            rs_max = total
        sci = rs_max / total

        # Residual Kurtosis
        n = len(counts_arr)
        x = np.arange(n)
        if n > 2 and np.std(counts_arr) > 0:
            coeffs = np.polyfit(x, counts_arr, 1)
            residuals = counts_arr - np.polyval(coeffs, x)
            r_std = residuals.std()
            k_value = np.mean(((residuals - residuals.mean()) / r_std) ** 4) if r_std > 0 else 0.0
        else:
            k_value = 0.0

        # PMR
        pmr = counts_arr.max() / (np.median(counts_arr) + cfg.EPSILON)

        is_mig = (sci > cfg.SCI_THRESHOLD) and (k_value > cfg.KURTOSIS_THRESHOLD) and (pmr > cfg.PMR_THRESHOLD)
        results.append({
            "Species": bird, "SCI": round(sci, 4), "Kurtosis": round(k_value, 2),
            "PMR": round(pmr, 2), "Total_Detections": int(total),
            "Classification": "Migratory" if is_mig else "Resident",
        })

    metrics_df = pd.DataFrame(results).sort_values("SCI", ascending=False)
    n_mig = (metrics_df["Classification"] == "Migratory").sum()
    n_res = (metrics_df["Classification"] == "Resident").sum()
    print(f"Migratory: {n_mig}, Resident: {n_res}")

    metrics_df.to_csv(os.path.join(output_dir, "migratory_classification_all_species.csv"), index=False)

    # Distributions
    colors = {"Migratory": "#e74c3c", "Resident": "#2ecc71"}
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle("Migratory Classification Metrics", fontsize=16, fontweight="bold")

    for ax, col, thresh, label in zip(
        axes, ["SCI", "Kurtosis", "PMR"],
        [cfg.SCI_THRESHOLD, cfg.KURTOSIS_THRESHOLD, cfg.PMR_THRESHOLD],
        ["SCI", "Residual Kurtosis (K)", "PMR"],
    ):
        for cls in ["Resident", "Migratory"]:
            subset = metrics_df[metrics_df["Classification"] == cls]
            data = subset[col].clip(upper=200) if col == "PMR" else subset[col]
            ax.hist(data, bins=20, alpha=0.7, label=cls, color=colors[cls])
        ax.axvline(thresh, color="black", linestyle="--", label=f"Threshold={thresh}")
        ax.set_xlabel(label)
        ax.set_title(f"{col} Distribution")
        ax.legend()

    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "migratory_classification_distributions.png"), dpi=300, bbox_inches="tight")
    plt.close()

    # SCI vs PMR scatter
    fig, ax = plt.subplots(figsize=(10, 8))
    for cls in ["Resident", "Migratory"]:
        subset = metrics_df[metrics_df["Classification"] == cls]
        ax.scatter(subset["SCI"], subset["PMR"].clip(upper=300), alpha=0.6, label=cls, color=colors[cls], s=40)
    ax.axvline(cfg.SCI_THRESHOLD, color="gray", linestyle="--", alpha=0.5)
    ax.axhline(cfg.PMR_THRESHOLD, color="gray", linestyle="--", alpha=0.5)
    ax.set_xlabel("SCI")
    ax.set_ylabel("PMR")
    ax.set_title("SCI vs PMR")
    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "migratory_sci_vs_pmr.png"), dpi=300, bbox_inches="tight")
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
        filter_confidence=cfg.FILTER_CONFIDENCE,
        filter_min_detections=cfg.FILTER_MIN_DETECTIONS,
    )
    if df.empty:
        print("ERROR: No data after filtering.")
        return
    run_classification(df, cfg.OUTPUT_DIR_07_MIGRATORY)


if __name__ == "__main__":
    main()
