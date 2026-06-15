"""
08: Solar Event Correlation (Sunrise/Sunset vs Peak Activity)
==============================================================
Flow: Aggregate → 3-step filter → Pearson(peak_hour, sunrise/sunset) → plots + CSV
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import pearsonr
import os

try:
    import pytz
    from astral import LocationInfo
    from astral.sun import sunrise, sunset
    ASTRAL_AVAILABLE = True
except ImportError:
    ASTRAL_AVAILABLE = False
    print("WARNING: 'astral' not installed. Using fallback sunrise/sunset estimation.")

import config as cfg
from filter_utils import filter_detections


# =============================================================================
# ANALYSIS
# =============================================================================
def run_solar_correlation(df, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    daily_counts = df.groupby(["common_name", "Date_Only"]).size().reset_index(name="daily_count")
    daily_counts = daily_counts[daily_counts["daily_count"] > 10]
    valid_birds = daily_counts["common_name"].value_counts()[lambda x: x > cfg.MIN_SOLAR_DAYS].index
    daily_counts = daily_counts[daily_counts["common_name"].isin(valid_birds)]

    filtered = df.merge(
        daily_counts[["common_name", "Date_Only"]],
        on=["common_name", "Date_Only"], how="inner",
    )
    print(f"Species with sufficient data: {len(valid_birds)}")

    peak = filtered.groupby(
        ["common_name", "Date_Only"]
    )["hour"].agg(lambda x: x.value_counts().idxmax()).reset_index()
    peak.rename(columns={"hour": "peak_hour"}, inplace=True)
    peak["date"] = pd.to_datetime(peak["Date_Only"])

    min_d, max_d = peak["Date_Only"].min(), peak["Date_Only"].max()
    date_range = pd.date_range(start=min_d, end=max_d)

    sun_data = []
    if ASTRAL_AVAILABLE:
        city = LocationInfo(cfg.LOCATION_NAME, "India", cfg.TIMEZONE_STR, cfg.LATITUDE, cfg.LONGITUDE)
        tz = pytz.timezone(cfg.TIMEZONE_STR)
        for d in date_range:
            try:
                sr = sunrise(city.observer, date=d.date(), tzinfo=tz)
                ss = sunset(city.observer, date=d.date(), tzinfo=tz)
                sun_data.append({
                    "Date_Only": d.date(),
                    "Sunrise": sr.hour + sr.minute / 60 + sr.second / 3600,
                    "Sunset": ss.hour + ss.minute / 60 + ss.second / 3600,
                })
            except ValueError:
                continue
    else:
        for d in date_range:
            doy = d.timetuple().tm_yday
            sr_approx = 6.0 + 0.75 * np.cos(2 * np.pi * (doy - 172) / 365)
            ss_approx = 18.0 + 0.75 * np.cos(2 * np.pi * (doy - 172) / 365)
            sun_data.append({"Date_Only": d.date(), "Sunrise": sr_approx, "Sunset": ss_approx})

    df_sun = pd.DataFrame(sun_data)
    merged = pd.merge(peak, df_sun, on="Date_Only")

    pearson_results = []
    for bird in merged["common_name"].unique():
        subset = merged[merged["common_name"] == bird]
        if len(subset) > 1 and subset["peak_hour"].std() > 0:
            r_sr, p_sr = pearsonr(subset["peak_hour"], subset["Sunrise"])
            r_ss, p_ss = pearsonr(subset["peak_hour"], subset["Sunset"])
            pearson_results.append({
                "Bird": bird,
                "Pearson_Sunrise": round(r_sr, 3), "P-Val_Sunrise": round(p_sr, 4),
                "Pearson_Sunset": round(r_ss, 3), "P-Val_Sunset": round(p_ss, 4),
                "Sample_Size": len(subset),
            })

    pearson_df = pd.DataFrame(pearson_results).sort_values("Pearson_Sunrise", ascending=False)
    pearson_df.to_csv(os.path.join(output_dir, "solar_correlation_results.csv"), index=False)

    # Overlay plots for top 3
    for bird in pearson_df.head(3)["Bird"].tolist():
        bird_data = merged[merged["common_name"] == bird].sort_values("date")
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(bird_data["date"], bird_data["peak_hour"], "o-", color="#2ecc71", lw=2, ms=4, label="Peak Hour")
        ax.plot(bird_data["date"], bird_data["Sunrise"], "-", color="#FF8C00", lw=2, label="Sunrise")
        r_val = pearson_df[pearson_df["Bird"] == bird]["Pearson_Sunrise"].values[0]
        ax.set_title(f"{bird}: Peak Activity vs Sunrise (r={r_val:.3f})")
        ax.set_xlabel("Date")
        ax.set_ylabel("Hour of Day")
        ax.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"solar_overlay_{bird.replace(' ', '_').lower()}.png"), dpi=150)
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
    run_solar_correlation(df, cfg.OUTPUT_DIR_08_SOLAR)


if __name__ == "__main__":
    main()
