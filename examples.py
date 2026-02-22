# =============================================================================
# Generations Study – Synthetic Dataset: Python Loading Examples
# =============================================================================
# Requirements: pip install pandas numpy matplotlib seaborn
# =============================================================================

import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ── 1. Load the dataset ───────────────────────────────────────────────────────
with open("synthetic_data.json") as f:
    records = json.load(f)

df = pd.DataFrame(records)
print(f"Loaded: {len(df):,} participants, {len(df.columns)} variables")
print(df.dtypes.value_counts())

# ── 2. Basic inspection ───────────────────────────────────────────────────────
print(df[["R0_TCode", "R0_Ethnicity", "R0_BMI", "R0_SmokingStatus"]].head(10))

# ── 3. Handle sentinel values (999 / 9999 = Not Applicable) ──────────────────
# Variables that use 999 to mean "not applicable" (not missing):
SENTINELS = {
    "R0_BMI":                  999,
    "R0_BMI20":                999,
    "R0_WaistHipRatio":        999,
    "R0_Height20":             999,
    "R0_Weight20":             999,
    "R0_AgeBirthFirst":        999,
    "R0_AgeBirthLast":         999,
    "R0_AgeStartedOC":         999,
    "R0_AgeLastUsedOC":        999,
    "R0_Breastfed":            999,
    "R0_PregAt20":             999,
    "R0_BreastfeedingDuration":9999,
}

df_clean = df.copy()
for col, sentinel in SENTINELS.items():
    if col in df_clean.columns:
        df_clean[col] = df_clean[col].replace(sentinel, np.nan)

# ── 4. Descriptive statistics ─────────────────────────────────────────────────
print("\n── BMI summary ──")
print(df_clean["R0_BMI"].describe())

print("\n── Ethnicity distribution ──")
print(df_clean["R0_Ethnicity"].value_counts(dropna=False))

print("\n── Smoking status ──")
print(df_clean["R0_SmokingStatus"].value_counts(dropna=False))

print("\n── Menopause status ──")
print(df_clean["R0_Menopause"].value_counts(dropna=False))

# ── 5. Missingness overview ───────────────────────────────────────────────────
miss = (df_clean.isna().mean() * 100).round(1).sort_values(ascending=False)
print("\n── Top 10 variables by missingness ──")
print(miss.head(10).to_string())

# ── 6. Apply sample cohort definition ─────────────────────────────────────────
mask = (
    (df_clean["R0_Menopause"] == 2)     &   # Premenopausal
    (df_clean["R0_Parous"] == 1)        &   # Parous
    (df_clean["R0_BMI"] > 25)           &   # BMI > 25
    (df_clean["R0_AlcoholStatus"] == 2) &   # Current drinker
    df_clean["R0_BMI"].notna()
)
df_cohort = df_clean.loc[mask].copy()
print(f"\nSample cohort: {len(df_cohort)} participants")

# ── 7. Table 1 style summary ───────────────────────────────────────────────────
def table1(data, numeric_vars, categorical_vars):
    rows = []
    for v in numeric_vars:
        col = data[v].dropna()
        rows.append({
            "Variable": v, "Type": "numeric",
            "N": len(col),
            "Mean (SD)": f"{col.mean():.1f} ({col.std():.1f})",
            "Median [IQR]": f"{col.median():.1f} [{col.quantile(.25):.1f}–{col.quantile(.75):.1f}]",
            "Missing %": f"{data[v].isna().mean()*100:.1f}%"
        })
    for v in categorical_vars:
        col = data[v].dropna()
        freq = col.value_counts()
        summary = " | ".join([f"{k}: {cnt} ({cnt/len(data)*100:.1f}%)" for k, cnt in freq.head(3).items()])
        rows.append({
            "Variable": v, "Type": "categorical",
            "N": len(col), "Mean (SD)": summary,
            "Median [IQR]": "", "Missing %": f"{data[v].isna().mean()*100:.1f}%"
        })
    return pd.DataFrame(rows)

t1 = table1(
    df_cohort,
    numeric_vars=["R0_BMI", "R0_AgeMenarche", "R0_Parity",
                  "R0_AlcoholUnitsPerWeek", "R0_PhysicalActivity"],
    categorical_vars=["R0_SmokingStatus", "R0_Menopause", "R0_FamHistBC"]
)
print("\n── Table 1 ──")
print(t1.to_string(index=False))

# ── 8. Visualisations ─────────────────────────────────────────────────────────
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle("Generations Study – Synthetic Dataset", fontsize=14, fontweight="bold", color="#1b3d6e")

# BMI histogram
ax = axes[0, 0]
df_clean["R0_BMI"].dropna().hist(bins=40, ax=ax, color="#1b3d6e", alpha=0.75, edgecolor="white")
ax.set_title("BMI at Study Entry"); ax.set_xlabel("BMI (kg/m²)"); ax.set_ylabel("Count")

# Smoking status bar chart
ax = axes[0, 1]
smk = df_clean["R0_SmokingStatus"].value_counts().sort_index()
smk.index = {0:"Never", 1:"Former", 2:"Current"}.get
smk.plot(kind="bar", ax=ax, color=["#1b3d6e","#c41e6e","#00808a"], alpha=0.85, edgecolor="white")
ax.set_title("Smoking Status"); ax.set_xlabel(""); ax.set_ylabel("Count"); ax.tick_params(axis="x", rotation=0)

# BMI by menopause status
ax = axes[1, 0]
menop_map = {1:"Postmeno", 2:"Premeno", 3:"Assumed post", 4:"Assumed pre"}
bmi_by_menop = {menop_map.get(k, k): df_clean.loc[df_clean["R0_Menopause"]==k, "R0_BMI"].dropna()
                for k in [1, 2, 3, 4]}
ax.boxplot([v for v in bmi_by_menop.values() if len(v)],
           labels=[k for k, v in bmi_by_menop.items() if len(v)], patch_artist=True,
           boxprops=dict(facecolor="#1b3d6e44"))
ax.set_title("BMI by Menopause Status"); ax.set_ylabel("BMI (kg/m²)")
ax.tick_params(axis="x", rotation=20)

# Missing data
ax = axes[1, 1]
top_miss = miss.head(15)
top_miss.plot(kind="barh", ax=ax, color="#c41e6e", alpha=0.8)
ax.set_title("Top 15 Variables by Missingness"); ax.set_xlabel("Missing (%)")
ax.invert_yaxis()

plt.tight_layout()
plt.savefig("exploratory_plots.png", dpi=150, bbox_inches="tight")
print("\nPlots saved to exploratory_plots.png")
plt.show()
