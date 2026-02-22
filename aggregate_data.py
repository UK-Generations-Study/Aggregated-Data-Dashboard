#!/usr/bin/env python3
"""
aggregate_data.py — Generations Study Dashboard
================================================
Reads individual-level synthetic_data.json and produces aggregated_data.json
containing only summary statistics (no individual records).

The output is safe to share: it contains counts, means, SDs, medians,
histograms and frequency tables — no individual-level data.

Usage:
    python3 aggregate_data.py
    python3 aggregate_data.py --input mydata.json --output myagg.json

Requirements: Python 3.8+  (no external packages needed)
"""

import json
import math
import argparse
from pathlib import Path
from datetime import datetime, timezone

# ── Built-in schema (mirrors app.js SCHEMA) ─────────────────────────────────
SCHEMA = {
    "R0_TCode":               {"desc": "Pseudo-anonymised 8-character study identifier", "group": "id",            "type": "string"},
    "R0_Ethnicity":           {"desc": "Ethnicity of the study participant",              "group": "demographics",  "type": "categorical",
                               "codes": {1: "White", 2: "Black", 3: "Asian", 4: "Other", 9: "Not known"}},
    "R0_AshkenaziAncestry":   {"desc": "Ashkenazi Jewish ancestry flag",                 "group": "demographics",  "type": "binary",
                               "codes": {0: "No", 1: "Yes"}},
    "R0_Height":              {"desc": "Height in centimetres at study entry",            "group": "anthropometry", "type": "numeric", "unit": "cm"},
    "R0_Weight":              {"desc": "Weight in kilograms at study entry",              "group": "anthropometry", "type": "numeric", "unit": "kg"},
    "R0_BMI":                 {"desc": "BMI at study entry (999=pregnant at entry)",      "group": "anthropometry", "type": "numeric", "unit": "kg/m²", "sentinel": 999},
    "R0_WaistCircum":         {"desc": "Waist circumference in centimetres at entry",    "group": "anthropometry", "type": "numeric", "unit": "cm"},
    "R0_HipCircum":           {"desc": "Hip circumference in centimetres at entry",      "group": "anthropometry", "type": "numeric", "unit": "cm"},
    "R0_WaistHipRatio":       {"desc": "Waist-to-hip ratio at entry (999=pregnant)",     "group": "anthropometry", "type": "numeric", "sentinel": 999},
    "R0_Height20":            {"desc": "Height in centimetres at age 20 (999=<20 at entry)", "group": "anthropometry", "type": "numeric", "unit": "cm", "sentinel": 999},
    "R0_Weight20":            {"desc": "Weight in kilograms at age 20 (999=<20 at entry)",   "group": "anthropometry", "type": "numeric", "unit": "kg",  "sentinel": 999},
    "R0_BMI20":               {"desc": "BMI at age 20 (999=<20 or pregnant at 20)",      "group": "anthropometry", "type": "numeric", "unit": "kg/m²", "sentinel": 999},
    "R0_PregAtEntry":         {"desc": "Pregnant at study entry",                        "group": "reproductive",  "type": "binary",
                               "codes": {0: "No", 1: "Yes"}},
    "R0_PregAt20":            {"desc": "Pregnant at age 20 (999=was <20 at entry)",      "group": "reproductive",  "type": "categorical",
                               "codes": {0: "No", 1: "Yes", 999: "NA (<20)"}},
    "R0_AgeMenarche":         {"desc": "Age at menarche (first period), whole years",    "group": "reproductive",  "type": "integer", "unit": "years"},
    "R0_Parous":              {"desc": "Parity status at entry",                         "group": "reproductive",  "type": "categorical",
                               "codes": {-1: "Never pregnant", 0: "Nulliparous", 1: "Parous", 9: "Ever preg, parity unknown"}},
    "R0_Parity":              {"desc": "Number of live-birth pregnancies at entry",      "group": "reproductive",  "type": "integer"},
    "R0_AgeBirthFirst":       {"desc": "Age at first live birth (999=no live birth)",    "group": "reproductive",  "type": "numeric", "unit": "years", "sentinel": 999},
    "R0_AgeBirthLast":        {"desc": "Age at last live birth (999=no live birth)",     "group": "reproductive",  "type": "numeric", "unit": "years", "sentinel": 999},
    "R0_BreastfeedingDuration": {"desc": "Total weeks breastfed across live births (9999=no live birth)", "group": "reproductive", "type": "numeric", "unit": "weeks", "sentinel": 9999},
    "R0_Breastfed":           {"desc": "Ever breastfed (999=no live birth)",             "group": "reproductive",  "type": "categorical",
                               "codes": {0: "No", 1: "Yes", 999: "NA (no live birth)"}},
    "R0_Menopause":           {"desc": "Menopausal status at baseline",                  "group": "reproductive",  "type": "categorical",
                               "codes": {1: "Postmenopausal", 2: "Premenopausal", 3: "Assumed postmeno", 4: "Assumed premeno", 9: "Never had periods"}},
    "R0_AgeMenopause":        {"desc": "Age at menopause (years)",                       "group": "reproductive",  "type": "integer", "unit": "years"},
    "R0_MenopauseReason":     {"desc": "Reason periods stopped",                         "group": "reproductive",  "type": "categorical",
                               "codes": {1: "Natural", 2: "Bilateral oophorectomy", 3: "Hysterectomy only", 4: "Surgery (type unknown)",
                                         5: "Chemo/radio/cancer tx", 6: "Unknown reason", 7: "Other reason", 8: "On hormones", 9: "On HRT",
                                         10: "Stress", 11: "Breastfeeding/pregnant", 12: "Perimenopausal", 13: "Natural on HRT/OC",
                                         14: "Eating disorder", 15: "Illness", 16: "Premenopausal", 17: "Status unknown",
                                         18: "Other surgery", 19: "Never had periods"}},
    "R0_OralContraceptiveStatus": {"desc": "Oral contraceptive use status at entry",     "group": "lifestyle",     "type": "categorical",
                               "codes": {0: "Never", 1: "Former", 2: "Current"}},
    "R0_AgeStartedOC":        {"desc": "Age first used oral contraceptives (999=never)", "group": "lifestyle",     "type": "numeric", "unit": "years", "sentinel": 999},
    "R0_AgeLastUsedOC":       {"desc": "Age last used OC (999=current user)",            "group": "lifestyle",     "type": "numeric", "unit": "years", "sentinel": 999},
    "R0_OCLength":            {"desc": "Total duration of OC use (years)",               "group": "lifestyle",     "type": "numeric", "unit": "years"},
    "R0_HRTStatus":           {"desc": "Menopausal hormone treatment status",            "group": "lifestyle",     "type": "categorical",
                               "codes": {0: "Never", 1: "Former", 2: "Current"}},
    "R0_HRTStartAge":         {"desc": "Age started HRT",                               "group": "lifestyle",     "type": "integer", "unit": "years"},
    "R0_HRTStopAge":          {"desc": "Age stopped HRT",                               "group": "lifestyle",     "type": "integer", "unit": "years"},
    "R0_HRTDuration":         {"desc": "Total duration of HRT use (years)",              "group": "lifestyle",     "type": "numeric", "unit": "years"},
    "R0_AlcoholStatus":       {"desc": "Alcohol use status at baseline",                 "group": "lifestyle",     "type": "categorical",
                               "codes": {0: "Never", 1: "Former", 2: "Current"}},
    "R0_AgeStartedDrinking":  {"desc": "Age started regularly drinking alcohol",         "group": "lifestyle",     "type": "integer", "unit": "years"},
    "R0_AgeStoppedDrinking":  {"desc": "Age stopped regularly drinking alcohol",         "group": "lifestyle",     "type": "integer", "unit": "years"},
    "R0_AlcoholUnitsPerWeek": {"desc": "Weekly alcohol units (current drinkers)",        "group": "lifestyle",     "type": "numeric", "unit": "units/wk"},
    "R0_SmokingStatus":       {"desc": "Cigarette smoking status at baseline",           "group": "lifestyle",     "type": "categorical",
                               "codes": {0: "Never", 1: "Former", 2: "Current"}},
    "R0_AgeStartedSmoking":   {"desc": "Age started cigarette smoking",                 "group": "lifestyle",     "type": "integer", "unit": "years"},
    "R0_AgeStoppedSmoking":   {"desc": "Age stopped cigarette smoking",                 "group": "lifestyle",     "type": "integer", "unit": "years"},
    "R0_CigsPerDay":          {"desc": "Cigarettes smoked per day (current smokers)",   "group": "lifestyle",     "type": "numeric", "unit": "cigs/day"},
    "R0_PackYears":           {"desc": "Cumulative smoking exposure (pack-years)",       "group": "lifestyle",     "type": "numeric", "unit": "pack-yrs"},
    "R0_PhysicalActivity":    {"desc": "Physical activity (MET-hours/week)",             "group": "lifestyle",     "type": "numeric", "unit": "MET-h/wk"},
    "R0_GreenVegDailyServings": {"desc": "Average daily green vegetable servings",      "group": "lifestyle",     "type": "integer", "unit": "servings/day"},
    "R0_FruitDailyServings":  {"desc": "Average daily fruit servings",                  "group": "lifestyle",     "type": "integer", "unit": "servings/day"},
    "R0_BBD":                 {"desc": "History of benign breast disease",               "group": "medical",       "type": "binary",
                               "codes": {0: "No", 1: "Yes"}},
    "R0_DiabetesStatus":      {"desc": "Diabetes diagnosis at baseline",                "group": "medical",       "type": "binary",
                               "codes": {0: "No", 1: "Yes"}},
    "R0_AgeDiabetes":         {"desc": "Age at diabetes diagnosis (years)",              "group": "medical",       "type": "integer", "unit": "years"},
    "R0_DiabetesInsulin":     {"desc": "Diabetes treated with insulin at baseline",     "group": "medical",       "type": "binary",
                               "codes": {0: "No", 1: "Yes"}},
    "R0_FamHistCancer":       {"desc": "Family history of any cancer (1st degree)",     "group": "family",        "type": "binary",
                               "codes": {0: "No", 1: "Yes"}},
    "R0_FamHistCancerNum":    {"desc": "No. of 1st-degree relatives with any cancer",   "group": "family",        "type": "integer"},
    "R0_FamHistBC":           {"desc": "Family history of breast cancer (1st degree)",  "group": "family",        "type": "binary",
                               "codes": {0: "No", 1: "Yes"}},
    "R0_FamHistBCNum":        {"desc": "No. of 1st-degree relatives with breast cancer", "group": "family",       "type": "integer"},
    "R0_FamHistOV":           {"desc": "Family history of ovarian cancer (1st degree)", "group": "family",        "type": "binary",
                               "codes": {0: "No", 1: "Yes"}},
    "R0_FamHistOVNum":        {"desc": "No. of 1st-degree relatives with ovarian cancer", "group": "family",      "type": "integer"},
    "R0_FamHistColo":         {"desc": "Family history of colorectal cancer (1st degree)", "group": "family",     "type": "binary",
                               "codes": {0: "No", 1: "Yes"}},
    "R0_FamHistColoNum":      {"desc": "No. of 1st-degree relatives with colorectal cancer", "group": "family",   "type": "integer"},
    "R0_FamHistProst":        {"desc": "Family history of prostate cancer (1st degree)", "group": "family",       "type": "binary",
                               "codes": {0: "No", 1: "Yes"}},
    "R0_FamHistProstNum":     {"desc": "No. of 1st-degree relatives with prostate cancer", "group": "family",     "type": "integer"},
}

GROUP_LABELS = {
    "id": "Identifier", "demographics": "Demographics", "anthropometry": "Anthropometry",
    "reproductive": "Reproductive", "lifestyle": "Lifestyle", "medical": "Medical",
    "family": "Family History",
}

SENTINELS = {999, 9999}


# ── Statistics helpers ────────────────────────────────────────────────────────

def get_valid(records, key, sentinel=None):
    """Return values that are not null and not sentinel."""
    out = []
    for r in records:
        v = r.get(key)
        if v is None:
            continue
        if sentinel is not None and v == sentinel:
            continue
        if v in SENTINELS and sentinel is not None:
            continue
        out.append(v)
    return out


def count_null(records, key):
    return sum(1 for r in records if r.get(key) is None)


def count_sentinel(records, key, sentinel):
    if sentinel is None:
        return 0
    return sum(1 for r in records if r.get(key) is not None and
               (r.get(key) == sentinel or (sentinel != 9999 and r.get(key) == 9999)))


def quantile(sorted_vals, p):
    n = len(sorted_vals)
    if n == 0:
        return None
    idx = p * (n - 1)
    lo, hi = int(idx), min(int(idx) + 1, n - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (idx - lo)


def numeric_stats(values):
    nums = [float(v) for v in values if v is not None]
    if not nums:
        return None
    nums.sort()
    n = len(nums)
    mean = sum(nums) / n
    variance = sum((x - mean) ** 2 for x in nums) / n
    sd = math.sqrt(variance)
    return {
        "n": n,
        "mean": round(mean, 4),
        "sd": round(sd, 4),
        "median": round(quantile(nums, 0.5), 4),
        "q1": round(quantile(nums, 0.25), 4),
        "q3": round(quantile(nums, 0.75), 4),
        "min": round(nums[0], 4),
        "max": round(nums[-1], 4),
    }


def make_histogram(values, is_integer=False, target_bins=30, min_cell=0):
    nums = [float(v) for v in values]
    if not nums:
        return {"labels": [], "counts": []}
    lo, hi = min(nums), max(nums)
    rng = hi - lo
    if rng == 0:
        # All values identical — single bin; suppress if below threshold
        cnt = len(nums)
        return {"labels": [str(round(lo, 2))],
                "counts": [None if (min_cell > 0 and cnt < min_cell) else cnt]}

    if is_integer and rng <= 50:
        # One bar per integer value
        lo_i, hi_i = int(round(lo)), int(round(hi))
        labels = [str(v) for v in range(lo_i, hi_i + 1)]
        counts = [0] * len(labels)
        for v in nums:
            idx = int(round(v)) - lo_i
            if 0 <= idx < len(counts):
                counts[idx] += 1
    elif is_integer:
        step = max(1, math.ceil(rng / target_bins))
        n_bins = math.ceil((rng + 1) / step)
        labels = []
        for i in range(n_bins):
            bin_lo = lo + i * step
            bin_hi = min(bin_lo + step - 1, hi)
            labels.append(str(int(round(bin_lo))) if step == 1 else f"{int(round(bin_lo))}–{int(round(bin_hi))}")
        counts = [0] * n_bins
        for v in nums:
            idx = min(n_bins - 1, int((v - lo) / step))
            counts[idx] += 1
    else:
        step = rng / target_bins
        labels = [f"{lo + i * step:.2f}" for i in range(target_bins)]
        counts = [0] * target_bins
        for v in nums:
            idx = min(target_bins - 1, int((v - lo) / step))
            counts[idx] += 1
        # Remove trailing empty bins
        while counts and counts[-1] == 0:
            counts.pop()
            labels.pop()

    # Suppress small bins — set to null so the dashboard can hide them
    if min_cell > 0:
        counts = [None if (c is not None and c < min_cell) else c for c in counts]

    return {"labels": labels, "counts": counts}


def freq_table(values, codes=None, min_cell=0):
    counts = {}
    for v in values:
        k = str(int(v)) if isinstance(v, float) and v.is_integer() else str(v)
        counts[k] = counts.get(k, 0) + 1
    result = {}
    for k, cnt in sorted(counts.items(), key=lambda x: -x[1]):
        label = str(codes.get(int(k), codes.get(k, k))) if codes else k
        if min_cell > 0 and cnt < min_cell:
            result[k] = {"count": None, "label": label, "suppressed": True}
        else:
            result[k] = {"count": cnt, "label": label}
    return result


# ── Per-variable aggregation ─────────────────────────────────────────────────

def aggregate_variable(key, records, schema_entry, min_cell=0):
    vtype    = schema_entry.get("type", "numeric")
    sentinel = schema_entry.get("sentinel")
    codes    = schema_entry.get("codes")
    n_total  = len(records)

    n_null     = count_null(records, key)
    n_sentinel = count_sentinel(records, key, sentinel)
    valid_vals = get_valid(records, key, sentinel)

    result = {
        "n_total":    n_total,
        "n_valid":    len(valid_vals),
        "n_null":     n_null,
        "n_sentinel": n_sentinel,
    }

    if vtype in ("numeric", "integer"):
        stats = numeric_stats(valid_vals)
        if stats:
            result.update(stats)
        result["histogram"] = make_histogram(valid_vals, is_integer=(vtype == "integer"), min_cell=min_cell)

    elif vtype in ("categorical", "binary"):
        result["frequencies"] = freq_table(valid_vals, codes, min_cell=min_cell)

    return result


# ── Main aggregation ─────────────────────────────────────────────────────────

def aggregate(data, schema, min_cell=5):
    """Produce the full aggregated output dict."""

    suppressed_strata = 0
    suppressed_cells  = 0

    # Identify stratification variables (categorical/binary with codes, not id group)
    strat_vars = [
        k for k, s in schema.items()
        if s.get("type") in ("categorical", "binary")
        and s.get("codes")
        and s.get("group") != "id"
        and k in data[0]
    ]

    # Build whole-cohort stats
    # Apply histogram bin suppression (min_cell) but not frequency suppression
    # (whole-cohort counts are large; strata get full suppression)
    print(f"  Aggregating whole cohort (n={len(data)})…")
    whole_cohort = {}
    for key, s in schema.items():
        if s.get("type") == "string" or s.get("group") == "id":
            continue
        if key not in data[0]:
            continue
        whole_cohort[key] = aggregate_variable(key, data, s, min_cell=min_cell)

    # Build stratified stats for every stratification variable
    strata_out = {}
    for strat_key in strat_vars:
        strat_schema = schema[strat_key]
        print(f"  Stratifying by {strat_key}…")

        # Group records by stratum value
        groups = {}
        for rec in data:
            gv = rec.get(strat_key)
            if gv is None:
                continue
            gk = str(int(gv)) if isinstance(gv, float) and gv.is_integer() else str(gv)
            groups.setdefault(gk, []).append(rec)

        strat_out = {}
        codes = strat_schema.get("codes", {})
        for gk, grp_records in sorted(groups.items()):
            try:
                label = str(codes.get(int(gk), codes.get(gk, gk)))
            except (ValueError, TypeError):
                label = str(codes.get(gk, gk))

            n_grp = len(grp_records)

            # Suppress entire stratum if too small
            if min_cell > 0 and n_grp < min_cell:
                strat_out[gk] = {"label": label, "n": None, "suppressed": True}
                suppressed_strata += 1
                continue

            grp_stats = {"label": label, "n": n_grp, "variables": {}}
            for key, s in schema.items():
                if s.get("type") == "string" or s.get("group") == "id":
                    continue
                if key not in data[0]:
                    continue
                var_stats = aggregate_variable(key, grp_records, s, min_cell=min_cell)
                # Count suppressed frequency cells
                if "frequencies" in var_stats:
                    suppressed_cells += sum(
                        1 for fc in var_stats["frequencies"].values()
                        if fc.get("suppressed")
                    )
                grp_stats["variables"][key] = var_stats
            strat_out[gk] = grp_stats

        strata_out[strat_key] = strat_out

    print(f"  Suppression (min_cell={min_cell}): {suppressed_strata} strata suppressed, "
          f"{suppressed_cells} frequency cells suppressed.")

    return whole_cohort, strata_out, strat_vars, suppressed_strata, suppressed_cells


def build_schema_block(schema):
    """Serialise schema to a JSON-safe dict (convert int keys to strings)."""
    out = {}
    for key, s in schema.items():
        if s.get("group") == "id":
            continue
        entry = {k: v for k, v in s.items() if k != "codes"}
        if s.get("codes"):
            entry["codes"] = {str(k): v for k, v in s["codes"].items()}
        out[key] = entry
    return out


# ── Entry point ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Aggregate individual-level data for the Generations dashboard.")
    parser.add_argument("--input",    default="synthetic_data.json",  help="Input JSON file (array of records)")
    parser.add_argument("--output",   default="aggregated_data.json", help="Output aggregated JSON file")
    parser.add_argument("--min-cell", default=5, type=int,
                        help="Suppress frequency counts below this threshold (default: 5)")
    args = parser.parse_args()
    min_cell = args.min_cell

    input_path  = Path(args.input)
    output_path = Path(args.output)

    print(f"Reading {input_path}…")
    with open(input_path) as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Input must be a JSON array of records.")
    print(f"  {len(data):,} records loaded.")

    # Filter schema to variables that actually exist in the data
    data_keys = set(data[0].keys()) if data else set()
    schema = {k: v for k, v in SCHEMA.items() if k in data_keys}
    print(f"  {len(schema)} schema variables matched to data columns.")

    print(f"Aggregating (min_cell={min_cell})…")
    whole_cohort, strata, strat_vars, supp_strata, supp_cells = aggregate(data, schema, min_cell=min_cell)

    output = {
        "meta": {
            "created":            datetime.now(timezone.utc).isoformat(),
            "source_file":        input_path.name,
            "n":                  len(data),
            "n_variables":        len(whole_cohort),
            "strat_variables":    strat_vars,
            "min_cell":           min_cell,
            "suppressed_strata":  supp_strata,
            "suppressed_cells":   supp_cells,
            "tool":               "Generations Study — aggregate_data.py",
        },
        "group_labels": GROUP_LABELS,
        "schema":        build_schema_block(schema),
        "whole_cohort":  whole_cohort,
        "strata":        strata,
    }

    print(f"Writing {output_path}…")
    with open(output_path, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    size_kb = output_path.stat().st_size / 1024
    print(f"Done. {output_path} ({size_kb:.1f} KB)")
    print(f"  Whole-cohort stats for {len(whole_cohort)} variables.")
    print(f"  Stratified by {len(strata)} variables.")
    print(f"  Suppressed: {supp_strata} strata and {supp_cells} frequency cells (min_cell={min_cell}).")


if __name__ == "__main__":
    main()
