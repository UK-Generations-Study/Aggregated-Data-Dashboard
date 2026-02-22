"""
Generations Study – Synthetic Dataset Generator
Produces 1000 participants conforming to DerivedVariables.json (Draft 2020-12).
Logical coherence is maintained between related variables.
Run: python3 generate_data.py
"""

import json
import random
import string
import math

random.seed(42)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def maybe_null(value, p_null=0.05):
    """Return None with probability p_null, otherwise return value."""
    return None if random.random() < p_null else value

def normal_clamp(mu, sigma, lo, hi, max_iter=2000):
    """Sample from normal distribution, clamped to [lo, hi]."""
    if lo >= hi:
        return lo  # guard against impossible range
    for _ in range(max_iter):
        v = random.gauss(mu, sigma)
        if lo <= v <= hi:
            return v
    # Fallback: uniform over valid range
    return random.uniform(lo, hi)

def weighted_choice(options, weights):
    """Random weighted choice."""
    r = random.random()
    cumulative = 0.0
    for opt, w in zip(options, weights):
        cumulative += w
        if r < cumulative:
            return opt
    return options[-1]

def make_tcode(used):
    """Generate a unique 8-character alphanumeric study ID."""
    chars = string.ascii_uppercase + string.digits
    while True:
        code = ''.join(random.choices(chars, k=8))
        if code not in used:
            used.add(code)
            return code

# ---------------------------------------------------------------------------
# Generate one participant
# ---------------------------------------------------------------------------

def generate_participant(used_tcodes):
    p = {}

    # R0_TCode
    p["R0_TCode"] = make_tcode(used_tcodes)

    # ── Latent: age at entry (not in schema, drives coherence) ────────────
    age_entry = round(normal_clamp(28, 7, 16, 40), 1)

    # ── Demographics ──────────────────────────────────────────────────────
    eth = weighted_choice(
        [1, 2, 3, 4, 9, None],
        [0.82, 0.04, 0.07, 0.04, 0.01, 0.02]
    )
    p["R0_Ethnicity"] = eth

    if eth == 1 and random.random() < 0.04:
        p["R0_AshkenaziAncestry"] = maybe_null(1, 0.03)
    elif eth is None:
        p["R0_AshkenaziAncestry"] = maybe_null(0, 0.20)
    else:
        p["R0_AshkenaziAncestry"] = maybe_null(0, 0.03)

    # ── Anthropometry at entry ─────────────────────────────────────────────
    height_true = normal_clamp(163.5, 6.5, 140, 195)
    p["R0_Height"] = maybe_null(round(height_true, 1), 0.04)

    bmi_entry = normal_clamp(24.5, 4.5, 15, 55)
    weight_true = bmi_entry * (height_true / 100) ** 2
    weight_true = max(35.0, min(180.0, weight_true))
    p["R0_Weight"] = maybe_null(round(weight_true, 1), 0.04)

    preg_at_entry = weighted_choice([0, 1, None], [0.88, 0.06, 0.06])
    p["R0_PregAtEntry"] = preg_at_entry

    if preg_at_entry == 1:
        p["R0_BMI"] = 999
    elif p["R0_Height"] is None or p["R0_Weight"] is None:
        p["R0_BMI"] = None
    else:
        p["R0_BMI"] = maybe_null(round(p["R0_Weight"] / (p["R0_Height"] / 100) ** 2, 1), 0.03)

    # Waist / hip / WHR
    waist_mu = 70 + (bmi_entry - 22) * 1.8
    waist = normal_clamp(waist_mu, 8, 55, 130)
    p["R0_WaistCircum"] = maybe_null(round(waist, 1), 0.08)

    whr_target = normal_clamp(0.80, 0.07, 0.62, 1.05)
    hip = waist / whr_target
    hip = max(70.0, min(145.0, hip))
    p["R0_HipCircum"] = maybe_null(round(hip, 1), 0.08)

    if preg_at_entry == 1:
        p["R0_WaistHipRatio"] = 999
    elif p["R0_WaistCircum"] is not None and p["R0_HipCircum"] is not None:
        p["R0_WaistHipRatio"] = maybe_null(round(p["R0_WaistCircum"] / p["R0_HipCircum"], 3), 0.05)
    else:
        p["R0_WaistHipRatio"] = None

    # ── Anthropometry at age 20 ────────────────────────────────────────────
    if age_entry < 20:
        p["R0_Height20"]  = 999
        p["R0_Weight20"]  = 999
        p["R0_BMI20"]     = 999
        p["R0_PregAt20"]  = 999
    else:
        h20 = max(140.0, min(195.0, height_true + random.gauss(-0.5, 1.5)))
        p["R0_Height20"] = maybe_null(round(h20, 1), 0.10)

        bmi20 = max(14.0, min(45.0, bmi_entry - normal_clamp(1.5, 1.5, -2, 8)))
        w20 = bmi20 * (h20 / 100) ** 2
        w20 = max(35.0, min(140.0, w20))
        p["R0_Weight20"] = maybe_null(round(w20, 1), 0.12)

        preg20 = weighted_choice([0, 1, None], [0.84, 0.10, 0.06])
        p["R0_PregAt20"] = preg20

        if preg20 == 1:
            p["R0_BMI20"] = 999
        elif p["R0_Height20"] is None or p["R0_Weight20"] is None:
            p["R0_BMI20"] = None
        else:
            h20v = p["R0_Height20"]
            w20v = p["R0_Weight20"]
            if h20v == 999 or w20v == 999:
                p["R0_BMI20"] = 999
            else:
                p["R0_BMI20"] = maybe_null(round(w20v / (h20v / 100) ** 2, 1), 0.08)

    # ── Menstrual / Menarche ───────────────────────────────────────────────
    menarche = maybe_null(int(round(normal_clamp(12.5, 1.3, 8, 18))), 0.05)
    p["R0_AgeMenarche"] = menarche

    # ── Parity ────────────────────────────────────────────────────────────
    # Parous probability increases with age
    parous_prob = min(0.72, 0.15 + max(0, age_entry - 18) * 0.028)
    never_preg  = max(0.08, 0.55 - max(0, age_entry - 18) * 0.020)
    nullip      = max(0.04, 0.22 - max(0, age_entry - 18) * 0.006)
    unk         = 0.02
    null_pr     = 0.02
    # Normalise to sum to 1
    total = parous_prob + never_preg + nullip + unk + null_pr
    parous_prob /= total; never_preg /= total; nullip /= total
    unk /= total; null_pr /= total

    parous_status = weighted_choice(
        [-1, 0, 1, 9, None],
        [never_preg, nullip, parous_prob, unk, null_pr]
    )
    p["R0_Parous"] = parous_status

    if parous_status == 1:
        parity = weighted_choice([1, 2, 3, 4, 5, 6], [0.28, 0.42, 0.19, 0.07, 0.03, 0.01])
        p["R0_Parity"] = maybe_null(parity, 0.03)

        # Age at first birth — must be ≥ menarche+2 and < age_entry
        afb_min = max(16.0, (menarche + 2) if menarche else 16.0)
        afb_max = max(afb_min + 0.5, age_entry - 0.5)   # guard lo < hi
        afb_max = min(afb_max, 44.0)
        afb_mu  = min(afb_min + 8, (afb_min + afb_max) / 2)
        afb = round(normal_clamp(afb_mu, 4, afb_min, afb_max), 1)
        p["R0_AgeBirthFirst"] = maybe_null(afb, 0.05)

        # Age at last birth — must be ≥ afb and < age_entry
        if parity == 1:
            p["R0_AgeBirthLast"] = p["R0_AgeBirthFirst"]
        else:
            alb_min = afb
            alb_max = max(alb_min + 0.5, age_entry - 0.5)
            alb_max = min(alb_max, 46.0)
            alb_mu  = min(alb_min + 3 * (parity - 1), (alb_min + alb_max) / 2)
            alb = round(normal_clamp(alb_mu, 2, alb_min, alb_max), 1)
            p["R0_AgeBirthLast"] = maybe_null(alb, 0.05)

        # Breastfeeding
        if random.random() < 0.70:
            bf_wks = round(min(1200.0, max(0.0, random.expovariate(1 / 18) * parity)), 1)
            p["R0_BreastfeedingDuration"] = maybe_null(bf_wks, 0.08)
            p["R0_Breastfed"] = maybe_null(1, 0.05)
        else:
            p["R0_BreastfeedingDuration"] = maybe_null(0.0, 0.08)
            p["R0_Breastfed"] = maybe_null(0, 0.05)

    elif parous_status in (-1, 0):
        p["R0_Parity"]               = 0 if parous_status == 0 else -1
        p["R0_AgeBirthFirst"]        = 999
        p["R0_AgeBirthLast"]         = 999
        p["R0_BreastfeedingDuration"]= 9999
        p["R0_Breastfed"]            = 999
    else:
        p["R0_Parity"]               = None
        p["R0_AgeBirthFirst"]        = None
        p["R0_AgeBirthLast"]         = None
        p["R0_BreastfeedingDuration"]= None
        p["R0_Breastfed"]            = None

    # ── Menopause ─────────────────────────────────────────────────────────
    post_prob = max(0.0, (age_entry - 30) * 0.04)
    pre_prob  = max(0.02, 0.78 - post_prob)
    apost_prob= min(0.10, post_prob * 0.5)
    apre_prob = 0.04
    nev_prob  = 0.01
    null_prob2= 0.05
    ws = [post_prob, pre_prob, apost_prob, apre_prob, nev_prob, null_prob2]
    s = sum(ws); ws = [w / s for w in ws]
    menop = weighted_choice([1, 2, 3, 4, 9, None], ws)
    p["R0_Menopause"] = menop

    if menop in (1, 3):
        age_meno_lo = max(38, int(age_entry) - 15)
        age_meno_hi = min(58, int(age_entry))
        if age_meno_lo >= age_meno_hi:
            age_meno_lo = max(38, age_meno_hi - 5)
        age_meno = maybe_null(int(round(normal_clamp(49, 3, age_meno_lo, age_meno_hi))), 0.10)
        p["R0_AgeMenopause"] = age_meno
        meno_reason = weighted_choice(
            [1, 2, 3, 4, 5, 6, 7, 13],
            [0.55, 0.12, 0.10, 0.05, 0.04, 0.04, 0.05, 0.05]
        )
        p["R0_MenopauseReason"] = maybe_null(meno_reason, 0.08)
    elif menop == 9:
        p["R0_AgeMenopause"]    = None
        p["R0_MenopauseReason"] = 19
    else:
        p["R0_AgeMenopause"]    = None
        p["R0_MenopauseReason"] = maybe_null(
            weighted_choice([11, 12, 16, None], [0.10, 0.10, 0.70, 0.10]), 0.10
        )

    # ── Oral Contraceptives ───────────────────────────────────────────────
    oc_status = weighted_choice([0, 1, 2, None], [0.12, 0.38, 0.44, 0.06])
    p["R0_OralContraceptiveStatus"] = oc_status

    if oc_status in (1, 2):
        oc_start_lo = max(14, int(menarche) + 1 if menarche else 14)
        oc_start_hi = min(35, int(age_entry))
        if oc_start_lo >= oc_start_hi:
            oc_start_lo = max(14, oc_start_hi - 2)
        oc_start = maybe_null(int(round(normal_clamp(18, 2.5, oc_start_lo, oc_start_hi))), 0.06)
        p["R0_AgeStartedOC"] = oc_start

        if oc_status == 1:  # former
            if oc_start:
                oc_stop_lo = oc_start + 1
                oc_stop_hi = max(oc_stop_lo + 1, int(age_entry) - 1)
                oc_stop = round(normal_clamp(oc_start + 5, 3, oc_stop_lo, oc_stop_hi), 1)
            else:
                oc_stop = None
            p["R0_AgeLastUsedOC"] = maybe_null(oc_stop, 0.06)
            oc_len = round(max(0.0, oc_stop - oc_start), 1) if (oc_stop and oc_start) else None
            p["R0_OCLength"] = maybe_null(oc_len, 0.06)
        else:  # current
            p["R0_AgeLastUsedOC"] = 999
            oc_len = round(max(0.0, age_entry - oc_start), 1) if oc_start else None
            p["R0_OCLength"] = maybe_null(oc_len, 0.06)
    else:
        p["R0_AgeStartedOC"]  = 999
        p["R0_AgeLastUsedOC"] = 999
        p["R0_OCLength"]      = maybe_null(0.0, 0.05)

    # ── HRT ───────────────────────────────────────────────────────────────
    if menop in (1, 3):
        hrt_status = weighted_choice([0, 1, 2, None], [0.40, 0.30, 0.25, 0.05])
    else:
        hrt_status = weighted_choice([0, 1, 2, None], [0.88, 0.05, 0.02, 0.05])
    p["R0_HRTStatus"] = hrt_status

    if hrt_status in (1, 2):
        hrt_lo = max(38, int(age_entry) - 15)
        hrt_hi = min(62, int(age_entry))
        if hrt_lo >= hrt_hi:
            hrt_lo = max(38, hrt_hi - 2)
        hrt_start = maybe_null(int(round(normal_clamp(50, 4, hrt_lo, hrt_hi))), 0.08)
        p["R0_HRTStartAge"] = hrt_start

        if hrt_status == 1:  # former
            if hrt_start:
                hs_lo = hrt_start + 1
                hs_hi = max(hs_lo + 1, int(age_entry))
                hrt_stop = int(round(normal_clamp(hrt_start + 5, 3, hs_lo, hs_hi)))
            else:
                hrt_stop = None
            p["R0_HRTStopAge"]  = maybe_null(hrt_stop, 0.08)
            hrt_dur = round(max(0.0, hrt_stop - hrt_start), 1) if (hrt_stop and hrt_start) else None
            p["R0_HRTDuration"] = maybe_null(hrt_dur, 0.08)
        else:
            p["R0_HRTStopAge"]  = None
            hrt_dur = round(max(0.0, age_entry - hrt_start), 1) if hrt_start else None
            p["R0_HRTDuration"] = maybe_null(hrt_dur, 0.08)
    else:
        p["R0_HRTStartAge"] = None
        p["R0_HRTStopAge"]  = None
        p["R0_HRTDuration"] = None

    # ── Alcohol ───────────────────────────────────────────────────────────
    alc_status = weighted_choice([0, 1, 2, None], [0.07, 0.18, 0.70, 0.05])
    p["R0_AlcoholStatus"] = alc_status

    if alc_status in (1, 2):
        alc_start_hi = max(15, int(age_entry) - 1)
        alc_start = maybe_null(int(round(normal_clamp(18, 2, 14, alc_start_hi))), 0.06)
        p["R0_AgeStartedDrinking"] = alc_start

        if alc_status == 1:  # former
            if alc_start:
                as_hi = max(alc_start + 1, int(age_entry) - 1)
                alc_stop = int(round(normal_clamp(alc_start + 8, 5, alc_start + 1, as_hi)))
            else:
                alc_stop = None
            p["R0_AgeStoppedDrinking"]  = maybe_null(alc_stop, 0.06)
            p["R0_AlcoholUnitsPerWeek"] = None
        else:
            p["R0_AgeStoppedDrinking"]  = None
            units = round(min(80.0, max(0.0, random.expovariate(1 / 9))), 1)
            p["R0_AlcoholUnitsPerWeek"] = maybe_null(units, 0.06)
    else:
        p["R0_AgeStartedDrinking"]  = None
        p["R0_AgeStoppedDrinking"]  = None
        p["R0_AlcoholUnitsPerWeek"] = None

    # ── Smoking ───────────────────────────────────────────────────────────
    smk_status = weighted_choice([0, 1, 2, None], [0.48, 0.32, 0.16, 0.04])
    p["R0_SmokingStatus"] = smk_status

    if smk_status in (1, 2):
        smk_start_hi = max(13, int(age_entry) - 1)
        smk_start = maybe_null(int(round(normal_clamp(17, 2.5, 12, smk_start_hi))), 0.06)
        p["R0_AgeStartedSmoking"] = smk_start

        if smk_status == 1:  # former
            if smk_start:
                ss_hi = max(smk_start + 1, int(age_entry) - 1)
                smk_stop = int(round(normal_clamp(smk_start + 10, 6, smk_start + 1, ss_hi)))
            else:
                smk_stop = None
            p["R0_AgeStoppedSmoking"] = maybe_null(smk_stop, 0.06)
            p["R0_CigsPerDay"]        = None
            cpd_hist = normal_clamp(12, 6, 1, 40)
            yrs = max(0, (smk_stop - smk_start)) if (smk_stop and smk_start) else 0
            p["R0_PackYears"] = maybe_null(round((cpd_hist / 20) * yrs, 1), 0.08)
        else:
            p["R0_AgeStoppedSmoking"] = None
            cpd = round(max(1.0, normal_clamp(12, 7, 1, 50)), 1)
            p["R0_CigsPerDay"] = maybe_null(cpd, 0.06)
            yrs = max(0.0, age_entry - smk_start) if smk_start else 0.0
            p["R0_PackYears"] = maybe_null(round((cpd / 20) * yrs, 1), 0.06)
    else:
        p["R0_AgeStartedSmoking"]  = None
        p["R0_AgeStoppedSmoking"]  = None
        p["R0_CigsPerDay"]         = None
        p["R0_PackYears"]          = maybe_null(0.0, 0.05)

    # ── Physical Activity (MET-hrs/week) ──────────────────────────────────
    pa = round(min(200.0, max(0.0, random.lognormvariate(math.log(25), 0.8))), 1)
    p["R0_PhysicalActivity"] = maybe_null(pa, 0.08)

    # ── Diet ──────────────────────────────────────────────────────────────
    p["R0_GreenVegDailyServings"] = maybe_null(
        weighted_choice([0, 1, 2, 3, 4, 5, 6, 7, 8],
                        [0.05, 0.15, 0.25, 0.25, 0.15, 0.08, 0.04, 0.02, 0.01]),
        0.08
    )
    p["R0_FruitDailyServings"] = maybe_null(
        weighted_choice([0, 1, 2, 3, 4, 5, 6],
                        [0.05, 0.18, 0.28, 0.24, 0.14, 0.07, 0.04]),
        0.08
    )

    # ── Medical history ───────────────────────────────────────────────────
    p["R0_BBD"] = weighted_choice([0, 1, None], [0.80, 0.15, 0.05])

    diab = weighted_choice([0, 1, None], [0.92, 0.05, 0.03])
    p["R0_DiabetesStatus"] = diab
    if diab == 1:
        diab_age_hi = max(19, int(age_entry))
        p["R0_AgeDiabetes"]     = maybe_null(int(round(normal_clamp(35, 8, 18, diab_age_hi))), 0.10)
        p["R0_DiabetesInsulin"] = maybe_null(weighted_choice([0, 1], [0.70, 0.30]), 0.10)
    else:
        p["R0_AgeDiabetes"]     = None
        p["R0_DiabetesInsulin"] = None

    # ── Family history ────────────────────────────────────────────────────
    fam_ca_n = maybe_null(
        weighted_choice([0, 1, 2, 3, 4], [0.45, 0.30, 0.15, 0.07, 0.03]),
        0.05
    )
    p["R0_FamHistCancerNum"] = fam_ca_n
    p["R0_FamHistCancer"]    = maybe_null(1 if fam_ca_n and fam_ca_n > 0 else (0 if fam_ca_n == 0 else None), 0.03)

    for field, numfield, prev in [
        ("R0_FamHistBC",   "R0_FamHistBCNum",   [0.50, 0.38, 0.12]),
        ("R0_FamHistOV",   "R0_FamHistOVNum",   [0.70, 0.25, 0.05]),
        ("R0_FamHistColo", "R0_FamHistColoNum", [0.72, 0.22, 0.06]),
        ("R0_FamHistProst","R0_FamHistProstNum",[0.75, 0.20, 0.05]),
    ]:
        if fam_ca_n and fam_ca_n > 0:
            n = weighted_choice([0, 1, 2], prev)
        else:
            n = 0
        n = maybe_null(n, 0.05)
        p[numfield] = n
        p[field]    = maybe_null((1 if n and n > 0 else 0) if n is not None else None, 0.04)

    return p


# ---------------------------------------------------------------------------
# Generate dataset
# ---------------------------------------------------------------------------

N = 1000
used_tcodes = set()
dataset = []

print(f"Generating {N} participants...", flush=True)
for i in range(N):
    if i % 100 == 0:
        print(f"  {i}/{N}", flush=True)
    dataset.append(generate_participant(used_tcodes))

# ---------------------------------------------------------------------------
# Write JSON (compact — no indent for smaller file; readable enough)
# ---------------------------------------------------------------------------

output_path = "/Users/mclosas/generations-demo/synthetic_data.json"
with open(output_path, "w") as f:
    json.dump(dataset, f, separators=(",", ":"))

import os
size_kb = os.path.getsize(output_path) / 1024
print(f"\n✓ {N} records → {output_path}  ({size_kb:.0f} KB)")

# Quick completeness report
all_keys = list(dataset[0].keys())
print("\n── Completeness report ────────────────────────────────────────────")
for key in all_keys:
    vals = [r.get(key) for r in dataset]
    n_null = sum(1 for v in vals if v is None)
    n_999  = sum(1 for v in vals if v in (999, 9999))
    n_valid = N - n_null - n_999
    print(f"  {key:<35} valid={n_valid:4d}  NA={n_999:4d}  null={n_null:4d}")
