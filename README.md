# Breast Cancer Now Generations Study – Aggregated Data Dashboard

A browser-based dashboard for exploring **pre-aggregated, privacy-safe summary statistics** from the Breast Cancer Now Generations Study.
No individual-level data is used or stored — all values are group-level summaries with small-cell suppression applied.

> **Live dashboard:** https://uk-generations-study.github.io/Aggregated-Data-Dashboard/

---

## Overview

This dashboard is designed for sharing aggregated study statistics without disclosing individual participant records.
It loads a single JSON file (`aggregated_data.json`) containing pre-computed summary statistics, and renders interactive charts and tables entirely in the browser.

### Privacy protections built in

- **No individual records** — the JSON file contains only means, medians, quartiles, histograms, and frequency counts
- **Small-cell suppression** — any count < 5 is suppressed and displayed as `<5`
- **Suppressed strata** — any subgroup with fewer than 5 participants is omitted entirely
- **No data transmission** — all rendering happens locally in the browser

---

## Quick Start

### Online (GitHub Pages)

Open the dashboard directly — no installation needed:

> **https://uk-generations-study.github.io/Aggregated-Data-Dashboard/**

The aggregated data file is loaded automatically from the same repository.

### Local use (with auto-refresh)

If you need to update `aggregated_data.json` and preview changes locally:

```bash
./start.sh          # opens browser at http://localhost:8080/
./start.sh 9090     # custom port
```

`start.sh` starts a local HTTP server so the dashboard can fetch the JSON file (required — browsers block `fetch()` on `file://` URLs).

---

## Dashboard Tabs

| Tab | What it shows |
|---|---|
| **Introduction** | Study background, data provenance, privacy notes |
| **Overview** | Participant count, completeness, variable groups |
| **Explore** | Per-variable charts (histogram, boxplot, bar) and summary stats for the whole cohort |
| **Missingness** | Proportion missing and sentinel-NA by variable |
| **Stratified** | Compare any variable's distribution across subgroups (e.g. by menopausal status) |
| **Descriptive** | Summary statistics table, optionally stratified, with CSV export |

---

## Repository Contents

```
├── index.html                 ← Main dashboard (open this in a browser)
├── aggregated_data.json       ← Pre-aggregated summary statistics (no individual records)
├── aggregate_data.py          ← Python script that generates aggregated_data.json
├── start.sh                   ← Local HTTP server launcher (for local preview)
├── logo-g.png                 ← Study logo (dark background)
├── logo-white.png             ← Study logo (light background)
└── README.md                  ← This file
```

---

## Updating the Aggregated Data

The `aggregated_data.json` file is generated centrally from individual-level data using:

```bash
python3 aggregate_data.py input_data.json --min-cell 5
```

| Argument | Default | Description |
|---|---|---|
| `input_data.json` | *(required)* | Individual-level data file |
| `--min-cell` | `5` | Suppress counts below this threshold |

Once regenerated, replace `aggregated_data.json` in the repository. The dashboard will load the new file automatically (no code changes needed).

---

## Suppression Rules

| Level | Rule |
|---|---|
| Frequency table cell | Count < 5 → displayed as `<5` |
| Stratified subgroup | n < 5 → entire stratum omitted |
| Histogram bin | Count < 5 → bin excluded from chart; note shown on chart |

---

## Requirements

- **Dashboard:** any modern browser; internet connection for initial Chart.js CDN load
- **aggregate_data.py:** Python 3.7+ (standard library only)
- **start.sh:** Python 3 (for local HTTP server)

---

*Breast Cancer Now Generations Study — [thegenerationsstudy.co.uk](https://thegenerationsstudy.co.uk/)*
