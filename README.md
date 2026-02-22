# Generations Study – Data Dashboard (Demo)

A browser-based prototype for exploring the Generations Study dataset.
All processing occurs **locally in your browser** — no data is ever transmitted externally.

---

## Quick Start

Open the dashboard directly in your browser — no installation needed:

> **https://uk-generations-study.github.io/data-dashboard-demo/**

Then:
1. Drag-and-drop `synthetic_data.json` onto the **Data File** drop zone
2. Drag-and-drop `DerivedVariables_Schema.json` onto the **Schema File** drop zone (enriches variable labels, types, units, and groupings)
3. The dashboard loads immediately

### Repository contents

```
├── index.html                   ← Dashboard (open locally as an alternative)
├── app.js                       ← Dashboard logic
├── synthetic_data.json          ← 1000-participant synthetic dataset
├── DerivedVariables_Schema.json ← JSON Schema (variable labels, types, codes, units)
├── sample_cohort.json           ← Example cohort definition (JSON)
├── generate_data.py             ← Python script that produced the dataset
├── examples.R                   ← R loading and analysis examples
├── examples.py                  ← Python loading and analysis examples
└── README.md                    ← This file
```

> **Note:** Chart.js is loaded from CDN (`cdn.jsdelivr.net`). An internet connection is required for the initial page load. Once loaded, all functionality works offline.

---

## Dashboard Features

| Tab | What it does |
|---|---|
| **Overview** | Dataset summary — participant count, completeness, variable groups |
| **Explore** | Click any variable for histogram / bar chart / violin plot + summary stats; download chart as PNG |
| **Missingness** | Horizontal bar chart of null % and sentinel NA % for all variables |
| **Stratified** | Compare a numeric variable's distribution across groups of a categorical variable |
| **Cohort Builder** | Build AND/OR filter chains with live cohort preview; export cohort as JSON; view attrition flow with PNG export |
| **Descriptive** | Summary statistics table for the active cohort; optional stratification; CSV export |

### Sidebar

- **Search box** — filter variables by name or description
- **Group buttons** — filter by variable group (e.g. Body, Reproductive, Lifestyle, Medical, Family History)
- **Click any variable** — jumps to Explore tab with that variable selected

### Schema file (optional)

Loading a JSON Schema file (Draft 2020-12) enriches the dashboard with:
- Human-readable variable labels and descriptions
- Correct variable types (numeric, integer, categorical, binary)
- Code labels (e.g. 0 = No, 1 = Yes)
- Units shown on chart axes
- Variable groupings for the sidebar filter buttons
- Sentinel value definitions (999 / 9999 = Not Applicable)

---

## Privacy

All data handling occurs entirely within your browser via the browser's `FileReader` API.
No participant data is uploaded, transmitted, or stored anywhere outside your device.
The only external network requests are to load the Chart.js library from CDN on first page load.

---

## Synthetic Dataset

- **1000 participants**, fields drawn from the Generations Study variable schema
- Realistic epidemiological distributions (BMI ~N(24.5, 4.5), smoking ~16%, etc.)
- Logical coherence maintained: BMI derived from height/weight; parity consistent with `R0_Parous`; HRT confined to older/postmenopausal participants; etc.
- Plausible missingness (~3–15% per variable; higher for age-conditional variables)
- Sentinel values (999 / 9999) preserved exactly as the schema defines them
- No real participant data — all values are synthetic

### Regenerate

```bash
python3 generate_data.py
```

---

## Variable Types

| Type pill | Meaning |
|---|---|
| `numeric` | Continuous measurement (e.g. BMI, physical activity) |
| `integer` | Whole-number count or age |
| `categorical` | Coded integer with labelled categories |
| `binary` | 0/1 flag |

### Sentinel values

Some variables use `999` (or `9999`) to mean "Not Applicable" — distinct from `null` (missing/unknown):

| Sentinel | Example | Meaning |
|---|---|---|
| `999` | `R0_BMI` | Pregnant at study entry — BMI not calculated |
| `999` | `R0_AgeBirthFirst` | No live birth |
| `9999` | `R0_BreastfeedingDuration` | No live birth |

Filter out sentinels before analysis: see `examples.R` / `examples.py`.

---

## Sample Cohort Definition

`sample_cohort.json` defines:

> Premenopausal + Parous + BMI > 25 + Current alcohol drinker

```json
{
  "filters": [
    { "field": "R0_Menopause",     "operator": "in", "value": "2", "logic": "AND" },
    { "field": "R0_Parous",        "operator": "in", "value": "1", "logic": "AND" },
    { "field": "R0_BMI",           "operator": ">",  "value": "25","logic": "AND" },
    { "field": "R0_AlcoholStatus", "operator": "in", "value": "2", "logic": "AND" }
  ]
}
```

Apply in **R**:
```r
df_cohort <- df[df$R0_Menopause==2 & df$R0_Parous==1 & df$R0_BMI>25 & df$R0_AlcoholStatus==2, ]
```

Apply in **Python**:
```python
mask = (df.R0_Menopause==2) & (df.R0_Parous==1) & (df.R0_BMI>25) & (df.R0_AlcoholStatus==2)
df_cohort = df[mask]
```

---

## Design Decisions

| Decision | Rationale |
|---|---|
| Single-page HTML + vanilla JS | No build step, no server, open and run |
| Chart.js via CDN | Mature, well-maintained; avoids embedding library code in the repository |
| Drag-and-drop data loading | Matches privacy requirement; data never leaves device |
| Sidebar + tab layout | Separates variable navigation from analytical views |
| Sentinel vs null distinction | Preserves schema semantics in missingness visualisation |
| Cohort export as JSON | Machine-readable; reproducible; includes full participant ID list |
| Generate-then-load architecture | Dataset is separate from app — analysts can load real data when available |
| JSON Schema support | Enriches any compatible dataset with labels, types, and groupings |

---

## Requirements

- **Dashboard:** any modern browser; internet for initial CDN load
- **generate_data.py:** Python 3.7+ (stdlib only)
- **examples.R:** R + `jsonlite` (optionally `tableone`)
- **examples.py:** Python + `pandas`, `numpy`, `matplotlib`

---

*This is a synthetic demonstration. All participant records are fictitious.*
*Schema: Generations Study variable definitions (JSON Schema Draft 2020-12).*
