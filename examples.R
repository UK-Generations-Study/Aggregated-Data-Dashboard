# =============================================================================
# Generations Study – Synthetic Dataset: R Loading Examples
# =============================================================================
# Requires: jsonlite (install.packages("jsonlite"))
# =============================================================================

library(jsonlite)

# ── 1. Load the dataset ───────────────────────────────────────────────────────
df <- fromJSON("synthetic_data.json", flatten = TRUE)
cat("Loaded:", nrow(df), "participants,", ncol(df), "variables\n")

# ── 2. Basic inspection ───────────────────────────────────────────────────────
str(df)
head(df[, c("R0_TCode", "R0_Ethnicity", "R0_BMI", "R0_SmokingStatus")], 10)

# ── 3. Handle sentinel values (999 = Not Applicable) ─────────────────────────
# Variables with sentinels: R0_BMI, R0_BMI20, R0_WaistHipRatio,
#   R0_Height20, R0_Weight20, R0_AgeBirthFirst, R0_AgeBirthLast,
#   R0_AgeStartedOC, R0_AgeLastUsedOC, R0_Breastfed (999), R0_PregAt20 (999)
#   R0_BreastfeedingDuration (9999 = NA)

# Convert sentinels to NA for analysis
df$R0_BMI_clean         <- ifelse(df$R0_BMI         %in% c(999),  NA, df$R0_BMI)
df$R0_BMI20_clean       <- ifelse(df$R0_BMI20       %in% c(999),  NA, df$R0_BMI20)
df$R0_WaistHipRatio_c   <- ifelse(df$R0_WaistHipRatio %in% c(999), NA, df$R0_WaistHipRatio)
df$R0_BreastfeedingDur_c<- ifelse(df$R0_BreastfeedingDuration == 9999, NA, df$R0_BreastfeedingDuration)

# ── 4. Descriptive statistics ─────────────────────────────────────────────────
summary(df$R0_BMI_clean)
table(df$R0_Ethnicity, useNA = "ifany")
table(df$R0_SmokingStatus, useNA = "ifany")
table(df$R0_Menopause, useNA = "ifany")

# ── 5. Missingness overview ───────────────────────────────────────────────────
miss_pct <- sapply(df, function(x) mean(is.na(x)) * 100)
miss_df  <- data.frame(variable = names(miss_pct), missing_pct = round(miss_pct, 1))
miss_df  <- miss_df[order(-miss_df$missing_pct), ]
head(miss_df, 15)

# ── 6. Apply the sample cohort definition ─────────────────────────────────────
# Premenopausal, parous, BMI > 25, current alcohol drinkers
df_cohort <- df[
  !is.na(df$R0_Menopause)    & df$R0_Menopause    == 2  &   # Premenopausal
  !is.na(df$R0_Parous)       & df$R0_Parous        == 1  &   # Parous
  !is.na(df$R0_BMI_clean)    & df$R0_BMI_clean     > 25  &   # BMI > 25
  !is.na(df$R0_AlcoholStatus)& df$R0_AlcoholStatus == 2,     # Current drinker
]
cat("Sample cohort size:", nrow(df_cohort), "\n")

# ── 7. Simple Table 1 using tableone ─────────────────────────────────────────
# install.packages("tableone")
if (requireNamespace("tableone", quietly = TRUE)) {
  library(tableone)
  vars_t1 <- c("R0_BMI_clean", "R0_AgeMenarche", "R0_Parity",
                "R0_AlcoholUnitsPerWeek", "R0_PhysicalActivity",
                "R0_SmokingStatus", "R0_FamHistBC")
  cat_vars <- c("R0_SmokingStatus", "R0_FamHistBC")
  t1 <- CreateTableOne(vars = vars_t1, factorVars = cat_vars, data = df_cohort)
  print(t1)
}

# ── 8. Simple visualisation ───────────────────────────────────────────────────
hist(df$R0_BMI_clean, breaks = 40, main = "BMI at study entry",
     xlab = "BMI (kg/m²)", col = "#1b3d6e88", border = "white")

boxplot(R0_BMI_clean ~ R0_SmokingStatus, data = df,
        main = "BMI by smoking status",
        xlab = "Smoking status (0=Never, 1=Former, 2=Current)",
        ylab = "BMI (kg/m²)", col = c("#1b3d6e88","#c41e6e88","#00808a88"))
