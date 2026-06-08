# Health Schema Documentation

## Overview

The `health` schema provides access to U.S. health and pharmaceutical data from FDA, NIH,
CMS, and CDC. It covers FDA drug product catalogs, approvals, recalls, and adverse events;
FDA medical device recalls; clinical trial registry; CDC COVID vaccination rates; CMS hospital
quality ratings; Medicaid drug utilization and spending; CDC mortality and BRFSS risk factor
data; CMS open payments (physician payments); and the RxNorm drug concept dictionary.

The schema is served by `HealthSchemaFactory` via `GovDataSchemaFactory` and is driven by
`health-schema.yaml`.

---

## Tables

| Table | Description | Primary source | Cadence |
|---|---|---|---|
| `fda_ndc_products` | FDA National Drug Code product catalog: NDC code, generic/brand name, labeler, dosage form, route, product type, marketing category, application number, RxNorm CUI | FDA openFDA drug/ndc API | Annual |
| `fda_drug_approvals` | FDA drug application approvals (Orange Book): application number, sponsor, brand/generic name, marketing status, therapeutic equivalence code, review priority, latest submission date | FDA openFDA drug/drugsfda API | Annual |
| `fda_drug_recalls` | FDA drug enforcement/recall actions: recall number, classification (I/II/III), status, firm, reason, product description, dates | FDA openFDA drug/enforcement API | Annual |
| `fda_adverse_events` | FAERS adverse event reports: safety report ID, receive date, seriousness flags (death, hospitalization), patient demographics, primary drug and indication, drug route, MedDRA reaction terms, reporter country | FDA openFDA drug/event API | Annual |
| `fda_device_recalls` | FDA medical device recall actions: CFRES ID, product code, classification, recall type, firm, reason, dates | FDA openFDA device/enforcement API | Annual |
| `clinical_trials` | ClinicalTrials.gov registry: NCT ID, title, status, phase, enrollment, start/completion dates, sponsor, conditions, interventions, primary endpoints | ClinicalTrials.gov API v2 | Annual |
| `clinical_trial_conditions` | Clinical trial conditions (diagnoses) — normalized from `clinical_trials` | ClinicalTrials.gov API v2 | Annual |
| `clinical_trial_interventions` | Clinical trial interventions (drugs, devices, procedures) — normalized from `clinical_trials` | ClinicalTrials.gov API v2 | Annual |
| `cdc_covid_vaccinations` | CDC county-level COVID-19 vaccination rates: doses administered, % fully vaccinated, booster coverage, by date and county | CDC COVID vaccination tracker | Daily |
| `cms_hospital_quality` | CMS Hospital Compare quality ratings: overall star rating, mortality/safety/readmission/patient experience/timely-care scores, hospital type and ownership | CMS Hospital Compare bulk | Annual |
| `medicaid_drug_utilization` | Medicaid drug utilization by state, NDC, quarter: number of prescriptions, reimbursement amount, unit count | CMS Medicaid Drug Rebate bulk | Quarterly |
| `cdc_mortality` | CDC Wonder mortality statistics: deaths by state, year, cause-of-death (ICD-10 codes), age-adjusted and crude rates | CDC Wonder bulk | Annual |
| `cdc_brfss` | CDC Behavioral Risk Factor Surveillance System: state-year-question responses with % prevalence and confidence intervals for health behaviors (obesity, smoking, exercise, diabetes, etc.) | CDC BRFSS API | Annual |
| `cms_open_payments` | CMS Sunshine Act open payments: physician and hospital payments by manufacturer, nature of payment, amount | CMS Open Payments bulk | Annual |
| `rxnorm_drugs` | RxNorm drug concept dictionary: RxCUI, name, TTY (term type), drug class, active ingredients | NLM RxNorm API | Annual |

---

## Views

| View | Description | Depends on |
|---|---|---|
| `drug_profile` | Complete drug profile: NDC product joined to FDA approval and RxNorm concept; left joins preserve NDC products without approval or rxcui | `fda_ndc_products`, `fda_drug_approvals`, `rxnorm_drugs` |
| `trial_full` | Denormalised clinical trial: one row per trial with conditions and interventions aggregated into semicolon-delimited strings | `clinical_trials`, `clinical_trial_conditions`, `clinical_trial_interventions` |
| `medicaid_drug_spend` | Medicaid utilization enriched with NDC product details and RxNorm name; enables spend analysis by brand, generic, labeler, rxcui | `medicaid_drug_utilization`, `fda_ndc_products`, `rxnorm_drugs` |
| `public_health_indicators` | State-year summary combining CDC mortality totals with BRFSS risk-factor averages; rows where only one source has data appear with NULLs in the other | `cdc_mortality`, `cdc_brfss` |
| `hospital_quality_by_state` | State-level hospital quality summary: counts, average star rating, quality tier breakdown | `cms_hospital_quality` |
| `hospital_with_geography` | Hospital quality enriched with county FIPS, county name, and rural-urban classification via ZIP crosswalk | `cms_hospital_quality`, `geo.zip_county_crosswalk` |
| `medicaid_spend_by_state_geo` | Medicaid spend aggregated by state and year, enriched with state name and FIPS for choropleth mapping | `medicaid_drug_utilization`, `geo.states` |
| `mortality_brfss_by_state` | CDC mortality joined to BRFSS risk factors, both enriched with state geography; filter on `cause_name` and `question` to avoid cross-product | `cdc_mortality`, `cdc_brfss`, `geo.states` |
| `drug_manufacturer_span` | Drug manufacturers across FDA approval, NDC catalog, and Medicaid spend; one row per labeler+application | `fda_drug_approvals`, `fda_ndc_products`, `medicaid_drug_utilization` |
| `rxnorm_medicaid_bridge` | RxNorm clinical concepts connected to Medicaid spending via NDC codes | `rxnorm_drugs`, `fda_ndc_products`, `medicaid_drug_utilization` |
| `hospital_county_summary` | CMS hospital quality aggregated to county level via ZIP-county crosswalk | `cms_hospital_quality`, `geo.zip_county_crosswalk` |
| `drug_spend_trends` | Annual Medicaid drug spend aggregated from quarterly utilization, enriched with drug details | `medicaid_drug_utilization`, `fda_ndc_products` |
| `health_social_equity` | Health outcomes correlated with social determinants (income, education, demographics) via census and econ joins | `cdc_mortality`, `cdc_brfss`, `census`, `econ` |
| `weather_health_correlation` | Correlates weather events (AQI, temperature) with health outcomes by state and year | `cdc_mortality`, `cdc_brfss`, `weather.epa_annual_aqi`, `weather.ghcnd_daily` |

---

## Environment Variables

### Required

| Variable | Description |
|---|---|
| `GOVDATA_PARQUET_DIR` | Root Parquet directory |

### Optional

| Variable | Description |
|---|---|
| `GOVDATA_START_YEAR` | Lower bound for all yearRange dimensions, including FAERS adverse-events day-level windows (default `2010`) |
| `HEALTH_FDA_API_KEY` | openFDA API key — required for FAERS day-level backfill (no-key quota is 1,000 requests/day) |

---

## Sample Queries

```sql
-- Most recalled drug products by firm (last 5 years)
SELECT recalling_firm, classification,
       COUNT(*) AS recalls
FROM health.fda_drug_recalls
WHERE recall_initiation_date >= '20210101'
GROUP BY recalling_firm, classification
ORDER BY recalls DESC
LIMIT 20;

-- Top adverse event reaction terms for a drug
SELECT reactions, COUNT(*) AS reports
FROM health.fda_adverse_events
WHERE primary_drug ILIKE '%metformin%'
  AND serious_death = 1
GROUP BY reactions
ORDER BY reports DESC
LIMIT 10;

-- State obesity prevalence trend (BRFSS)
SELECT state_abbr, year, response_pct AS obesity_pct
FROM health.cdc_brfss
WHERE question ILIKE '%obese%'
ORDER BY year DESC, obesity_pct DESC;

-- Medicaid spend by drug class (latest quarter)
SELECT n.generic_name, n.dosage_form,
       SUM(m.total_amount_reimbursed) AS total_spend
FROM health.medicaid_drug_utilization m
JOIN health.fda_ndc_products n ON m.ndc = n.product_ndc
WHERE m.year = (SELECT MAX(year) FROM health.medicaid_drug_utilization)
GROUP BY n.generic_name, n.dosage_form
ORDER BY total_spend DESC
LIMIT 20;

-- Clinical trials by phase and status
SELECT phase, status, COUNT(*) AS trials
FROM health.clinical_trials
GROUP BY phase, status
ORDER BY phase, trials DESC;
```
