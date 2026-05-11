# Health Schema — Implementation Plan

## Overview

A `health` schema covering pharmaceutical safety, clinical research, Medicaid
drug utilization, and public health surveillance. All sources verified as
publicly accessible without mandatory authentication. An optional openFDA API
key (`HEALTH_FDA_API_KEY`) raises the anonymous rate limit from 1,000 to
120,000 requests/day; required for full FAERS ingestion.

**Total planned**: 14 tables across 3 phases.

---

## API Key

| Key | Where to get | Effect |
|---|---|---|
| `HEALTH_FDA_API_KEY` | `open.fda.gov/apis/authentication` (free, instant) | openFDA: 1,000 → 120,000 req/day |

Wire as `${HEALTH_FDA_API_KEY:}` — empty default works anonymously.

---

## Phase 1 — openFDA (verified: all return HTTP 200 without key)

All five tables share the `api.fda.gov` surface, consistent `skip`/`limit`
pagination, and the same optional `api_key` header.

---

### `fda_ndc_products`

**Source**: `GET https://api.fda.gov/drug/ndc.json`  
**Total records**: ~134,500  
**PK**: `product_ndc`  
**Join hub**: Referenced by `fda_drug_recalls`, `fda_adverse_events`, `clinical_trial_interventions`, `medicaid_drug_utilization` on drug name or NDC.

| Column | Type | Source Field | Notes |
|---|---|---|---|
| `product_ndc` | string NOT NULL | `product_ndc` | e.g. `"24909-042"` |
| `generic_name` | string | `generic_name` | INN name |
| `brand_name` | string | `brand_name` | Proprietary name |
| `brand_name_base` | string | `brand_name_base` | Base brand (without suffix) |
| `labeler_name` | string | `labeler_name` | Manufacturer/distributor |
| `dosage_form` | string | `dosage_form` | e.g. `"TABLET"`, `"INJECTION"` |
| `route` | string | `route[0]` | e.g. `"ORAL"`, `"TOPICAL"` |
| `product_type` | string | `product_type` | `"HUMAN OTC DRUG"` / `"HUMAN PRESCRIPTION DRUG"` |
| `marketing_category` | string | `marketing_category` | `"NDA"`, `"ANDA"`, `"OTC MONOGRAPH DRUG"` |
| `marketing_start_date` | string | `marketing_start_date` | `"YYYYMMDD"` |
| `application_number` | string | `application_number` | e.g. `"NDA022369"` — FK to `fda_drug_approvals` |
| `rxcui` | string | `openfda.rxcui[0]` | RxNorm concept ID — FK to `rxnorm_drugs` |
| `finished` | boolean | `finished` | True = finished drug product |

---

### `fda_drug_approvals`

**Source**: `GET https://api.fda.gov/drug/drugsfda.json`  
**Total records**: ~29,000 applications  
**PK**: `application_number`

| Column | Type | Source Field | Notes |
|---|---|---|---|
| `application_number` | string NOT NULL | `application_number` | e.g. `"NDA022369"` |
| `sponsor_name` | string | `sponsor_name` | Applicant/manufacturer |
| `brand_name` | string | `openfda.brand_name[0]` | |
| `generic_name` | string | `openfda.generic_name[0]` | |
| `product_type` | string | `openfda.product_type[0]` | |
| `dosage_form` | string | `products[0].dosage_form` | From first product |
| `route` | string | `products[0].route[0]` | From first product |
| `marketing_status` | string | `products[0].marketing_status` | e.g. `"Prescription"` |
| `te_code` | string | `products[0].te_code` | Therapeutic equivalence |
| `latest_submission_type` | string | last `submissions[].submission_type` | `"ORIG"`, `"SUPPL"` |
| `latest_submission_status` | string | last `submissions[].submission_status` | `"AP"` = approved |
| `latest_submission_date` | string | last `submissions[].submission_status_date` | `"YYYYMMDD"` |
| `review_priority` | string | last `submissions[].review_priority` | `"STANDARD"`, `"PRIORITY"` |

---

### `fda_drug_recalls`

**Source**: `GET https://api.fda.gov/drug/enforcement.json`  
**PK**: `recall_number`

| Column | Type | Source Field | Notes |
|---|---|---|---|
| `recall_number` | string NOT NULL | `recall_number` | e.g. `"D-321-2016"` |
| `event_id` | string | `event_id` | |
| `status` | string | `status` | `"Terminated"`, `"Ongoing"`, `"Pending"` |
| `classification` | string | `classification` | `"Class I"`, `"Class II"`, `"Class III"` |
| `voluntary_mandated` | string | `voluntary_mandated` | `"Voluntary: Firm initiated"` etc. |
| `recalling_firm` | string | `recalling_firm` | |
| `city` | string | `city` | |
| `state` | string | `state` | |
| `country` | string | `country` | |
| `product_description` | string | `product_description` | |
| `reason_for_recall` | string | `reason_for_recall` | |
| `product_quantity` | string | `product_quantity` | |
| `distribution_pattern` | string | `distribution_pattern` | e.g. `"Nationwide"` |
| `code_info` | string | `code_info` | Lot numbers |
| `recall_initiation_date` | string | `recall_initiation_date` | `"YYYYMMDD"` |
| `report_date` | string | `report_date` | |
| `termination_date` | string | `termination_date` | Null if ongoing |

---

### `fda_adverse_events`

**Source**: `GET https://api.fda.gov/drug/event.json`  
**Total records**: ~20M+ (partition by `receive_year`)  
**PK**: `safety_report_id`  
**Note**: Transformer flattens the nested `patient.drug[]` and `patient.reaction[]` arrays; one row per report (reactions joined as comma-delimited string).

| Column | Type | Source Field | Notes |
|---|---|---|---|
| `safety_report_id` | string NOT NULL | `safetyreportid` | |
| `receive_date` | string | `receivedate` | `"YYYYMMDD"` |
| `receive_year` | string | derived from `receivedate` | Partition column |
| `serious` | string | `serious` | `"1"` = serious |
| `serious_death` | string | `seriousnessdeath` | `"1"` = resulted in death |
| `patient_age` | string | `patient.patientonsetage` | |
| `patient_age_unit` | string | `patient.patientonsetageunit` | `"801"` = years |
| `patient_sex` | string | `patient.patientsex` | `"1"` = male, `"2"` = female |
| `primary_drug` | string | `patient.drug[0].medicinalproduct` | First/suspect drug name |
| `drug_indication` | string | `patient.drug[0].drugindication` | |
| `drug_route` | string | `patient.drug[0].drugadministrationroute` | |
| `reactions` | string | `patient.reaction[].reactionmeddrapt` joined | Comma-delimited MedDRA terms |
| `reporter_country` | string | `primarysource.reportercountry` | |

---

### `fda_device_recalls`

**Source**: `GET https://api.fda.gov/device/recall.json`  
**PK**: `cfres_id`

| Column | Type | Source Field | Notes |
|---|---|---|---|
| `cfres_id` | string NOT NULL | `cfres_id` | CFRES identifier |
| `product_res_number` | string | `product_res_number` | e.g. `"Z-0001-04"` |
| `recall_status` | string | `recall_status` | `"Terminated"`, `"Ongoing"` |
| `product_code` | string | `product_code` | FDA device product classification code |
| `recalling_firm` | string | `recalling_firm` | |
| `city` | string | `city` | |
| `state` | string | `state` | |
| `product_description` | string | `product_description` | |
| `reason_for_recall` | string | `reason_for_recall` | |
| `root_cause_description` | string | `root_cause_description` | e.g. `"Design"`, `"Other"` |
| `action` | string | `action` | Corrective action taken |
| `product_quantity` | string | `product_quantity` | |
| `distribution_pattern` | string | `distribution_pattern` | |
| `k_numbers` | string | `k_numbers[0]` | 510(k) premarket notification number |
| `event_date_initiated` | string | `event_date_initiated` | `"YYYY-MM-DD"` |
| `event_date_terminated` | string | `event_date_terminated` | |

---

## Phase 2 — REST APIs with structure (all verified HTTP 200)

---

### `clinical_trials`

**Source**: `GET https://clinicaltrials.gov/api/v2/studies`  
**PK**: `nct_id`

| Column | Type | Source Field | Notes |
|---|---|---|---|
| `nct_id` | string NOT NULL | `protocolSection.identificationModule.nctId` | e.g. `"NCT03534635"` |
| `brief_title` | string | `identificationModule.briefTitle` | |
| `overall_status` | string | `statusModule.overallStatus` | `"RECRUITING"`, `"COMPLETED"`, etc. |
| `study_type` | string | `designModule.studyType` | `"INTERVENTIONAL"`, `"OBSERVATIONAL"` |
| `phase` | string | `designModule.phases[0]` | `"PHASE1"`, `"PHASE3"` etc. |
| `enrollment_count` | integer | `designModule.enrollmentInfo.count` | |
| `lead_sponsor` | string | `sponsorCollaboratorsModule.leadSponsor.name` | |
| `funder_type` | string | `sponsorCollaboratorsModule.leadSponsor.class` | `"INDUSTRY"`, `"NIH"`, etc. |
| `start_date` | string | `statusModule.startDateStruct.date` | |
| `primary_completion_date` | string | `statusModule.primaryCompletionDateStruct.date` | |
| `completion_date` | string | `statusModule.completionDateStruct.date` | |
| `first_submit_date` | string | `statusModule.studyFirstSubmitDate` | |
| `last_update_date` | string | `statusModule.lastUpdateSubmitDate` | |
| `brief_summary` | string | `descriptionModule.briefSummary` (truncated 2000) | |

---

### `clinical_trial_conditions`

**Source**: Same API — expanded from `conditionsModule.conditions[]`  
**PK**: (`nct_id`, `condition_name`)

| Column | Type | Notes |
|---|---|---|
| `nct_id` | string NOT NULL | FK → `clinical_trials` |
| `condition_name` | string NOT NULL | e.g. `"Metastatic Melanoma"` |

---

### `clinical_trial_interventions`

**Source**: Same API — expanded from `armsInterventionsModule.interventions[]`  
**PK**: (`nct_id`, `intervention_name`)

| Column | Type | Notes |
|---|---|---|
| `nct_id` | string NOT NULL | FK → `clinical_trials` |
| `intervention_type` | string | `"DRUG"`, `"DEVICE"`, `"PROCEDURE"`, etc. |
| `intervention_name` | string NOT NULL | Drug or device name — joins to `fda_ndc_products` |
| `description` | string | |

---

### `cdc_covid_vaccinations`

**Source**: `GET https://data.cdc.gov/resource/km4m-vcsb.json` (Socrata SODA)  
**PK**: (`date`, `demographic_category`)  
**Note**: Dataset covers COVID-19 vaccinations only; rename reflects actual scope.

| Column | Type | Source Field | Notes |
|---|---|---|---|
| `date` | string NOT NULL | `date` | `"YYYY-MM-DDT00:00:00.000"` |
| `demographic_category` | string NOT NULL | `demographic_category` | Race/eth, age group, sex |
| `dose1_administered` | string | `administered_dose1` | Cumulative count |
| `dose1_pct_us` | string | `administered_dose1_pct_us` | % of US population |
| `series_complete_count` | string | `series_complete_yes` | Fully vaccinated count |
| `series_complete_pct` | string | `series_complete_pop_pct` | % of population |
| `booster_count` | string | `booster_doses_yes` | Booster dose count |
| `booster_pct` | string | `booster_doses_vax_pct_agegroup` | % of vaccinated |
| `bivalent_booster_count` | string | `bivalent_booster` | Updated bivalent booster |
| `bivalent_booster_pct` | string | `bivalent_booster_pop_pct_agegroup` | |

---

### `cms_hospital_quality`

**Source**: `GET https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0`  
**PK**: `facility_id`  
**Joins**: `geo.counties` via `state` + `countyparish`; `cms_open_payments` via NPI (future).

| Column | Type | Source Field | Notes |
|---|---|---|---|
| `facility_id` | string NOT NULL | `facility_id` | CMS provider ID |
| `facility_name` | string | `facility_name` | |
| `address` | string | `address` | |
| `city` | string | `citytown` | |
| `state` | string | `state` | |
| `zip_code` | string | `zip_code` | |
| `county` | string | `countyparish` | Joins to `geo.counties` |
| `hospital_type` | string | `hospital_type` | e.g. `"Acute Care Hospitals"` |
| `hospital_ownership` | string | `hospital_ownership` | Government, proprietary, non-profit |
| `emergency_services` | string | `emergency_services` | `"Yes"` / `"No"` |
| `overall_rating` | string | `hospital_overall_rating` | 1–5 stars |
| `mort_measures_better` | string | `count_of_mort_measures_better` | |
| `mort_measures_worse` | string | `count_of_mort_measures_worse` | |
| `safety_measures_better` | string | `count_of_safety_measures_better` | |
| `safety_measures_worse` | string | `count_of_safety_measures_worse` | |
| `readm_measures_better` | string | `count_of_readm_measures_better` | |
| `readm_measures_worse` | string | `count_of_readm_measures_worse` | |
| `patient_exp_rating` | string | `patient_exp_group_measure_count` | Patient experience measure |
| `birthing_friendly` | string | `meets_criteria_for_birthing_friendly_designation` | |

---

### `medicaid_drug_utilization`

**Source**: `GET https://data.medicaid.gov/api/1/datastore/query/{dataset_id}/0`  
**Dataset IDs**: One per year (1991–present); e.g. `ae4d5347-5137-5f6c-b66c-3420fa0316d8` = 1991  
**PK**: (`state`, `ndc`, `year`, `quarter`, `utilization_type`)  
**Total records**: ~2.5M per year  
**Replaces**: Originally planned as `cms_drug_spending_partd` — Medicaid SDUD is the accessible REST equivalent.

| Column | Type | Source Field | Notes |
|---|---|---|---|
| `state` | string NOT NULL | `state` | 2-letter state code |
| `ndc` | string NOT NULL | `ndc` | 11-digit NDC — FK to `fda_ndc_products` |
| `year` | string NOT NULL | `year` | |
| `quarter` | string NOT NULL | `quarter` | 1–4 |
| `utilization_type` | string NOT NULL | `utilization_type` | `"FFSU"` = Fee-for-service |
| `product_name` | string | `product_name` | |
| `labeler_code` | string | `labeler_code` | |
| `units_reimbursed` | double | `units_reimbursed` | |
| `number_of_prescriptions` | integer | `number_of_prescriptions` | |
| `total_amount_reimbursed` | double | `total_amount_reimbursed` | USD |
| `medicaid_amount_reimbursed` | double | `medicaid_amount_reimbursed` | Federal + state share |
| `non_medicaid_amount_reimbursed` | double | `non_medicaid_amount_reimbursed` | |
| `suppression_used` | boolean | `suppression_used` | True = values suppressed for privacy |

---

## Phase 3 — High value, higher complexity

---

### `cdc_mortality`

**Source**: CDC WONDER — `POST https://wonder.cdc.gov/controller/datarequest/D76`  
**Complexity**: Requires a structured POST body specifying dimensions (cause of death ICD-10 codes, age group, sex, race, geography, year). Not a simple REST call — needs a custom `ResponseTransformer` that posts a parameterized query and parses the TSV response.  
**PK**: Composite of all requested dimensions

| Column | Type | Notes |
|---|---|---|
| `year` | string NOT NULL | |
| `state` | string NOT NULL | FIPS or name — FK to `geo.states` |
| `cause_of_death` | string NOT NULL | ICD-10 chapter/code |
| `age_group` | string | |
| `sex` | string | |
| `race` | string | |
| `deaths` | integer | Count (suppressed if <10) |
| `population` | integer | |
| `crude_rate` | double | Deaths per 100,000 |
| `age_adjusted_rate` | double | |

---

### `cdc_brfss`

**Source**: CDC BRFSS annual CSV downloads (`https://www.cdc.gov/brfss/annual_data/`)  
**Complexity**: Large SAS/CSV exports; one file per year (~400k rows). Transformer reads CSV, filters to key behavioral indicators.  
**PK**: (`year`, `state`, `question`, `response`)

| Column | Type | Notes |
|---|---|---|
| `year` | string NOT NULL | Survey year |
| `state` | string NOT NULL | State abbreviation |
| `topic` | string | e.g. `"Tobacco Use"`, `"Obesity"` |
| `question` | string NOT NULL | Survey question text |
| `response` | string NOT NULL | Response category |
| `sample_size` | integer | Respondents in cell |
| `pct` | double | Weighted percentage |
| `confidence_low` | double | 95% CI lower bound |
| `confidence_high` | double | 95% CI upper bound |

---

### `cms_open_payments`

**Source**: CMS Sunshine Act bulk CSV — `https://openpaymentsdata.cms.gov`  
**Complexity**: 10M+ rows/year; no REST API (download-only). Partition by `payment_year` + `payment_type`.  
**PK**: `record_id`

| Column | Type | Notes |
|---|---|---|
| `record_id` | string NOT NULL | |
| `payment_year` | string NOT NULL | Partition column |
| `payment_type` | string NOT NULL | `"General"`, `"Research"`, `"Ownership"` |
| `physician_npi` | string | FK to `cms_hospital_quality` (via facility) |
| `physician_first_name` | string | |
| `physician_last_name` | string | |
| `physician_specialty` | string | |
| `recipient_city` | string | |
| `recipient_state` | string | |
| `manufacturer_name` | string | Reporting entity |
| `product_name` | string | Associated drug/device |
| `product_type` | string | `"Drug"`, `"Device"`, `"Biological"` |
| `total_amount` | double | USD |
| `payment_nature` | string | e.g. `"Food and Beverage"`, `"Consulting Fee"` |
| `payment_date` | string | |

---

### `rxnorm_drugs`

**Source**: `GET https://rxnav.nlm.nih.gov/REST/drugs.json?name={name}`  
**Complexity**: Lookup-per-drug (not a bulk endpoint); populated by iterating `fda_ndc_products.generic_name` values. Rate-limited to ~20 req/sec.  
**PK**: `rxcui`

| Column | Type | Notes |
|---|---|---|
| `rxcui` | string NOT NULL | RxNorm concept unique identifier |
| `name` | string NOT NULL | Canonical drug name |
| `tty` | string | Term type: `"IN"` (ingredient), `"BN"` (brand), `"SCD"` (clinical drug), etc. |
| `synonym` | string | Alternate name |
| `language` | string | `"ENG"` |
| `suppress` | string | `"N"` = not suppressed |
| `source_name` | string | Lookup term used to retrieve this concept |

---

## Source Verification Summary

| Table | Endpoint | HTTP | Records |
|---|---|---|---|
| `fda_ndc_products` | `api.fda.gov/drug/ndc.json` | 200 ✓ | ~134,500 |
| `fda_drug_approvals` | `api.fda.gov/drug/drugsfda.json` | 200 ✓ | ~29,000 |
| `fda_drug_recalls` | `api.fda.gov/drug/enforcement.json` | 200 ✓ | ongoing |
| `fda_adverse_events` | `api.fda.gov/drug/event.json` | 200 ✓ | ~20M+ |
| `fda_device_recalls` | `api.fda.gov/device/recall.json` | 200 ✓ | ongoing |
| `clinical_trials` | `clinicaltrials.gov/api/v2/studies` | 200 ✓ | ~500k |
| `clinical_trial_conditions` | Same | 200 ✓ | derived |
| `clinical_trial_interventions` | Same | 200 ✓ | derived |
| `cdc_covid_vaccinations` | `data.cdc.gov/resource/km4m-vcsb.json` | 200 ✓ | ongoing |
| `cms_hospital_quality` | `data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0` | 200 ✓ | ~5,000 |
| `medicaid_drug_utilization` | `data.medicaid.gov/api/1/datastore/query/{id}/0` | 200 ✓ | ~2.5M/year |
| `cdc_mortality` | `wonder.cdc.gov/controller/datarequest/D76` (POST) | needs transformer | high |
| `cdc_brfss` | Annual CSV downloads | bulk download | ~400k/year |
| `cms_open_payments` | Bulk CSV only — no REST API | bulk download | ~10M/year |
| `rxnorm_drugs` | `rxnav.nlm.nih.gov/REST/drugs.json` | 200 ✓ | derived |

---

## PK / FK Relationships

Two join types are distinguished throughout:
- **Structural** — a direct key column is present; join is exact and reliable.
- **Text** — join is on a name field that is not normalized; may require `LOWER()` / `TRIM()` and will have some miss-matches due to brand vs. generic naming, abbreviations, or free-text entry.

### Within `health` schema

| FK Table | FK Column | PK Table | PK Column | Type | Cardinality |
|---|---|---|---|---|---|
| `fda_ndc_products` | `application_number` | `fda_drug_approvals` | `application_number` | Structural | Many NDC products per approval (different strengths/forms) |
| `fda_ndc_products` | `rxcui` | `rxnorm_drugs` | `rxcui` | Structural | Many NDCs may share one RxNorm concept |
| `fda_drug_recalls` | _(openfda.product_ndc)_ | `fda_ndc_products` | `product_ndc` | Structural (when populated) | openfda block is empty on ~30% of records; fall back to text |
| `fda_drug_recalls` | `recalling_firm` ≈ `labeler_name` | `fda_ndc_products` | `labeler_name` | Text | Fuzzy — normalize case before joining |
| `fda_adverse_events` | `primary_drug` ≈ `generic_name` | `fda_ndc_products` | `generic_name` | Text | FAERS drug names are reporter-entered; expect 60–80% match rate |
| `fda_adverse_events` | `primary_drug` → resolve via RxNorm | `rxnorm_drugs` | `name` | Text | Use RxNorm normalization to improve match rate |
| `medicaid_drug_utilization` | `ndc` | `fda_ndc_products` | `product_ndc` | Structural | NDC format in SDUD is 11-digit; NDC in products may need zero-padding |
| `clinical_trial_conditions` | `nct_id` | `clinical_trials` | `nct_id` | Structural | Many conditions per trial |
| `clinical_trial_interventions` | `nct_id` | `clinical_trials` | `nct_id` | Structural | Many interventions per trial |
| `clinical_trial_interventions` | `intervention_name` ≈ `generic_name` | `fda_ndc_products` | `generic_name` | Text | Trials use INN names; matches well for single-ingredient drugs |
| `clinical_trial_interventions` | `intervention_name` → resolve via RxNorm | `fda_drug_approvals` | `generic_name` | Text | Best path: trial drug → RxNorm → approval |
| `cms_open_payments` | `product_name` ≈ `brand_name` | `fda_ndc_products` | `brand_name` | Text | Payments reference brand names; normalize case |

### Across schemas

| FK Table | FK Column | PK Schema.Table | PK Column | Type |
|---|---|---|---|---|
| `medicaid_drug_utilization` | `state` | `geo.states` | `state_abbr` | Structural |
| `cms_hospital_quality` | `state` | `geo.states` | `state_abbr` | Structural |
| `cms_hospital_quality` | `zip_code` | `geo.zip_codes` | `zip_code` | Structural |
| `cms_open_payments` | `recipient_state` | `geo.states` | `state_abbr` | Structural |
| `cdc_mortality` | `state` | `geo.states` | `state_abbr` | Structural |
| `cdc_brfss` | `state` | `geo.states` | `state_abbr` | Structural |
| `cdc_covid_vaccinations` | _(no state column — demographic only)_ | — | — | — |

### Key join paths (narrative)

```
Drug safety lifecycle (all structural except FAERS):
  fda_drug_approvals
    ← fda_ndc_products          (application_number)
        ← medicaid_drug_utilization  (product_ndc = ndc)
        ← rxnorm_drugs               (rxcui)
        ≈ fda_adverse_events         (primary_drug ≈ generic_name)
        ≈ fda_drug_recalls           (via openfda.product_ndc when populated)

Clinical pipeline:
  clinical_trials
    ← clinical_trial_conditions     (nct_id)
    ← clinical_trial_interventions  (nct_id)
        ≈ fda_ndc_products           (intervention_name ≈ generic_name)
            ← fda_drug_approvals     (application_number)

Geographic cost/quality:
  cms_hospital_quality → geo.states / geo.zip_codes
  medicaid_drug_utilization → geo.states
  cms_open_payments → geo.states
```

---

## Convenience Views

### `drug_profile`
**Grain**: one row per NDC product, enriched with approval and RxNorm metadata.  
**Purpose**: Primary hub view — replaces the three-way join in 90% of queries.

```sql
SELECT
  n.product_ndc, n.generic_name, n.brand_name, n.labeler_name,
  n.dosage_form, n.route, n.product_type, n.marketing_category,
  n.marketing_start_date,
  a.application_number, a.sponsor_name, a.latest_submission_date,
  a.review_priority, a.marketing_status,
  r.rxcui, r.name AS rxnorm_name, r.tty
FROM fda_ndc_products n
LEFT JOIN fda_drug_approvals a ON n.application_number = a.application_number
LEFT JOIN rxnorm_drugs r       ON n.rxcui = r.rxcui
```

---

### `drug_recall_history`
**Grain**: one row per recall event, with product metadata when the openfda NDC is populated.  
**Purpose**: Safety audit — "which of our formulary drugs have Class I recalls?"

```sql
SELECT
  r.recall_number, r.classification, r.status,
  r.recalling_firm, r.reason_for_recall,
  r.recall_initiation_date, r.termination_date,
  r.distribution_pattern,
  n.product_ndc, n.generic_name, n.brand_name, n.product_type
FROM fda_drug_recalls r
LEFT JOIN fda_ndc_products n ON LOWER(r.recalling_firm) = LOWER(n.labeler_name)
```

---

### `adverse_event_summary`
**Grain**: one row per (drug, reaction) pair, aggregated across all FAERS reports.  
**Purpose**: Signal detection — "what reactions are most reported for a given drug?"

```sql
SELECT
  primary_drug,
  reactions,
  COUNT(*)                                    AS total_reports,
  SUM(CASE WHEN serious = '1' THEN 1 ELSE 0 END)       AS serious_reports,
  SUM(CASE WHEN serious_death = '1' THEN 1 ELSE 0 END)  AS death_reports
FROM fda_adverse_events
GROUP BY primary_drug, reactions
```

---

### `trial_drug_pipeline`
**Grain**: one row per (trial, intervention), with approval status.  
**Purpose**: Pipeline analysis — "which drugs in active Phase 3 trials are not yet approved?"

```sql
SELECT
  t.nct_id, t.brief_title, t.overall_status, t.phase,
  t.lead_sponsor, t.funder_type,
  t.start_date, t.primary_completion_date,
  i.intervention_name, i.intervention_type,
  a.application_number, a.latest_submission_status,
  a.latest_submission_date, a.review_priority
FROM clinical_trials t
JOIN clinical_trial_interventions i ON t.nct_id = i.nct_id
LEFT JOIN fda_drug_approvals a
  ON LOWER(a.generic_name) = LOWER(i.intervention_name)
WHERE i.intervention_type = 'DRUG'
```

---

### `medicaid_drug_cost`
**Grain**: one row per (generic_name, year, state), aggregated from NDC-level utilization.  
**Purpose**: Cost analysis — "what is annual Medicaid spend per drug across states?"

```sql
SELECT
  n.generic_name,
  n.labeler_name,
  u.year,
  u.state,
  SUM(u.number_of_prescriptions)       AS total_prescriptions,
  SUM(u.units_reimbursed)              AS total_units,
  SUM(u.total_amount_reimbursed)       AS total_reimbursed,
  SUM(u.medicaid_amount_reimbursed)    AS medicaid_reimbursed
FROM medicaid_drug_utilization u
JOIN fda_ndc_products n ON u.ndc = n.product_ndc
WHERE u.suppression_used = false
GROUP BY n.generic_name, n.labeler_name, u.year, u.state
```

---

### `hospital_quality_geo`
**Grain**: one row per hospital, with state-level geographic context.  
**Purpose**: Geographic quality analysis — "which states have the highest concentration of 1-star hospitals?"

```sql
SELECT
  h.facility_id, h.facility_name, h.hospital_type, h.hospital_ownership,
  h.overall_rating, h.emergency_services,
  h.mort_measures_better, h.mort_measures_worse,
  h.safety_measures_better, h.safety_measures_worse,
  h.readm_measures_better, h.readm_measures_worse,
  h.city, h.county, h.zip_code,
  s.state_name, s.region, s.division
FROM cms_hospital_quality h
LEFT JOIN geo.states s ON h.state = s.state_abbr
```

---

## Cross-Schema Joins

| Join | Via |
|---|---|
| `health` → `geo.states` | `state` / `state_abbr` on all CDC and CMS tables |
| `health` → `geo.zip_codes` | `cms_hospital_quality.zip_code` |
| `health` → `fec` | `cms_open_payments.physician_npi` → address → donor proximity matching |
| `health` → `econ` | `medicaid_drug_cost.year + state` alongside BLS healthcare sector employment |

---

## Excluded Sources

| Source | Reason |
|---|---|
| CMS Medicare Part D spending (data.cms.gov) | Portal is a JS SPA with no REST API; Medicaid SDUD covers equivalent use case |
| AHRQ HCUP | License agreement required |
| SEER cancer microdata | Registration-gated; complex SAS format |
| CDC NCHS vital statistics microdata | Registration-gated; WONDER covers aggregate use case |
| FDA food recalls | Low analytical value relative to drug/device safety |
