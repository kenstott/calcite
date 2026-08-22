# Banking Schema Data Plan

## Implementation Status (2026-08-22)

**Status: Proposed.** Schema YAML and transformers are being authored concurrently with this
plan; nothing here has been verified against a live `banking-schema.yaml` or built output yet.
Treat every table/column list below as the target spec, not a delivered-vs-planned diff.

---

## Strategic Context

`banking` is a standalone schema, at the same tier as `sec` and `cftc` — financial-sector
institutional and market data — not a subdomain of `econ` (macro time series) or `fiscal`
(government revenue/outlay flows). `econ` answers "how is the economy doing" and `fiscal`
answers "how is government money moving"; `banking` answers a different question entirely:
"who are the regulated depository institutions, where do they operate, and how are they
performing and being complained about." That's an institutional/entity-registry question with
the same shape as `sec` (who files, what do they report) rather than a time-series or
government-flow question.

HMDA mortgage-lending data deliberately stays in the `housing` schema — a sibling `hmda_lar`
table is being added there in parallel — rather than moving into `banking`. HMDA is a
lending-by-geography question (who is originating mortgages where, at what terms, to which
applicants), which is a housing-market question, not an institutions/regulators/complaints
question. `banking` covers the institution side (FDIC-insured entities, branches, structure
changes, failures, deposits, call-report financials) and the consumer-complaint side (CFPB);
it does not carry loan-level or geography-of-lending data.

---

## Data Sources

| Source | Publisher | API / Format | Access | Cadence |
|---|---|---|---|---|
| BankFind Suite | FDIC | REST JSON (`api.fdic.gov/banks`) | Free, no key | Continuous |
| Consumer Complaint Database | CFPB | REST JSON (`consumerfinance.gov/data-research/consumer-complaints/search/api/v1/`) | Free, no key | Daily |

**Investigated and excluded:**
- **FFIEC CRA tract-level lending data** (`www.ffiec.gov`) — excluded. Every automated request
  attempted in this session (plain curl, full browser headers, multiple paths) hit a CAPTCHA
  wall. That's a hard bot-wall, not an intermittent block, and it would stop production ETL the
  same way every run. Not viable as a source until FFIEC offers a non-interactive path.
- **FDIC `demographics` endpoint** — excluded as redundant with market-context data already
  covered by `geo`/`census` (county/state population, income, and demographic aggregates).

---

## Proposed Tables

### `institutions`

One row per FDIC-insured entity — the hub table for the schema. Snapshot, not partitioned by
year; ~27,836 rows (active and inactive).

**Source:** FDIC BankFind `institutions` endpoint
**Partition:** `type`
**Auth:** None
**Cadence:** Continuous (FDIC updates as charters change); snapshot refresh
**Release window:** None — always-current snapshot, no seasonal gate

| Column | Type | Description |
|---|---|---|
| cert | VARCHAR | FDIC certificate number — primary key, hub FK target |
| name | VARCHAR | Institution legal name |
| active | BOOLEAN | Currently FDIC-insured and operating |
| charter_class | VARCHAR | Charter type (national bank, state nonmember, savings, etc.) |
| charter_agency | VARCHAR | Chartering authority |
| regulator | VARCHAR | Primary federal regulator (FDIC, OCC, Federal Reserve) |
| city | VARCHAR | Main office city |
| state_abbr | VARCHAR | Main office state — FK to geo.states |
| zip | VARCHAR | Main office ZIP |
| county_fips | VARCHAR | Main office county — FK to geo.counties |
| latitude | DOUBLE | Main office latitude |
| longitude | DOUBLE | Main office longitude |
| established_date | DATE | Charter establishment date |
| effective_date | DATE | Most recent structure-effective date |
| inactive_date | DATE | Date institution became inactive (null if active) |
| total_assets_thousands | DOUBLE | Total assets, most recent report (USD thousands) |
| total_deposits_thousands | DOUBLE | Total deposits, most recent report (USD thousands) |
| webaddr | VARCHAR | Institution website |
| specialization_group | VARCHAR | FDIC business-line specialization category |
| report_date | DATE | Most recent call-report date reflected in the snapshot |

---

### `locations`

One row per branch/office. Snapshot, not partitioned by year; ~70K+ rows nationwide.

**Source:** FDIC BankFind `locations` endpoint
**Partition:** `type`
**Auth:** None
**Cadence:** Continuous; snapshot refresh
**Release window:** None — always-current snapshot

| Column | Type | Description |
|---|---|---|
| uninum | VARCHAR | FDIC unique branch identifier — primary key |
| cert | VARCHAR | FK to institutions.cert |
| name | VARCHAR | Branch name |
| address | VARCHAR | Street address |
| city | VARCHAR | Branch city |
| state_abbr | VARCHAR | Branch state — FK to geo.states |
| zip | VARCHAR | Branch ZIP |
| county_fips | VARCHAR | Branch county — FK to geo.counties |
| established_date | DATE | Branch open date |
| branch_type | VARCHAR | Main office, full-service branch, limited-service, etc. |

---

### `history`

One row per structure-change event (branch open/close, merger, name change, charter
conversion). Back to 1934.

**Source:** FDIC BankFind `history` endpoint
**Partition:** `type`, `year` (EFFYEAR)
**Auth:** None
**Cadence:** Continuous; year-sliced delta load
**Release window:** None — no strong seasonal pattern, FDIC publishes on a rolling basis

| Column | Type | Description |
|---|---|---|
| transnum | VARCHAR | FDIC transaction identifier — primary key |
| cert | VARCHAR | FK to institutions.cert |
| effective_date | DATE | Date the structure change took effect |
| changecode | VARCHAR | FDIC structure-change code |
| changecode_label | VARCHAR | Human-readable label for changecode (open, close, merger, conversion, etc.) |

---

### `failures`

One row per bank failure resolution. ~4,115 total since 1934.

**Source:** FDIC BankFind `failures` endpoint
**Partition:** `type`, `year` (FAILYR)
**Auth:** None
**Cadence:** Continuous; year-sliced delta load
**Release window:** None. Failures are naturally sparse — some years have zero failures
nationwide, a genuinely correct result in calm banking periods, not a defect

| Column | Type | Description |
|---|---|---|
| id | VARCHAR | FDIC failure record identifier — primary key |
| cert | VARCHAR | FK to institutions.cert |
| name | VARCHAR | Failed institution name |
| city | VARCHAR | Institution city |
| state_abbr | VARCHAR | Institution state — FK to geo.states |
| fail_date | DATE | Failure resolution date |
| resolution_type | VARCHAR | Purchase and assumption, payout, etc. |
| cost_thousands | DOUBLE | Estimated cost to the Deposit Insurance Fund (USD thousands) |
| total_deposits_thousands | DOUBLE | Total deposits at failure (USD thousands) |
| total_assets_thousands | DOUBLE | Total assets at failure (USD thousands) |

---

### `sod`

One row per branch per annual Summary of Deposits survey.

**Source:** FDIC Summary of Deposits (SOD) endpoint
**Partition:** `type`, `year` (YEAR)
**Auth:** None
**Cadence:** Annual
**Release window:** None — no strong seasonal pattern

| Column | Type | Description |
|---|---|---|
| uninum | VARCHAR | FDIC unique branch identifier — unique per year, not a global primary key across years |
| cert | VARCHAR | FK to institutions.cert |
| branch_number | VARCHAR | Branch sequence number within the institution |
| deposits_thousands | DOUBLE | Branch deposits at survey date (USD thousands) |
| address | VARCHAR | Branch address |
| city | VARCHAR | Branch city |
| state_abbr | VARCHAR | Branch state — FK to geo.states |
| zip | VARCHAR | Branch ZIP |
| county_fips | VARCHAR | Branch county — FK to geo.counties |

---

### `financials`

One row per institution per quarterly call report. Huge table; capped by `dqRowLimit` in the
schema YAML (exact cap TBD by the sibling agent's YAML).

**Source:** FDIC BankFind `financials` endpoint (call report data)
**Partition:** `type`, `year` (REPDTE)
**Auth:** None
**Cadence:** Quarterly
**Release window:** None. Windowed by year, not quarter — the year dimension already bounds
each fetch, so no additional quarterly release-window gate is needed

| Column | Type | Description |
|---|---|---|
| cert | VARCHAR | FK to institutions.cert — leading component of the composite grain |
| repdte | DATE | Call-report date — trailing component of the composite grain (cert + repdte, no single-column PK) |
| total_assets_thousands | DOUBLE | Total assets (USD thousands) |
| total_deposits_thousands | DOUBLE | Total deposits (USD thousands) |
| net_income_thousands | DOUBLE | Net income for the reporting period (USD thousands) |
| equity_capital_thousands | DOUBLE | Total equity capital (USD thousands) |
| roa | DOUBLE | Return on assets |
| roe | DOUBLE | Return on equity |

---

### `summary`

Industry-wide rollup, one row per year (x state, with `state_abbr` null for the nationwide
rollup row). ~8,107 rows.

**Source:** FDIC BankFind industry-summary endpoint
**Partition:** `type`, `year` (YEAR)
**Auth:** None
**Cadence:** Annual
**Release window:** None — no strong seasonal pattern

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Report year — no single-column PK exists (grain is year x state); year is the one column guaranteed non-null on every row |
| state_abbr | VARCHAR | State — FK to geo.states; NULL is expected and correct for the nationwide rollup row, not a defect |
| num_institutions | INTEGER | Count of reporting institutions |
| total_assets_thousands | DOUBLE | Aggregate total assets (USD thousands) |
| total_deposits_thousands | DOUBLE | Aggregate total deposits (USD thousands) |
| net_income_thousands | DOUBLE | Aggregate net income (USD thousands) |

---

### `consumer_complaints`

One row per CFPB complaint, scoped to 5 bank-relevant products (checking/savings, mortgage,
credit card, money transfer, debt collection). CFPB's overall complaint volume is dominated by
credit-reporting and student-loan complaints, which are deliberately excluded here — they don't
fit a banking-institution scope. Source volume is 17.2M complaints unfiltered; this table only
ever holds the scoped subset, and is further capped by `dqRowLimit`.

**Source:** CFPB Consumer Complaint Database search API
**Partition:** `type`, `year`, `state`, `product` (5 bank-relevant values)
**Auth:** None
**Cadence:** Daily (CFPB updates continuously); this table only refreshes via its year/state/product delta windows
**Release window:** None — delta windows on year/state/product already bound each fetch

| Column | Type | Description |
|---|---|---|
| complaint_id | VARCHAR | CFPB complaint identifier — primary key |
| date_received | DATE | Date CFPB received the complaint |
| product | VARCHAR | Scoped to checking/savings, mortgage, credit card, money transfer, or debt collection |
| sub_product | VARCHAR | CFPB sub-product classification |
| issue | VARCHAR | CFPB issue classification |
| company | VARCHAR | Institution named in the complaint |
| state_abbr | VARCHAR | Consumer state — FK to geo.states |
| zip | VARCHAR | Consumer ZIP (CFPB does not expose county-level geography) |
| timely_response | BOOLEAN | Whether the company responded within CFPB's timeliness standard |
| consumer_disputed | BOOLEAN | Whether the consumer disputed the company's response |

---

## Join Architecture

```
banking.institutions (cert)
    ├── banking.locations (cert, uninum)
    ├── banking.history (cert, transnum, year)
    ├── banking.failures (cert, id, year)
    ├── banking.sod (cert, uninum, year)
    └── banking.financials (cert, repdte, year)

geo.counties (county_fips) / geo.states (state_abbr)
    ├── banking.institutions (county_fips, state_abbr)
    ├── banking.locations (county_fips, state_abbr)
    ├── banking.sod (county_fips, state_abbr)
    ├── banking.history (via institutions.cert -> county_fips/state_abbr)
    └── banking.consumer_complaints (state_abbr only — CFPB source has no county-level geography)
```

`institutions.cert` is the hub — `locations`, `history`, `failures`, `sod`, and `financials` all
FK to it. `institutions`, `locations`, `sod`, and `history` (via the institution) carry
`county_fips`/`state_abbr` for joining to `geo.counties`/`geo.states`. `consumer_complaints`
joins to `geo` via `state_abbr` only, since CFPB does not expose county-level geography.

**Not built, flagged as a natural follow-up:** no FK from `banking` to `sec` for bank holding
companies that also file with the SEC. That join (`institutions.cert` <-> `sec` CIK, via an
external bank-holding-company crosswalk) is out of scope for this pass.

---

## Worker Assignment

`banking` uses the existing split-schema worker pattern (like `housing`/`disasters`/`ag`) — not
a dedicated worker script. `worker.sh`'s generic
`housing|transport|environment|ag|disasters|fiscal|census|banking)` case arm and
`build_inline_model()` are already schema-name-generic, so `banking` requires zero new
worker-script code: only its name needed adding to that pipe-list (already done) and to
`run-pool.sh`'s six queue blocks (already done).

**Table split:**
- `:once` snapshot slot — `institutions`, `locations`
- Year-sliced tables — `history`, `failures`, `sod`, `financials`, `summary`,
  `consumer_complaints`

---

## Release Windows

| Table | Window | Rationale |
|---|---|---|
| `institutions` | None | Always-current snapshot |
| `locations` | None | Always-current snapshot |
| `history` | None | Annual, no strong seasonal pattern — FDIC publishes on a rolling basis |
| `failures` | None | Same as `history`; a zero-failure year is expected, not a gap to gate around |
| `sod` | None | Same as `history` |
| `financials` | None | Quarterly call-report cycle, but windowed by year not quarter — the year dimension already bounds each fetch |
| `summary` | None | Same as `history` |
| `consumer_complaints` | None | CFPB updates daily; this table only refreshes via its year/state/product delta windows |

---

## Environment Variables

None beyond the universal `${GOVDATA_PARQUET_DIR}`, `${GOVDATA_CACHE_DIR}`, and
`${GOVDATA_START_YEAR}` — neither FDIC BankFind nor the CFPB Consumer Complaint Database
requires an API key.
