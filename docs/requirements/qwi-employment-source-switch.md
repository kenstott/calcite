# qwi_employment — config applied, DQ-validated, production reprocess pending

**Status:** the config is applied and validated end to end against the DQ bucket
(govdata-parquet-v1-dq): 51/51 states, 3,176,253 rows, 0 errors, every check below passing.
**Production still needs the purge + reprocess in the runbook, which needs go-ahead** — the old
time= partitions do not migrate, so production currently holds the old scheme.

**Four corrections the block needed before it would run.** It had never been executed:
- `state: *census_state_fips` — that anchor does not exist. Defined `all_state_abbrs_lower`
  instead, and named it for what it holds: LEHD addresses files as `/ak/qwi_ak_…`, so these are
  lowercase USPS abbreviations, not FIPS. The host 404s `AK` and serves `ak`. All 51 verified.
- `<<: *iceberg_defaults` — also undefined; expanded to the block its siblings spell out.
- `materialize.output` — absent, so the config would not build at all.
- `format`/`compression` at source level — ignored there, so the gzip body was parsed as JSON and
  failed on the magic byte. They belong under `response:` as `format`/`compressed`, which is what
  lodes_workplace already does for the same host.
- `rawCache` — absent means disabled, and with it off the fetch returns the body where a path is
  expected, so the CSV header line was parsed as an S3 URI. Required, not optional, here.

**Why this is staged rather than applied:** it changes the partition scheme from `[type, time]` to
`[type, state]`. Applying the config alone would have the table writing new-scheme partitions
alongside the existing old-scheme ones. The config change, the purge and the reprocess must land
together, and the purge needs explicit go-ahead.

## Runbook

```bash
# 1. apply the block below to govdata/src/main/resources/census/census-schema.yaml
# 2. purge the table (the old time= partitions do not migrate)
GOVDATA_DQ_BUCKET=govdata-parquet-v1-dq ./govdata/scripts/data_purge.sh --schema census --table qwi_employment
# 3. full reprocess — 51 files, whole history each
GOVDATA_DQ_BUCKET=govdata-parquet-v1-dq ./govdata/scripts/parallel/worker-dq-run.sh \
    --schema census --rebuild --start-year 1990
```

Validate against the DQ bucket first; `GOVDATA_DQ_BUCKET` defaults to production. Note
`--rebuild` ignores an exported `GOVDATA_START_YEAR`, so the year must be passed as a flag.

## What changes and why

| | Before | After |
|---|---|---|
| Source | `api.census.gov` per (quarter, state) | one bulk CSV per state |
| Fetches per release | ~4,700 | **51** |
| Columns materialized | 5 | **81** |
| Suppression flags | none | **32** |
| Demographic breakouts | collapsed away | `sex, agegrp, race, ethnicity, education, firmage, firmsize` |
| Partition | `[type, time]` | `[type, state]` |
| Lookback | — | **none** (see below) |

**No lookback, deliberately.** Census rebuilds the *complete* QWI series on every quarterly
release and warns against combining data across releases, so "which recent periods changed?" has
no useful answer — it is always all of them. A single per-file freshness check governs instead.

**Partition `[type, state]` matches the fetch unit**, so a replace swaps exactly one state's data.
Measured: ~55,763 rows/state → 51 partitions of ~56k rows. Adding `year`/`quarter` would produce
~400-row partitions.

**The 32 `s*` flags are not optional.** Each measure has a paired suppression/reliability flag;
without them a withheld value is indistinguishable from a genuine zero.

**Types are from the QWI documentation, not inference.** `geography` is `"02"` for Alaska and
inference makes it `2`, silently breaking every join to a geography reference.

## Verification after reprocess

```sql
-- per-state coverage varies by decades and that is expected, not a defect
SELECT geography, min(year), max(year), count(*) FROM qwi_employment GROUP BY geography;
-- CA ~1991-2025; AK ends 2016 (a participation lag MSHA-style, documented in the notices PDF)
SELECT count(*) FROM qwi_employment WHERE sEmp IS NULL;  -- expect 0: flags must be populated
```

## Replacement block

```yaml
  - name: qwi_employment
    # Census rebuilds the COMPLETE QWI time series every quarterly release and advises against
    # combining data across releases, so there is no bounded set of "recently changed" periods --
    # deliberately NO lookbackPeriods here; do not add one. A per-file freshness check governs.
    # https://lehd.ces.census.gov/doc/QWI_data_notices.pdf
    pattern: "type=qwi/state=*/*.parquet"
    partitions:
      style: hive
      columnDefinitions:
        - name: type
          type: string
        - name: state
          type: string
    comment: >
      LEHD Quarterly Workforce Indicators, Sex-by-Age tabulation at state level (all industries,
      private ownership). One row per state x quarter x demographic cell. Ingested from the
      per-state bulk CSV rather than the per-quarter API: one file carries that state's whole
      history, so a release costs 51 fetches instead of ~4,700, and every published measure and
      suppression flag is retained rather than a five-variable projection.
    dimensions:
      type:
        - qwi
      state: *census_state_fips
    source:
      type: http
      url: "https://lehd.ces.census.gov/data/qwi/latest_release/{state}/qwi_{state}_sa_f_gs_ns_op_u.csv.gz"
      format: csv
      compression: gzip
    freshness:
      # HEAD the data file itself. latest_release/ is a live alias for the newest release
      # (verified 2026-08-31: byte-identical to R2026Q3), so this tracks releases without
      # hardcoding a label.
      type: last_modified
    columns:
      - {name: periodicity, type: varchar}
      - {name: seasonadj, type: varchar}
      - {name: geo_level, type: varchar}
      - {name: geography, type: varchar}
      - {name: ind_level, type: varchar}
      - {name: industry, type: varchar}
      - {name: ownercode, type: varchar}
      - {name: sex, type: varchar}
      - {name: agegrp, type: varchar}
      - {name: race, type: varchar}
      - {name: ethnicity, type: varchar}
      - {name: education, type: varchar}
      - {name: firmage, type: varchar}
      - {name: firmsize, type: varchar}
      - {name: year, type: int}
      - {name: quarter, type: int}
      - {name: agg_level, type: varchar}
      - {name: Emp, type: bigint}
      - {name: EmpEnd, type: bigint}
      - {name: EmpS, type: bigint}
      - {name: EmpTotal, type: bigint}
      - {name: EmpSpv, type: bigint}
      - {name: HirA, type: bigint}
      - {name: HirN, type: bigint}
      - {name: HirR, type: bigint}
      - {name: Sep, type: bigint}
      - {name: HirAEnd, type: bigint}
      - {name: SepBeg, type: bigint}
      - {name: HirAEndRepl, type: bigint}
      - {name: HirAEndR, type: double}
      - {name: SepBegR, type: double}
      - {name: HirAEndReplR, type: double}
      - {name: HirAS, type: bigint}
      - {name: HirNS, type: bigint}
      - {name: SepS, type: bigint}
      - {name: SepSnx, type: bigint}
      - {name: TurnOvrS, type: double}
      - {name: FrmJbGn, type: bigint}
      - {name: FrmJbLs, type: bigint}
      - {name: FrmJbC, type: bigint}
      - {name: FrmJbGnS, type: bigint}
      - {name: FrmJbLsS, type: bigint}
      - {name: FrmJbCS, type: bigint}
      - {name: EarnS, type: bigint}
      - {name: EarnBeg, type: bigint}
      - {name: EarnHirAS, type: bigint}
      - {name: EarnHirNS, type: bigint}
      - {name: EarnSepS, type: bigint}
      - {name: Payroll, type: bigint}
      - {name: sEmp, type: int}
      - {name: sEmpEnd, type: int}
      - {name: sEmpS, type: int}
      - {name: sEmpTotal, type: int}
      - {name: sEmpSpv, type: int}
      - {name: sHirA, type: int}
      - {name: sHirN, type: int}
      - {name: sHirR, type: int}
      - {name: sSep, type: int}
      - {name: sHirAEnd, type: int}
      - {name: sSepBeg, type: int}
      - {name: sHirAEndRepl, type: int}
      - {name: sHirAEndR, type: int}
      - {name: sSepBegR, type: int}
      - {name: sHirAEndReplR, type: int}
      - {name: sHirAS, type: int}
      - {name: sHirNS, type: int}
      - {name: sSepS, type: int}
      - {name: sSepSnx, type: int}
      - {name: sTurnOvrS, type: int}
      - {name: sFrmJbGn, type: int}
      - {name: sFrmJbLs, type: int}
      - {name: sFrmJbC, type: int}
      - {name: sFrmJbGnS, type: int}
      - {name: sFrmJbLsS, type: int}
      - {name: sFrmJbCS, type: int}
      - {name: sEarnS, type: int}
      - {name: sEarnBeg, type: int}
      - {name: sEarnHirAS, type: int}
      - {name: sEarnHirNS, type: int}
      - {name: sEarnSepS, type: int}
      - {name: sPayroll, type: int}
    materialize:
      enabled: true
      format: iceberg
      partition:
        columns: [type, state]
      iceberg:
        <<: *iceberg_defaults
        tableName: qwi_employment
        overwritePartitions: true
        sortOrder: [year, quarter]
```
