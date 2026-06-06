# cyber_vuln Refresh Architecture Plan

**Status:** design agreed; not yet implemented.
**Date:** 2026-06-06.
**Goal:** make every cyber_vuln table refresh correctly after initial load, using one
clean `dataset_type` + freshness per source, with history via Iceberg snapshots.

---

## 1. Problem

cyber_vuln base tables partition only by `columns: [type]` (a single static value), with
no period dimension and (mostly) no `overwritePartitions`. After the first load the
incremental tracker marks `type=<table>` complete and the delta workers get skipped, so
the catalogs go stale — new/revised CVEs and advisories never land. DQ does not catch
this because `dq-rebuild` does a full teardown + fresh fetch and never exercises the
periodic-update path.

---

## 2. Architecture principles

1. **One ETL table = one raw source.** The transformer standardizes *that source's* shape
   and nothing else. Same principle as the patents schema (each ETL table mirrors one raw
   dump, no ETL joins).
2. **Combine with SQL, not Java.** Cross-source and derived tables are **views** (or, if a
   physical copy is needed for performance, a materialized view / CTAS over the **parent
   table** — never a second pull of the parent's source).
3. **No multi-source transformers.** A transformer that fetches/merges more than one
   upstream is split into one raw table per source plus a combining view.
4. **History is an outcome of `dataset_type`,** always via Iceberg snapshots + time travel;
   depth governed by `snapshotRetentionDays`.

---

## 3. Verified engine constraints (these drive every decision)

Verified by code-reading and live probes on 2026-06-06:

- **The Iceberg writer has exactly two modes:** `append` (`IcebergTableWriter.newAppend`)
  or **replace-partition** (`replacePartitionsDataFiles`, enabled by
  `materialize.iceberg.overwritePartitions: true`; parsed at `MaterializeConfig:779`;
  already used by crime/ref/weather/census/geo/energy/fec/fedregister/lands).
- **No row-level merge / equality-delete exists** (`newRowDelta`/`equalityField` → not
  present). Read-time "accession dedup" is SEC-specific (`accessionColumn` default
  `accession_number`) and does not apply here.
- ⇒ **Refresh = re-fetch a whole partition and replace it. Partition granularity must
  equal the re-fetch unit.**
- ⇒ **`computed_delta` is not correctly supportable** for these tables: it fetches only
  changed rows, but `append` duplicates them (no read-dedup) and replace-partition deletes
  the unchanged rows. **Dropped from all plans.** Everything is `snapshot` (full-partition
  replace) or `delta` (period-partition replace of the open period).
- **Freshness skip-gate** (`EtlPipeline` Phase 3b) is **pipeline-level, single HEAD, empty
  context** — only meaningful for single-file HTTP sources. `last_modified`/`etag`/`size`
  come from the HEAD; `version` requires a body download (wrong for a pre-download gate).
- **`enrichWithPeriodBounds`** injects `period_start`/`period_end` (ISO `YYYY-MM-DD`) for
  **delta only** when `backfill_period` is set; the values reach `HttpSource.fetch`, and
  `params:` values are variable-substituted, so `pubStartDate={period_start}T00:00:00.000`
  is config-only.
- **NVD CVE 2.0:** `pubStartDate`/`pubEndDate` works (Q1-2024 → 8905 results); a
  **>120-day window returns HTTP 404**. ⇒ annual is infeasible; **quarterly is the fetch
  unit**.
- **Chained custom dimension providers are supported:**
  `DimensionResolver.resolve(name, config, context, storageProvider)` receives a context
  map of already-resolved parent dimensions **and** a `StorageProvider`, so a provider can
  source a file to drive a downstream provider (`DimensionIterator.expandWithContext`;
  in-tree precedent: `ori` resolved per `state_abbr` in crime).

---

## 4. Raw source tables (one source each)

All raw tables set `materialize.iceberg.overwritePartitions: true`.

| Raw table | Source | `dataset_type` | Dimensions | Freshness | Transformer change |
|---|---|---|---|---|---|
| `cwe_catalog` | MITRE CWE (`cwec_latest.xml.zip`, ~2 MB, ~quarterly) | `snapshot` | `type` | `last_modified` (server sends no etag) | none |
| `vulnerabilities` | NVD CVE 2.0 (API) | `delta` · `backfill_period: quarterly` | `type` + `year(yearRange 2010→current)` + `quarter`; partition `[type, year, month]` via `effectiveYearField/MonthField: published` | none (API; refresh via period re-fetch) | none (already emits `published`) |
| `kev_catalog` | CISA KEV (`known_exploited_vulnerabilities.json`, ~1.4 MB, ~2–3×/wk) | `snapshot` | `type` | `etag` | none |
| `osv_vulnerabilities` | OSV per-ecosystem `all.zip` (PyPI alone ~24 MB) | `snapshot` | `type` + **`ecosystem` (custom provider)** | none (multi-file) | **lift the `CYBER_OSV_ECOSYSTEMS` loop out into a chained custom `DimensionResolver`** |
| `advisories` | **CISA CSAF via GitHub `cisagov/CSAF`** (see §6) | `delta` · `backfill_period: annual` | `type` + `year(yearRange 2017→current)` + **custom provider on `changes.csv`** | per-file `etag` / `changes.csv` HWM | **rewrite** to parse CSAF 2.0 JSON |
| `ghsa_advisories` *(new)* | GitHub Security Advisories GraphQL | `snapshot`/`delta` | `type` | none (API) | split out of `vuln_cross_refs` |
| `mitre_cve_refs` *(new, if needed)* | MITRE CVE list | `delta` | `type` | none (API) | split out of `vuln_cross_refs` |

### Per-source notes
- **`cwe_catalog`:** confirmed via HEAD — server sends only `Last-Modified`, no `ETag`; 2 MB
  full file; single `type` partition replaced on change; one Iceberg snapshot per CWE
  release = history. Drop the existing `${GOVDATA_CURRENT_YEAR}`/`${GOVDATA_CURRENT_QUARTER}`
  pseudo-period dims.
- **`vulnerabilities`:** the only table that needs **period dimensions** — NVD's 120-day cap
  forces quarter-sized fetches. `backfill_period: quarterly` →
  `params: {pubStartDate: '{period_start}T00:00:00.000', pubEndDate: '{period_end}T23:59:59.999'}`.
  Past quarters frozen by completion markers; open quarter re-fetched and partition-replaced.
  **Known blind spot:** publication-date partitioning does not catch retroactive
  `lastModified` changes to old CVEs (needs a lastMod pass = code; "close enough" per prior
  agreement).
- **`kev_catalog`:** confirmed `etag` + `last_modified` present; small full file → snapshot.
- **`osv_vulnerabilities`:** `ecosystem` is a categorical (custom/list) dimension, not a
  period one. Promoting it to a dimension + partition lets `overwritePartitions` replace
  only the changed ecosystem; the transformer shrinks to "standardize one ecosystem's zip."

---

## 5. Derived tables → views (no ETL, no fetch)

| View | Definition | Replaces |
|---|---|---|
| `vulnerability_cwes` | `SELECT cve_id, UNNEST(cwe_ids)…` over `vulnerabilities` | a 2nd full NVD pull |
| `kev_cwes` | `UNNEST(cwes)` over `kev_catalog` | a 2nd full KEV pull |
| `vuln_cross_refs` | `UNION`/`JOIN` over `ghsa_advisories` + `mitre_cve_refs` + `osv_vulnerabilities` aliases on `cve_id` | a 3-source transformer |

If a junction must be physical for performance, make it a materialized view / CTAS over its
**parent table**, never an independent source fetch. The existing enrichment views
(`kev_enriched`, `vuln_cwe_enriched`, `kev_cwe_enriched`, `vuln_threat_chain`,
`kev_cross_threat`) continue to work, layered over the new views.

---

## 6. CISA advisories — Akamai block & solution

**Block (diagnosed):** `cisa.gov/cybersecurity-advisories/all.xml` → **403 from Akamai
bot-management** (`edgesuite.net` / `x-reference-error` signature). It blocks the dynamic
application/feed paths (UA spoofing fails — TLS/JA3 fingerprinting). Only static
`/sites/default/files/feeds/` (e.g. KEV) is allowlisted.

**Solution:** pull from CISA's official machine-readable repo **`github.com/cisagov/CSAF`**
(branch `develop`), served by GitHub (not Akamai) → 200 server-side, updated daily.

- Layout: `csaf_files/{IT,OT}/white/{year}/{advisory-id}.json` — ~11,430 CSAF 2.0 advisories,
  2017→2026. Per `white/` dir: `index.txt` (path list), `changes.csv` (`path,ISO-ts`
  changelog), `cisa-csaf-*-feed-tlp-white.json` (ROLIE feed).
- **Backfill:** one tarball fetch `codeload.github.com/cisagov/CSAF/tar.gz/refs/heads/develop`
  (~57 MB), extract all JSON.
- **Incremental:** a **custom dimension provider sources `changes.csv`** → emits only paths
  changed since the HWM → downstream provider fans out per-file raw fetch (the chained-provider
  pattern).
- **Transformer rewrite:** parse CSAF 2.0 (`document.title`, `document.tracking.*`,
  `vulnerabilities[].cve` / CVSS, `product_tree`) — strictly simpler and richer than the old
  HTML scrape + PDF fallback; CVE/CVSS/remediation come natively. Fetch TLP `white` only.
- IT vs OT: one `advisories` table with a `track` column (or partition).

---

## 7. Implementation phases (ordered)

**Status (2026-06-06): all 8 tables now carry refresh config + `overwritePartitions`, and the
code changes are in (govdata compiles clean; 8 new unit cases pass; YAML parses). NOT yet run
end-to-end — a live per-table ETL rebuild is the remaining gate. Phase 2 changed during
implementation: junctions stay materialized because a Calcite schema view cannot split a
delimited string (no `string_split`; `UNNEST` needs a real array), so `vulnerability_cwes` /
`kev_cwes` were given their parents' refresh config instead of becoming views.**

**Phase 1 — config-only wins (no transformer work):**
1. `cwe_catalog`: `dataset_type: snapshot`, `freshness: {type: last_modified}`,
   `iceberg.overwritePartitions: true`, drop CURRENT_* dims (keep `type`),
   `snapshotRetentionDays`.
2. `kev_catalog`: `dataset_type: snapshot`, `freshness: {type: etag}`,
   `iceberg.overwritePartitions: true`.
3. `vulnerabilities`: `dataset_type: delta`, `backfill_period: quarterly`, add
   `year`+`quarter` dims, `params` for `pubStartDate`/`pubEndDate`, partition
   `[type, year, month]` via `effectiveYearField/MonthField: published`,
   `iceberg.overwritePartitions: true`.

**Phase 2 — junctions → views:**
4. Delete `vulnerability_cwes` and `kev_cwes` as ETL tables; re-add as views (`UNNEST`).
   Re-point dependent enrichment views.

**Phase 3 — split multi-source `vuln_cross_refs`:**
5. New raw `ghsa_advisories` (GitHub GraphQL); optional `mitre_cve_refs`.
6. Re-define `vuln_cross_refs` as a UNION/JOIN view.

**Phase 4 — OSV fan-out:**
7. Custom `DimensionResolver` sourcing the ecosystem list; `source.url` templates
   `/{ecosystem}/all.zip`; partition `[type, ecosystem]`, `overwritePartitions`; shrink
   `OsvResponseTransformer` to one ecosystem.

**Phase 5 — advisories on CSAF:**
8. Re-point `advisories` source to `cisagov/CSAF` (tarball backfill + `changes.csv` custom
   provider); rewrite `CisaAdvisoryResponseTransformer` for CSAF 2.0.

**Per phase:** rebuild + a refresh test (backfill, then prove a new current-period record
AND a revision both land in the correct partition without duplicates).

---

## 8. Open engine follow-ups (not blockers, but enable cheaper refresh)

1. **Iceberg row-level merge (equality deletes)** → enables true `computed_delta` and lets
   `vulnerabilities` catch retroactive `lastModified` changes; would let the junctions be
   physical without full rebuilds.
2. **Per-combination freshness probe** (currently pipeline-level only) → enables per-ecosystem
   / per-file skip for OSV and advisories.

---

## 9. Related notes
- Memory: `cyber-vuln-refresh-design.md` (the durable summary of this plan).
- `period_dimensions_design.md` / `period_dimensions_implementation_plan.md` (file adapter).
- Principle alignment: patents faithful-recreation (one source per table, no ETL joins).
