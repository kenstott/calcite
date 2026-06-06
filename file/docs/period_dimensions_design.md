# Period Dimensions Design

Status: design agreed, not yet implemented. Code lives in `file/src/main/java/org/apache/calcite/adapter/file/etl/` (`DimensionType`, `DimensionConfig`, `DimensionResolver`, `DimensionIterator`, `IncrementalTracker`).

This captures the decisions from the period-model design discussion so they don't have to be re-derived. It extends the broader direction recorded in the `govdata-period-model` memory (day is canonical, derive year/quarter/month, kill the global "current quarter").

A table's spec keeps **data sourcing** and **storage layout** strictly separate — **partition strategy is unrelated to how the data is sourced**:

**Data sourcing** (how rows are obtained):
1. **Period dimensions** — which calendar periods the fetch walks and the completion key tracks, via standard providers.
2. **Dataset type** — `snapshot | delta | computed_delta` (with `backfill_period` as a **delta-only** feature). **History is an outcome of the dataset type, not a separate knob — we always retain it** (§7).

**Storage layout** (independent of sourcing):
3. **Partition strategy** — `partition.columns`, chosen solely for query/storage efficiency. **Never derived from the period dimensions or the dataset type.** You can fetch in quarterly windows and partition by year, by month, or by `[type]` only; you can fetch a full snapshot and partition by the row's published year. Sourcing decides *what rows you get*; partitioning decides *how they're laid out* — the two don't constrain each other.

---

## 1. Standard dynamic period providers

Static lists (`month: {type: list, values: [1..12]}`, `day: [1..31]`) are wrong — the calendar isn't uniform (Feb 28/29, 30/31, 52/53 ISO weeks, years start mid-week). Replace them with **calendar-aware dynamic providers**, selected by `type:` on each dimension:

```yaml
dimensions:
  year:
    type: yearRange
    start: "${GOVDATA_START_YEAR:2010}"
    end: current
  quarter:
    type: quarter
  month:
    type: month
  week:
    type: week
    weekYear: iso        # or calendar
  day:
    type: day
```

Rules:
- **`type: year|quarter|month|week|day|day_of_week`** routes to a built-in provider — parallel to `type: yearRange`, distinct from `type: list` (static) and `type: custom` (per-schema resolver).
- The **key name must be the canonical slot** (`quarter`, `month`, …). The name drives (a) completion-keying (`IncrementalTracker.PERIOD_SLOTS`) and (b) which period the provider emits. Explicit `type:` is preferred over name-only inference (avoids colliding with a user's static list named `quarter`).
- **Resolved in declared order, with context.** Finer providers read coarser ones from the resolution context: `day` needs `{year, month}` (to emit 28 vs 31), `quarter`/`month` need `{year}` (to cap the current year). This is the existing `DimensionResolver.resolve(name, cfg, context, …)` seam.
- The provider computes calendar math (leap days, 30/31, ISO 52/53 weeks) — the author never hand-lists values.
- **Year bounds come from the standard knob only** (`GOVDATA_START_YEAR`, default 2010, `end: current`). No per-schema `*_START_YEAR` aliases (those were removed).

### Per-dimension `type:` chosen over a flat `period_dimensions: [...]` list

A flat `period_dimensions: ["year","quarter"]` list is more concise but can't carry per-period config. The per-dimension block wins because each period needs its own options — `week` needs `weekYear`, `year` needs `start/end`, a snapshot dim needs `mode: current`. So: per-dimension `type:`, self-describing.

---

## 2. Partial / open current period

- A provider **stops at "now"** — never emits future periods.
- The **open** current period (this quarter/month/week/day) keeps being re-emitted as it accrues data — that *is* the cache-bust.
- A **closed** period is immutable: mark complete, never re-run. The **open** period re-runs until it closes.

---

## 3. week-year decision (`weekYear: iso | calendar`)

ISO weeks don't nest under a calendar year: ISO-8601 week 1 = the week containing the first Thursday (≈ Jan 4), weeks start Monday, a year has 52 or 53 weeks, and boundary dates have a week-year ≠ calendar-year (Dec 31 2024 is ISO week 1 of 2025). So `week` must declare its mode:
- **`iso`** — `year` is the ISO week-year (clean weeks; "year" no longer matches the calendar/publish year).
- **`calendar`** — calendar-year + week-of-year (year stays the publish year; weeks 1 and 52/53 are partial at the edges).

For publish-date partitioning, **`calendar`** is usually wanted (year stays the publish year), accepting partial boundary weeks. It's a declared choice, not a default.

---

## 4. `format:` — API substitution only; markers/partitions are framework-consistent

- **`format:`** on a dimension controls **only the API/URL substitution rendering** — `"Q%d"` → `Q3`, `"%02d"` → `03`, `"%B"` → `March`. It never touches keys.
- **Completion markers** use **one consistent canonical rendering the framework fixes** (e.g. zero-padded ordinal — `year=2024/quarter=03`). Not configurable; consistency is all that matters.
- **Partition values ride the same canonical form as the marker** (so they stay sortable / range-queryable) — never the free-form `format` (a `month=March` partition breaks `BETWEEN` and chronological range scans).

So each provider holds three renderings: the **ordinal** (math, current-capping, ordering) → the **canonical string** (marker + partition) → the **`format`** (outbound request only). The provider can expose a few standard renderings for use-sites (`{month}`, `{month:02d}`, `{month:name}`, `{quarter:Q}`) so any source's URL shape is satisfied without per-schema hacks.

---

## 5. Dataset type: `dataset_type: snapshot | delta | computed_delta`

The dataset type is determined by the **source's freshness capability**. `backfill_period` (§6) is a feature of **`delta` only** — `snapshot` and `computed_delta` don't take it (a snapshot has no history to walk; `computed_delta` pulls the whole dump every time).

| Mode | Source capability | Fetch | Materialize | Period model |
|---|---|---|---|---|
| `snapshot` | no freshness sniff | full download every time | overwrite **only the open period**; closed periods frozen → point-in-time history | `*_CURRENT_*` — current period only, no walk |
| `delta` | native *changed-since* (modified/updated/window API) | fetch only what changed | append changed rows **as versions `(id, modified)`** | year-range **walk** (history) + open tip |
| `computed_delta` | full dump only, but rows carry `modified`/version | full download, framework **computes** the changed set | append the computed changed set **as versions** | no fetch walk (full dump); `modified` drives the changed set |

What each needs beyond the period dims:
- **snapshot** — nothing extra (full fetch + overwrite/current-period).
- **delta** — the changed-since mechanism: the since/window param name + the field it maps to, plus the high-water mark the tracker carries.
- **computed_delta** — the `modified`/version field (to compute "newer than last seen"), or a full-set diff fallback; plus `incrementalKeys` for the upsert.

### Default & backward compatibility

`dataset_type` does not exist in current schema YAMLs, so its **default is `delta`** — chosen because the existing engine behavior already *is* delta semantics, so no current schema breaks:

- Dimensions (year × …) already define the fetch periods; the incremental tracker already skips completed partitions and (re)fetches new/open ones — that's the `delta` "walk + incremental" model.
- Year/period-partitioned sources (fedregister monthly ZIPs, weather/fec year walks) → `delta` with the declared period dimensions as the walk.
- True-snapshot reference tables (`ref`, `econ_reference`, `cwe_catalog`) already pin a single current period via `GOVDATA_CURRENT_*` dims; under a `delta` default that still yields "single current period, re-fetched as it advances, overwrite" — the same behavior. They can be relabeled `dataset_type: snapshot` for clarity but don't break.
- `type`-only tables (the cyber staleness case) keep current behavior (fetch once, tracker-complete, no refresh). `delta` default preserves it; it does **not** auto-fix the staleness — that stays an explicit change.

`delta`'s sub-features are optional: absent `backfill_period` / a changed-since param, the declared period dimensions drive the walk (= today's behavior). `snapshot` and `computed_delta` are **opt-in** via an explicit `dataset_type`.

---

## 6. Backfill for delta: `backfill_period: annual | quarterly | weekly | daily`

The *window granularity* of the historical walk is a declarative knob, not bespoke code. `backfill_period: quarterly` → "generate quarter windows over `[start_year..current]`, expose `{period_start}/{period_end}`," and the source URL template consumes them.

- **The daily-tip asymmetry falls out for free**: closed windows are fetched once → tracker-complete → never re-run; the **open** window (current quarter) re-runs on the worker cadence (daily) until it closes, upserting by id. No custom resolver needed for the granularity asymmetry.
- **`backfill_period` (fetch-window granularity) is independent of partition grain** — you may fetch quarterly but partition by month/quarter/year.
- **Must respect the source's window cap** — NVD's 120-day limit makes `quarterly` the finest *legal* choice there. The framework can't know the cap; the author picks.

### Residual dataset-specific surface (does NOT generalize)

How you backfill a delta is ultimately a function of what historical access the source exposes:

| Source's historical access | Backfill method | Example |
|---|---|---|
| Full dump available | download the full dump (one pull) | OSV `all.zip` |
| Published-date query | walk `pubStart/End` windows (`backfill_period`) | NVD |
| Full pagination | page through everything once | GHSA |
| Only modified-since, no historical query | **no backfill possible** — history starts at adoption | many feeds |

And **dual-date sources** stay source-specific: NVD backfill by *published* date gets every CVE published in a window but **not revisions to CVEs in already-closed windows** (a 2015 CVE rescored today). Catching those needs a *separate* ongoing `lastModStartDate` pass spanning all years. `backfill_period` covers the published walk + new records; the revision-catch is a distinct delta pass.

So the residual bespoke code shrinks to: (a) map `{period_start/end}` → the source's actual param names, (b) for dual-date sources, the extra `lastMod` revision pass, (c) fall back to a full custom resolver only when a source's backfill truly doesn't fit a fixed-granularity window walk.

---

## 7. History — an outcome of the dataset type (always retained)

History is **always retained** — and Iceberg gives most of it for free, so it's not a thing each schema hand-rolls.

**Iceberg snapshots = native table-level temporal history.** Every ETL commit creates a snapshot; time-travel (`FOR TIMESTAMP AS OF …` / `FOR VERSION AS OF <snapshot-id>`, plus the `.snapshots` / `.history` metadata tables) reconstructs the table as of any past commit. Consequences:

- **Overwrite does NOT destroy history.** A replace/overwrite creates a *new* snapshot; the prior snapshot (old data) is retained until expired, so you can time-travel to the pre-overwrite state. *(Corrects an earlier claim in this doc that overwrite discards history.)*
- **Simplest model: keep the table at latest-state (overwrite/upsert) and let Iceberg snapshots be the history.** Each run = one snapshot = one point-in-time. "Current state" = just query the table; "as of date X" = time-travel to that commit. No per-period snapshot partitions and no `(id, modified)` rows are needed for table-level as-of history.
- **History depth = the snapshot-expiration policy** — that's the one knob. Set retention (keep N snapshots / N years) to the desired window; expiry bounds history, not the write mode. (So `expireSnapshots` cadence becomes a deliberate per-table setting, not a default cleanup.)

**Two history mechanisms — pick per need:**
- **Iceberg snapshots (default)** — table-level, free on every commit, time-travel. Granularity = run/commit cadence. Best for "what did the table/record look like on date X."
- **Application-level row-versioning** (`(id, modified)` append) — only when you need efficient *row-level* version queries in a single scan ("every version of CVE-X and when each changed"), which time-travel answers only by diffing across snapshots. Costs unbounded row growth — use sparingly.

**Per dataset type** (both ride on Iceberg snapshots):
- **`snapshot`** → overwrite the table each run; the Iceberg snapshot-per-run *is* the point-in-time series — no current-period partitioning needed to preserve it.
- **`delta` / `computed_delta`** → upsert changed rows to latest; Iceberg snapshots carry the as-of history. Add `(id, modified)` rows only for row-level version analytics.

### History extent is bounded by the source, not by us

Iceberg only snapshots what you **commit**, so "always retain" is the policy but how far *back* history reaches depends on backfillability (§6):

- **Backfillable** (full dump or published-date query that includes old records — NVD, OSV) → **record** history reaches back to the source's earliest data.
- **Go-forward only** — a true current-snapshot with no historical access, *or* a delta source with only a modified-since query and no historical fetch, **cannot be backfilled**. Its history starts accumulating the day you begin ingesting; the past is unrecoverable.
- **Revision history is inherently go-forward for *every* source** — you capture each version only as you observe it; no source hands you a record's *prior* versions. A record that existed before adoption appears at its *current* state, with as-of history only from adoption onward.

So a "today's list" snapshot source gives a clean **go-forward series of Iceberg snapshots** (one per run) but **no pre-adoption history** — expected, not a gap to fix.

---

## 8. Freshness check (`snapshot` / `computed_delta`)

Optional, opt-in, **off by default** (today's behavior = run every cadence). Only for `snapshot` and `computed_delta` — `delta`'s changed-since *is* its freshness. Its job: when the source hasn't changed since the last successful run, **skip the full pull and the commit**, so we neither waste bandwidth nor create a redundant Iceberg snapshot (history stays one-snapshot-per-real-change, not per run).

Rests on a distinction:
- **Freshness *sniff*** (row-level "what changed") = what enables `delta`. A true snapshot lacks it.
- **Freshness *check*** (whole-object "did anything change") = a cheaper, coarser signal a snapshot almost always has.

### `freshness.type` enum

| Value | Signal | Stage | Needs body? |
|---|---|---|---|
| `etag` | HTTP `ETag` (conditional `If-None-Match` / 304) | pre-download | no |
| `last_modified` | HTTP `Last-Modified` (`If-Modified-Since`) / file mtime | pre-download | no |
| `version` | version/date value in cheap metadata or payload head (`catalogVersion`, `dateReleased`) | pre-download | no/tiny |
| `checksum` | digest the **source provides** (sidecar `.sha256`, object `md5Hash`/`generation`) | pre-download | no |
| `size` | HTTP `Content-Length` / object size, compared to last-seen | pre-download | no |
| `count` | source record count (API `totalResults` / count call / object count), compared to last-seen | pre-download | no/tiny |
| `hash` | digest **you compute** over the downloaded content | post-download | yes |

- The six pre-download checks can **skip the download entirely**; `hash` only skips the **commit/snapshot** (body already fetched).
- `size` and `count` are the **weakest** — content can change at the same byte length, and a count is unchanged by in-place revisions or net-zero add/delete. Use them as a fast first-pass filter (great for append-mostly catalogs where new records bump the count), not where catching *revisions* matters; `checksum`/`hash` supersede them.
- Reserved for later: **`query`** — a generalized probe (e.g. `max(modified)`) for an API-shaped snapshot with no HTTP metadata, version, or count. Not needed for the current file/bulk sources.

### Mechanism

- The freshness token (etag / last-modified / version / checksum / size / content-hash) is stored in the **tracker** as a high-water mark.
- Each run: probe → compare to stored token → **skip** or **proceed** → on a successful commit, store the new token.

`freshness` is a **`type`** discriminator plus a **per-type options** block (each check needs different inputs):

```yaml
freshness:
  type: count                       # etag | last_modified | size | version | checksum | count | hash
  count_probe:                      # options are per-type
    url: "https://services.nvd.nist.gov/rest/json/cves/2.0?resultsPerPage=1&startIndex=0"
    path: "$.totalResults"
```

Per-type options:

| `type` | options |
|---|---|
| `etag` / `last_modified` / `size` | `probe_url` (optional; defaults to the source URL — uses `HEAD` / conditional GET) |
| `version` | `version_field` (path into the metadata/payload head), `probe_url` (optional, if the version lives at a different endpoint) |
| `checksum` | `checksum_url` (sidecar digest) **or** `object_metadata: true` (use storage `md5Hash`/`generation`) |
| `count` | `count_probe: { url, path }` — the cheap count call + path to the number |
| `hash` | `normalize` (optional — what to hash; default = raw body) |

### Per-source picks (cyber)

- **CWE** → `version` (`catalogVersion` / release date)
- **KEV / kev_cwes** → `version` (`dateReleased`)
- **OSV `all.zip`** → `etag` or `checksum` (GCS exposes `ETag` + `md5Hash`) — biggest win, skips a ~250 MB–1 GB pull
- **bare HTTP file, no version** → `last_modified`/`etag` via `HEAD`, else `hash` fallback

Composes with the `*_CURRENT_*` cadence: the dimension decides *when to check*; the freshness check decides *whether to actually write*.

---

## Generalizable vs dataset-specific (summary)

- **Framework (generalizable):** standard period providers (`type:`/`format:`/`weekYear:`), the `snapshot|delta|computed_delta` mode contract, `backfill_period` window-walk, completion-key/partition/overwrite-vs-append mechanics.
- **Adapter (dataset-specific):** mapping window boundaries → source API params; dual-date revision passes; backfills that don't fit fixed-granularity windows (full custom resolver fallback). The NVD published-window resolver is correctly here — dataset-specificity *should* live in the adapter.

---

## Gap analysis — existing file adapter vs this design

All of this belongs in the **file adapter** (`file/src/main/java/org/apache/calcite/adapter/file/etl/` + `…/partition/` + `…/iceberg/`), not govdata. Audit of what exists today:

| Design element | Status | Where it is / what's missing |
|---|---|---|
| Year-range provider (`type: yearRange`, `end: current`, `dataLag`, `releaseMonth`) | ✅ exists | `DimensionType.YEAR_RANGE`; `DimensionConfig` (`getEnd()==null ⇒ current`, `dataLag`, `releaseMonth`) |
| Dynamic sub-year providers (`type: quarter|month|week|day|day_of_week`) | ❌ missing | Sub-year periods are static `type: list`. No `DimensionType` entries for them, **no calendar math** (no `isLeapYear`/`lengthOfMonth`/ISO `WeekFields`), no current-period capping, no `weekYear` mode |
| Dimension `format:` (API render) | ❌ missing | No `format` field on `DimensionConfig`; padding/`Q`-prefix done today via `materialize` column SQL expressions (`SUBSTR(period,1,2)`). No `{var:fmt}` spec in `substituteVariables` |
| Canonical completion-key slots | ✅ exists | `IncrementalTracker.PERIOD_SLOTS = {year,quarter,month,week,day,day_of_week}`, `periodCompletionKey()`, `markPeriodComplete()` |
| Partition strategy (independent of sourcing) | ✅ exists | `MaterializeConfig` `partition.columns`, `batchPartitionColumns`, `effectiveYearField`/`effectiveMonthField` |
| Context-aware resolver (`day` reads `{year,month}`) | ✅ exists | `DimensionResolver.resolve(name,cfg,context,…)`, `DimensionIterator` |
| Dataset type (`snapshot|delta|computed_delta`) | ❌ missing | No explicit field; behavior is *emergent* from dimensions + tracker + `overwritePartitions` + `incrementalTtlDays`. `delta` default = today's behavior, so back-compatible |
| `backfill_period` (delta) | ❌ missing | No "fetch-window granularity" concept; the open-period-reruns rule isn't expressed |
| Freshness check (`etag|last_modified|size|version|checksum|count|hash`) | ❌ missing | **Nothing** in `HttpSource`/`HttpSourceConfig` — no conditional HTTP (`If-None-Match`/`If-Modified-Since`/304/`HEAD`), no skip-if-unchanged gate in `EtlPipeline`, no freshness token in the tracker |
| History via Iceberg snapshots (commit = snapshot; time-travel read) | ✅ exists | `IcebergEnumerator` `asOfTimestamp`/`snapshotId`/`asOfTime()`; every commit already snapshots |
| Snapshot retention as a **history-depth** knob | ⚠️ partial | `MaterializeConfig.snapshotRetentionDays` exists **but defaults to 7** (`IcebergMaterializationWriter:1869` expires older) — that's cleanup, not history. Design wants it a deliberate per-table depth setting (and the default reconsidered) |
| Row-versioning `(id, modified)` | ➖ optional | Not a framework feature; opt-in analytics layer, only if row-level version queries are needed |
| TTL refresh (`incrementalTtlDays`) | ⚠️ exists → deprecate | `MaterializeConfig.incrementalTtlDays` + `IncrementalTracker.isProcessedWithTtl()`. Design **replaces TTL with dimension-busts-cache** — to be retired |

### Net-new vs reuse (build order)

- **Net-new code:**
  1. A calendar-aware **period-provider family** (`quarter/month/week/day/day_of_week`) + `DimensionType` entries + dependency-on-context wiring. The hardest is `week` (ISO 52/53, `weekYear`).
  2. Dimension **`format:`** + a `{var:fmt}` render in `substituteVariables` (canonical form for marker/partition, `format` for API only).
  3. **`dataset_type`** + **`backfill_period`** — mostly *orchestration* over existing dimensions/tracker/materialize, not new storage primitives.
  4. A **freshness subsystem**: conditional fetch + the 7 check types in `HttpSource`, a freshness-token high-water mark in the tracker, and a skip gate in `EtlPipeline`.
- **Reuse / already there:** completion-key slots, partition + `effective*Field` fanout, resolver context, Iceberg commit-snapshots + `AS OF` time-travel, `snapshotRetentionDays`.
- **Config-only changes:** make `snapshotRetentionDays` a deliberate per-table history knob (revisit the 7-day default).
- **Removal:** deprecate `incrementalTtlDays` once dimension-cache-busting lands.

The biggest genuinely-new subsystem is **freshness** (nothing exists); the period providers are the second; everything in the dataset-type/backfill/history layers is largely *wiring over primitives that already exist*.

---

## Open questions / still to pin

- Exact config placement for layer 2 — a `mode:` vs `refresh:` vs `processing:` block; is the history choice implicit from the mode or its own explicit flag?
- Exposing `{period_start}` / `{period_end}` (canonical date bounds) as substitution variables for windowed fetches.
- Snapshot "current-period" path: a `mode: current` on a period dimension vs keeping the `GOVDATA_CURRENT_*` vars.
- **No finer current-period tokens exist** — only `GOVDATA_CURRENT_YEAR` / `_QUARTER` / `_MONTH` (no `_WEEK` / `_DAY`). So a true-snapshot that updates daily (KEV) can only bust *monthly* unless a finer current-period token is added.
- Completion-key is canonical-names-only today; making it configurable is the deferred extension (see `govdata-completion-key-extension` memory).
