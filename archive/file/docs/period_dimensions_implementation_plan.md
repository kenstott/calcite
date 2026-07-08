# Period Dimensions — Implementation Plan

Companion to `period_dimensions_design.md` (the *what/why*); this is the *how/sequencing*. Addresses the gaps in that doc's "Gap analysis". All work is in the **file adapter** (`file/src/main/java/org/apache/calcite/adapter/file/`).

## Ground rules (per `file/CLAUDE.md`)

- Java 8 only — no `var`, records, switch-expressions, text blocks.
- Calendar math uses `java.time` (`LocalDate`, `YearMonth`, `IsoFields`, `WeekFields`) computed in **UTC**; never `java.sql.Time`, never local-tz day offsets.
- No `System.out`/`err`; `logger.debug("…{}", () -> …)` lazy.
- Tests: `@Tag("unit")` / `@Tag("integration")`; **numeric** temporal expectations (epoch / date-parts), never formatted date strings.
- **No behavior change for existing schemas** until they opt in. `dataset_type` defaults to `delta` = today's behavior.
- PR-sized phases, each independently green.

## Sequencing & dependencies

```
Phase 1 (period providers) ──┬─> Phase 2 (format)
                             └─> Phase 4 (dataset_type + backfill_period)
Phase 3 (freshness) ─────────────> (independent; parallel with 1/2)
Phase 5 (history/TTL) ───────────> (independent; touches 4 for TTL removal)
```

Recommended order: **1 and 3 in parallel → 2 → 4 → 5**. 1 and 3 are the two genuinely-new subsystems and don't depend on each other.

---

## Phase 1 — Standard dynamic period providers

**Goal:** calendar-aware `quarter | month | week | day | day_of_week` dimensions selected by `type:`, replacing static lists, with current-period capping and `weekYear` mode.

**New classes** (`…/etl/`):
- `CalendarPeriodProvider` — one provider parameterized by unit (quarter/month/week/day/day_of_week), implementing the resolver contract `List<String> resolve(name, cfg, context, storage)`. Reads coarser values from `context` (`{year}`, `{year,month}`). Emits **canonical zero-padded ordinals**; keeps the ordinal for math/ordering. Exposes `{period_start}`/`{period_end}` (UTC `LocalDate`) for each cell (consumed in Phase 2/4).
- Calendar helpers: leap (`Year.isLeap`), `YearMonth.lengthOfMonth`, ISO weeks (`IsoFields.WEEK_OF_WEEK_BASED_YEAR` / `WeekFields.ISO`).

**Changed:**
- `etl/DimensionType.java` — add `QUARTER, MONTH, WEEK, DAY, DAY_OF_WEEK`; `fromString` cases.
- `etl/DimensionConfig.java` — parse `weekYear: iso|calendar`; route period types to `CalendarPeriodProvider`.
- `etl/DimensionIterator.java` — resolve period types with context; enforce coarse→fine order; **cap at "now"** (no future periods; open period included).

**Config surface:**
```yaml
dimensions:
  year:  { type: yearRange, start: "${GOVDATA_START_YEAR:2010}", end: current }
  quarter: { type: quarter }
  week:  { type: week, weekYear: iso }
  day:   { type: day }
```

**Tests** (`file/src/test/.../etl/`, `@Tag("unit")`):
- `DayProvider`: Feb 2024 → 29 days, Feb 2023 → 28; 30 vs 31 months; current month capped at today (assert max day-of-month numerically).
- `WeekProvider`: a 53-week year (e.g. 2020) vs 52-week; `iso` vs `calendar` boundary (Dec 31 2024 → ISO week 1 of week-year 2025); year starting mid-week.
- `Quarter`/`Month`: current year capped at the open quarter/month.
- `DimensionIterator`: `year×month×day` cross-product is calendar-correct and context-threaded; no future cells.

**Acceptance:** a `year(yearRange)×month×day` table generates exactly the valid calendar days from start through today — leap-correct, no future days.

**Risk:** `week` (ISO 52/53 + week-year). Isolate in its own provider + heavy unit tests.

---

## Phase 2 — Dimension `format:` (API substitution only)

**Goal:** `format:` renders the period for API/URL only; marker/partition stay on the canonical ordinal.

**New/changed:**
- `etl/DimensionConfig.java` — `format` field (parse).
- `etl/PeriodFormat.java` (new) — small renderer: `%d`, `%02d`, `%b`/`%B` (month short/full name), `Q%d`, `W%02d`. Operates on the provider's ordinal.
- `etl/HttpSource.java` `substituteVariables` — support `{var}` (canonical) and `{var:fmt}` / apply the dimension's `format` for **request** rendering; also expose `{period_start}`/`{period_end}` variables.

**Tests:** `format: "%B"` → `March` in URL but `03` in partition/marker; `Q%d` → `Q3`; assert partition column stays sortable (numeric/zero-padded), never the name form.

**Acceptance:** one dimension renders `March` into a URL and `03` into the partition path in the same run.

---

## Phase 3 — Freshness subsystem (greenfield)

**Goal:** optional skip-if-unchanged for `snapshot`/`computed_delta` — skip the pull and the Iceberg commit when the source is unchanged.

**New classes** (`…/etl/freshness/`):
- `FreshnessConfig` — `type` + per-type options (`probe_url`, `version_field`, `checksum_url`/`object_metadata`, `count_probe{url,path}`, `normalize`).
- `FreshnessCheck` interface — `Token probe(ctx)` (pre-download) and/or `Token compute(body)` (post-download); `boolean changed(prev, cur)`.
- Impls: `EtagCheck`, `LastModifiedCheck`, `SizeCheck`, `VersionCheck`, `ChecksumCheck`, `CountCheck`, `HashCheck`.

**Changed:**
- `etl/HttpSource.java` / `HttpSourceConfig.java` — conditional requests (`HEAD`, `If-None-Match`, `If-Modified-Since`), capture `ETag`/`Last-Modified`/`Content-Length`; a probe-only request path.
- `partition/IncrementalTracker.java` — store/read a per-pipeline **freshness token** (high-water mark) alongside completion state.
- `etl/EtlPipeline.java` — **skip gate**: probe → compare to stored token → skip (no fetch, no commit) or proceed → store token only after a successful commit.

**Config surface:**
```yaml
freshness:
  type: count
  count_probe: { url: "…?resultsPerPage=1", path: "$.totalResults" }
```

**Tests:**
- Per type: unchanged → skip (assert no commit / no new snapshot); changed → proceed; token round-trips through the tracker.
- `etag`/`last_modified` 304 path; `HEAD` probe; `hash`/`checksum`/`size`/`count` paths; `hash` skips commit-only (body fetched).
- Integration: a snapshot table with `version` check produces **no new Iceberg snapshot** on an unchanged run.

**Acceptance:** daily-cadence snapshot whose source changes weekly creates ~1 snapshot/week, not 7.

**Risk:** highest-new-surface. Keep checks behind the interface; default **off** (absent `freshness:` = current behavior).

---

## Phase 4 — `dataset_type` + `backfill_period`

**Goal:** explicit `dataset_type: snapshot|delta|computed_delta` (default `delta`) and delta-only `backfill_period`.

**New/changed:**
- Config field `dataset_type` (default `delta`) + `backfill_period: annual|quarterly|weekly|daily` (parsed on the table/source).
- `etl/EtlPipeline.java` orchestration:
  - **delta + backfill_period:** generate period windows over `[start..current]` at the `backfill_period` granularity (Phase 1 providers + `{period_start/end}`); **closed windows fetched once** (tracker-complete), **open window re-runs** on cadence. Window granularity independent of partition grain.
  - **snapshot:** current period only + overwrite (+ Phase 3 freshness).
  - **computed_delta:** full pull → `modified`-filter → upsert by `incrementalKeys`.
- Reuse existing `incrementalKeys`/`overwritePartitions` for upsert.

**Tests:**
- delta+backfill: closed periods fetched once, open re-runs; window cap respected; routing by row value independent of fetch window.
- snapshot / computed_delta behaviors.
- **Back-compat:** schemas with no `dataset_type` behave exactly as today (delta default; declared dimensions drive the walk).

**Acceptance:** an NVD-shaped table with `dataset_type: delta, backfill_period: quarterly` emits quarterly `pubStart/End` windows and re-runs only the open quarter.

---

## Phase 5 — History (Iceberg) + deprecate TTL

**Goal:** make snapshot retention a deliberate history-depth knob; retire `incrementalTtlDays`.

**Changed:**
- `etl/MaterializeConfig.java` / `IcebergMaterializationWriter.java` — keep `snapshotRetentionDays` but **revisit the 7-day default** (the always-keep-history policy implies a long/explicit retention; document that `expireSnapshots` only runs per the configured policy). Consider a sentinel for "retain all".
- **Deprecate `incrementalTtlDays`** (`MaterializeConfig`, `IncrementalTracker.isProcessedWithTtl`): mark deprecated, ensure dimension-cache-busting (Phase 1/4) covers every former TTL use, then remove. Coordinate with consumers (cyber etc. — a govdata follow-up).
- Optional: a documented *latest-per-id* view pattern for delta tables that opt into `(id, modified)` row-versioning.

**Tests:**
- Retention: snapshots older than the policy are expired; `AS OF` time-travel within the policy returns prior state.
- TTL removal: schemas that used TTL migrate to a period dimension with equivalent refresh; no regression.

**Acceptance:** `AS OF` a prior commit returns prior state within retention; no schema references `incrementalTtlDays`.

---

## First real consumer

`cyber_vuln` is the validating consumer (it motivated all this): NVD pair = `delta` + `backfill_period: quarterly` + Phase-1 providers; CWE/KEV/advisories = `snapshot` + freshness; OSV = `computed_delta`. **That migration is a separate govdata task** gated on Phases 1–4 landing in the file adapter — it is *not* part of these framework PRs.

## Effort shape (rough)

- Phase 1: medium (calendar math + `week` edge cases dominate testing).
- Phase 2: small.
- Phase 3: medium-large (greenfield subsystem, HTTP + tracker + pipeline touchpoints).
- Phase 4: medium (orchestration over existing primitives).
- Phase 5: small-medium (mostly config + deprecation/removal).
