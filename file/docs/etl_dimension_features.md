# File Adapter ETL — Dimension, Dataset, Freshness & History Features

Reference for the period-model features in the file-adapter ETL layer
(`file/src/main/java/org/apache/calcite/adapter/file/etl/`). Design rationale
and the staged plan live in `period_dimensions_design.md` and
`period_dimensions_implementation_plan.md`.

A table's spec separates **data sourcing** (which rows, fetched how) from
**storage layout** (partition) — the two never constrain each other.

## 1. Standard dynamic period dimensions

Declare a calendar period with `type:` instead of a static list. The provider is
calendar-correct (leap days, 30/31, ISO 52/53 weeks), **descending** (newest
period first), and **capped at the current open period** (never emits a future
period). Coarser periods are read from the resolution context.

```yaml
dimensions:
  year:    { type: yearRange, start: "${GOVDATA_START_YEAR:2010}", end: current }
  quarter: { type: quarter }
  month:   { type: month }
  week:    { type: week, weekYear: iso }   # iso | calendar
  day:     { type: day }                   # needs year + month in context
```

- Types: `quarter`, `month`, `week`, `day`, `day_of_week` (plus the existing
  `yearRange`). Declaration order is coarse→fine; `day` needs `{year, month}`,
  `quarter`/`month`/`week` need `{year}`.
- Canonical value is zero-padded and sortable (`month=03`) — used for the
  completion-marker and partition keys.
- `week` requires a `weekYear` mode: `iso` (ISO week-based-year) or `calendar`
  (calendar year + week-of-year).
- These names are the canonical completion-key slots
  (`year/quarter/month/week/day/day_of_week`), so per-period tracking works for
  free.

**Year cadence (distinct from refresh frequency).** The `year` dimension can step
at an interval via `cadenceStart` + `cadenceLength` (e.g. biennial cycles
anchored at 2010, step 2). Emitted years are the anchored series
`{cadenceStart + n·cadenceLength}` **filtered to the `[start, end]` window** — the
anchor sets cycle *alignment*, `start` is the floor, `end` the ceiling. So a
biennial-anchored-2010 schema with `start: 2011` emits 2012, 2014, 2016…. The
sub-year providers run for whichever cadence-aligned years the year dimension
emits; they never touch the year axis. (Here "cadence" = the *year-step rate*,
not how often data is refreshed.)

## 2. `format:` — API render only

`format:` (or an inline `{var:fmt}` spec in a URL/param template) renders a
period **only for the outbound request** — the canonical value still goes to the
partition/marker keys.

```yaml
month: { type: month, format: "%02d" }      # → "03" in the request
```

Inline specs override and are the active mechanism:
`{month:02d}` → `03`, `{quarter:Q}` → `Q3`, `{month:B}` → `March`,
`{month:b}` → `Mar`, `{week:W%02d}` → `W03`. Never use a name format (`%B`) as a
partition column — it breaks chronological range scans.

## 3. Dataset type

```yaml
dataset_type: snapshot | delta | computed_delta   # default: delta
```

- **delta** (default = current behavior): walk the declared period dimensions;
  the tracker skips completed partitions and re-fetches new/open ones.
- **snapshot**: full download every run; for a true snapshot with no freshness
  sniff, drive the refresh frequency with the `*_CURRENT_*` cache-bust vars.
- **computed_delta**: full dump every run, but only the rows whose `modified`
  advanced are materialized (upsert by `incrementalKeys`).

### `backfill_period` (delta only)

```yaml
backfill_period: annual | quarterly | weekly | daily
```

Sets the fetch-window granularity of the historical walk (independent of the
partition grain). Closed windows are fetched once (tracker-complete); the open
window re-runs each refresh. Must respect the source's date-range cap (e.g. NVD's
120-day limit → `quarterly` is the finest legal choice there).

## 4. Freshness check (`snapshot` / `computed_delta`)

Optional, off by default. When the source is unchanged since the last run, skip
the pull and the commit (no redundant Iceberg snapshot).

```yaml
freshness:
  type: count                       # etag | last_modified | size | version | checksum | count | hash
  count_probe: { url: "...?resultsPerPage=1", path: "$.totalResults" }
```

Per-type options: `probe_url` (etag/last_modified/size), `version_field`
(version), `checksum_url` or `object_metadata: true` (checksum),
`count_probe {url, path}` (count), `normalize` (hash). The five pre-download
checks skip the download entirely; `hash` skips only the commit (body already
fetched). `size`/`count` are weak (miss in-place revisions) — use as a fast
first pass.

## 5. History — via Iceberg snapshots (always retained)

Every ETL commit is an Iceberg snapshot; `AS OF` time-travel reconstructs any
past commit. Overwrite/upsert to latest-state is fine — prior snapshots are
retained until expired, so **history is not lost**. The single knob is snapshot
retention:

```yaml
materialize:
  iceberg:
    snapshotRetentionDays: 3650   # history depth; expireSnapshots runs per this policy
```

Row-level versioning (`(id, modified)` append) is an optional layer only when you
need efficient per-record version queries. The TTL refresh field
(`incrementalTtlDays`) is **deprecated** — use a period dimension to bust the
cache instead.

## Passthrough from govdata

govdata schema-YAML `dimensions:`/`freshness:`/`dataset_type:` flow verbatim
through `SchemaConfig.fromMap` → `EtlPipelineConfig.fromMap` →
`DimensionConfig.fromMap` (no key whitelist), so these features are declarable in
a govdata schema with no adapter-specific glue.
