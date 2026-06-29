# Test Rebuild — Shared Core

The strategy common to **every** adapter we own (`file`, `govdata`, `splunk`, `sharepoint-list`). We
do not test Calcite core. Adapter-specific strategy lives alongside this file — the testable *shape*
differs per adapter:

- [file.md](file.md) — the matrix theory (format × engine × storageType × …; decomposition; seams)
- [govdata.md](govdata.md) — per-schema; recorded-HTTP fixtures; idempotence/freshness
- splunk.md, sharepoint-list.md — TBD

Ledgers: [../requirements/](../requirements/) · Plan: [plan.md](plan.md)

## 1. Why a rebuild

The legacy `file`/`govdata` suites verify *"something non-empty came back,"* not *"the table is
correct and stable."* Evidence:

- `BlsResponseTransformerTest` asserts `assertTrue(result.contains("LAUCN..."))` — a substring
  survives; passes even if a column were dropped or a decimal became a string.
- `EnergyAllTablesSmokeTest` needs a live `ENERGY_EIA_API_KEY`, hits the real EIA API, asserts
  `assertRowCount(..., 100)` — a table of garbage passes.
- govdata: 703 `assertTrue` vs 655 `assertEquals`; `CsvTypeInferenceTest` asserts
  `type == INTEGER || BIGINT` and `type == TIME || 2000` — engines diverging, hidden in assertions.

The suite is slow, weak, and combinatorial because **everything is tested through the whole
engine/JDBC stack**. None of it checks correctness or runs twice — which is why our production
defects (idempotence re-writes, resume reprocessing, etag/freshness, silent fallbacks) all slipped
through.

## 2. The golden principle (universal)

Every adapter's tests follow the same shape, proven in `file/CsvGoldenIngestTest` (4/4 green; caught
the empty-string-vs-NULL contract on its first run):

- **Hermetic** — committed fixtures, never a live API. (Live calls become a tiny separate
  `@Tag("live")` canary, not the backbone.)
- **Full-value golden** — assert the *exact* output (every cell / every produced artifact), SQL NULL
  distinct from `""`. Never `contains()`, never `count > 0`, never typed-`OR` tolerance.
- **Exact types** — the precise inferred type per column, no OR-ing.
- **Idempotent** — run the ingestion/transform twice; the second result is identical. The invariant
  the legacy suite never tested and whose absence produced the resume/re-write bugs.
- **No silent fallback** — malformed input must surface an error, not coerce to a default (CLAUDE.md
  rule #6).

### Seam testing — test up to the seam, not through the stack

Assert the artifact a component produces at its **handoff boundary**, then stop; downstream is owned
by the next layer. Most tests become pure-function goldens at a seam (bytes→catalog, document→JSON,
response→rows) — **fast, exactly assertable, and non-multiplying** — the inverse of the legacy
full-stack approach. Only the irreducible basis and a short list of cross-seam interaction cells run
the full stack. (The concrete seams differ per adapter; see the adapter docs.)

## 3. The requirements ledger (data model)

`docs/requirements/<adapter>.yaml`, one file per adapter, module-prefixed IDs. Schema and lifecycle:
[../requirements/README.md](../requirements/README.md). Retained from provisa's template, trimmed to
this domain. The ledger is both the rebuild spec and the coverage matrix. Lifecycle:
`proposed → accepted` (reverse-engineered, golden pending) `→ complete` (green golden). `supersedes`
is the archive deletion gate.

## 4. Unit of work differs per adapter

| Adapter | Unit | Reverse-engineer from |
|---|---|---|
| `file` | a matrix cell (format × engine × storageType × …) | seam code (converter/scanner/provider/format) + tests |
| `govdata` | a **schema** (econ, sec, …) | schema YAML + transformers + DQ constraints + tests |
| `splunk` / `sharepoint-list` | TBD | TBD |

## 5. Migration mechanic — strangler, not big-bang

Archive, don't delete. Legacy tests **move** out of `src/test` (a dir outside the source set → not
compiled) into a mining backlog — reversible, and it preserves the fixtures, which encode hard-won
domain knowledge. Deletion is gated: an archived test is removable only once every requirement that
`supersedes` it is `complete` with a green golden. Same end state — all-new tests on one pattern —
with no coverage gap and no lost knowledge.
