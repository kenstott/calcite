# Plan — Attacking the Requirements

How we build out the [ledgers](../requirements/) and golden suites, per the
[strategy](strategy.md). Order favors proving each *method* on the smallest unit before scaling.

## Phases

### Phase 0 — Platform & data model — DONE
- Golden pattern proven: `file/CsvGoldenIngestTest` (4/4 green).
- Ledger schema finalized; strategy + per-adapter docs written.

### Phase 1 — Skeleton & tooling
- Draft each adapter's ledger (file.yaml drafted; govdata seeded with econ).
- Port from provisa, scoped to the 4 modules: `validate_requirements.py` (schema + `--coverage-check`
  + `--orphan-check`), `gen_requirements_md.py`, and the 3-state coverage-matrix view from
  `applies` − `exceptions` vs `tests`.
- Build wiring: make `-PincludeTags=<ID>` **replace** the default `unit` tag (today it ANDs, so an
  ID tag does not isolate). One-line change in `file/build.gradle.kts`.
- Stand up `file/test-archive/` (outside the source set) as the mining backlog.

### Phase 2 — Reverse-engineer requirements (build-out) — the first real step
For each unit, mine the sources and emit `accepted` requirements with `source:` citations; mine old
tests for scenarios + fixtures (tag fixtures to preserve, then archive the test).
- **file** method: the seam code (converter/scanner/provider/format/rule) + tests; set `seam`,
  `cardinality`, `mode`, `applies`/`exceptions`. (file.yaml is a first draft from the design
  conversation — refine it against the code here.)
- **govdata** method: schema YAML + transformers + DQ + tests, per [govdata.md](govdata.md).

Parallelizable — one pass per schema / per format/seam. A candidate for a multi-agent workflow once
the method is proven on the two pilots.

### Phase 3 — Write goldens, flip `accepted → complete`
Per requirement: build the hermetic fixture, write the golden **up to its seam**, link `tests:`, set
`status: complete`.

### Phase 4 — Archive & gate
As requirements reach `complete`, archive superseded legacy tests (deletion gated on `supersedes`
being fully green). Wire the validator into CI, scoped to the 4 modules.

## Pilots (prove the two methods first)

- **govdata:** `econ` — GOV-001..005 sketch the shape; complete the schema 1:1.
- **file:** `json` (simple basis) + upgrade `csv` to the engine matrix — proves seam + invariance.

## Work-breakdown skeleton

- **file basis (simple 1:1):** csv (started) · json · parquet
- **file complex (1:N, decompose→JSON):** xlsx · docx · html · pptx · xml · markdown
- **file seams:** storage providers (local/s3/hdfs/ftp/sftp/http/sharepoint) · walking · refresh
- **file features:** type-inference · partitioning/incremental · materialization (write) · compaction ·
  schema-evolution · compression · **optimizations** (HLL/count-star/pushdown/stats)
- **file interaction cells:** engine×format invariance · duckdb×storageType · write×format×engine ·
  refresh×storageType · walking×refresh×storageType
- **govdata:** one group per schema — econ (pilot) · sec · geo · census · energy · health · crime ·
  cyber · edu · fec · fedregister · patents · lands · cftc · weather · worldbank · ref
- **splunk · sharepoint-list:** after file + govdata methods proven.

## Definition of done (per unit)
- Every MUST guarantee is an `accepted`→`complete` requirement with a green golden at its seam.
- Every legacy test is superseded+archived or its scenario explicitly dropped (recorded, not lost).
- Coverage matrix shows no ⬜ gaps for MUST; all 🚫 cells carry a reason.
