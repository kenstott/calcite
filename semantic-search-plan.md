# Cross-Schema Semantic Search (Free-Text Row Index)

## Context

AskAmerica's schemas carry thousands of free-text `string` columns with no way to search
them semantically — only exact/`ILIKE` matching today. This plan builds one shared
vector index over row-level text across every govdata schema, using the same wide-table
shape as [`entity-resolution-plan.md`](entity-resolution-plan.md) (nullable per-source FK
columns keyed off a generic `source_schema`/`source_table`/`stringified_fk` triple), but the
matching problem is fundamentally simpler than entity resolution: there's no
resolve-to-canonical-entity step, no confidence tiering, no fuzzy scoring — just "embed
this text, index it, let a query find nearest neighbors." One table does the whole job.

A static inventory pass (`type: string` across all 26 govdata schema YAMLs, PyYAML-parsed,
`column_templates` anchors resolved) found **2675 string columns**. The original plan
column-classified each one into free-text/categorical/blob buckets to decide what's "worth"
embedding — **rejected during design**: excluding a column on a judgment call about its
content is exactly as likely to discard signal as noise (a status code can carry as much
retrieval value as a name), so there is no column-level filtering. Every `string` column on
an included table goes in.

What *is* still curated is decided at the **table** level, and for a different reason:
volume, not content. See "Table curation" below.

## Existing infrastructure — what this would duplicate or extend

Live research (repo-wide grep, not just govdata/, plus live row counts via AskAmerica)
found a real, working vectorization pipeline already in production — scoped to one schema,
split across two generations, and built on different technology than this plan originally
proposed. Both facts change the design below.

**SEC has a real chunk+embed+search pipeline today.** `sec.vectorized_chunks`
(`sec-schema.yaml:1395-1520`) is populated via `DocumentETLProcessor`/`IcebergMaterializer`,
driven by real Java — `SemanticTextChunker.java`, `SecTextVectorizer.java`,
`TextNormalizer.java`, invoked from `XbrlToParquetConverter.java:5584-5980` — and holds
6,446,801 live rows today. Its `embedding` column is dead, though: `bulkGenerator:
gpu_embeddings` points at `scripts/vss-gpu-runner.sh`, which self-disables ("RETIRED —
embeddings run locally via vss-local.sh"). The actual live embedding path is a second,
separate system: `govdata/scripts/vss-local.py`/`.sh` reads `chunk_text` via
`iceberg_scan`, embeds locally on CPU (arctic-embed-xs, 384-d), and writes quantized
binary+int8 codes to a **non-Calcite-registered** lake path
(`s3://govdata-parquet-v1/sec/vectorized_chunk_codes/`) — queried through
`file/src/main/java/.../similarity/SemanticSearch.java`'s `SEMANTIC_SEARCH(query, k)` table
function (Hamming-distance prefilter over the binary codes, then int8 rerank), backed by a
long-lived local embedder process in `EmbeddingService.java`. Confirmed live in code
(`SemanticSearch.java`, `DuckDBJdbcSchemaFactory.java:565` calls `configure(...)` at schema
setup): this isn't dead infrastructure, it's a **deliberate two-store split**. `chunk_text`/
metadata stay in Iceberg, where columnar scan, partition pruning, and SQL joins are the
right fit; the vectors live in a separate, non-Iceberg, flat-Parquet "codes" store that
DuckDB reads directly via `read_parquet()` + `httpfs` at query time (no persistent local
index, no HNSW — a `bit_count(xor(...))` Hamming prefilter then a cosine rerank, both plain
SQL over the raw file). Iceberg has no native vector index and its snapshot/manifest
overhead buys nothing for a fully-recomputed, read-only search artifact — semantic search
against Iceberg directly is impractical, which is exactly why this second store exists. The
dead `embedding` column on `vectorized_chunks` is leftover from an earlier (abandoned)
attempt to put vectors inside the Iceberg table itself, not something the working system
depends on.

**The quantization is doing double duty, not just enabling the Hamming-prefilter
algorithm.** Packing to a 48-byte binary sign-code plus a 384-byte int8 rerank vector
(~432 bytes/chunk, versus 3072 bytes for a raw `double[384]`) also keeps the whole codes
dataset small enough to stay effectively cache-resident in DuckDB's `httpfs` read cache
after the first scan — roughly 2-3GB for SEC's 6.4M chunks. There's no explicit
download-to-local step; the size reduction is what lets the transparent read-cache achieve
the same practical effect (fast repeat queries against what's functionally a local copy)
without one. This is a real sizing constraint on the generalized design, not just a SEC
implementation detail: extending this store to every curated table across 26 schemas grows
it well past SEC's current footprint, and whether it stays cache-tractable at that size
needs to be checked, not assumed, once the full table registry (see "Table curation") is
known.

**No other schema has a real pipeline.** `patents-schema.yaml:49-53,720-722,794` claims
FLOAT[] embedding columns on `patent_claims`/`patent_summaries`, "populated at ingestion
time via batched embedding calls" — verified false, both live (`describe_table` shows no
such column) and in code (`PatentClaimsTransformer.java`/`PatentSummariesTransformer.java`
have no embedding logic). Same aspirational-comment pattern as `fiscal.sba_loan_approvals`'s
false EIN-join claim — a follow-up comment fix, independent of this plan.

**Consequence: don't introduce DuckDB VSS/HNSW as a second vector-search technology, and
don't invent a second table either.** `SemanticSearch.java`/`EmbeddingService`/the
binary-quantized-code lake layout is already built and already in production for SEC — one
query surface (`SEMANTIC_SEARCH(...)`) worth extending, not duplicating with a competing
index. **This plan now promotes `sec.vectorized_chunks` itself to `ref.vectorized_chunks`**,
generalized to cover every curated table, rather than building a parallel
`ref.semantic_search_index` alongside it — one artifact, not two. See "Promoting
`vectorized_chunks`" below for the shape and migration cost.

**Scheduling precedent — directly answers the "time-boxed end-of-daily job" question**:
`run-pool.sh:835-852` already does exactly this. When a full daily run (no `--schema`
filter) finishes with zero failed workers, it runs `vss-local.sh backlog` once, after the
whole pool drains. That's the real precedent to extend, not a `TableLifecycleListener` —
`run-pool.sh:308-317`'s own comment on `ref:daily` confirms (again) the caveat already
raised in `entity-resolution-plan.md`: queue position is memory-budget-gated, not
dependency-gated, so `TableLifecycleListener.afterMaterialize` gives **no cross-schema
ordering guarantee**. A cross-schema semantic index needs every source schema already
materialized, which only the post-drain hook actually guarantees. One caveat inherited
along with it: the hook is skipped entirely if `failed_count > 0` (an `exit 2` at line 832
happens first) — a bad daily run means no semantic-index refresh that day, not a degraded
partial one. Worth an explicit decision, not silently inherited.

**Side-finding — not part of this plan, but changes its own precedent**:
`EntityBridgeListener`, which this plan's original extension-mechanism section cited as
"design only, not yet built" (per `entity-resolution-plan.md`), is actually already
implemented on an unmerged branch (`entity_resolution` worktree, commit `d7fac2bd0` — the
985-line listener + 459-line schema YAML addition, plus two data-quality fix commits tuning
match thresholds and assignee-type codes). It chose the `TableLifecycleListener`
soft-ordering approach and accepted the staleness caveat rather than avoiding it — a real
implemented decision, not a paper one, worth reading directly before this plan repeats or
diverges from that choice.

## Promoting `vectorized_chunks`

**Current live shape** (`sec-schema.yaml:1395-1520`, confirmed via live `describe_table`):
`cik`, `accession_number`, `year`, `chunk_id`, `source_type` (enum: `mda_paragraph` /
`footnote` / `risk_factor` / `earnings`), `section`, `subsection`, `section_path`,
`paragraph_continuation`, `sequence`, `filing_date`, `chunk_text`, `enriched_text`,
`embedding` (dead — see above), `content_type`, `financial_concepts`, plus
earnings-specific `exhibit_number`/`speaker_name`/`speaker_role`/`paragraph_number`. PK
`[cik, accession_number, chunk_id]`. 6,446,801 live rows.

**Two content modes unify into one promoted table via `source_type`, not two pipelines
into two tables:**
- **Document-chunk mode** (what SEC already does): reuse `SemanticTextChunker`/
  `SecTextVectorizer`/`TextNormalizer` as-is, extending `source_type`'s enum with a value
  per new document-blob source — `patent_claim`, `patent_summary`, `patent_abstract`,
  `trademark_statement`, `cyber_control_description`, `clinical_trial_summary`,
  `ntsb_probable_cause`, etc. — covering the 18-blob-column list from "Chunk-count
  evidence" below far better than a naive fixed-window chunker would. `section`/
  `subsection`/`speaker_name`/`exhibit_number`/`financial_concepts` stay null for sources
  where they don't apply (e.g. a patent claim has no `speaker_name`) — same
  nullable-per-source-shape convention already used on `canonical_org_entity`.
- **Row-concat mode** (the plain entity-grain dimension-table path from "Table curation"
  below): `source_type = 'row_concat'`, `chunk_text` = the delimited concat of that row's
  `string` columns, every document-structure column (`section`, `subsection`,
  `speaker_name`, `exhibit_number`, `financial_concepts`, `paragraph_continuation`) null —
  it's simply a table with no interesting document structure, not a special case requiring
  its own columns.

`sequence` already does what a generic `chunk_index` would (order within a source row's
chunks) — no new ordinal column needed. `cik`/`accession_number` stay as first-class
columns (SEC's own natural key, not renamed) and become one instance of the wide
per-table-FK pattern; every other included table gets its own nullable FK column pair the
same way, per "Storage shape" below. Generalized PK:
`[source_schema, source_table, stringified_fk, sequence]` — `chunk_id` remains a stored
convenience unique string, no longer the PK component itself.

**Migration touches ~20 files, but the data itself can move, not recompute.** `sec` and
`ref` share the same Hadoop-catalog warehouse root (`warehousePath:
"${GOVDATA_PARQUET_DIR}/sec"` vs `.../ref`, both `catalogType: hadoop`, confirmed live in
both schema YAMLs) — table identity is directory-path-based, so relocating
`${GOVDATA_PARQUET_DIR}/sec/vectorized_chunks/` to `.../ref/vectorized_chunks/` is a
same-bucket directory move, not a re-chunk of the source filings. Iceberg also supports
adding nullable columns without rewriting existing data files, so the new generalization
columns (`source_schema`, `source_table`, `stringified_fk`, wide FK columns) schema-evolve
in and backfill from the already-present `cik`/`accession_number` in one cheap rewrite pass
— `source_schema='sec'` is a constant, `stringified_fk` is a simple derived expression, no
`SemanticTextChunker`/`XbrlToParquetConverter` re-run needed. The `embedding` column swap
is similarly a join-in, not a recompute: `vss-local.py`'s quantized codes already exist at
`vectorized_chunk_codes`, keyed by the same `chunk_id`/`cik`/`accession_number` — pull them
across rather than regenerating.

What's *not* avoidable by moving data: the ~20 referencing files still need their
`sec.vectorized_chunks` references repointed to `ref.vectorized_chunks` —
`govdata/src/main/resources/sec/sec-schema.yaml`,
`govdata/src/main/java/.../sec/SecSchemaFactory.java`,
`govdata/src/main/java/.../sec/XbrlToParquetConverter.java`,
`govdata/src/main/java/.../etl/IcebergMaintenanceRunner.java`,
`file/src/main/java/.../similarity/SemanticSearch.java`,
`file/src/main/java/.../etl/PostProcessConfig.java` and `SchemaLifecycleProcessor.java`,
seven `govdata/scripts/vss-*.py`/`.sh` scripts, plus `reset-sec-iceberg-tables.sh`,
`parallel/fix-sec.sh`, `iceberg-compact.sh`, `spot-check-iceberg.sh`, `model-verify.sh`,
`data_purge.sh`, and three test classes — that's real work regardless of move-vs-recompute.
Nor is generating the *new* content avoidable: the row-concat mode and the new
document-blob sources (patents/cyber/health/NTSB) have no pre-existing chunked data to
move, so those are necessarily built fresh, same cost either way.

## Row-level design

**Row-concat mode, for every included entity-grain table that isn't a document-blob
source**: one source row → one concatenated string → naively chunked → each chunk
vectorized. Concatenate every `type: string` column's non-null value into one text string
— `col_name: value | col_name: value | ...`, source column order, nulls skipped, no
per-column inclusion/exclusion logic. Run that concatenation through a single naive
fixed-size chunker (e.g. 1000 chars, 200-char overlap — exact numbers tuned at
implementation time against the embedding model's token limit) and embed each resulting
chunk separately, written with `source_type = 'row_concat'` per "Promoting
`vectorized_chunks`" above.

A short row's concatenation fits in one chunk, so it produces exactly one embedding — no
separate detection step for which columns are blob-scale, nothing content-shaped, within
this mode. The 18 columns below are handled differently: not by this naive chunker, but by
generalizing SEC's existing `SemanticTextChunker` document-chunk pipeline to them (see
above) — the two modes share one table, not one chunking mechanism.

### Document-blob sources (document-chunk mode, not the naive row chunker)

The static inventory found **18 of 2675 columns (0.7%)** that are blob-scale — full prose or
document fragments rather than short fields — concentrated in four schemas. These are the
sources that get `SemanticTextChunker`-style document chunking (new `source_type` values),
per "Promoting `vectorized_chunks`" above, not the naive fixed-window row chunker:

| schema.table.column | why |
|---|---|
| `sec.mda_sections.paragraph_text` | MD&A paragraph text |
| `sec.earnings_transcripts.paragraph_text` | earnings call transcript paragraphs |
| `sec.financial_line_items.full_text` | XBRL narrative disclosure blocks |
| `sec.vectorized_chunks.chunk_text` / `enriched_text` | already the *target* of an existing chunking pipeline — not a new source, see note below |
| `sec.filing_contexts.segment` / `scenario` | XBRL dimension XML fragments (structured, not prose — exclude, not chunk) |
| `patents.patent_abstracts.patent_abstract` | patent abstract |
| `patents.patent_claims.claim_text` | full claim text |
| `patents.patent_summaries.summary_text` | brief summary section |
| `patents.trademark_statement.statement_text` | goods/services description |
| `cyber.nist_controls.description` | up to 4000 chars |
| `cyber.cis_controls.description` | up to 2000 chars |
| `cyber.owasp_top10.overview` | up to 2000 chars |
| `cyber.osv_vulnerabilities.database_specific` | JSON blob — structured, not prose, exclude |
| `health.clinical_trials.brief_summary` | up to 2000 chars |
| `health.clinical_trial_interventions.description` | up to 2000 chars |
| `transport.ntsb_aviation_accidents.probable_cause` | up to 4000 chars |

Two of these (`sec.filing_contexts.segment`/`scenario`, structured XML fragments, and
`cyber.osv_vulnerabilities.database_specific`, a JSON blob) aren't prose regardless of
length — excluded entirely, from both modes, the one remaining content-shaped exclusion,
made on structural grounds (not text) rather than a "is this worth it" judgment.

`sec.mda_sections.paragraph_text`/`sec.earnings_transcripts.paragraph_text` are very likely
already covered by `vectorized_chunks`'s existing `mda_paragraph`/`earnings` chunks (it's
built from the same source content) — confirm during implementation before double-chunking
the same underlying text under a new `source_type`.

## Table curation

Column-level filtering was rejected; **table-level filtering is real and necessary**,
because the two things being weighed differ: a column's *content* is never reliably
"noise," but a table's *grain* directly determines embedding volume, and some grains don't
pay for themselves.

**The test is grain, not size**: does one row represent one real-world entity (a company, a
person, a patent, a facility, a filer) or one event/transaction (a trade, a contribution, a
payment, a single XBRL fact)? Entity-grain tables are included regardless of size.
Event-grain tables are excluded from v1 regardless of whether they also contain useful
text, because nobody queries this index for "which specific transaction" — they query it
for "which entity."

Row counts pulled live (via AskAmerica) confirm size alone is the wrong signal:

| table | rows | grain | verdict |
|---|--:|---|---|
| `fec.individual_contributions` | 147,355,287 | one row per contribution | **exclude** — event-grain |
| `health.cms_open_payments` | 76,520,401 | one row per payment | **exclude** — event-grain |
| `sec.financial_line_items` | 39,897,742 | one row per XBRL fact | **exclude** — event-grain (its `full_text` blob column still gets chunked per above, independent of whether the table's ordinary rows are embedded) |
| `sec.insider_transactions` | 1,606,962 | one row per Form 3/4/5 transaction | **exclude** — event-grain, despite moderate size |
| `transport.fmcsa_carriers` | 4,469,030 | one row per carrier | **include** — entity-grain, size is irrelevant |

`fmcsa_carriers` is the deliberate counter-example: it's larger than `insider_transactions`
but stays in, because each row is a carrier, not an event. A row-count threshold alone
would get this backwards in one direction or the other no matter where it's set.

**This needs a full per-table pass during implementation**, not just the five tables
spot-checked above — every table across all 26 schemas needs the same entity-vs-event grain
call before it's added to the v1 registry. That pass is a natural follow-up sweep (grep
each table's declared PK shape for an event/sequence-id component as a first-pass signal,
spot-check row counts to confirm), not attempted exhaustively in this doc.

## Storage shape

One table, `ref.vectorized_chunks` (promoted from `sec.vectorized_chunks`, not a new
sibling) — no tall/wide split, unlike entity resolution, because there's no separate
matching step producing two different grains here. Existing SEC columns are kept, not
replaced:

| column | type | notes |
|---|---|---|
| `source_schema` | string, new | e.g. `'fec'`, `'patents'`, `'sec'` — generalizes identity beyond SEC-only |
| `source_table` | string, new | |
| `stringified_fk` | string, new | the source row's own primary key, stringified (handles composite/non-string PKs uniformly); `cik`+`accession_number` remain SEC's own typed pair below, this is the generic cross-schema fallback |
| `sequence` | int, existing | order within a source row's chunks — already does what a generic `chunk_index` would; almost always `0` except the document-blob sources and any row-concat that spills past one naive chunk |
| `source_type` | string, existing, enum extended | SEC's existing `mda_paragraph`/`footnote`/`risk_factor`/`earnings`, plus one new value per document-blob source (`patent_claim`, `ntsb_probable_cause`, etc.) and `row_concat` for the generic entity-grain path |
| `chunk_text` / `enriched_text` | string, existing | as today for document-chunk mode; for `row_concat` rows, `chunk_text` is the delimited concat, `enriched_text` can just mirror it (no `TextNormalizer` pass) unless that turns out to help retrieval |
| `section` / `subsection` / `section_path` / `paragraph_continuation` / `speaker_name` / `speaker_role` / `exhibit_number` / `financial_concepts` / `paragraph_number` | existing, nullable | populated where meaningful (documents with real structure), null for `row_concat` and structure-less sources — same nullable-per-source-shape convention as `canonical_org_entity`, not new columns |
| `embedding` | drop or leave inert | **not** where vectors live — see "Existing infrastructure": the codes deliberately stay in a separate non-Iceberg store, not this table. This column is leftover from an abandoned earlier design; don't populate it, just decide whether to drop it as cleanup or leave it as harmless dead weight |
| `cik` / `accession_number` | existing | SEC's own natural key, unchanged — the first instance of the wide-FK-per-table pattern below (a pair, since a filing needs both to identify uniquely) |
| wide nullable FK columns, new | string, nullable | one or more per additional included source table, matching that table's own PK shape — usually a single column (`patents_patent_id`, `fec_committee_id`, `transport_fmcsa_dot_number`), a pair/tuple only where the source table's own PK is composite, same as `cik`+`accession_number` is for SEC; populated only on rows from that table, every other FK column null on that row; mechanically generated from each table's declared PK at build time, not hand-curated |

Primary key: `[source_schema, source_table, stringified_fk, sequence]`.

`stringified_fk` and the wide FK columns exist for two different reasons, not as
redundant copies of the same value: `stringified_fk` is what the table's actual grain runs
on — it works for any source with zero schema change, including a 27th source added
tomorrow, which is why the primary key is built on it rather than on the wide columns.
The wide columns are pure convenience for a consumer who already knows which table they
want (`WHERE patents_patent_id IS NOT NULL` reads better and type-checks better than
comparing `source_table`/`stringified_fk` strings) — they need a small migration
(schema-evolve one nullable column) each time a new source is added, `stringified_fk`
does not.

Index: reuse the existing binary-quantized-code + Hamming-prefilter + int8-rerank scheme
`vectorized_chunk_codes`/`SemanticSearch.java` already runs for SEC, not DuckDB's VSS
extension — see "Existing infrastructure" above. The codes store **stays separate from
`ref.vectorized_chunks`**, by design — generalized to cover every curated source, laid out
**Hive-partitioned by `source_schema`** (`.../vectorized_chunk_codes/source_schema=sec/`,
`.../source_schema=patents/`, etc.) rather than one flat glob (today it's hardcoded to
`s3://govdata-parquet-v1/sec/vectorized_chunk_codes/` via `SemanticSearch.DEFAULT_CODES`).
Partitioning on the same column the metadata filter already needs solves both problems at
once: a schema-scoped search only pulls that partition into cache (bounding the per-query
working set regardless of total corpus size), and DuckDB can prune partitions directly from
a `source_schema` predicate instead of scanning-then-filtering. `SEMANTIC_SEARCH(...)`
needs a way to pass that scope through (a new parameter, or a `WHERE source_schema = ...`
pushdown against the Hive-partition column read_parquet exposes) — an implementation-time
call, not decided here. Each codes row carries the same generalized key
(`source_schema`/`source_table`/`stringified_fk`/`sequence`, or `chunk_id` as today) so
results join back to `ref.vectorized_chunks` for text/metadata, exactly as `chunk_id`
already joins back to `vectorized_chunks` today — just widened to every source instead of
SEC only.

## Extension mechanism

**Reuse the post-drain hook in `run-pool.sh:835-852`, not a `TableLifecycleListener`.** That
block already runs once, after the full daily pool finishes with zero failed workers, and
already calls `vss-local.sh backlog` for SEC's document-chunk mode — the natural place to
add the row-concat mode for every other curated table alongside it, rather than the
`EntityBridgeListener`-style per-schema hook this plan originally proposed, which can't
guarantee every source schema is materialized yet (see above). Concretely: `vss-local.sh`
(or a new sibling script) grows a table registry — for each document-blob source, extend
the existing `SemanticTextChunker` call with the new `source_type` values; for each
entity-grain dimension table, run the row-concat-then-naive-chunk pass — both writing into
`ref.vectorized_chunks` via the same embedder (`EmbeddingService`) and
`overwritePartitions: true` semantics already used today.

## Phasing

**Phase 0: repartition the codes store by `source_schema`, and decide the dead `embedding`
column's fate — no Iceberg rewrite, no move yet.** The working query path
(`SEMANTIC_SEARCH`) never touches `vectorized_chunks`'s `embedding` column, so there's
nothing to backfill there — that was the wrong Phase 0 goal (see "Existing infrastructure").
The real first step: `SemanticSearch.DEFAULT_CODES` is hardcoded to SEC's single flat glob
(`s3://govdata-parquet-v1/sec/vectorized_chunk_codes/*.parquet`); moving it to a
Hive-partitioned layout keyed on `source_schema` (see "Storage shape" → Index) is required
infrastructure before any new source's codes are searchable at all, and directly bounds the
per-query cache footprint as the corpus grows past SEC-only — a schema-scoped query only
pulls its own partition, so total corpus size across 26 schemas stops being a cache-risk
question once this lands. Can be done and verified against SEC's existing codes alone
(re-laid-out into a `source_schema=sec/` partition) before anything new is generated.
Separately, decide whether to drop the vestigial `embedding` column and the self-disabled
`gpu_embeddings` `bulkGenerator`/`vss-gpu-runner.sh` hook as cleanup, or leave them as
harmless dead weight — low-stakes either way now that it's clear the working system doesn't
depend on them.

**Phase 1 onward** is everything already described above, sequenced relative to each other
at implementation time, not fixed here: the directory move + schema evolution ("Promoting
`vectorized_chunks`"), the full entity-vs-event table curation pass ("Table curation"), the
new document-blob sources and row-concat mode ("Row-level design"), and wiring it all into
the `run-pool.sh` post-drain hook ("Extension mechanism").

## Explicitly deferred

- The full entity-vs-event grain classification across all ~100+ included-candidate tables
  (only 5 spot-checked here) — a follow-up sweep before the table registry is final.
- Exact chunk size/overlap for `row_concat` mode, and whether every source shares
  arctic-embed-xs 384-d or some need a different model — tuned at implementation time.
- Whether `mda_sections`/`earnings_transcripts` are already fully covered by
  `vectorized_chunks`'s existing `mda_paragraph`/`earnings` chunks (built from the same
  source content) — confirm before double-chunking under a new `source_type`.
- The actual mechanics of the directory move + schema-evolution backfill + updating the
  ~20 referencing files for promoting `sec.vectorized_chunks` → `ref.vectorized_chunks` —
  see "Promoting `vectorized_chunks`" above.
- Fixing `patents-schema.yaml`'s false embedding-column comments — a real, separate bug
  found during research, unrelated to whether this plan proceeds.
- A live ad-hoc vector-search SQL function through Calcite itself beyond generalizing
  `SEMANTIC_SEARCH(...)` — same deferral rationale as `entity-resolution-plan.md`'s
  `JARO_WINKLER()` deferral.

This plan is design only — implementation requires explicit go-ahead, same as
`entity-resolution-plan.md`.
