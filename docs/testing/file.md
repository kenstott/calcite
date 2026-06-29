# File Adapter — Test Strategy

The file adapter's specialization of the [shared core](strategy.md). Its testable unit is a **cell
in a matrix**, and the art is keeping that matrix coverable. Ledger:
[../requirements/file.yaml](../requirements/file.yaml).

## Axes

| Axis | Values |
|---|---|
| source format | csv, json, parquet (simple 1:1) · xlsx, docx, html, pptx, xml, markdown (complex 1:N) |
| engine (config) | linq4j, parquet, arrow, duckdb (+ trino/spark/clickhouse, external) |
| storageType | local, s3, hdfs, ftp, sftp, http, sharepoint |
| operation | read · write |
| topology | single · walking |
| features | refresh, partitioning, materialization, compaction, schema-evolution, type-inference, **optimizations** |

Full cartesian ≈ thousands → uncoverable. It collapses because **every axis lands in one of three
buckets, none of which reopen the product**:

1. **Separable seam → sum, not product.** storage (`StorageProvider.openInputStream`/`listFiles`),
   format parsing, and engine execution are distinct layers; test each at its seam, once.
2. **Bounded within-fixture expansion.** complex 1:N (N tables/workbook) and walking catalogs
   (N files/tree) grow *inside one fixture*, never against the other axes.
3. **A short list of real interaction cells** (below).

## The decomposition law

*Complex file formats decompose into simple file formats.* A `*TableScanner`/converter writes **one
JSON file per discovered table** (`MultiTableExcelToJsonConverter`, `DocxTableScanner`,
`HtmlTableScanner`, `PptxTableScanner`, `XmlTableScanner`, `MarkdownTableScanner`), which flow
through the simple JSON path. Same law at both discovery levels:

- **walking source** = enumerate files (`listFiles` → catalog) + simple per-file ingestion
- **complex format** = scan to JSON (`TableScanner` → JSON-set) + simple JSON ingestion

The irreducible **basis** is the simple formats (csv/json/parquet): golden round-trip ×
engine-invariance. Everything else is a *transformation producing basis inputs*, tested as that
transformation. Engine/storage axes bind only to the basis, so they never multiply against the
decompositions.

## Seam map — test up to the seam, not through the stack

| Component | Seam — assert this artifact | Not through to |
|---|---|---|
| StorageProvider | bytes (`openInputStream`) / catalog (`listFiles`) | a table |
| TableScanner / converter | the emitted JSON file-set (names + content) | JDBC |
| walking source | file catalog + assembly | per-file parsing |
| **simple format** (basis) | the table — full stack via JDBC | — (terminal seam) |
| engine | execution over parsed data (invariance) | parsing/storage |
| **optimization rule** | result equivalence (rule on==off, or within bound) + plan fires | — |

Most tests are pure-function seam goldens (bytes→catalog, doc→JSON) — fast, exactly assertable,
non-multiplying. Only the basis and the interaction cells run the full stack.

## Optimizations — a distinct contract

Optimizations (HLL sketches, count-star-from-stats, pushdown/pruning, engine-specific rules) test
two things, not data fidelity: **transparency** (optimized result == unoptimized, or within an error
bound for approximate ones like HLL) and **activation** (the rule fires — assert on EXPLAIN). They
live at the `execution` seam: rule-on-vs-off equivalence + a plan assertion. Wrong statistics
silently corrupt pruning/count-star, so the stats themselves get goldens.

## Coverage = a 3-state matrix

✅ tested-green · ⬜ gap · 🚫 accepted-unsupported (**with a recorded reason**). The reason is the
honest replacement for the legacy `type == INTEGER || BIGINT` / `TIME || 2000` tolerances (e.g.
"duckdb returns TIME as JAVA_OBJECT"). Invalid cells are pruned, not filled. Encoded per requirement
as `applies` − `exceptions`.

## Interaction cells (the only genuine products)

- **engine × format** type-fidelity — engine-invariance (the DuckDB-`TIME` class)
- **duckdb × storageType** — duckdb bypasses `StorageProvider` via httpfs (`DuckDBFunctionMapping`)
- **write × format × engine** — materialization correctness
- **refresh × storageType** — change signal differs (mtime vs ETag; `ConversionMetadata`)
- **walking × refresh × storageType** — incremental re-walk; the keystone, where ETL idempotence
  bugs concentrate
