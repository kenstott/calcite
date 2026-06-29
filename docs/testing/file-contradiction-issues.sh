#!/usr/bin/env bash
# Files one GitHub issue per resolved file-adapter contradiction (see docs/testing/contradictions.md).
# PREREQUISITE: `gh auth login` (or export GH_TOKEN). Targets the repo of the current dir (kenstott/calcite).
#
# Usage:
#   bash docs/testing/file-contradiction-issues.sh           # create all issues
#   DRY_RUN=1 bash docs/testing/file-contradiction-issues.sh  # print titles only
set -euo pipefail

DOC="docs/testing/contradictions.md"
mk() {  # mk <labels> <title> <body>
  local labels="$1" title="$2" body="$3"
  if [ "${DRY_RUN:-0}" = "1" ]; then echo "[$labels] $title"; return; fi
  gh issue create --title "$title" --label "$labels" \
    --body "$body"$'\n\nResolution & context: '"$DOC"
}

# ---- Bugs ----
mk "bug,file-adapter" "CsvTypeConverter: unparseable DATE throws NPE (C-17)" \
"An unparseable DATE crashes with NullPointerException (parseDate returns null, then debug logging dereferences it), unlike TIME/TIMESTAMP. Resolution (C-16/C-17): raise a clear parse error instead. Req FILE-101."

mk "bug,file-adapter" "RefreshablePartitionedParquetTable.scan() NPE on empty file list (C-20)" \
"scan() dereferences currentTable without the null guard getRowType() has -> NPE on an empty/zero-file partition set. Resolution: add the guard (empty -> empty result) + regression test. Req FILE-135."

mk "bug,file-adapter" "MaterializedViewTable stuck after a failed materialize (C-21)" \
"materialize() flips its CAS flag true BEFORE the try, so a thrown materialize leaves materialized=true and a partial parquet that is never retried. Resolution: set flag on success only; on failure reset and delete partial. Req FILE-137."

mk "bug,file-adapter" "Iceberg time-travel statistics use the current snapshot (C-22)" \
"IcebergTable.getStatistic ignores snapshotId/asOfTimestamp and always uses currentSnapshot, so time-traveled scans plan against wrong stats. Resolution: align stats to the selected snapshot + test. Req FILE-145."

mk "bug,file-adapter,correctness" "Plain COUNT(DISTINCT) silently returns an HLL estimate (C-18)" \
"SimpleHLLCountDistinctRule.APPROX_ONLY_INSTANCE ignores its approxOnly flag and rewrites EVERY COUNT(DISTINCT) to an HLL approximation. Resolution: exact COUNT(DISTINCT) stays exact; HLL only for APPROX_COUNT_DISTINCT (or explicit opt-in). Req FILE-124."

mk "bug,file-adapter" "Silent parse fallbacks + synthetic HLL seeds violate no-fallback rule (C-16)" \
"CsvTypeConverter/FileRowConverter return null on unparseable values; StatisticsBuilder seeds synthetic 1000/10000-value HLL sketches on missing data. Resolution: raise on parse failure; remove synthetic seeds. Reqs FILE-101, FILE-126."

mk "bug,file-adapter,cleanup" "DuckDBHLLCountDistinctRule writes /tmp debug artifacts (C-19)" \
"Writes /tmp/duckdb_hll_rule_loaded.txt (class init) and appends /tmp/duckdb_hll_rule_matched.txt per match, plus WARN logs. Resolution: gate behind a debug flag; no hard-coded /tmp by default. Req FILE-125."

mk "bug,file-adapter" "csvTypeInference confidenceThreshold is dead config (C-08)" \
"determineType never consults confidenceThreshold; any single string sample forces the column to VARCHAR. Resolution: honor it -> promote when the non-conforming ratio is below 1-threshold. Req FILE-096."

mk "bug,file-adapter" "Invalid batch_size silently skips the whole schema (C-13)" \
"One JDBC code path defaults batch_size to 2048, another silently drops the entire schema on an invalid value. Resolution: fail-fast with a clear error. Req FILE-168."

mk "bug,file-adapter" "Foreign keys matched by position instead of name (C-24)" \
"TableConstraints matches FK target columns positionally (IntPair.of(sourceIndex,i)), not by name. Resolution: match by name (+ test with differing names/orders). Req FILE-149."

# ---- Behavior / doc-vs-code corrections ----
mk "file-adapter" "recursive default should be true; string \"true\" is ignored (C-01)" \
"Docs say recursive defaults true, code defaults false (operand==Boolean.TRUE), and a JSON string \"true\" never enables it. Resolution: default true + accept string \"true\". Req FILE-110."

mk "file-adapter" "Default execution engine should be DuckDB-if-available (C-02)" \
"getDefaultEngine() always returns parquet despite javadoc claiming DuckDB-when-on-classpath. Resolution: implement DuckDB-if-available detection. Req FILE-119."

mk "file-adapter" "Optimization rules should be on by default (C-04)" \
"filter/column-prune/join-reorder/HLL rules are gated by calcite.file.statistics.*.enabled defaulting false. Resolution: enable by default (result-preserving), keep a disable switch; document the sketch-gen vs rule-activation layers. Req FILE-121."

mk "file-adapter" "s3:// URLs should route via the AWS default credential chain (C-06)" \
"createFromUrl(s3://...) throws; only createFromType with explicit creds works. Resolution: route s3:// and fall back to the AWS default chain (env/IAM/profile). Req FILE-112."

mk "file-adapter" "HDFS listFiles should throw on a missing directory (C-07)" \
"Local/S3 throw on a missing dir; HDFS returns an empty list. Resolution: standardize on throw. Reqs FILE-113, FILE-117."

mk "file-adapter" "Scientific notation without a decimal point should infer numeric (C-09)" \
"FLOAT_PATTERN requires a literal dot, so 1e5 -> VARCHAR. Resolution: recognize dot-less scientific notation as DOUBLE; also document yes/y/no/n booleans. Reqs FILE-097, FILE-100."

mk "file-adapter,cleanup" "Markdown converter naming should be lowercase like the others (C-10)" \
"Markdown uses PascalCase base name + on-disk filenames; excel/html/docx/pptx are lowercase. Resolution: make Markdown lowercase snake_case. Reqs FILE-105, FILE-171."

mk "file-adapter" "FileReaderException should carry SQLStates (C-11)" \
"Documented SQLStates (42S02/22018/08001/HY000/42000) are not on FileReaderException (plain checked Exception). Resolution: make it a SQLException mapping each failure to its SQLState. Req FILE-151."

mk "file-adapter,tech-debt" "Consolidate the two HLL rules (C-23)" \
"Non-Simple HLLCountDistinctRule.onMatch is disabled; SimpleHLLCountDistinctRule does the work. Resolution: re-enable intended capability and merge into one rule."

mk "file-adapter,tech-debt" "Consolidate JDBC drivers on AperioDriver (C-12)" \
"Two drivers (jdbc:aperio: vs jdbc:calcite: schema=file) with different param styles and engine defaults; only AperioDriver is documented. Resolution: consolidate on AperioDriver, deprecate FileJdbcDriver, align to DuckDB-if-available. Reqs FILE-146/147/168."

# ---- Investigations ----
mk "file-adapter,tech-debt" "AperioDriver: reuse VariableResolver, delete expandVarString (C-14)" \
"Analysis found NO 2-level pass-through: govdata and AperioDriver are independent entry points each resolving once; AperioDriver.expandVarString is a weaker duplicate lacking \${VAR:default}/\${VAR:-default}. Resolution: delete expandVarString and route buildOperand output through the shared VariableResolver. Req FILE-147."

mk "govdata,investigation" "Confirm shouldAutoDownload default (test says true, code false) (C-15)" \
"Test asserts default true; code returns false ('reads must never trigger ETL'). Tentatively ruled change-to-true, but this reverses a prior safety decision — confirm before implementing."

echo "Done."
