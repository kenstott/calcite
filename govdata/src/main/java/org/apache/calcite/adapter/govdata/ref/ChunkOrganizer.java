/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.govdata.ref;

import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.EtlResult;
import org.apache.calcite.adapter.file.etl.MaterializationWriter;
import org.apache.calcite.adapter.file.etl.MaterializationWriterFactory;
import org.apache.calcite.adapter.file.etl.MaterializeConfig;
import org.apache.calcite.adapter.file.etl.TableContext;
import org.apache.calcite.adapter.file.etl.TableLifecycleListener;
import org.apache.calcite.adapter.govdata.sec.SemanticTextChunker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Organizes chunks for {@code ref.vectorized_chunks}, in both of its two content modes: see
 * semantic-search-plan.md "Row-level design" / "Table curation".
 * <ul>
 *   <li><b>Row-concat mode</b> ({@code source_type='row_concat'}): concatenates an included
 *   entity-grain dimension table's string columns per row ({@code col: value | col: value}),
 *   naive-chunks the result.</li>
 *   <li><b>Document-blob mode</b> (one {@code source_type} value per source, e.g.
 *   {@code nist_control_description}): runs a single prose column through
 *   {@link SemanticTextChunker#chunkPlainText}, the same sentence-boundary-aware chunker
 *   SEC's own document-chunk mode uses, reused as-is via its plain-text entry point (no HTML
 *   structure needed for a flat blob column).</li>
 * </ul>
 *
 * <p>This class does the organizing only -- the vector writer is always an external script
 * ({@code govdata/scripts/vss-local.py}), which finds chunks with no codes yet (a simple
 * anti-join against {@code vectorized_chunk_codes}) and embeds them, time-boxed and
 * resumable across runs, exactly as it already does for SEC's document-chunk mode. Java's
 * job stops at organizing text into chunk rows; it never touches embeddings.
 *
 * <p>Wired via {@code hooks.tableLifecycleListener} on {@code ref.vectorized_chunks} (it has
 * no {@code source:} block, so {@code afterTable} is the only lifecycle hook that fires for
 * it -- same wiring rationale as {@link EntityBridgeListener}).
 */
public class ChunkOrganizer implements TableLifecycleListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChunkOrganizer.class);

  private static final String TRIGGER_TABLE = "vectorized_chunks";

  // Per-batch row cap for the SEC backfill's real-write path (see backfillSecChunks): bounds
  // peak JVM heap for one batch's full row content (chunk_text/enriched_text included) to a
  // constant regardless of how large a single year's backlog is -- some SEC years run into the
  // millions of rows / multiple GiB, far past what one unbatched SELECT s.* can safely hold in
  // a Java List at once.
  private static final int SEC_BACKFILL_BATCH_SIZE = 5000;

  // Naive fixed-window chunk size / overlap over the delimited column concatenation (row-concat
  // mode only -- document-blob mode uses SemanticTextChunker's own target/min/max sizing).
  // Matches the plan's stated default; tunable, not tied to any model's token limit here
  // since row-concat sources are short (reference/dimension rows).
  private static final int CHUNK_SIZE = 1000;
  private static final int CHUNK_OVERLAP = 200;

  /** One row-concat source: an included entity-grain dimension table, per the v1 registry
   *  in semantic-search-plan.md's "Table curation". Add an entry here (and the matching
   *  wide FK column in ref-schema.yaml) to onboard a new source -- no other code change. */
  private static final List<RowConcatSource> ROW_CONCAT_SOURCES = Arrays.asList(
      new RowConcatSource("ref", "naics", Arrays.asList("naics_code"),
          Arrays.asList("naics_code", "naics_title"), "ref_naics_code"),
      // stringColumns lists only columns declared in the schema's own `columns:` block --
      // NOT `type`, which is a synthesized Hive-partition dimension marker (declared under
      // `dimensions:`, not `columns:`) on both sources below. Constant-per-row and carries
      // no real content, so it's a structural exclusion (same category as excluding a
      // non-string column like `level`/`as_of`), not the column-level content judgment the
      // plan rejected.
      new RowConcatSource("ref", "naics_vintage", Arrays.asList("vintage", "naics_code"),
          Arrays.asList("naics_code", "naics_title"), null),
      // ticker is part of the PK, not just content: a cik can carry multiple tickers
      // (multiple share classes) on the same as_of -- see the primaryKey comment in
      // ref-schema.yaml's constraints block.
      new RowConcatSource("ref", "sec_company_tickers",
          Arrays.asList("type", "as_of", "cik", "ticker"),
          Arrays.asList("cik", "ticker", "title"), null),
      // Cross-schema source (fedregister, not ref) -- proves the base-path resolution
      // (context.getSchemaContext().getMaterializeDirectory() is the bucket root, not
      // ref-scoped) works for any schema, same as EntityBridgeListener already relies on.
      new RowConcatSource("fedregister", "fr_documents", Arrays.asList("document_number"),
          Arrays.asList("document_number", "title", "doc_type", "publication_date",
              "effective_on", "action", "agency_names", "cfr_references", "rin",
              "docket_ids", "signing_date"), "fedregister_document_number"));

  /** Document-blob sources per semantic-search-plan.md's "Document-blob sources" table.
   *  Each gets its own source_type value and reuses SemanticTextChunker's plain-text chunker
   *  (target/min/max sizing per its DEFAULT_* constants -- these are short-to-medium prose
   *  fields, not full filings, so the SEC defaults apply without retuning). */
  private static final List<DocumentBlobSource> DOCUMENT_BLOB_SOURCES = Arrays.asList(
      new DocumentBlobSource("cyber_threat", "nist_controls", Arrays.asList("control_id"),
          "description", "nist_control_description", null),
      new DocumentBlobSource("cyber_threat", "cis_controls", Arrays.asList("safeguard_id"),
          "description", "cis_control_description", null),
      new DocumentBlobSource("cyber_threat", "owasp_top10", Arrays.asList("entry_id"),
          "overview", "owasp_entry_overview", null));

  @Override public void beforeTable(TableContext context) {
    // No-op: this listener does all its work in afterTable, once, on TRIGGER_TABLE.
  }

  @Override public boolean onTableError(TableContext context, Exception error) {
    LOGGER.error("ChunkOrganizer: table '{}' failed upstream of chunk organization",
        context.getTableName(), error);
    return true;
  }

  @Override public void afterTable(TableContext context, EtlResult result) {
    if (!TRIGGER_TABLE.equals(context.getTableName())) {
      return;
    }
    LOGGER.info("ChunkOrganizer: organizing chunks for {} row-concat + {} document-blob "
        + "source(s)", ROW_CONCAT_SOURCES.size(), DOCUMENT_BLOB_SOURCES.size());
    try (Connection conn = openDuckDb(context)) {
      String base = context.getSchemaContext().getMaterializeDirectory();
      List<Map<String, Object>> chunkRows = new ArrayList<Map<String, Object>>();
      for (RowConcatSource src : ROW_CONCAT_SOURCES) {
        chunkRows.addAll(chunkRowConcatSource(conn, base, src));
      }
      for (DocumentBlobSource src : DOCUMENT_BLOB_SOURCES) {
        chunkRows.addAll(chunkDocumentBlobSource(conn, base, src));
      }
      if (chunkRows.isEmpty()) {
        LOGGER.info("ChunkOrganizer: no chunks produced, nothing to write");
      } else {
        writeTable(context, TRIGGER_TABLE, chunkRows);
        LOGGER.info("ChunkOrganizer: wrote {} chunk rows to ref.vectorized_chunks",
            chunkRows.size());
      }
      backfillSecChunks(context, conn, base);
    } catch (Exception e) {
      // afterTable declares no throws clause; this is a best-effort post-processing pass over
      // already-committed data, so a failure here must not fail the ref schema's own ETL run.
      LOGGER.error("ChunkOrganizer: chunk organization failed", e);
    }
  }

  // ========================================================================
  // sec.vectorized_chunks -> ref.vectorized_chunks backfill (one-time migration)
  // ========================================================================
  //
  // See semantic-search-plan.md "Promoting vectorized_chunks". This is NOT a source in
  // ROW_CONCAT_SOURCES/DOCUMENT_BLOB_SOURCES: SEC's chunks are already organized (chunked +
  // enriched) by DocumentETLProcessor, this just reshapes and copies them into the shared
  // table. Off by default (GOVDATA_CHUNKS_BACKFILL) so routine ref schema runs never pay for
  // it. Time-boxed and resumable across separate invocations by construction: each year is
  // migrated via an anti-join against chunk_id values already present under
  // source_schema='sec', so re-running (whether after a clean stop at the time budget or an
  // interrupted/crashed run) only ever migrates what's still missing -- no separate progress
  // marker needed, same anti-join idea vss-local.py's cmd_backlog already uses for embeddings.
  //
  // Controlled by env vars, not the schema YAML/ModelOperand: this is a one-time operator
  // switch for a migration run, not per-schema adapter config, and GovDataSchemaFactory's
  // operand pipeline only carries a narrow, curated set of keys (dataSource/directory/tables/
  // casing/engine) through to what ModelOperand captures -- a schema-level custom key like this
  // one is silently unreachable there regardless of YAML declaration. GOVDATA_CHUNKS_BACKFILL /
  // the ETL_ prefix are exactly the run/infra-flag category model-operand-guard.py's own
  // EXEMPT_NAMES/EXEMPT_PREFIX carve out for direct System.getenv reads.

  private void backfillSecChunks(TableContext context, Connection conn, String base)
      throws SQLException, IOException {
    boolean enabled = "true".equalsIgnoreCase(System.getenv("GOVDATA_CHUNKS_BACKFILL"));
    LOGGER.info("ChunkOrganizer: SEC backfill env check: GOVDATA_CHUNKS_BACKFILL={}", enabled);
    if (!enabled) {
      return;
    }
    boolean dryRun = "true".equalsIgnoreCase(System.getenv("ETL_CHUNKS_BACKFILL_DRY_RUN"));
    long maxSeconds = 1800L;
    String maxSecondsEnv = System.getenv("ETL_CHUNKS_BACKFILL_MAX_SECONDS");
    if (maxSecondsEnv != null && !maxSecondsEnv.trim().isEmpty()) {
      try {
        maxSeconds = Long.parseLong(maxSecondsEnv.trim());
      } catch (NumberFormatException e) {
        LOGGER.warn("ChunkOrganizer: ETL_CHUNKS_BACKFILL_MAX_SECONDS='{}' not a number, "
            + "using default {}s", maxSecondsEnv, maxSeconds);
      }
    }

    String secLoc = base + "/sec/vectorized_chunks";
    String destLoc = base + "/" + context.getSchemaName() + "/" + TRIGGER_TABLE;

    int minYear;
    int maxYear;
    String rangeSql = "SELECT MIN(year) AS min_year, MAX(year) AS max_year FROM iceberg_scan('"
        + secLoc + "', allow_moved_paths=true)";
    List<Map<String, Object>> range = queryRows(conn, rangeSql);
    Object minObj = range.isEmpty() ? null : range.get(0).get("min_year");
    Object maxObj = range.isEmpty() ? null : range.get(0).get("max_year");
    if (minObj == null || maxObj == null) {
      LOGGER.info("ChunkOrganizer: SEC backfill enabled but sec.vectorized_chunks is empty "
          + "or unreadable, nothing to migrate");
      return;
    }
    minYear = ((Number) minObj).intValue();
    maxYear = ((Number) maxObj).intValue();

    LOGGER.info("ChunkOrganizer: SEC backfill starting, years {}-{}, dryRun={}, "
        + "maxSeconds={}", minYear, maxYear, dryRun, maxSeconds);
    java.time.Instant start = java.time.Instant.now();
    int yearsDone = 0;
    int yearsSkipped = 0;
    long totalRows = 0;
    for (int year = minYear; year <= maxYear; year++) {
      if (dryRun) {
        // Dry-run only ever reports a count, so let DuckDB compute it server-side and never
        // materialize row content (chunk_text/enriched_text) into the JVM at all. Recent SEC
        // years run into the millions of rows / multiple GiB (2003-2022 combined is a few MB;
        // 2023 alone is 2.7GiB) -- pulling that through as SELECT s.* into a Java List is what
        // actually exhausted heap, well before the time budget below ever got a chance to bite.
        String countSql = "SELECT COUNT(*) AS n FROM iceberg_scan('" + secLoc
            + "', allow_moved_paths=true) s WHERE s.year = " + year + " AND NOT EXISTS ("
            + "SELECT 1 FROM iceberg_scan('" + destLoc + "', allow_moved_paths=true) d "
            + "WHERE d.source_schema = 'sec' AND d.chunk_id = s.chunk_id)";
        long count = ((Number) queryRows(conn, countSql).get(0).get("n")).longValue();
        if (count == 0) {
          yearsSkipped++;
        } else {
          LOGGER.info("ChunkOrganizer: SEC backfill dry-run year {}: {} rows would be written",
              year, count);
          totalRows += count;
          yearsDone++;
        }
        long elapsedSeconds =
            java.time.Duration.between(start, java.time.Instant.now()).getSeconds();
        if (elapsedSeconds >= maxSeconds && year < maxYear) {
          LOGGER.info("ChunkOrganizer: SEC backfill time budget ({}s) reached after year {}; "
              + "{} year(s) remaining, will resume on next run", maxSeconds, year,
              maxYear - year);
          return;
        }
        continue;
      }

      // Real write: page through the year in bounded batches. Peak heap for one batch stays
      // constant regardless of the year's total backlog. Each batch is written and committed
      // immediately, so a mid-year time-budget stop is safe to resume from.
      //
      // Resume state is NOT a chunk_id high-water mark: chunk_id is unique within a year but
      // NOT across years -- the same accession/chunk occasionally appears under two different
      // `year` values upstream (a data-quality property of sec.vectorized_chunks, confirmed
      // live: e.g. accession 0000103730-24-000034's chunks are filed under both year=2023 and
      // year=2024). A MAX(chunk_id)-among-already-migrated cursor treats any such collision as
      // proof everything below it was migrated for THIS year, when it may only mean an EARLIER
      // year's migration happened to write a chunk_id that collides with one of this year's --
      // silently skipping the rest of the year. Measured live: this dropped ~4.7M of 9.6M rows
      // (years 2024-2025) before being caught by a post-migration row-count audit.
      //
      // Nor is it a per-batch anti-join against iceberg_scan(secLoc): re-running that same
      // WHERE-year-AND-NOT-EXISTS query, unindexed, once per 5000-row batch means DuckDB has to
      // rescan this year's whole remaining backlog on EVERY batch to find the next one -- also
      // measured live, ~80s/batch once a year reached a few hundred thousand still-unmigrated
      // rows, which does not finish a multi-million-row year in practical time.
      //
      // The rows still needing migration are computed ONCE per year (one full-row anti-join
      // against destLoc) into a local DuckDB temp table. Every batch after that pages the
      // already-materialized table -- no further S3/Iceberg scanning, so batch cost stays flat
      // regardless of how many batches the year takes. A cheap COUNT-only comparison first
      // skips this entirely for years already fully migrated, so a `starting`-to-first-batch
      // resume doesn't pay a full scan for every one of the 24 years on every future invocation.
      // This materialization is what backfill mode's raised memory_limit (see openDuckDb) is
      // for: a full year runs ~6GB of raw chunk_text/enriched_text through it at once.
      long yearSourceCount = ((Number) queryRows(conn,
          "SELECT COUNT(*) AS n FROM iceberg_scan('" + secLoc + "', allow_moved_paths=true) "
          + "WHERE year = " + year).get(0).get("n")).longValue();
      long yearMigratedCount = ((Number) queryRows(conn,
          "SELECT COUNT(*) AS n FROM iceberg_scan('" + secLoc + "', allow_moved_paths=true) s "
          + "WHERE s.year = " + year + " AND EXISTS ("
          + "SELECT 1 FROM iceberg_scan('" + destLoc + "', allow_moved_paths=true) d "
          + "WHERE d.source_schema = 'sec' AND d.chunk_id = s.chunk_id)").get(0).get("n"))
          .longValue();
      if (yearMigratedCount >= yearSourceCount) {
        yearsSkipped++;
        continue;
      }
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS tmp_sec_backfill_todo");
        stmt.execute("CREATE TEMP TABLE tmp_sec_backfill_todo AS "
            + "SELECT s.* FROM iceberg_scan('" + secLoc + "', allow_moved_paths=true) s "
            + "WHERE s.year = " + year + " AND NOT EXISTS ("
            + "SELECT 1 FROM iceberg_scan('" + destLoc + "', allow_moved_paths=true) d "
            + "WHERE d.source_schema = 'sec' AND d.chunk_id = s.chunk_id)");
      }
      boolean yearHasWork = false;
      while (true) {
        // No chunk_id > cursor filter: rows are DELETEd from tmp_sec_backfill_todo right after
        // being written (below), so the table shrinks as the year progresses instead of every
        // batch re-sorting the same untouched rows it already passed over -- a
        // "no chunk_id > cursor" version still costs O(remaining size) per batch even off a
        // local temp table (a plain ORDER BY + LIMIT with no index still scans everything ahead
        // of it). Deleting keeps each batch's scan bounded to what is actually still left.
        String sql = "SELECT * FROM tmp_sec_backfill_todo ORDER BY chunk_id LIMIT "
            + SEC_BACKFILL_BATCH_SIZE;
        List<Map<String, Object>> sourceRows = queryRows(conn, sql);
        if (sourceRows.isEmpty()) {
          break;
        }
        yearHasWork = true;
        List<Map<String, Object>> destRows = new ArrayList<Map<String, Object>>(sourceRows.size());
        for (Map<String, Object> row : sourceRows) {
          destRows.add(transformSecChunkRow(row));
        }
        writeAppendBatch(context, TRIGGER_TABLE, destRows);
        totalRows += destRows.size();
        String cursor = String.valueOf(sourceRows.get(sourceRows.size() - 1).get("chunk_id"));
        try (Statement stmt = conn.createStatement()) {
          stmt.execute("DELETE FROM tmp_sec_backfill_todo WHERE chunk_id <= '"
              + cursor.replace("'", "''") + "'");
        }
        LOGGER.info("ChunkOrganizer: SEC backfill year {}: wrote {} rows (batch, cursor={})",
            year, destRows.size(), cursor);

        long elapsedSeconds =
            java.time.Duration.between(start, java.time.Instant.now()).getSeconds();
        if (elapsedSeconds >= maxSeconds) {
          LOGGER.info("ChunkOrganizer: SEC backfill time budget ({}s) reached mid-year {}; "
              + "will resume on next run", maxSeconds, year);
          return;
        }
      }
      if (yearHasWork) {
        yearsDone++;
      } else {
        yearsSkipped++;
      }
    }
    LOGGER.info("ChunkOrganizer: SEC backfill complete: {} year(s) migrated ({} rows), "
        + "{} year(s) already up to date", yearsDone, totalRows, yearsSkipped);
  }

  /** Reshapes one sec.vectorized_chunks row into ref.vectorized_chunks's generalized shape.
   *  chunk_id is preserved as-is (not regenerated) so vectorized_chunk_codes' existing
   *  embeddings, keyed by this same chunk_id, keep resolving after the migration. */
  static Map<String, Object> transformSecChunkRow(Map<String, Object> row) {
    Object cik = row.get("cik");
    Object accessionNumber = row.get("accession_number");
    Object filingDate = row.get("filing_date");
    Map<String, Object> out = new LinkedHashMap<String, Object>();
    out.put("chunk_id", row.get("chunk_id"));
    out.put("source_schema", "sec");
    out.put("source_table", "vectorized_chunks");
    out.put("stringified_fk", cik + ":" + accessionNumber);
    // Coerce to Long: ResultSet#getObject doesn't guarantee a consistent Number subtype for
    // this BIGINT column, and ref.vectorized_chunks has no source: block, so its Iceberg schema
    // is inferred from this value's runtime type on first write.
    Object sequence = row.get("sequence");
    out.put("sequence", sequence == null ? null : ((Number) sequence).longValue());
    out.put("source_type", row.get("source_type"));
    out.put("chunk_text", row.get("chunk_text"));
    out.put("enriched_text", row.get("enriched_text"));
    out.put("section", row.get("section"));
    out.put("subsection", row.get("subsection"));
    out.put("section_path", row.get("section_path"));
    out.put("paragraph_continuation", row.get("paragraph_continuation"));
    // Same coercion as sequence above.
    Object paragraphNumber = row.get("paragraph_number");
    out.put("paragraph_number",
        paragraphNumber == null ? null : ((Number) paragraphNumber).longValue());
    out.put("content_type", row.get("content_type"));
    out.put("financial_concepts", row.get("financial_concepts"));
    out.put("exhibit_number", row.get("exhibit_number"));
    out.put("speaker_name", row.get("speaker_name"));
    out.put("speaker_role", row.get("speaker_role"));
    out.put("cik", cik);
    out.put("accession_number", accessionNumber);
    // ISO 8601 date string, matching the destination column's documented shape -- not the raw
    // java.sql.Date object, so schema inference lands on VARCHAR rather than guessing from a
    // JDBC type it has never seen from any other source.
    out.put("filing_date", filingDate == null ? null : filingDate.toString());
    return out;
  }

  // ========================================================================
  // Row-concat mode
  // ========================================================================

  private List<Map<String, Object>> chunkRowConcatSource(Connection conn, String base,
      RowConcatSource src) throws SQLException {
    String loc = base + "/" + src.sourceSchema + "/" + src.sourceTable;
    // SELECT DISTINCT pk cols + string cols together: a column can be both (e.g. naics_code
    // is the PK and also carries real text), so query each column once, not once per role.
    List<String> selectCols = new ArrayList<String>(src.pkColumns);
    for (String c : src.stringColumns) {
      if (!selectCols.contains(c)) {
        selectCols.add(c);
      }
    }
    String sql = "SELECT " + String.join(", ", selectCols) + " FROM iceberg_scan('"
        + loc + "', allow_moved_paths=true)";
    List<Map<String, Object>> sourceRows = queryRows(conn, sql);
    List<Map<String, Object>> chunkRows = new ArrayList<Map<String, Object>>();
    for (Map<String, Object> row : sourceRows) {
      String pkValue = stringifyPk(row, src.pkColumns);
      String text = buildRowConcatText(row, src.stringColumns);
      List<String> chunks = chunkFixed(text);
      for (int seq = 0; seq < chunks.size(); seq++) {
        Map<String, Object> chunkRow = new LinkedHashMap<String, Object>();
        chunkRow.put("chunk_id",
            src.sourceSchema + ":" + src.sourceTable + ":" + pkValue + ":" + seq);
        chunkRow.put("source_schema", src.sourceSchema);
        chunkRow.put("source_table", src.sourceTable);
        chunkRow.put("stringified_fk", pkValue);
        chunkRow.put("sequence", seq);
        chunkRow.put("source_type", "row_concat");
        chunkRow.put("chunk_text", chunks.get(seq));
        chunkRow.put("enriched_text", chunks.get(seq));
        if (src.wideFkColumn != null) {
          chunkRow.put(src.wideFkColumn, pkValue);
        }
        chunkRows.add(chunkRow);
      }
    }
    LOGGER.info("ChunkOrganizer: row-concat {}.{} -> {} chunks from {} rows",
        src.sourceSchema, src.sourceTable, chunkRows.size(), sourceRows.size());
    return chunkRows;
  }

  /** Stringifies a (possibly composite) primary key as ':'-joined column values -- uniform
   *  handling for single-column and composite PKs alike, per semantic-search-plan.md's
   *  "Storage shape". */
  static String stringifyPk(Map<String, Object> row, List<String> pkColumns) {
    StringBuilder sb = new StringBuilder();
    for (String col : pkColumns) {
      if (sb.length() > 0) {
        sb.append(':');
      }
      sb.append(row.get(col));
    }
    return sb.toString();
  }

  /** Builds 'col: value | col: value | ...' from a row's non-null values, in the given
   *  (source-declared) column order. No per-column inclusion/exclusion -- every listed
   *  column goes in if non-null, per semantic-search-plan.md's "no column-level filtering"
   *  rule. */
  static String buildRowConcatText(Map<String, Object> row, List<String> columns) {
    StringBuilder sb = new StringBuilder();
    for (String col : columns) {
      Object val = row.get(col);
      if (val == null) {
        continue;
      }
      if (sb.length() > 0) {
        sb.append(" | ");
      }
      sb.append(col).append(": ").append(val);
    }
    return sb.toString();
  }

  /** Naive fixed-window chunker with overlap. A short row-concat text (the common case)
   *  fits in one chunk -- see semantic-search-plan.md "Row-level design". */
  static List<String> chunkFixed(String text) {
    List<String> chunks = new ArrayList<String>();
    if (text == null || text.isEmpty()) {
      return chunks;
    }
    if (text.length() <= CHUNK_SIZE) {
      chunks.add(text);
      return chunks;
    }
    int step = CHUNK_SIZE - CHUNK_OVERLAP;
    int start = 0;
    while (start < text.length()) {
      chunks.add(text.substring(start, Math.min(start + CHUNK_SIZE, text.length())));
      if (start + CHUNK_SIZE >= text.length()) {
        break;
      }
      start += step;
    }
    return chunks;
  }

  // ========================================================================
  // Document-blob mode
  // ========================================================================

  private List<Map<String, Object>> chunkDocumentBlobSource(Connection conn, String base,
      DocumentBlobSource src) throws SQLException {
    String loc = base + "/" + src.sourceSchema + "/" + src.sourceTable;
    List<String> selectCols = new ArrayList<String>(src.pkColumns);
    if (!selectCols.contains(src.blobColumn)) {
      selectCols.add(src.blobColumn);
    }
    String sql = "SELECT " + String.join(", ", selectCols) + " FROM iceberg_scan('"
        + loc + "', allow_moved_paths=true)";
    List<Map<String, Object>> sourceRows = queryRows(conn, sql);
    List<Map<String, Object>> chunkRows = new ArrayList<Map<String, Object>>();
    SemanticTextChunker chunker = new SemanticTextChunker();
    for (Map<String, Object> row : sourceRows) {
      Object blobValue = row.get(src.blobColumn);
      if (blobValue == null) {
        continue;
      }
      String pkValue = stringifyPk(row, src.pkColumns);
      List<SemanticTextChunker.Chunk> chunks = chunker.chunkPlainText(blobValue.toString());
      for (SemanticTextChunker.Chunk chunk : chunks) {
        // SemanticTextChunker's own sequence numbers are 1-based; 0-based here matches
        // row-concat mode's convention (see vectorized_chunks.sequence's column comment).
        int seq = chunk.getSequenceNumber() - 1;
        Map<String, Object> chunkRow = new LinkedHashMap<String, Object>();
        chunkRow.put("chunk_id",
            src.sourceSchema + ":" + src.sourceTable + ":" + pkValue + ":" + seq);
        chunkRow.put("source_schema", src.sourceSchema);
        chunkRow.put("source_table", src.sourceTable);
        chunkRow.put("stringified_fk", pkValue);
        chunkRow.put("sequence", seq);
        chunkRow.put("source_type", src.sourceType);
        chunkRow.put("chunk_text", chunk.getText());
        chunkRow.put("enriched_text", chunk.getText());
        chunkRow.put("paragraph_continuation", chunk.isParagraphContinuation());
        if (src.wideFkColumn != null) {
          chunkRow.put(src.wideFkColumn, pkValue);
        }
        chunkRows.add(chunkRow);
      }
    }
    LOGGER.info("ChunkOrganizer: document-blob {}.{} -> {} chunks from {} rows",
        src.sourceSchema, src.sourceTable, chunkRows.size(), sourceRows.size());
    return chunkRows;
  }

  // ========================================================================
  // DuckDB / write-out (same pattern as EntityBridgeListener)
  // ========================================================================

  private Connection openDuckDb(TableContext context) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try (Statement stmt = conn.createStatement()) {
      // The one-time SEC chunks migration (GOVDATA_CHUNKS_BACKFILL) materializes a whole year's
      // still-to-migrate rows at once -- up to ~2.5M rows of chunk_text/enriched_text, ~6GB raw,
      // which blows well past the 2GB every OTHER ChunkOrganizer run uses for its normal
      // incremental per-source chunking. 8GB still spilled several GB (raw data + DuckDB's own
      // processing overhead exceeds 8GB); that spilling was also driving S3 write retries by
      // contending with MinIO for local disk I/O on the same host. 16GB is comfortably inside
      // the host's consistently-observed ~19-25GB free. More threads (host has 16 cores)
      // parallelizes the year-wide sort/materialize. The permanent, ongoing incremental path
      // (2 threads / 2GB) is unaffected either way.
      boolean backfillMode = "true".equalsIgnoreCase(System.getenv("GOVDATA_CHUNKS_BACKFILL"));
      stmt.execute("SET threads=" + (backfillMode ? "8" : "2"));
      stmt.execute("SET memory_limit='" + (backfillMode ? "16GB" : "2GB") + "'");
      String tempDir = System.getProperty("java.io.tmpdir", "/tmp") + "/chunk-organizer-duckdb";
      stmt.execute("SET temp_directory='" + tempDir + "'");
      try {
        stmt.execute("INSTALL parquet");
        stmt.execute("LOAD parquet");
      } catch (SQLException e) {
        LOGGER.debug("Parquet extension already loaded or built-in");
      }
    }
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSTALL iceberg");
      stmt.execute("LOAD iceberg");
      stmt.execute("SET unsafe_enable_version_guessing = true");
    } catch (SQLException e) {
      LOGGER.warn("DuckDB Iceberg extension unavailable: {}", e.getMessage());
    }
    Map<String, String> s3Config = context.getStorageProvider() != null
        ? context.getStorageProvider().getS3Config() : null;
    if (s3Config != null && !s3Config.isEmpty()) {
      try (Statement stmt = conn.createStatement()) {
        configureS3(stmt, s3Config);
      }
    }
    return conn;
  }

  private static void configureS3(Statement stmt, Map<String, String> s3Config)
      throws SQLException {
    stmt.execute("INSTALL httpfs");
    stmt.execute("LOAD httpfs");
    stmt.execute("SET http_timeout=10000");
    stmt.execute("SET http_retries=2");
    stmt.execute("SET http_retry_wait_ms=500");
    String accessKey = s3Config.get("accessKeyId");
    String secretKey = s3Config.get("secretAccessKey");
    String endpoint = s3Config.get("endpoint");
    String region = s3Config.containsKey("region") ? s3Config.get("region") : "auto";
    if (accessKey != null && secretKey != null) {
      StringBuilder secret = new StringBuilder("CREATE OR REPLACE SECRET calcite_s3 (TYPE S3");
      secret.append(", KEY_ID '").append(accessKey).append('\'');
      secret.append(", SECRET '").append(secretKey).append('\'');
      if (endpoint != null && !endpoint.isEmpty()) {
        String endpointHost = endpoint.replaceFirst("^https?://", "");
        secret.append(", ENDPOINT '").append(endpointHost).append('\'');
        secret.append(", URL_STYLE 'path'");
        secret.append(", USE_SSL ").append(endpoint.startsWith("http://") ? "false" : "true");
      }
      secret.append(", REGION '").append(region).append('\'');
      secret.append(')');
      stmt.execute(secret.toString());
    }
  }

  private static void writeTable(TableContext context, String tableName,
      List<Map<String, Object>> rows) throws IOException {
    EtlPipelineConfig tableConfig = tableConfigOf(context, tableName);
    MaterializeConfig matConfig =
        MaterializeConfig.withTableDefaults(tableConfig.getMaterialize(), tableConfig);
    String schemaMaterializeDir = context.getSchemaContext().getMaterializeDirectory()
        + "/" + context.getSchemaName();
    MaterializationWriter writer = MaterializationWriterFactory.createFromConfig(
        matConfig, context.getStorageProvider(), schemaMaterializeDir,
        context.getIncrementalTracker());
    writer.initialize(matConfig);
    writer.writeBatch(rows.iterator(), Collections.<String, String>emptyMap());
    writer.commit();
    writer.close();
  }

  /** Same target table as {@link #writeTable}, but {@code overwritePartitions: false} (plain
   *  append) instead of the main config's dynamic-overwrite-of-touched-partitions -- needed so
   *  each backfilled year adds to the {@code source_schema='sec'} partition instead of
   *  replacing it, since the anti-join in {@link #backfillSecChunks} (not partition-scoped
   *  overwrite) is what keeps repeated/resumed runs from duplicating rows. */
  private static void writeAppendBatch(TableContext context, String tableName,
      List<Map<String, Object>> rows) throws IOException {
    EtlPipelineConfig tableConfig = tableConfigOf(context, tableName);
    MaterializeConfig baseConfig = tableConfig.getMaterialize();
    List<String> partitionColumns = baseConfig.getPartition() != null
        && baseConfig.getPartition().getColumns() != null
        ? baseConfig.getPartition().getColumns()
        : Collections.<String>emptyList();
    String targetTableId =
        baseConfig.getTargetTableId() != null ? baseConfig.getTargetTableId() : tableName;

    MaterializeConfig.Builder builder = MaterializeConfig.builder()
        .enabled(true)
        .format(MaterializeConfig.Format.ICEBERG)
        .name(tableName)
        .targetTableId(targetTableId)
        .output(org.apache.calcite.adapter.file.etl.MaterializeOutputConfig.builder().build())
        .iceberg(MaterializeConfig.IcebergConfig.builder()
            .catalogType(MaterializeConfig.IcebergConfig.CatalogType.HADOOP)
            .overwritePartitions(false)
            .build());
    if (!partitionColumns.isEmpty()) {
      builder.partition(org.apache.calcite.adapter.file.etl.MaterializePartitionConfig.builder()
          .columns(partitionColumns)
          .build());
    }
    MaterializeConfig appendConfig =
        MaterializeConfig.withTableDefaults(builder.build(), tableConfig);

    String schemaMaterializeDir = context.getSchemaContext().getMaterializeDirectory()
        + "/" + context.getSchemaName();
    MaterializationWriter writer = MaterializationWriterFactory.createFromConfig(
        appendConfig, context.getStorageProvider(), schemaMaterializeDir,
        context.getIncrementalTracker());
    writer.initialize(appendConfig);
    writer.writeBatch(rows.iterator(), Collections.<String, String>emptyMap());
    writer.commit();
    writer.close();
  }

  private static EtlPipelineConfig tableConfigOf(TableContext context, String tableName) {
    for (EtlPipelineConfig cfg : context.getSchemaContext().getTables()) {
      if (tableName.equals(cfg.getName())) {
        return cfg;
      }
    }
    throw new IllegalStateException(
        "ChunkOrganizer: table config not found for " + tableName);
  }

  private static List<Map<String, Object>> queryRows(Connection conn, String sql)
      throws SQLException {
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
      ResultSetMetaData md = rs.getMetaData();
      int n = md.getColumnCount();
      while (rs.next()) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        for (int i = 1; i <= n; i++) {
          row.put(md.getColumnLabel(i), rs.getObject(i));
        }
        rows.add(row);
      }
    }
    return rows;
  }

  /** One row-concat source registry entry. */
  private static final class RowConcatSource {
    final String sourceSchema;
    final String sourceTable;
    /** One or more columns forming the source table's own primary key -- stringified_fk is
     *  their ':'-joined values, uniform for single-column and composite PKs alike. */
    final List<String> pkColumns;
    final List<String> stringColumns;
    /** Convenience wide FK column, only meaningful for a single-column PK -- a composite PK
     *  doesn't map to one column simply, so this is null for those sources (the generic
     *  stringified_fk still works for any PK shape). */
    final String wideFkColumn;

    RowConcatSource(String sourceSchema, String sourceTable, List<String> pkColumns,
        List<String> stringColumns, String wideFkColumn) {
      this.sourceSchema = sourceSchema;
      this.sourceTable = sourceTable;
      this.pkColumns = pkColumns;
      this.stringColumns = stringColumns;
      this.wideFkColumn = wideFkColumn;
    }
  }

  /** One document-blob source registry entry. */
  private static final class DocumentBlobSource {
    final String sourceSchema;
    final String sourceTable;
    final List<String> pkColumns;
    final String blobColumn;
    final String sourceType;
    final String wideFkColumn;

    DocumentBlobSource(String sourceSchema, String sourceTable, List<String> pkColumns,
        String blobColumn, String sourceType, String wideFkColumn) {
      this.sourceSchema = sourceSchema;
      this.sourceTable = sourceTable;
      this.pkColumns = pkColumns;
      this.blobColumn = blobColumn;
      this.sourceType = sourceType;
      this.wideFkColumn = wideFkColumn;
    }
  }
}
