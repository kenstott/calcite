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
 * Organizes chunks for {@code ref.vectorized_chunks}'s row-concat mode: concatenates each
 * included source row's string columns, naive-chunks the result, and writes the chunk
 * metadata (no vectors) into {@code ref.vectorized_chunks}. See
 * semantic-search-plan.md "Row-level design" / "Table curation".
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
public class RowConcatChunker implements TableLifecycleListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(RowConcatChunker.class);

  private static final String TRIGGER_TABLE = "vectorized_chunks";

  // Naive fixed-window chunk size / overlap over the delimited column concatenation.
  // Matches the plan's stated default; tunable, not tied to any model's token limit here
  // since row-concat sources are short (reference/dimension rows).
  private static final int CHUNK_SIZE = 1000;
  private static final int CHUNK_OVERLAP = 200;

  /** One row-concat source: an included entity-grain dimension table, per the v1 registry
   *  in semantic-search-plan.md's "Table curation". Add an entry here (and the matching
   *  wide FK column in ref-schema.yaml) to onboard a new source -- no other code change. */
  private static final List<RowConcatSource> SOURCES = Arrays.asList(
      new RowConcatSource("ref", "naics", Arrays.asList("naics_code"),
          Arrays.asList("naics_code", "naics_title"), "ref_naics_code"),
      // "type" is a real string column on both sources below (an Iceberg dimension marker,
      // constant per row) -- included per the plan's "no column-level filtering" rule even
      // though its retrieval value is near-zero; excluding it would be exactly the
      // judgment-call filtering the plan rejected.
      new RowConcatSource("ref", "naics_vintage", Arrays.asList("vintage", "naics_code"),
          Arrays.asList("naics_code", "naics_title", "type"), null),
      // ticker is part of the PK, not just content: a cik can carry multiple tickers
      // (multiple share classes) on the same as_of -- see the primaryKey comment in
      // ref-schema.yaml's constraints block.
      new RowConcatSource("ref", "sec_company_tickers",
          Arrays.asList("type", "as_of", "cik", "ticker"),
          Arrays.asList("cik", "ticker", "title", "type"), null));

  @Override public void beforeTable(TableContext context) {
    // No-op: this listener does all its work in afterTable, once, on TRIGGER_TABLE.
  }

  @Override public boolean onTableError(TableContext context, Exception error) {
    LOGGER.error("RowConcatChunker: table '{}' failed upstream of chunk organization",
        context.getTableName(), error);
    return true;
  }

  @Override public void afterTable(TableContext context, EtlResult result) {
    if (!TRIGGER_TABLE.equals(context.getTableName())) {
      return;
    }
    LOGGER.info("RowConcatChunker: organizing row-concat chunks for {} source(s)",
        SOURCES.size());
    try (Connection conn = openDuckDb(context)) {
      String base = context.getSchemaContext().getMaterializeDirectory();
      List<Map<String, Object>> chunkRows = new ArrayList<Map<String, Object>>();
      for (RowConcatSource src : SOURCES) {
        chunkRows.addAll(chunkSource(conn, base, src));
      }
      if (chunkRows.isEmpty()) {
        LOGGER.info("RowConcatChunker: no chunks produced, nothing to write");
        return;
      }
      writeTable(context, TRIGGER_TABLE, chunkRows);
      LOGGER.info("RowConcatChunker: wrote {} chunk rows to ref.vectorized_chunks",
          chunkRows.size());
    } catch (Exception e) {
      // afterTable declares no throws clause; this is a best-effort post-processing pass over
      // already-committed data, so a failure here must not fail the ref schema's own ETL run.
      LOGGER.error("RowConcatChunker: chunk organization failed", e);
    }
  }

  private List<Map<String, Object>> chunkSource(Connection conn, String base,
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
    LOGGER.info("RowConcatChunker: {}.{} -> {} chunks from {} rows",
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
  // DuckDB / write-out (same pattern as EntityBridgeListener)
  // ========================================================================

  private Connection openDuckDb(TableContext context) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:duckdb:");
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("SET threads=2");
      stmt.execute("SET memory_limit='2GB'");
      String tempDir = System.getProperty("java.io.tmpdir", "/tmp") + "/row-concat-chunker-duckdb";
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
    MaterializeConfig matConfig = tableConfig.getMaterialize();
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

  private static EtlPipelineConfig tableConfigOf(TableContext context, String tableName) {
    for (EtlPipelineConfig cfg : context.getSchemaContext().getTables()) {
      if (tableName.equals(cfg.getName())) {
        return cfg;
      }
    }
    throw new IllegalStateException(
        "RowConcatChunker: table config not found for " + tableName);
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
}
