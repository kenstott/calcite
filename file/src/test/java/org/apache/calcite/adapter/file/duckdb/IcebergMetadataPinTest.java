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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.storage.S3StorageProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** The pinned-metadata rewrite: hits cost no pointer read, expiry and invalidation re-read, DDL is untouched. */
@Tag("unit")
@org.junit.jupiter.api.parallel.ResourceLock("iceberg.metadata.pins")
class IcebergMetadataPinTest {

  private static final String TABLE = "s3://bucket/econ/average_prices";
  private static final String WRAPPED_TABLE = "s3://nowhere/main/t";
  private static final long TTL_MS = 30_000;

  private final AtomicInteger pointerReads = new AtomicInteger();
  private final AtomicInteger version = new AtomicInteger(5);
  private final AtomicLong nanos = new AtomicLong();
  private EtagRetryConnection.PinRewriter rewriter;

  @BeforeEach void setUp() {
    IcebergMetadataPins.invalidateAll();
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.conversionType = "ICEBERG_PARQUET";
    record.sourceFile = TABLE;
    Map<String, ConversionMetadata.ConversionRecord> records = new HashMap<>();
    records.put("average_prices", record);
    ConversionMetadata meta = mock(ConversionMetadata.class);
    when(meta.getAllConversions()).thenReturn(records);
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getConversionMetadata()).thenReturn(meta);
    when(fileSchema.getStorageProvider()).thenReturn(mock(S3StorageProvider.class));
    rewriter = new EtagRetryConnection.PinRewriter(fileSchema, TTL_MS,
        location -> {
          pointerReads.incrementAndGet();
          return location + "/metadata/v" + version.get() + ".metadata.json";
        },
        nanos::get);
  }

  @AfterEach void tearDown() {
    IcebergMetadataPins.invalidateAll();
  }

  private static final String SQL =
      "SELECT \"value\" FROM \"econ\".\"average_prices\" AS \"t\" WHERE \"year\" = 2024";

  @Test void rewritesBaseTableToPinnedMetadataFile() {
    String out = rewriter.rewrite(SQL);
    assertTrue(out.contains("iceberg_scan('" + TABLE + "/metadata/v5.metadata.json', "
        + "allow_moved_paths=true)"), out);
    assertTrue(out.contains(") AS \"t\" WHERE"), out);
    assertFalse(out.contains("\"econ\".\"average_prices\""), out);
  }

  @Test void aliaslessReferenceKeepsTheTableNameAsAlias() {
    String out = rewriter.rewrite("SELECT \"average_prices\".\"value\" FROM \"econ\".\"average_prices\"");
    assertTrue(out.endsWith(") AS \"average_prices\""), out);
  }

  @Test void hitWithinTtlReadsNoPointer() {
    rewriter.rewrite(SQL);
    rewriter.rewrite(SQL);
    nanos.addAndGet(TTL_MS * 1_000_000L - 1);
    rewriter.rewrite(SQL);
    assertEquals(1, pointerReads.get());
  }

  @Test void expiredPinRereadsAndFollowsTheNewVersion() {
    String first = rewriter.rewrite(SQL);
    version.set(6);
    nanos.addAndGet(TTL_MS * 1_000_000L);
    String second = rewriter.rewrite(SQL);
    assertEquals(2, pointerReads.get());
    assertNotEquals(first, second);
    assertTrue(second.contains("v6.metadata.json"), second);
  }

  @Test void invalidationIsSeenImmediately() {
    rewriter.rewrite(SQL);
    version.set(1);
    IcebergMetadataPins.invalidate(TABLE + "/");
    assertTrue(rewriter.rewrite(SQL).contains("v1.metadata.json"));
    IcebergMetadataPins.invalidateAll();
    version.set(2);
    assertTrue(rewriter.rewrite(SQL).contains("v2.metadata.json"));
    assertEquals(3, pointerReads.get());
  }

  @Test void zeroTtlTurnsPinningOff() {
    EtagRetryConnection.PinRewriter off = new EtagRetryConnection.PinRewriter(
        mock(FileSchema.class), 0, location -> {
          pointerReads.incrementAndGet();
          return location;
        }, nanos::get);
    assertFalse(off.enabled());
    assertEquals(SQL, off.rewrite(SQL));
    assertEquals(0, pointerReads.get());
  }

  @Test void ddlAndUnknownTablesAreNotRewritten() {
    String ddl = "CREATE VIEW \"econ\".\"v\" AS SELECT * FROM \"econ\".\"average_prices\"";
    assertEquals(ddl, rewriter.rewrite(ddl));
    String other = "SELECT 1 FROM \"econ\".\"not_iceberg\"";
    assertEquals(other, rewriter.rewrite(other));
    assertEquals(0, pointerReads.get());
  }

  @Test void unreadablePointerLeavesTheStatementUnpinned() {
    EtagRetryConnection.PinRewriter failing = new EtagRetryConnection.PinRewriter(
        mock(FileSchema.class), TTL_MS, location -> {
          throw new java.io.IOException("no hint");
        }, nanos::get);
    assertEquals(SQL, failing.rewrite(SQL));
  }

  @Test void ttlOperandDefaultsToThirtySecondsAndZeroDisables() {
    assertEquals(30_000L, DuckDBJdbcSchemaFactory.metadataPinTtlMillis(null));
    Map<String, Object> operand = new HashMap<>();
    assertEquals(30_000L, DuckDBJdbcSchemaFactory.metadataPinTtlMillis(operand));
    operand.put("icebergMetadataPinTtlSeconds", 0);
    assertEquals(0L, DuckDBJdbcSchemaFactory.metadataPinTtlMillis(operand));
    operand.put("icebergMetadataPinTtlSeconds", "60");
    assertEquals(60_000L, DuckDBJdbcSchemaFactory.metadataPinTtlMillis(operand));
  }

  private Connection pinnedConnection(java.sql.Connection raw, long ttlMs) throws Exception {
    ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
    record.conversionType = "ICEBERG_PARQUET";
    record.sourceFile = WRAPPED_TABLE;
    Map<String, ConversionMetadata.ConversionRecord> records = new HashMap<>();
    records.put("t", record);
    ConversionMetadata meta = mock(ConversionMetadata.class);
    when(meta.getAllConversions()).thenReturn(records);
    S3StorageProvider storage = mock(S3StorageProvider.class);
    when(storage.openInputStream(WRAPPED_TABLE + "/metadata/version-hint.text"))
        .thenAnswer(inv -> new java.io.ByteArrayInputStream("3".getBytes(
            java.nio.charset.StandardCharsets.UTF_8)));
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getConversionMetadata()).thenReturn(meta);
    when(fileSchema.getStorageProvider()).thenReturn(storage);
    return EtagRetryConnection.wrap(raw, fileSchema, ttlMs);
  }

  @Test void statementFailingAgainstAPinIsRetriedUnpinnedAndDropsThePins() throws Exception {
    try (java.sql.Connection raw = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
      raw.createStatement().execute("CREATE TABLE t AS SELECT 42 AS v");
      try (Connection conn = pinnedConnection(raw, TTL_MS)) {
        try (java.sql.Statement st = conn.createStatement();
            java.sql.ResultSet rs = st.executeQuery("SELECT \"v\" FROM \"memory\".\"main\".\"t\"")) {
          // three-part names are not matched, so this one is untouched and proves the plain path
          assertTrue(rs.next());
          assertEquals(42, rs.getInt(1));
        }
        try (java.sql.Statement st = conn.createStatement();
            java.sql.ResultSet rs = st.executeQuery("SELECT \"v\" FROM \"main\".\"t\" AS \"x\"")) {
          assertTrue(rs.next());
          assertEquals(42, rs.getInt(1));
        }
        assertFalse(IcebergMetadataPins.contains(WRAPPED_TABLE));
        try (java.sql.PreparedStatement ps =
                conn.prepareStatement("SELECT \"v\" FROM \"main\".\"t\" WHERE \"v\" = ?")) {
          ps.setInt(1, 42);
          try (java.sql.ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
          }
        }
        assertFalse(IcebergMetadataPins.contains(WRAPPED_TABLE));
      }
    }
  }

  @Test void ddlThroughTheConnectionNeverCarriesAPinnedPath() throws Exception {
    try (java.sql.Connection raw = java.sql.DriverManager.getConnection("jdbc:duckdb:")) {
      raw.createStatement().execute("CREATE TABLE t AS SELECT 42 AS v");
      try (Connection conn = pinnedConnection(raw, TTL_MS)) {
        conn.createStatement().execute(
            "CREATE VIEW dependent AS SELECT \"v\" FROM \"main\".\"t\"");
        try (java.sql.ResultSet rs = raw.createStatement().executeQuery(
            "SELECT sql FROM duckdb_views() WHERE view_name = 'dependent'")) {
          assertTrue(rs.next());
          assertFalse(rs.getString(1).contains("metadata.json"), rs.getString(1));
        }
        assertFalse(IcebergMetadataPins.contains(WRAPPED_TABLE));
      }
    }
  }
}
