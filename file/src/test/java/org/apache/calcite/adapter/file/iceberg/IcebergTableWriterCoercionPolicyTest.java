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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the per-column {@code onCoercionFailure} policy (FAIL/WARN/DROP) added to
 * {@link IcebergTableWriter#coerceValue}, exercised through the public {@link
 * IcebergTableWriter#writeRecords} API against a real local Iceberg table (Hadoop catalog,
 * temp-dir warehouse) — the same pattern {@link IcebergTableWriterTest} and {@link
 * IcebergTableWriterCoverageTest} already use.
 */
@Tag("unit")
class IcebergTableWriterCoercionPolicyTest {

  @TempDir
  Path tempDir;

  private Map<String, Object> catalogConfig;
  private StorageProvider storageProvider;
  private int tableCounter;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
    catalogConfig = new HashMap<>();
    catalogConfig.put("catalogType", "hadoop");
    catalogConfig.put("warehousePath", tempDir.resolve("warehouse").toString());
  }

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  private Table newTable(Types.NestedField... fields) {
    Schema schema = new Schema(fields);
    return IcebergCatalogManager.createTable(
        catalogConfig, "t" + (tableCounter++), schema, PartitionSpec.unpartitioned());
  }

  private static Map<String, Object> row(String name, String col, Object value) {
    Map<String, Object> r = new HashMap<>();
    r.put("name", name);
    r.put(col, value);
    return r;
  }

  // ---- WARN (default — no policy configured for the field) ----

  @Test void testDefaultPolicyIsWarnWritesNullOnBadInteger() throws Exception {
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "n", Types.IntegerType.get()));
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(row("bad", "n", "not-a-number"));

    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    assertEquals(1, df.recordCount(), "WARN writes the row (with NULL), never drops it");
  }

  // ---- FAIL ----

  @Test void testFailPolicyThrowsOnBadInteger() {
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "n", Types.IntegerType.get()));
    Map<String, String> policies = Collections.singletonMap("n", "FAIL");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider, policies);

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(row("bad", "n", "not-a-number"));

    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> writer.writeRecords(records, null));
    assertTrue(ex.getMessage().contains("n"), "error should name the failing column");
    assertTrue(ex.getMessage().contains("not-a-number"), "error should include the raw value");
  }

  @Test void testFailPolicyThrowsOnBadDate() {
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "d", Types.DateType.get()));
    Map<String, String> policies = Collections.singletonMap("d", "FAIL");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider, policies);

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(row("bad", "d", "27-SEP-24")); // not ISO — the raw coerceValue path is strict ISO

    assertThrows(IllegalStateException.class, () -> writer.writeRecords(records, null));
  }

  // ---- Implausible-epoch-day Number guard (fec.committee_contributions.transaction_date) ----

  @Test void testImplausibleEpochDayNumberFailsInsteadOfSilentlyCorrupting() {
    // Reproduces the live corruption: an unparsed 8-digit MMDDYYYY date string ("12242020",
    // meant to become 2020-12-24) reaching coerceValue's DATE case as a raw Number instead of
    // going through its declared dateFormat parser. Before the bound check, LocalDate.ofEpochDay
    // happily accepted it and silently wrote 35487-07-07 -- no error, no warning that would
    // survive a WARN policy's log line, just a wrong date. FAIL makes the corruption visible here.
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "d", Types.DateType.get()));
    Map<String, String> policies = Collections.singletonMap("d", "FAIL");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider, policies);

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(row("bad", "d", 12242020L));

    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> writer.writeRecords(records, null));
    assertTrue(ex.getMessage().contains("d"), "error should name the failing column");
  }

  @Test void testPlausibleEpochDayNumberStillWritesCorrectDate() throws Exception {
    // A genuine epoch-day count for a real calendar date (2024-01-15) must still work --
    // the guard bounds the year, it doesn't reject every Number.
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "d", Types.DateType.get()));
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(row("ok", "d", (int) java.time.LocalDate.of(2024, 1, 15).toEpochDay()));

    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    assertEquals(1, df.recordCount());
  }

  // ---- DROP ----

  @Test void testDropPolicyOmitsOnlyTheBadRow() throws Exception {
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "n", Types.IntegerType.get()));
    Map<String, String> policies = Collections.singletonMap("n", "DROP");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider, policies);

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(row("good-1", "n", "42"));
    records.add(row("bad", "n", "not-a-number"));
    records.add(row("good-2", "n", "7"));

    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    assertEquals(2, df.recordCount(), "only the row with the unparseable value is dropped");
  }

  // ---- BOOLEAN tri-state ----

  @Test void testBooleanRecognizesCommonTrueFalseTokens() throws Exception {
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "b", Types.BooleanType.get()));
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(row("yes", "b", "yes"));
    records.add(row("Y", "b", "Y"));
    records.add(row("1", "b", "1"));
    records.add(row("no", "b", "no"));
    records.add(row("N", "b", "N"));
    records.add(row("0", "b", "0"));

    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    assertEquals(6, df.recordCount());
  }

  @Test void testBooleanFailPolicyThrowsOnGarbageInsteadOfSilentFalse() {
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "b", Types.BooleanType.get()));
    Map<String, String> policies = Collections.singletonMap("b", "FAIL");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider, policies);

    List<Map<String, Object>> records = new ArrayList<>();
    // Boolean.parseBoolean("maybe") would have silently returned false under the old behavior —
    // this must now be treated as a genuine coercion failure, not a value.
    records.add(row("ambiguous", "b", "maybe"));

    assertThrows(IllegalStateException.class, () -> writer.writeRecords(records, null));
  }

  @Test void testBooleanDropPolicyOmitsGarbageRow() throws Exception {
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "b", Types.BooleanType.get()));
    Map<String, String> policies = Collections.singletonMap("b", "DROP");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider, policies);

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(row("good", "b", "true"));
    records.add(row("bad", "b", "maybe"));

    DataFile df = writer.writeRecords(records, null);
    assertEquals(1, df.recordCount());
  }

  // ---- Comma-formatted numbers are values, not coercion failures ----
  // (regression: caught live in the cftc DQ reingest — "550,000,000"-style notional amounts
  // were being nulled under WARN before this fix; they are valid numbers, not malformed data.)

  @Test void testCommaFormattedDoubleParsesCorrectly() throws Exception {
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "amount", Types.DoubleType.get()));
    Map<String, String> policies = Collections.singletonMap("amount", "FAIL");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider, policies);

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(row("notional", "amount", "550,000,000"));
    records.add(row("negative", "amount", "-12,834"));
    records.add(row("decimal", "amount", "714,147.23996"));
    // The actual shape observed live in the cftc DQ run: the source CSV quotes any field
    // containing the comma delimiter, and that quoting survives as literal characters (not
    // just log formatting) through this pipeline's JSON round-trip.
    records.add(row("quoted", "amount", "\"96,000,000\""));

    // FAIL policy: if comma/quote-stripping didn't work, this throws instead of writing.
    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    assertEquals(4, df.recordCount());
  }

  @Test void testCommaFormattedIntegerParsesCorrectly() throws Exception {
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "n", Types.IntegerType.get()));
    Map<String, String> policies = Collections.singletonMap("n", "FAIL");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider, policies);

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(row("thousands", "n", "2,000"));

    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    assertEquals(1, df.recordCount());
  }

  // ---- CFTC large-trade threshold marker ("+" suffix) is a value, not a coercion failure ----
  // (found live in the cftc DQ reingest — CFTC's Part 43 dissemination data marks
  // notional/quantity amounts at or above the public disclosure threshold with a trailing '+',
  // e.g. "250,000,000+"; the underlying number must still coerce. The censoring itself is
  // captured separately by a companion *_at_or_above_threshold expression column in
  // cftc-schema.yaml, not by this test — this test only covers the numeric-coercion half.)

  @Test void testTrailingPlusThresholdMarkerStrippedFromDouble() throws Exception {
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "amount", Types.DoubleType.get()));
    Map<String, String> policies = Collections.singletonMap("amount", "FAIL");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider, policies);

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(row("censored", "amount", "250,000,000+"));
    records.add(row("quoted-censored", "amount", "\"96,000,000+\""));

    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    assertEquals(2, df.recordCount());
  }

  // ---- Semicolon-delimited multi-value fields become LIST elements ----
  // (found live in the cftc DQ reingest — amortizing swaps report a schedule of end dates and
  // tiered payments as one semicolon-delimited field instead of a single scalar, e.g.
  // "2030-01-31;2030-02-28;2030-03-31".)

  @Test void testSemicolonDelimitedStringBecomesDateList() throws Exception {
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "dates",
            Types.ListType.ofOptional(100, Types.DateType.get())));
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(row("schedule", "dates", "2030-01-31;2030-02-28;2030-03-31"));
    records.add(row("single", "dates", "2030-01-31"));

    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    assertEquals(2, df.recordCount());
  }

  // ---- Non-String/List/Array wrapper types fall back to their text form ----
  // (found live in the cftc DQ reingest — org.duckdb.JsonNode, DuckDB's JDBC representation for
  // a column it infers as JSON rather than VARCHAR, was written straight to Parquet and failed
  // with "class org.duckdb.JsonNode cannot be cast to class java.util.Collection" inside
  // Iceberg's ParquetValueWriters$CollectionWriter. Simulated here with a minimal stand-in
  // object rather than a real DuckDB connection.)

  private static final class TextWrapper {
    private final String text;
    TextWrapper(String text) {
      this.text = text;
    }
    @Override public String toString() {
      return text;
    }
  }

  @Test void testUnrecognizedWrapperTypeFallsBackToTextForm() throws Exception {
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "dates",
            Types.ListType.ofOptional(100, Types.DateType.get())));
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(row("wrapped", "dates", new TextWrapper("2030-01-31;2030-02-28")));

    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    assertEquals(1, df.recordCount());
  }

  @Test void testSemicolonDelimitedStringBecomesDoubleList() throws Exception {
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "amounts",
            Types.ListType.ofOptional(100, Types.DoubleType.get())));
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(row("tiered", "amounts", "1,000,000;2,500,000;500,000"));

    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    assertEquals(1, df.recordCount());
  }

  // ---- TIMESTAMP coercion must always produce LocalDateTime, never epoch micros ----
  // (regression: BaseParquetWriter$TimestampWriter requires LocalDateTime for TIMESTAMP
  // WITHOUT TIMEZONE; a Long here throws ClassCastException at write time, caught live during
  // the cftc_trades DQ reingest once event_timestamp started round-tripping through DuckDB.)

  @Test void testTimestampFromLocalDateTimeWritesSuccessfully() throws Exception {
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "ts", Types.TimestampType.withoutZone()));
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider);

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(row("ldt", "ts", java.time.LocalDateTime.of(2026, 6, 4, 13, 38, 30)));
    records.add(row("instant", "ts", java.time.Instant.parse("2026-06-04T13:38:30Z")));
    records.add(row("sqlts", "ts", java.sql.Timestamp.valueOf("2026-06-04 13:38:30")));

    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    assertEquals(3, df.recordCount(), "all three input types must coerce and write, not throw");
  }

  // ---- Existing null-indicator behavior is unaffected ----

  @Test void testDashNullIndicatorStillBypassesPolicyEntirely() throws Exception {
    Table table = newTable(
        Types.NestedField.optional(1, "name", Types.StringType.get()),
        Types.NestedField.optional(2, "n", Types.IntegerType.get()));
    // FAIL policy configured, but "-" is the BLS missing-value sentinel handled before any
    // type-specific parsing is attempted — it must still become a quiet NULL, not a failure.
    Map<String, String> policies = Collections.singletonMap("n", "FAIL");
    IcebergTableWriter writer = new IcebergTableWriter(table, storageProvider, policies);

    List<Map<String, Object>> records = new ArrayList<>();
    records.add(row("missing", "n", "-"));

    DataFile df = writer.writeRecords(records, null);
    assertNotNull(df);
    assertEquals(1, df.recordCount());
  }
}
