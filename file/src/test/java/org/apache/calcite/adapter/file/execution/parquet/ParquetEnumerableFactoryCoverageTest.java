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
package org.apache.calcite.adapter.file.execution.parquet;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for ParquetEnumerableFactory inner classes:
 * ParquetEnumerator, FilteredParquetEnumerator, TimeFilteredParquetEnumerator.
 */
@SuppressWarnings("deprecation")
@Tag("unit")
public class ParquetEnumerableFactoryCoverageTest {

  @TempDir
  java.nio.file.Path tempDir;

  private File parquetFile;
  private File timestampParquetFile;
  private File mixedTypesParquetFile;

  @BeforeEach
  void setUp() throws IOException {
    // Ensure vectorized reader is disabled for these tests
    System.setProperty("parquet.enable.vectorized.reader", "false");
    parquetFile = createSimpleParquetFile();
    timestampParquetFile = createTimestampParquetFile();
    mixedTypesParquetFile = createMixedTypesParquetFile();
  }

  private File createSimpleParquetFile() throws IOException {
    File file = new File(tempDir.toFile(), "simple.parquet");
    Schema schema = SchemaBuilder.record("SimpleRecord")
        .fields()
        .requiredString("name")
        .requiredInt("age")
        .requiredDouble("score")
        .endRecord();

    Path hadoopPath = new Path(file.getAbsolutePath());
    Configuration conf = new Configuration();

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
        .withSchema(schema)
        .withConf(conf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .build()) {

      for (int i = 0; i < 5; i++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", "Person_" + i);
        record.put("age", 20 + i);
        record.put("score", 90.0 + i);
        writer.write(record);
      }
    }
    return file;
  }

  private File createTimestampParquetFile() throws IOException {
    File file = new File(tempDir.toFile(), "timestamps.parquet");

    // Create schema with date, time, and timestamp fields
    Schema dateSchema = Schema.create(Schema.Type.INT);
    LogicalTypes.date().addToSchema(dateSchema);

    Schema timeSchema = Schema.create(Schema.Type.INT);
    LogicalTypes.timeMillis().addToSchema(timeSchema);

    Schema timestampSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.timestampMillis().addToSchema(timestampSchema);

    Schema schema = SchemaBuilder.record("TimestampRecord")
        .fields()
        .requiredString("id")
        .name("event_date").type(dateSchema).noDefault()
        .name("event_time").type(Schema.createUnion(Schema.create(Schema.Type.NULL), timeSchema)).noDefault()
        .name("event_ts").type(timestampSchema).noDefault()
        .endRecord();

    Path hadoopPath = new Path(file.getAbsolutePath());
    Configuration conf = new Configuration();

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
        .withSchema(schema)
        .withConf(conf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .build()) {

      // Row with all values
      GenericRecord record1 = new GenericData.Record(schema);
      record1.put("id", "A");
      record1.put("event_date", 19000);  // days since epoch
      record1.put("event_time", 3600000);  // 1 hour in millis
      record1.put("event_ts", 1700000000000L);
      writer.write(record1);

      // Row with null time
      GenericRecord record2 = new GenericData.Record(schema);
      record2.put("id", "B");
      record2.put("event_date", 19001);
      record2.put("event_time", null);
      record2.put("event_ts", 1700000001000L);
      writer.write(record2);

      // Row with all values
      GenericRecord record3 = new GenericData.Record(schema);
      record3.put("id", "C");
      record3.put("event_date", 19002);
      record3.put("event_time", 7200000);
      record3.put("event_ts", 1700000002000L);
      writer.write(record3);
    }
    return file;
  }

  private File createMixedTypesParquetFile() throws IOException {
    File file = new File(tempDir.toFile(), "mixed.parquet");

    Schema schema = SchemaBuilder.record("MixedRecord")
        .fields()
        .requiredString("name")
        .optionalString("nullable_string")
        .requiredInt("count")
        .requiredBoolean("active")
        .endRecord();

    Path hadoopPath = new Path(file.getAbsolutePath());
    Configuration conf = new Configuration();

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
        .withSchema(schema)
        .withConf(conf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .build()) {

      GenericRecord record1 = new GenericData.Record(schema);
      record1.put("name", "Alice");
      record1.put("nullable_string", "hello");
      record1.put("count", 10);
      record1.put("active", true);
      writer.write(record1);

      GenericRecord record2 = new GenericData.Record(schema);
      record2.put("name", "Bob");
      record2.put("nullable_string", null);
      record2.put("count", 20);
      record2.put("active", false);
      writer.write(record2);
    }
    return file;
  }

  // === ParquetEnumerator tests (via enumerable()) ===

  @Test void testEnumerableReadsAllRows() {
    Enumerable<Object[]> enumerable =
        ParquetEnumerableFactory.enumerable(parquetFile.getAbsolutePath());
    assertNotNull(enumerable);

    Enumerator<Object[]> enumerator = enumerable.enumerator();
    int count = 0;
    while (enumerator.moveNext()) {
      Object[] row = enumerator.current();
      assertNotNull(row);
      assertEquals(3, row.length);
      count++;
    }
    assertEquals(5, count);
    enumerator.close();
  }

  @Test void testEnumeratorCurrent() {
    Enumerable<Object[]> enumerable =
        ParquetEnumerableFactory.enumerable(parquetFile.getAbsolutePath());
    Enumerator<Object[]> enumerator = enumerable.enumerator();

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);
    assertEquals("Person_0", row[0].toString());
    assertEquals(20, row[1]);
    assertEquals(90.0, row[2]);

    enumerator.close();
  }

  @Test void testEnumeratorReset() {
    Enumerable<Object[]> enumerable =
        ParquetEnumerableFactory.enumerable(parquetFile.getAbsolutePath());
    Enumerator<Object[]> enumerator = enumerable.enumerator();

    // Read all rows
    int firstPass = 0;
    while (enumerator.moveNext()) {
      firstPass++;
    }
    assertEquals(5, firstPass);

    // Reset and read again
    enumerator.reset();
    int secondPass = 0;
    while (enumerator.moveNext()) {
      secondPass++;
    }
    assertEquals(5, secondPass);

    enumerator.close();
  }

  @Test void testEnumeratorCloseIdempotent() {
    Enumerable<Object[]> enumerable =
        ParquetEnumerableFactory.enumerable(parquetFile.getAbsolutePath());
    Enumerator<Object[]> enumerator = enumerable.enumerator();
    enumerator.moveNext();

    // Close multiple times should not throw
    enumerator.close();
    enumerator.close();
  }

  @Test void testEnumeratorWithTimestampTypes() {
    Enumerable<Object[]> enumerable =
        ParquetEnumerableFactory.enumerable(timestampParquetFile.getAbsolutePath());
    Enumerator<Object[]> enumerator = enumerable.enumerator();

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);
    assertEquals("A", row[0].toString());
    // Date should be Integer (days since epoch)
    assertEquals(19000, row[1]);
    // Time should be Integer (millis since midnight)
    assertEquals(3600000, row[2]);
    // Timestamp should be Long (millis since epoch)
    assertEquals(1700000000000L, row[3]);

    enumerator.close();
  }

  @Test void testEnumeratorMovesNextReturnsFalseWhenFinished() {
    Enumerable<Object[]> enumerable =
        ParquetEnumerableFactory.enumerable(parquetFile.getAbsolutePath());
    Enumerator<Object[]> enumerator = enumerable.enumerator();

    while (enumerator.moveNext()) {
      // consume all
    }

    // Should return false consistently
    assertFalse(enumerator.moveNext());
    assertFalse(enumerator.moveNext());

    enumerator.close();
  }

  // === FilteredParquetEnumerator tests (via enumerableWithFilters()) ===

  @Test void testFilteredEnumerableNoFilters() {
    boolean[] filters = new boolean[]{false, false, false, false};
    Enumerable<Object[]> enumerable =
        ParquetEnumerableFactory.enumerableWithFilters(
            mixedTypesParquetFile.getAbsolutePath(), filters);
    Enumerator<Object[]> enumerator = enumerable.enumerator();

    int count = 0;
    while (enumerator.moveNext()) {
      count++;
    }
    assertEquals(2, count);
    enumerator.close();
  }

  @Test void testFilteredEnumerableFiltersNulls() {
    // Filter on nullable_string (index 1) - should skip rows where it's null
    boolean[] filters = new boolean[]{false, true, false, false};
    Enumerable<Object[]> enumerable =
        ParquetEnumerableFactory.enumerableWithFilters(
            mixedTypesParquetFile.getAbsolutePath(), filters);
    Enumerator<Object[]> enumerator = enumerable.enumerator();

    int count = 0;
    while (enumerator.moveNext()) {
      Object[] row = enumerator.current();
      assertNotNull(row[1], "Filtered column should not be null");
      count++;
    }
    // Only Alice should pass (Bob has null nullable_string)
    assertEquals(1, count);
    enumerator.close();
  }

  @Test void testFilteredEnumeratorReset() {
    boolean[] filters = new boolean[]{false, true, false, false};
    Enumerable<Object[]> enumerable =
        ParquetEnumerableFactory.enumerableWithFilters(
            mixedTypesParquetFile.getAbsolutePath(), filters);
    Enumerator<Object[]> enumerator = enumerable.enumerator();

    int firstPass = 0;
    while (enumerator.moveNext()) {
      firstPass++;
    }

    enumerator.reset();

    int secondPass = 0;
    while (enumerator.moveNext()) {
      secondPass++;
    }
    assertEquals(firstPass, secondPass);
    enumerator.close();
  }

  @Test void testFilteredEnumeratorWithTimestamps() {
    boolean[] filters = new boolean[]{false, false, false, false};
    Enumerable<Object[]> enumerable =
        ParquetEnumerableFactory.enumerableWithFilters(
            timestampParquetFile.getAbsolutePath(), filters);
    Enumerator<Object[]> enumerator = enumerable.enumerator();

    // Should read all 3 rows without filtering
    int count = 0;
    while (enumerator.moveNext()) {
      Object[] row = enumerator.current();
      assertNotNull(row);
      count++;
    }
    assertEquals(3, count);
    enumerator.close();
  }

  // === TimeFilteredParquetEnumerator tests (via enumerableWithTimeFiltering()) ===

  @Test void testTimeFilteredEnumerableSkipsNullTime() {
    Enumerable<Object[]> enumerable =
        ParquetEnumerableFactory.enumerableWithTimeFiltering(
            timestampParquetFile.getAbsolutePath());
    Enumerator<Object[]> enumerator = enumerable.enumerator();

    List<String> ids = new ArrayList<>();
    while (enumerator.moveNext()) {
      Object[] row = enumerator.current();
      ids.add(row[0].toString());
    }

    // Row B has null time, should be filtered out
    assertEquals(2, ids.size());
    assertTrue(ids.contains("A"));
    assertTrue(ids.contains("C"));
    assertFalse(ids.contains("B"));

    enumerator.close();
  }

  @Test void testTimeFilteredEnumeratorReset() {
    Enumerable<Object[]> enumerable =
        ParquetEnumerableFactory.enumerableWithTimeFiltering(
            timestampParquetFile.getAbsolutePath());
    Enumerator<Object[]> enumerator = enumerable.enumerator();

    int firstPass = 0;
    while (enumerator.moveNext()) {
      firstPass++;
    }

    enumerator.reset();

    int secondPass = 0;
    while (enumerator.moveNext()) {
      secondPass++;
    }
    assertEquals(firstPass, secondPass);
    enumerator.close();
  }

  @Test void testTimeFilteredEnumeratorCurrentReturnsCorrectData() {
    Enumerable<Object[]> enumerable =
        ParquetEnumerableFactory.enumerableWithTimeFiltering(
            timestampParquetFile.getAbsolutePath());
    Enumerator<Object[]> enumerator = enumerable.enumerator();

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertEquals("A", row[0].toString());
    assertEquals(19000, row[1]); // date
    assertEquals(3600000, row[2]); // time
    assertEquals(1700000000000L, row[3]); // timestamp

    enumerator.close();
  }

  @Test void testEnumerableWithNullableFields() {
    Enumerable<Object[]> enumerable =
        ParquetEnumerableFactory.enumerable(mixedTypesParquetFile.getAbsolutePath());
    Enumerator<Object[]> enumerator = enumerable.enumerator();

    assertTrue(enumerator.moveNext());
    Object[] row1 = enumerator.current();
    assertEquals("Alice", row1[0].toString());
    assertEquals("hello", row1[1].toString());

    assertTrue(enumerator.moveNext());
    Object[] row2 = enumerator.current();
    assertEquals("Bob", row2[0].toString());
    assertNull(row2[1]); // nullable_string is null

    enumerator.close();
  }
}
