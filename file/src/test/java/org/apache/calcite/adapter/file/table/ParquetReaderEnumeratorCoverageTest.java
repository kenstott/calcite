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
package org.apache.calcite.adapter.file.table;

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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@code ParquetTranslatableTable.ParquetReaderEnumerator}.
 *
 * <p>Creates real Parquet files with various field types (string, int, long, date,
 * time-millis, timestamp-millis, union/nullable) and exercises the inner enumerator
 * via reflection. Tests cover:
 * <ul>
 *   <li>initReader - successful initialization</li>
 *   <li>current() - returns the current row</li>
 *   <li>moveNext() - iterates through rows, handles end-of-file</li>
 *   <li>moveNext() - converts Avro Utf8 to String</li>
 *   <li>moveNext() - handles date logical type (keeps as Integer)</li>
 *   <li>moveNext() - handles time-millis logical type, skips null time rows</li>
 *   <li>moveNext() - handles timestamp-millis logical type (keeps as long)</li>
 *   <li>moveNext() - handles union types (nullable fields)</li>
 *   <li>moveNext() - cancel flag stops iteration</li>
 *   <li>reset() - re-initializes reader</li>
 *   <li>close() - closes reader, idempotent</li>
 * </ul>
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
public class ParquetReaderEnumeratorCoverageTest {

  @TempDir
  java.nio.file.Path tempDir;

  /**
   * Creates a Parquet file with simple string and int fields.
   */
  private File createSimpleParquetFile(String name, int numRows) throws Exception {
    Schema schema = SchemaBuilder.record("TestRecord")
        .fields()
        .requiredString("name")
        .requiredInt("id")
        .endRecord();

    File file = tempDir.resolve(name).toFile();
    Path hadoopPath = new Path(file.getAbsolutePath());

    @SuppressWarnings("deprecation")
    ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
        .withSchema(schema)
        .withConf(new Configuration())
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .build();

    for (int i = 0; i < numRows; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("name", "row_" + i);
      record.put("id", i);
      writer.write(record);
    }
    writer.close();
    return file;
  }

  /**
   * Creates a Parquet file with various logical types.
   */
  private File createLogicalTypesParquetFile(String name) throws Exception {
    // Build schema with logical types
    Schema dateSchema = Schema.create(Schema.Type.INT);
    LogicalTypes.date().addToSchema(dateSchema);

    Schema timeMillisSchema = Schema.create(Schema.Type.INT);
    LogicalTypes.timeMillis().addToSchema(timeMillisSchema);

    Schema timestampMillisSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.timestampMillis().addToSchema(timestampMillisSchema);

    // Nullable string field (union with null)
    Schema nullableStringSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.STRING));

    Schema recordSchema = Schema.createRecord("LogicalRecord", null, null, false);
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    fields.add(new Schema.Field("date_field", dateSchema, null, null));
    fields.add(new Schema.Field("time_field", timeMillisSchema, null, null));
    fields.add(new Schema.Field("ts_field", timestampMillisSchema, null, null));
    fields.add(new Schema.Field("nullable_name", nullableStringSchema, null, null));
    recordSchema.setFields(fields);

    File file = tempDir.resolve(name).toFile();
    Path hadoopPath = new Path(file.getAbsolutePath());

    @SuppressWarnings("deprecation")
    ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
        .withSchema(recordSchema)
        .withConf(new Configuration())
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .build();

    // Row 1: all fields present
    GenericRecord row1 = new GenericData.Record(recordSchema);
    row1.put("date_field", 19000);      // days since epoch
    row1.put("time_field", 3600000);    // 1 hour in millis
    row1.put("ts_field", 1700000000000L); // some timestamp
    row1.put("nullable_name", "hello");
    writer.write(row1);

    // Row 2: nullable field is null
    GenericRecord row2 = new GenericData.Record(recordSchema);
    row2.put("date_field", 19001);
    row2.put("time_field", 7200000);
    row2.put("ts_field", 1700000001000L);
    row2.put("nullable_name", null);
    writer.write(row2);

    writer.close();
    return file;
  }

  /**
   * Creates a Parquet file where a time field has null values to test skip logic.
   */
  private File createNullTimeParquetFile(String name) throws Exception {
    Schema timeMillisSchema = Schema.create(Schema.Type.INT);
    LogicalTypes.timeMillis().addToSchema(timeMillisSchema);

    // Nullable time field
    Schema nullableTimeSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        timeMillisSchema);

    Schema recordSchema = Schema.createRecord("NullTimeRecord", null, null, false);
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    fields.add(new Schema.Field("id", Schema.create(Schema.Type.INT), null, null));
    fields.add(new Schema.Field("time_field", nullableTimeSchema, null, null));
    recordSchema.setFields(fields);

    File file = tempDir.resolve(name).toFile();
    Path hadoopPath = new Path(file.getAbsolutePath());

    @SuppressWarnings("deprecation")
    ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
        .withSchema(recordSchema)
        .withConf(new Configuration())
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .build();

    // Row 1: time is not null
    GenericRecord row1 = new GenericData.Record(recordSchema);
    row1.put("id", 1);
    row1.put("time_field", 3600000);
    writer.write(row1);

    // Row 2: time is null => should be skipped by enumerator
    GenericRecord row2 = new GenericData.Record(recordSchema);
    row2.put("id", 2);
    row2.put("time_field", null);
    writer.write(row2);

    // Row 3: time is not null
    GenericRecord row3 = new GenericData.Record(recordSchema);
    row3.put("id", 3);
    row3.put("time_field", 7200000);
    writer.write(row3);

    writer.close();
    return file;
  }

  /**
   * Creates a ParquetReaderEnumerator via reflection.
   */
  @SuppressWarnings("unchecked")
  private Enumerator<Object[]> createEnumerator(File parquetFile, AtomicBoolean cancelFlag)
      throws Exception {
    // ParquetReaderEnumerator is a private inner class of ParquetTranslatableTable
    // We need to create an instance via the outer class
    Class<?>[] innerClasses = ParquetTranslatableTable.class.getDeclaredClasses();
    Class<?> enumeratorClass = null;
    for (Class<?> c : innerClasses) {
      if (c.getSimpleName().equals("ParquetReaderEnumerator")) {
        enumeratorClass = c;
        break;
      }
    }
    assertNotNull(enumeratorClass, "ParquetReaderEnumerator class should exist");

    // Create the outer table object first (needs schemaName)
    ParquetTranslatableTable table = new ParquetTranslatableTable(
        parquetFile, "test_schema");

    // Wait briefly for background stats thread to complete or start
    Thread.sleep(100);

    // Get the constructor: ParquetReaderEnumerator(ParquetTranslatableTable outer, AtomicBoolean cancelFlag)
    Constructor<?> ctor = enumeratorClass.getDeclaredConstructor(
        ParquetTranslatableTable.class, AtomicBoolean.class);
    ctor.setAccessible(true);

    return (Enumerator<Object[]>) ctor.newInstance(table, cancelFlag);
  }

  // === Tests ===

  @Test void testSimpleIteration() throws Exception {
    File file = createSimpleParquetFile("simple.parquet", 3);
    AtomicBoolean cancel = new AtomicBoolean(false);
    Enumerator<Object[]> enumerator = createEnumerator(file, cancel);

    try {
      // Iterate through all rows
      int count = 0;
      while (enumerator.moveNext()) {
        Object[] row = enumerator.current();
        assertNotNull(row);
        assertEquals(2, row.length);
        // name field should be a String (Avro Utf8 converted to String)
        assertTrue(row[0] instanceof String, "name should be String, was: "
            + (row[0] != null ? row[0].getClass().getName() : "null"));
        assertEquals("row_" + count, row[0].toString());
        assertEquals(count, row[1]);
        count++;
      }
      assertEquals(3, count);

      // moveNext after end should return false
      assertFalse(enumerator.moveNext());
    } finally {
      enumerator.close();
    }
  }

  @Test void testCurrentReturnsCurrentRow() throws Exception {
    File file = createSimpleParquetFile("current_test.parquet", 1);
    AtomicBoolean cancel = new AtomicBoolean(false);
    Enumerator<Object[]> enumerator = createEnumerator(file, cancel);

    try {
      assertTrue(enumerator.moveNext());
      Object[] first = enumerator.current();
      assertNotNull(first);
      // Calling current again should return the same row
      Object[] same = enumerator.current();
      assertArrayEquals(first, same);
    } finally {
      enumerator.close();
    }
  }

  @Test void testLogicalTypes() throws Exception {
    File file = createLogicalTypesParquetFile("logical_types.parquet");
    AtomicBoolean cancel = new AtomicBoolean(false);
    Enumerator<Object[]> enumerator = createEnumerator(file, cancel);

    try {
      // Row 1: all fields present
      assertTrue(enumerator.moveNext());
      Object[] row1 = enumerator.current();
      assertNotNull(row1);
      assertEquals(4, row1.length);
      // date_field: kept as Integer
      assertEquals(19000, row1[0]);
      // time_field: kept as Integer (millis since midnight)
      assertEquals(3600000, row1[1]);
      // ts_field: kept as long (timestamp millis)
      assertEquals(1700000000000L, row1[2]);
      // nullable_name: should be String
      assertTrue(row1[3] instanceof String || row1[3] == null);

      // Row 2: nullable_name is null
      assertTrue(enumerator.moveNext());
      Object[] row2 = enumerator.current();
      assertNotNull(row2);
      assertNull(row2[3], "nullable_name should be null");

      assertFalse(enumerator.moveNext());
    } finally {
      enumerator.close();
    }
  }

  @Test void testNullTimeSkipsRow() throws Exception {
    File file = createNullTimeParquetFile("null_time.parquet");
    AtomicBoolean cancel = new AtomicBoolean(false);
    Enumerator<Object[]> enumerator = createEnumerator(file, cancel);

    try {
      // Row 1 (id=1, time_field=3600000) should be returned
      assertTrue(enumerator.moveNext());
      Object[] row1 = enumerator.current();
      assertEquals(1, row1[0]);

      // Row 2 (id=2, time_field=null) should be SKIPPED
      // Row 3 (id=3, time_field=7200000) should be returned
      assertTrue(enumerator.moveNext());
      Object[] row3 = enumerator.current();
      assertEquals(3, row3[0]);

      assertFalse(enumerator.moveNext());
    } finally {
      enumerator.close();
    }
  }

  @Test void testCancelFlagStopsIteration() throws Exception {
    File file = createSimpleParquetFile("cancel.parquet", 100);
    AtomicBoolean cancel = new AtomicBoolean(false);
    Enumerator<Object[]> enumerator = createEnumerator(file, cancel);

    try {
      // Read a few rows then cancel
      assertTrue(enumerator.moveNext());
      assertTrue(enumerator.moveNext());
      cancel.set(true);
      assertFalse(enumerator.moveNext(), "Should stop when cancel flag is set");
    } finally {
      enumerator.close();
    }
  }

  @Test void testResetReInitializesReader() throws Exception {
    File file = createSimpleParquetFile("reset.parquet", 3);
    AtomicBoolean cancel = new AtomicBoolean(false);
    Enumerator<Object[]> enumerator = createEnumerator(file, cancel);

    try {
      // Read all rows
      int count1 = 0;
      while (enumerator.moveNext()) {
        count1++;
      }
      assertEquals(3, count1);

      // Reset and read again
      enumerator.reset();
      int count2 = 0;
      while (enumerator.moveNext()) {
        count2++;
      }
      assertEquals(3, count2);
    } finally {
      enumerator.close();
    }
  }

  @Test void testCloseIdempotent() throws Exception {
    File file = createSimpleParquetFile("close_test.parquet", 1);
    AtomicBoolean cancel = new AtomicBoolean(false);
    Enumerator<Object[]> enumerator = createEnumerator(file, cancel);

    // Close multiple times should not throw
    enumerator.close();
    enumerator.close();
    enumerator.close();
  }

  @Test void testEmptyFile() throws Exception {
    File file = createSimpleParquetFile("empty.parquet", 0);
    AtomicBoolean cancel = new AtomicBoolean(false);
    Enumerator<Object[]> enumerator = createEnumerator(file, cancel);

    try {
      assertFalse(enumerator.moveNext(), "Empty file should return false immediately");
    } finally {
      enumerator.close();
    }
  }

  @Test void testMoveNextAfterFinished() throws Exception {
    File file = createSimpleParquetFile("finished.parquet", 1);
    AtomicBoolean cancel = new AtomicBoolean(false);
    Enumerator<Object[]> enumerator = createEnumerator(file, cancel);

    try {
      assertTrue(enumerator.moveNext());
      assertFalse(enumerator.moveNext()); // finished=true now
      // Calling again should still return false (finished flag)
      assertFalse(enumerator.moveNext());
    } finally {
      enumerator.close();
    }
  }

  @Test void testTimestampMillisKeepsAsLong() throws Exception {
    // Create file with only timestamp field
    Schema tsSchema = Schema.create(Schema.Type.LONG);
    LogicalTypes.timestampMillis().addToSchema(tsSchema);

    Schema recordSchema = Schema.createRecord("TsRecord", null, null, false);
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    fields.add(new Schema.Field("ts", tsSchema, null, null));
    recordSchema.setFields(fields);

    File file = tempDir.resolve("ts_only.parquet").toFile();
    Path hadoopPath = new Path(file.getAbsolutePath());

    @SuppressWarnings("deprecation")
    ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(hadoopPath)
        .withSchema(recordSchema)
        .withConf(new Configuration())
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .build();

    long expectedTs = 1609459200000L; // 2021-01-01T00:00:00Z
    GenericRecord record = new GenericData.Record(recordSchema);
    record.put("ts", expectedTs);
    writer.write(record);
    writer.close();

    AtomicBoolean cancel = new AtomicBoolean(false);
    Enumerator<Object[]> enumerator = createEnumerator(file, cancel);

    try {
      assertTrue(enumerator.moveNext());
      Object[] row = enumerator.current();
      // Timestamp should remain as a long value
      assertTrue(row[0] instanceof Long, "Timestamp should be kept as Long, was: "
          + (row[0] != null ? row[0].getClass().getName() : "null"));
      assertEquals(expectedTs, row[0]);
    } finally {
      enumerator.close();
    }
  }

  @Test void testLargeFile() throws Exception {
    File file = createSimpleParquetFile("large.parquet", 500);
    AtomicBoolean cancel = new AtomicBoolean(false);
    Enumerator<Object[]> enumerator = createEnumerator(file, cancel);

    try {
      int count = 0;
      while (enumerator.moveNext()) {
        Object[] row = enumerator.current();
        assertNotNull(row);
        count++;
      }
      assertEquals(500, count);
    } finally {
      enumerator.close();
    }
  }
}
