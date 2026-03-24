/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.format.parquet;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Integration tests for Parquet format operations including write and read round-trips.
 */
@Tag("integration")
public class ParquetFormatIntegrationTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ParquetFormatIntegrationTest.class);

  @TempDir
  java.nio.file.Path tempDir;

  @Test public void testWriteAndReadParquet() throws IOException {
    Schema schema = SchemaBuilder.record("TestRecord")
        .fields()
        .requiredInt("id")
        .requiredString("name")
        .optionalDouble("value")
        .endRecord();

    File parquetFile = tempDir.resolve("test.parquet").toFile();

    // Write
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(
            HadoopOutputFile.fromPath(
                new Path(parquetFile.toURI()), new Configuration()))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {

      for (int i = 0; i < 10; i++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", i);
        record.put("name", "name_" + i);
        record.put("value", i * 1.5);
        writer.write(record);
      }
    }

    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);

    // Read back
    List<GenericRecord> records = new ArrayList<>();
    try (ParquetReader<GenericRecord> reader = AvroParquetReader
        .<GenericRecord>builder(
            org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                new Path(parquetFile.toURI()), new Configuration()))
        .build()) {

      GenericRecord record;
      while ((record = reader.read()) != null) {
        records.add(record);
      }
    }

    assertEquals(10, records.size());
    assertEquals(0, records.get(0).get("id"));
    assertEquals("name_0", records.get(0).get("name").toString());
    assertEquals(0.0, (double) records.get(0).get("value"), 0.001);
    assertEquals(9, records.get(9).get("id"));
  }

  @Test public void testParquetWithNullableColumns() throws IOException {
    Schema schema = SchemaBuilder.record("NullableRecord")
        .fields()
        .requiredInt("id")
        .name("nullable_name").type().nullable().stringType().noDefault()
        .name("nullable_value").type().nullable().doubleType().noDefault()
        .endRecord();

    File parquetFile = tempDir.resolve("nullable.parquet").toFile();

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(
            HadoopOutputFile.fromPath(
                new Path(parquetFile.toURI()), new Configuration()))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {

      // Row with all values
      GenericRecord r1 = new GenericData.Record(schema);
      r1.put("id", 1);
      r1.put("nullable_name", "Alice");
      r1.put("nullable_value", 100.0);
      writer.write(r1);

      // Row with nulls
      GenericRecord r2 = new GenericData.Record(schema);
      r2.put("id", 2);
      r2.put("nullable_name", null);
      r2.put("nullable_value", null);
      writer.write(r2);

      // Row with partial nulls
      GenericRecord r3 = new GenericData.Record(schema);
      r3.put("id", 3);
      r3.put("nullable_name", "Charlie");
      r3.put("nullable_value", null);
      writer.write(r3);
    }

    List<GenericRecord> records = readParquet(parquetFile);

    assertEquals(3, records.size());
    assertEquals("Alice", records.get(0).get("nullable_name").toString());
    assertNull(records.get(1).get("nullable_name"));
    assertNull(records.get(1).get("nullable_value"));
    assertEquals("Charlie", records.get(2).get("nullable_name").toString());
    assertNull(records.get(2).get("nullable_value"));
  }

  @Test public void testParquetWithAllDataTypes() throws IOException {
    Schema schema = SchemaBuilder.record("AllTypesRecord")
        .fields()
        .requiredInt("col_int")
        .requiredLong("col_long")
        .requiredFloat("col_float")
        .requiredDouble("col_double")
        .requiredBoolean("col_bool")
        .requiredString("col_string")
        .endRecord();

    File parquetFile = tempDir.resolve("all_types.parquet").toFile();

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(
            HadoopOutputFile.fromPath(
                new Path(parquetFile.toURI()), new Configuration()))
        .withSchema(schema)
        .build()) {

      GenericRecord record = new GenericData.Record(schema);
      record.put("col_int", 42);
      record.put("col_long", 9999999999L);
      record.put("col_float", 3.14f);
      record.put("col_double", 2.71828);
      record.put("col_bool", true);
      record.put("col_string", "test_value");
      writer.write(record);
    }

    List<GenericRecord> records = readParquet(parquetFile);
    assertEquals(1, records.size());

    GenericRecord r = records.get(0);
    assertEquals(42, r.get("col_int"));
    assertEquals(9999999999L, r.get("col_long"));
    assertEquals(3.14f, (float) r.get("col_float"), 0.01);
    assertEquals(2.71828, (double) r.get("col_double"), 0.0001);
    assertEquals(true, r.get("col_bool"));
    assertEquals("test_value", r.get("col_string").toString());
  }

  @Test public void testParquetEmptyFile() throws IOException {
    Schema schema = SchemaBuilder.record("EmptyRecord")
        .fields()
        .requiredInt("id")
        .endRecord();

    File parquetFile = tempDir.resolve("empty.parquet").toFile();

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(
            HadoopOutputFile.fromPath(
                new Path(parquetFile.toURI()), new Configuration()))
        .withSchema(schema)
        .build()) {
      // Write nothing
    }

    List<GenericRecord> records = readParquet(parquetFile);
    assertEquals(0, records.size());
  }

  @Test public void testParquetLargerDataset() throws IOException {
    Schema schema = SchemaBuilder.record("LargeRecord")
        .fields()
        .requiredInt("id")
        .requiredString("category")
        .requiredDouble("amount")
        .endRecord();

    File parquetFile = tempDir.resolve("large.parquet").toFile();

    int numRecords = 10000;

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(
            HadoopOutputFile.fromPath(
                new Path(parquetFile.toURI()), new Configuration()))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {

      for (int i = 0; i < numRecords; i++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("id", i);
        record.put("category", "cat_" + (i % 5));
        record.put("amount", i * 0.99);
        writer.write(record);
      }
    }

    List<GenericRecord> records = readParquet(parquetFile);
    assertEquals(numRecords, records.size());
    assertEquals(0, records.get(0).get("id"));
    assertEquals(numRecords - 1, records.get(numRecords - 1).get("id"));
  }

  @Test public void testParquetWithArrayColumn() throws IOException {
    Schema elementSchema = Schema.create(Schema.Type.DOUBLE);
    Schema arraySchema = Schema.createArray(elementSchema);

    Schema schema = SchemaBuilder.record("ArrayRecord")
        .fields()
        .requiredInt("id")
        .name("scores").type(arraySchema).noDefault()
        .endRecord();

    File parquetFile = tempDir.resolve("array.parquet").toFile();

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(
            HadoopOutputFile.fromPath(
                new Path(parquetFile.toURI()), new Configuration()))
        .withSchema(schema)
        .build()) {

      GenericRecord record = new GenericData.Record(schema);
      record.put("id", 1);
      List<Double> scores = new ArrayList<>();
      scores.add(95.5);
      scores.add(87.3);
      scores.add(92.1);
      record.put("scores", scores);
      writer.write(record);
    }

    List<GenericRecord> records = readParquet(parquetFile);
    assertEquals(1, records.size());

    Object scoresObj = records.get(0).get("scores");
    assertNotNull(scoresObj);
    assertTrue(scoresObj instanceof List);
    @SuppressWarnings("unchecked")
    List<Double> readScores = (List<Double>) scoresObj;
    assertEquals(3, readScores.size());
    assertEquals(95.5, readScores.get(0), 0.001);
  }

  // --- ParquetConversionUtil tests ---

  @Test public void testGetParquetCacheDirDefault() {
    File baseDir = tempDir.resolve("base").toFile();
    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir);

    assertNotNull(cacheDir);
    assertTrue(cacheDir.getAbsolutePath().contains("parquet_cache")
        || cacheDir.getAbsolutePath().contains("base"));
  }

  @Test public void testGetParquetCacheDirWithCustomDir() {
    File baseDir = tempDir.resolve("base").toFile();
    String customDir = tempDir.resolve("custom-cache").toString();

    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir, customDir);

    assertNotNull(cacheDir);
    assertEquals(customDir, cacheDir.getAbsolutePath());
  }

  @Test public void testGetParquetCacheDirWithCustomDirAndSchemaName() {
    File baseDir = tempDir.resolve("base").toFile();
    String customDir = tempDir.resolve("custom-cache").toString();

    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir, customDir, "mySchema");

    assertNotNull(cacheDir);
    assertTrue(cacheDir.getAbsolutePath().contains("schema_mySchema"));
  }

  @Test public void testGetParquetCacheDirNullCustomDir() {
    File baseDir = tempDir.resolve("base").toFile();

    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir, null, "schema1");

    assertNotNull(cacheDir);
    // Should use default directory based on baseDir
  }

  @Test public void testGetParquetCacheDirEmptyCustomDir() {
    File baseDir = tempDir.resolve("base").toFile();

    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir, "", null);

    assertNotNull(cacheDir);
  }

  @Test public void testGetParquetCacheDirEmptySchemaName() {
    File baseDir = tempDir.resolve("base").toFile();
    String customDir = tempDir.resolve("custom").toString();

    File cacheDir = ParquetConversionUtil.getParquetCacheDir(baseDir, customDir, "");

    assertNotNull(cacheDir);
    // Empty schema name should be treated like null
    assertEquals(customDir, cacheDir.getAbsolutePath());
  }

  // --- StorageProvider writeAvroParquet / readParquet round-trip ---

  @Test public void testStorageProviderParquetRoundTrip() throws IOException {
    org.apache.calcite.adapter.file.storage.LocalFileStorageProvider provider =
        new org.apache.calcite.adapter.file.storage.LocalFileStorageProvider();

    Schema schema = SchemaBuilder.record("RoundTrip")
        .fields()
        .requiredInt("id")
        .requiredString("name")
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("id", i);
      record.put("name", "item_" + i);
      records.add(record);
    }

    String parquetPath = tempDir.resolve("round-trip.parquet").toString();

    provider.writeAvroParquet(parquetPath, schema, records, "test");

    assertTrue(provider.exists(parquetPath));

    // Read back
    List<java.util.Map<String, Object>> readBack = provider.readParquet(parquetPath);

    assertEquals(5, readBack.size());
    assertEquals(0, readBack.get(0).get("id"));
    assertEquals("item_0", readBack.get(0).get("name").toString());
    assertEquals(4, readBack.get(4).get("id"));
  }

  @Test public void testStorageProviderParquetEmptyRecords() throws IOException {
    org.apache.calcite.adapter.file.storage.LocalFileStorageProvider provider =
        new org.apache.calcite.adapter.file.storage.LocalFileStorageProvider();

    Schema schema = SchemaBuilder.record("Empty")
        .fields()
        .requiredInt("id")
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    String parquetPath = tempDir.resolve("empty-round-trip.parquet").toString();

    provider.writeAvroParquet(parquetPath, schema, records, "empty");

    assertTrue(provider.exists(parquetPath));

    List<java.util.Map<String, Object>> readBack = provider.readParquet(parquetPath);
    assertEquals(0, readBack.size());
  }

  private List<GenericRecord> readParquet(File parquetFile) throws IOException {
    List<GenericRecord> records = new ArrayList<>();

    try (ParquetReader<GenericRecord> reader = AvroParquetReader
        .<GenericRecord>builder(
            org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                new Path(parquetFile.toURI()), new Configuration()))
        .build()) {

      GenericRecord record;
      while ((record = reader.read()) != null) {
        records.add(record);
      }
    }

    return records;
  }
}
