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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link IcebergEnumerator}.
 */
@Tag("unit")
public class IcebergEnumeratorTest {

  @TempDir
  Path tempDir;

  private Table emptyTable;
  private Table populatedTable;
  private Schema schema;

  @BeforeEach
  void setUp() throws Exception {
    String warehousePath = tempDir.resolve("warehouse").toString();
    Configuration conf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

    schema = new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get()),
        Types.NestedField.optional(3, "score", Types.DoubleType.get()));

    // Empty table
    emptyTable = catalog.createTable(
        TableIdentifier.of("empty_table"), schema, PartitionSpec.unpartitioned());

    // Populated table with 3 records
    populatedTable = catalog.createTable(
        TableIdentifier.of("populated_table"), schema, PartitionSpec.unpartitioned());

    OutputFile outputFile = populatedTable.io().newOutputFile(
        populatedTable.location() + "/data/file-" + UUID.randomUUID() + ".parquet");

    DataWriter<Record> writer = Parquet.writeData(outputFile)
        .schema(schema)
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .withSpec(PartitionSpec.unpartitioned())
        .build();

    GenericRecord r1 = GenericRecord.create(schema);
    r1.setField("id", 1);
    r1.setField("name", "Alice");
    r1.setField("score", 95.5);
    writer.write(r1);

    GenericRecord r2 = GenericRecord.create(schema);
    r2.setField("id", 2);
    r2.setField("name", "Bob");
    r2.setField("score", 87.3);
    writer.write(r2);

    GenericRecord r3 = GenericRecord.create(schema);
    r3.setField("id", 3);
    r3.setField("name", "Charlie");
    r3.setField("score", null);
    writer.write(r3);

    writer.close();
    populatedTable.newAppend().appendFile(writer.toDataFile()).commit();
  }

  @AfterEach
  void tearDown() {
    IcebergCatalogManager.clearCache();
  }

  @Test
  public void testEnumerateEmptyTable() {
    AtomicBoolean cancelFlag = new AtomicBoolean(false);
    IcebergEnumerator enumerator = new IcebergEnumerator(
        emptyTable, null, null, cancelFlag);

    assertFalse(enumerator.moveNext());
    enumerator.close();
  }

  @Test
  public void testEnumeratePopulatedTable() {
    AtomicBoolean cancelFlag = new AtomicBoolean(false);
    IcebergEnumerator enumerator = new IcebergEnumerator(
        populatedTable, null, null, cancelFlag);

    int count = 0;
    while (enumerator.moveNext()) {
      Object[] row = enumerator.current();
      assertNotNull(row);
      assertEquals(3, row.length);
      count++;
    }
    assertEquals(3, count);
    enumerator.close();
  }

  @Test
  public void testCurrentBeforeMoveNextThrows() {
    AtomicBoolean cancelFlag = new AtomicBoolean(false);
    IcebergEnumerator enumerator = new IcebergEnumerator(
        emptyTable, null, null, cancelFlag);

    assertThrows(IllegalStateException.class, new org.junit.jupiter.api.function.Executable() {
      @Override public void execute() {
        enumerator.current();
      }
    });
    enumerator.close();
  }

  @Test
  public void testCancelFlagStopsEnumeration() {
    AtomicBoolean cancelFlag = new AtomicBoolean(false);
    IcebergEnumerator enumerator = new IcebergEnumerator(
        populatedTable, null, null, cancelFlag);

    // Read first record
    assertTrue(enumerator.moveNext());
    assertNotNull(enumerator.current());

    // Cancel
    cancelFlag.set(true);

    // Should stop
    assertFalse(enumerator.moveNext());
    enumerator.close();
  }

  @Test
  public void testResetThrowsUnsupported() {
    AtomicBoolean cancelFlag = new AtomicBoolean(false);
    IcebergEnumerator enumerator = new IcebergEnumerator(
        emptyTable, null, null, cancelFlag);

    assertThrows(UnsupportedOperationException.class,
        new org.junit.jupiter.api.function.Executable() {
          @Override public void execute() {
            enumerator.reset();
          }
        });
    enumerator.close();
  }

  @Test
  public void testColumnProjection() {
    AtomicBoolean cancelFlag = new AtomicBoolean(false);
    // Project only columns 0 (id) and 2 (score)
    int[] projectedColumns = new int[]{0, 2};
    IcebergEnumerator enumerator = new IcebergEnumerator(
        populatedTable, null, null, cancelFlag, projectedColumns);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertNotNull(row);
    assertEquals(2, row.length);
    // First projected column is id (integer)
    assertNotNull(row[0]);
    enumerator.close();
  }

  @Test
  public void testSnapshotIdQuery() {
    // Get the current snapshot id
    long snapshotId = populatedTable.currentSnapshot().snapshotId();

    AtomicBoolean cancelFlag = new AtomicBoolean(false);
    IcebergEnumerator enumerator = new IcebergEnumerator(
        populatedTable, snapshotId, null, cancelFlag);

    int count = 0;
    while (enumerator.moveNext()) {
      assertNotNull(enumerator.current());
      count++;
    }
    assertEquals(3, count);
    enumerator.close();
  }

  @Test
  public void testInvalidTimestampFormat() {
    AtomicBoolean cancelFlag = new AtomicBoolean(false);
    assertThrows(IllegalArgumentException.class,
        new org.junit.jupiter.api.function.Executable() {
          @Override public void execute() {
            new IcebergEnumerator(populatedTable, null, "not-a-timestamp", cancelFlag);
          }
        });
  }

  @Test
  public void testNullValuesInRecords() {
    AtomicBoolean cancelFlag = new AtomicBoolean(false);
    IcebergEnumerator enumerator = new IcebergEnumerator(
        populatedTable, null, null, cancelFlag);

    boolean foundNull = false;
    while (enumerator.moveNext()) {
      Object[] row = enumerator.current();
      if (row[2] == null) {
        foundNull = true;
      }
    }
    assertTrue(foundNull, "Should find a null score value");
    enumerator.close();
  }

  @Test
  public void testCloseIsIdempotent() {
    AtomicBoolean cancelFlag = new AtomicBoolean(false);
    IcebergEnumerator enumerator = new IcebergEnumerator(
        emptyTable, null, null, cancelFlag);

    // Close multiple times should not throw
    enumerator.close();
    enumerator.close();
  }

  @Test
  public void testProjectionSingleColumn() {
    AtomicBoolean cancelFlag = new AtomicBoolean(false);
    int[] projectedColumns = new int[]{1}; // name only
    IcebergEnumerator enumerator = new IcebergEnumerator(
        populatedTable, null, null, cancelFlag, projectedColumns);

    assertTrue(enumerator.moveNext());
    Object[] row = enumerator.current();
    assertEquals(1, row.length);
    assertNotNull(row[0]); // name should be non-null
    enumerator.close();
  }

  @Test
  public void testMoveNextAfterExhaustion() {
    AtomicBoolean cancelFlag = new AtomicBoolean(false);
    IcebergEnumerator enumerator = new IcebergEnumerator(
        populatedTable, null, null, cancelFlag);

    // Exhaust all records
    while (enumerator.moveNext()) {
      // consume
    }

    // Additional moveNext should still return false
    assertFalse(enumerator.moveNext());
    assertFalse(enumerator.moveNext());
    enumerator.close();
  }
}
