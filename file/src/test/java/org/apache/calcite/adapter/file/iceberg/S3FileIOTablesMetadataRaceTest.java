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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code v{N}.metadata.json} can hit the same read-after-write lag as {@code version-hint.text}
 * (see {@link S3FileIOTablesVersionHintRaceTest}) on a table that was just created or just
 * compacted, since it is read moments after being written by the same commit. Unlike the
 * version-hint read, {@link StaticTableOperations#current()} (invoked lazily by whichever caller
 * first calls {@code schema()}/{@code currentSnapshot()}) had no retry of its own -- these tests
 * exercise {@code S3FileIOTables.retryMetadataRead}, which gives it the same bounded retry via
 * reflection (same approach as {@link S3FileIOTablesVersionHintRaceTest}).
 */
@Tag("unit")
class S3FileIOTablesMetadataRaceTest {

  private static final String LOCATION = "s3://bucket/schema/table";
  private static final String METADATA_PATH = LOCATION + "/metadata/v0.metadata.json";

  private static String validMetadataJson() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.unpartitioned();
    TableMetadata metadata =
        TableMetadata.newTableMetadata(schema, spec, LOCATION, Collections.emptyMap());
    return TableMetadataParser.toJson(metadata);
  }

  private static SeekableInputStream streamOf(String content) {
    byte[] bytes = content.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    return new SeekableInputStream() {
      private final ByteArrayInputStream delegate = new ByteArrayInputStream(bytes);

      @Override public long getPos() {
        return bytes.length - delegate.available();
      }

      @Override public void seek(long newPos) {
        throw new UnsupportedOperationException("not needed by this test");
      }

      @Override public int read() {
        return delegate.read();
      }
    };
  }

  /** A FileIO whose newInputFile(METADATA_PATH).newStream() throws NotFoundException the first
   * failuresBeforeSuccess calls, then returns valid metadata JSON. */
  private static FileIO flakyFileIo(int failuresBeforeSuccess) {
    int[] calls = {0};
    FileIO io = org.mockito.Mockito.mock(FileIO.class);
    InputFile inputFile = org.mockito.Mockito.mock(InputFile.class);
    org.mockito.Mockito.when(io.newInputFile(METADATA_PATH)).thenReturn(inputFile);
    org.mockito.Mockito.when(inputFile.location()).thenReturn(METADATA_PATH);
    org.mockito.Mockito.when(inputFile.newStream()).thenAnswer(invocation -> {
      calls[0]++;
      if (calls[0] <= failuresBeforeSuccess) {
        throw new NotFoundException("Location does not exist: " + METADATA_PATH);
      }
      return streamOf(validMetadataJson());
    });
    return io;
  }

  private static void invokeRetryMetadataRead(StaticTableOperations ops, String metadataLocation)
      throws Throwable {
    Method m = S3FileIOTables.class.getDeclaredMethod("retryMetadataRead",
        StaticTableOperations.class, String.class);
    m.setAccessible(true);
    try {
      m.invoke(null, ops, metadataLocation);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  @Test void aTransientNotFoundOnAJustWrittenMetadataFileIsRetriedUntilItClears() throws Throwable {
    FileIO io = flakyFileIo(2); // fails twice, succeeds on the 3rd attempt
    StaticTableOperations ops = new StaticTableOperations(METADATA_PATH, io);

    assertDoesNotThrow(() -> invokeRetryMetadataRead(ops, METADATA_PATH));

    // A successful retryMetadataRead memoizes inside StaticTableOperations -- the schema a real
    // caller reads afterward comes from that memo, not a fresh (and now unmocked) read.
    assertTrue(ops.current().schema().findField("id") != null);
  }

  @Test void aPersistentNotFoundStillFailsInsteadOfRetryingForever() {
    FileIO io = flakyFileIo(Integer.MAX_VALUE); // never clears
    StaticTableOperations ops = new StaticTableOperations(METADATA_PATH, io);

    RuntimeException e = assertThrows(RuntimeException.class,
        () -> invokeRetryMetadataRead(ops, METADATA_PATH));
    assertTrue(e.getMessage().contains(METADATA_PATH),
        "failure must name the metadata file, got: " + e.getMessage());
  }
}
