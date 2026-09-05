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

import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A version-hint.text written moments ago by this same JVM (a table just created, or just
 * dropped-and-recreated) can read back as absent on S3-compatible stores with read-after-write
 * lag -- reproduced in production against MinIO: {@code cftc_trades}'s brand-new table failed
 * within 620ms of creation with {@code NotFoundException: Location does not exist:
 * .../version-hint.text}, because {@link S3FileIOTables}'s private {@code readVersionHint}
 * (backing the read-only {@link S3FileIOTables#load} path) had no retry and no catch for that
 * exception -- unlike its three sibling version-hint readers in this codebase, which already
 * treat it as "not there yet". These tests exercise the retry added to close that gap, invoking
 * the private method via reflection (same approach as
 * {@link S3FileIOTableOperationsStaleCommitTest}) rather than adding a test-only seam.
 */
@Tag("unit")
class S3FileIOTablesVersionHintRaceTest {

  private static final String LOCATION = "s3://bucket/schema/table";
  private static final String HINT_PATH = LOCATION + "/metadata/version-hint.text";

  private static SeekableInputStream streamOf(String content) {
    byte[] bytes = content.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    return new SeekableInputStream() {
      private final ByteArrayInputStream delegate = new ByteArrayInputStream(bytes);

      @Override public long getPos() {
        return bytes.length - delegate.available();
      }

      @Override public void seek(long newPos) {
        throw new UnsupportedOperationException("not needed by readVersionHint");
      }

      @Override public int read() {
        return delegate.read();
      }
    };
  }

  /** A FileIO whose newInputFile(...).newStream() throws NotFoundException the first
   * failuresBeforeSuccess calls, then returns a stream containing "0". */
  private static FileIO flakyFileIo(int failuresBeforeSuccess) {
    int[] calls = {0};
    FileIO io = org.mockito.Mockito.mock(FileIO.class);
    InputFile inputFile = org.mockito.Mockito.mock(InputFile.class);
    org.mockito.Mockito.when(io.newInputFile(HINT_PATH)).thenReturn(inputFile);
    org.mockito.Mockito.when(inputFile.newStream()).thenAnswer(invocation -> {
      calls[0]++;
      if (calls[0] <= failuresBeforeSuccess) {
        throw new NotFoundException("Location does not exist: " + HINT_PATH);
      }
      return streamOf("0");
    });
    return io;
  }

  private static String invokeReadVersionHint(FileIO io, String root) throws Throwable {
    Method m = S3FileIOTables.class.getDeclaredMethod("readVersionHint", FileIO.class, String.class);
    m.setAccessible(true);
    try {
      return (String) m.invoke(null, io, root);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  @Test void aTransientNotFoundOnAJustCreatedTableIsRetriedUntilItClears() throws Throwable {
    FileIO io = flakyFileIo(2); // fails twice, succeeds on the 3rd attempt

    String version = invokeReadVersionHint(io, LOCATION);

    assertEquals("0", version);
  }

  @Test void aPersistentNotFoundStillFailsInsteadOfRetryingForever() {
    FileIO io = flakyFileIo(Integer.MAX_VALUE); // never clears -- table genuinely doesn't exist

    RuntimeException e = assertThrows(RuntimeException.class,
        () -> invokeReadVersionHint(io, LOCATION));
    assertTrue(e.getMessage().contains("version-hint.text"),
        "failure must name the version-hint file, got: " + e.getMessage());
  }
}
