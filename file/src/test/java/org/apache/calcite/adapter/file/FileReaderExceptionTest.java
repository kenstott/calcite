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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FileReaderException}.
 */
@Tag("unit")
class FileReaderExceptionTest {

  @Test void testMessageAndSqlStateConstructor() {
    FileReaderException ex =
        new FileReaderException("test error", FileReaderException.TABLE_NOT_FOUND);
    assertEquals("test error", ex.getMessage());
    assertEquals("42S02", ex.getSQLState());
    assertNull(ex.getCause());
  }

  @Test void testMessageSqlStateAndCauseConstructor() {
    IOException cause = new IOException("io failure");
    FileReaderException ex =
        new FileReaderException("wrapped error", FileReaderException.FILE_ACCESS, cause);
    assertEquals("wrapped error", ex.getMessage());
    assertEquals("HY000", ex.getSQLState());
    assertEquals(cause, ex.getCause());
  }

  @Test void testIsSqlException() {
    FileReaderException ex = new FileReaderException("test", FileReaderException.CONFIGURATION);
    assertTrue(ex instanceof SQLException);
    assertEquals("42000", ex.getSQLState());
  }

  // Need to use a real IOException for the cause
  private static class IOException extends Exception {
    IOException(String message) {
      super(message);
    }
  }
}
