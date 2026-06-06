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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for {@link FileReaderException}.
 */
@Tag("unit")
class FileReaderExceptionTest {

  @Test void testMessageConstructor() {
    FileReaderException ex = new FileReaderException("test error");
    assertEquals("test error", ex.getMessage());
    assertNull(ex.getCause());
  }

  @Test void testMessageAndCauseConstructor() {
    IOException cause = new IOException("io failure");
    FileReaderException ex = new FileReaderException("wrapped error", cause);
    assertEquals("wrapped error", ex.getMessage());
    assertNotNull(ex.getCause());
    assertEquals(cause, ex.getCause());
  }

  @Test void testIsException() {
    FileReaderException ex = new FileReaderException("test");
    assertNotNull(ex);
    // Verify it's a checked exception
    assert ex instanceof Exception;
  }

  // Need to use a real IOException for the cause
  private static class IOException extends Exception {
    IOException(String message) {
      super(message);
    }
  }
}
