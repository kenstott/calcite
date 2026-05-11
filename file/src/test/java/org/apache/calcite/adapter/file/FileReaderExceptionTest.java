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
