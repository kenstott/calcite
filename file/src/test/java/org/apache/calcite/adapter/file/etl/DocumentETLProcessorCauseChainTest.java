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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
class DocumentETLProcessorCauseChainTest {
  @Test void chainNamesRootCause() {
    IOException e = new IOException("wrapper",
        new IllegalStateException("sec.edgarUserAgent is not set"));
    assertEquals("java.io.IOException: wrapper <- "
        + "java.lang.IllegalStateException: sec.edgarUserAgent is not set",
        DocumentETLProcessor.describeCauseChain(e));
  }
}
