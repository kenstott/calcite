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

import org.apache.calcite.adapter.file.etl.IncompleteFetchException;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.SkippedBatchException;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-139 — ETL error-fatality classes: IncompleteFetchException is an unchecked failure (re-fetch
 * next run), SkippedBatchException is a checked non-fatal skip, and a StreamingResponseTransformer
 * must be driven via HttpSource (its string transform() always throws).
 */
@Tag("unit")
public class EtlErrorClassesTest {

  @Test @Tag("FILE-139") void incompleteFetchIsUnchecked() {
    assertTrue(RuntimeException.class.isAssignableFrom(IncompleteFetchException.class),
        "IncompleteFetchException must be unchecked — a required fetch that could not complete is a "
            + "FAILURE, not a partial success");
  }

  @Test @Tag("FILE-139") void skippedBatchIsCheckedIoExceptionNotRuntime() {
    assertTrue(IOException.class.isAssignableFrom(SkippedBatchException.class),
        "SkippedBatchException must be a checked IOException (non-fatal skip)");
    assertFalse(RuntimeException.class.isAssignableFrom(SkippedBatchException.class),
        "and must NOT be unchecked");
  }

  @Test @Tag("FILE-139") void streamingTransformerStringTransformAlwaysThrows() {
    StreamingResponseTransformer t = new StreamingResponseTransformer() {
      @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext ctx) {
        return Collections.emptyIterator();
      }
    };
    assertThrows(UnsupportedOperationException.class, () -> t.transform("body", null),
        "a streaming transformer must be invoked via HttpSource.fetch(), not transform()");
  }
}
