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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.io.IOException;

/**
 * Enumerator that reads selected Arrow columns without Gandiva.
 *
 * <p>Used when the optional Gandiva native library is not on the runtime classpath, or is present
 * but cannot start its LLVM JIT on this platform — see {@link GandivaAvailability}.
 *
 * <p>No evaluation is needed to serve a scan. {@link AbstractArrowEnumerator#loadNextArrowBatch()}
 * already loads exactly the requested field ordinals out of the batch's
 * {@code VectorSchemaRoot}, and {@link AbstractArrowEnumerator#current()} reads values straight off
 * those vectors — neither goes through Gandiva. The projector the accelerated path builds for a
 * condition-free query is an identity projection over the same fields, so skipping it changes
 * nothing but the speed. Filters are not pushed down at all in this mode; Calcite's Enumerable
 * convention applies them above the scan, which it can do for strictly more predicates than the
 * Gandiva translator accepts (it rejects disjunctions outright).
 */
class ArrowScanEnumerator extends AbstractArrowEnumerator {
  private final ArrowFileReader arrowFileReader;

  ArrowScanEnumerator(ArrowFileReader arrowFileReader, ImmutableIntList fields) {
    super(arrowFileReader, fields);
    this.arrowFileReader = arrowFileReader;
  }

  @Override void evaluateOperator(ArrowRecordBatch arrowRecordBatch) {
    // Nothing to evaluate: the requested vectors are already loaded and are read directly.
  }

  @Override public boolean moveNext() {
    if (currRowIndex >= rowCount - 1) {
      this.valueVectors.clear();
      loadNextArrowBatch();
      if (rowCount > 0) {
        currRowIndex = 0;
        return true;
      }
      return false;
    }
    currRowIndex++;
    return true;
  }

  @Override public void close() {
    try {
      arrowFileReader.close();
    } catch (IOException e) {
      throw Util.toUnchecked(e);
    }
  }
}
