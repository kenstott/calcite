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

import org.apache.arrow.gandiva.evaluator.Filter;
import org.apache.arrow.gandiva.evaluator.SelectionVector;
import org.apache.arrow.gandiva.evaluator.SelectionVectorInt16;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;

/**
 * Enumerator that reads from an Arrow file and applies a filter using Gandiva's
 * selection-vector path. Used only when {@link GandivaAvailability#isAvailable()} — see
 * {@link ArrowJavaFilterEnumerator} for the plain-Java fallback used otherwise.
 */
class ArrowGandivaFilterEnumerator extends AbstractArrowEnumerator {
  private final ArrowFileReader arrowFileReader;
  private final BufferAllocator allocator;
  private final Filter filter;
  private @Nullable ArrowBuf buf;
  private @Nullable SelectionVector selectionVector;
  private int selectionVectorIndex;

  ArrowGandivaFilterEnumerator(ArrowFileReader arrowFileReader, ImmutableIntList fields,
      Object filter) {
    super(arrowFileReader, fields);
    this.arrowFileReader = arrowFileReader;
    this.allocator = new RootAllocator(Long.MAX_VALUE);
    this.filter = (Filter) filter;
  }

  @Override void evaluateOperator(ArrowRecordBatch arrowRecordBatch) {
    try {
      this.buf = this.allocator.buffer((long) arrowRecordBatch.getLength() * 2);
      this.selectionVector = new SelectionVectorInt16(buf);
      filter.evaluate(arrowRecordBatch, selectionVector);
    } catch (GandivaException e) {
      throw Util.toUnchecked(e);
    }
  }

  private boolean loadNextBatch() {
    this.valueVectors.clear();
    loadNextArrowBatch();
    return rowCount > 0;
  }

  @Override public boolean moveNext() {
    if (selectionVector == null || selectionVectorIndex >= selectionVector.getRecordCount()) {
      boolean hasNextBatch = loadNextBatch();
      if (hasNextBatch) {
        selectionVectorIndex = 0;
        if (selectionVector.getRecordCount() == 0) {
          return moveNext(); // Skip empty batches
        }
        currRowIndex = selectionVector.getIndex(selectionVectorIndex++);
      }
      return hasNextBatch;
    } else {
      currRowIndex = selectionVector.getIndex(selectionVectorIndex++);
      return true;
    }
  }

  @Override public void close() {
    try {
      if (buf != null) {
        buf.close();
      }
      filter.close();
      arrowFileReader.close();
    } catch (IOException | GandivaException e) {
      throw Util.toUnchecked(e);
    }
  }
}
