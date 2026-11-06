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

import java.io.IOException;

/**
 * Signals that the source refuses every further call for the rest of the run (a spent daily call
 * quota, a rejected key). Unlike a failed batch, retrying or moving on to the next batch cannot
 * succeed, so {@link EtlPipeline} stops issuing batches for the table and fails it once.
 */
public class SourceExhaustedException extends IOException {
  public SourceExhaustedException(String message, Throwable cause) {
    super(message, cause);
  }
}
