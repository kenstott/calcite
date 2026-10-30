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

/**
 * Signals that a unit was fast-skipped because its origin's circuit breaker is open. Unlike a
 * {@code skipOn} match, which declares an expected gap, the unit may have data upstream, so a
 * replace-partitions commit must not proceed without it.
 */
public class CircuitOpenSkipException extends SkippedBatchException {
  public CircuitOpenSkipException(String message) {
    super(message);
  }
}
