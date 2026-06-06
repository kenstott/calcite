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
 * Signals that an HTTP response status code matched the source's {@code skipOn} list.
 * The batch should be silently skipped without incrementing the consecutive-failure counter.
 */
public class SkippedBatchException extends IOException {
  public SkippedBatchException(String message) {
    super(message);
  }
}
