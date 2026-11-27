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
package org.apache.calcite.adapter.govdata;

import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Raw cache keys for a source that revises in place and publishes no revision of its own, and
 * whose full re-read is too large to repeat on every run: the key carries the UTC calendar month,
 * so an entry is read once within a month and the source is read again in the next.
 */
public final class MonthlyCacheKey {

  private MonthlyCacheKey() {
  }

  /**
   * Returns {@code key} stamped with the current UTC month.
   *
   * @param key identity of the content, as for a raw cache entry
   * @return the key for this month's entry
   */
  public static String of(String key) {
    LocalDate today = LocalDate.now(ZoneOffset.UTC);
    return key + "#asof=" + today.getYear() + "-" + today.getMonthValue();
  }
}
