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
package org.apache.calcite.adapter.servicenow;

import java.util.Set;

/**
 * Told, for every scan, what was pushed to ServiceNow. Used by the harness and the tests to see
 * the exact query text and the entries it relied on; the adapter itself needs no observer. Passed
 * as the {@code pushdownObserver} operand, which only a Java caller can supply.
 */
public interface PushdownObserver {
  /**
   * Called when a scan starts.
   *
   * @param table     table being scanned
   * @param query     pushed {@code sysparm_query} terms, empty if nothing was pushed
   * @param entries   capability entries the pushed terms rely on
   * @param remaining number of filters left for Calcite to evaluate
   */
  void scan(String table, String query, Set<String> entries, int remaining);
}
