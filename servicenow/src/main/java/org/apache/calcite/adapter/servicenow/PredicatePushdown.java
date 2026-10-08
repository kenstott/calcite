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

import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Decides which filters of a scan are sent to ServiceNow in {@code sysparm_query}.
 *
 * <p>The only implementation is {@link EncodedQueryTranslator}, which pushes a filter only when
 * every {@link PushdownCapabilities} entry it needs is verified.
 */
interface PredicatePushdown {

  /** What was pushed. */
  final class Result {
    /** Encoded-query terms joined with {@code ^}, or empty if nothing was pushed. */
    final String query;
    /** The capability entries the pushed terms rely on. */
    final Set<String> entries;

    Result(String query, Set<String> entries) {
      this.query = query;
      this.entries = Collections.unmodifiableSet(entries);
    }
  }

  /**
   * Chooses the filters to send to ServiceNow.
   *
   * <p>Removes each filter it takes from {@code filters}; what is left is evaluated by Calcite over
   * the rows ServiceNow returns. A filter is taken whole or not at all.
   *
   * @param filters conjuncts of the WHERE clause; filters taken are removed from this list
   * @param columns the table's columns, indexed as the filters' input references
   */
  Result push(List<RexNode> filters, List<ServiceNowColumn> columns);
}
