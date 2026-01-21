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
package org.apache.calcite.adapter.graphql;

import org.apache.calcite.rel.RelFieldCollation;

import java.util.Locale;

public class OrderByField {
  private final Integer field;
  private final RelFieldCollation.Direction direction;

  public OrderByField(Integer field, RelFieldCollation.Direction direction) {
    this.field = field;
    this.direction = direction;
  }

  /**
   * Converts to Hasura order_by format
   */
  public String toHasuraFormat() {
    return String.format(Locale.ROOT, "%s: %s", field, direction == RelFieldCollation.Direction.ASCENDING ? "Asc" : "Desc");
  }
}
