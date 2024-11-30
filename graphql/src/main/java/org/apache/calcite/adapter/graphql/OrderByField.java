package org.apache.calcite.adapter.graphql;

import org.apache.calcite.rel.RelFieldCollation;

import java.util.List;

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
  public String toHasuraFormat(List<String> fieldNames) {
    return String.format("%s: %s", fieldNames.get(field), direction == RelFieldCollation.Direction.ASCENDING ? "Asc" : "Desc");
  }
}
