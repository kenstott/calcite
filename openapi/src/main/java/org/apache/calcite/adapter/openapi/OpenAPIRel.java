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
package org.apache.calcite.adapter.openapi;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;

import java.util.HashMap;
import java.util.Map;

/**
 * Relational expression that uses OpenAPI calling convention.
 */
public interface OpenAPIRel extends RelNode {

  /** Calling convention for relational operations that occur in OpenAPI. */
  Convention CONVENTION = new Convention.Impl("OPENAPI", OpenAPIRel.class);

  /**
   * Callback for the implementation process that converts a tree of
   * {@link OpenAPIRel} nodes into an OpenAPI request.
   */
  class Implementor {
    public final Map<String, Object> filters = new HashMap<>();
    public final Map<String, Class> projections = new HashMap<>();
    public final Map<String, RelFieldCollation.Direction> sorts = new HashMap<>();
    public Long offset;
    public Long fetch;

    public OpenAPITable openAPITable;
    public RelOptTable table;

    /**
     * Visit a child of the current node, asking it to contribute to the current request.
     */
    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((OpenAPIRel) input).implement(this);
    }
  }

  /**
   * Called during query planning to push operations down to the OpenAPI layer.
   *
   * @param implementor the context for building the OpenAPI request
   */
  void implement(Implementor implementor);
}
