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
package org.apache.calcite.adapter.salesforce;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

/**
 * Relational expression that uses Salesforce calling convention.
 */
public interface SalesforceRel extends RelNode {

  Convention CONVENTION = new Convention.Impl("SALESFORCE", SalesforceRel.class);

  /**
   * Callback for the implementation process.
   */
  void implement(Implementor implementor);

  /**
   * Shared context for implementing a Salesforce relational expression.
   */
  class Implementor {
    SalesforceTable salesforceTable;
    RelOptTable table;
    String sObjectType;

    // Query components
    String selectClause;
    String fromClause;
    String whereClause;
    String orderByClause;
    Integer limitValue;
    Integer offsetValue;

    void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((SalesforceRel) input).implement(this);
    }
  }
}
