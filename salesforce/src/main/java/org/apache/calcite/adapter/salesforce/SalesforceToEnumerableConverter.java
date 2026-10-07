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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.List;

/**
 * Relational expression representing a scan of a table in a Salesforce database.
 */
public class SalesforceToEnumerableConverter extends ConverterImpl
    implements EnumerableRel {

  protected SalesforceToEnumerableConverter(RelOptCluster cluster,
      RelTraitSet traits, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SalesforceToEnumerableConverter(getCluster(), traitSet, sole(inputs));
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(.1);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // Build the SOQL query from the relational tree
    SalesforceRel.Implementor salesforceImplementor = new SalesforceRel.Implementor();
    ((SalesforceRel) getInput()).implement(salesforceImplementor);

    String soql = buildSOQL(salesforceImplementor);

    // Generate the enumerable implementation
    final BlockBuilder builder = new BlockBuilder();
    // Rows are always Object[]; do not let a single-column result collapse to a scalar
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(),
            pref.preferArray(), false);

    // Get the table reference
    Expression table = salesforceImplementor.table.getExpression(SalesforceTable.class);

    // Call the query method
    Expression query =
        Expressions.call(table,
        "query",
        DataContext.ROOT,
        Expressions.constant(soql),
        Expressions.constant(String.join(",", selectFields(salesforceImplementor))));

    builder.add(Expressions.return_(null, query));

    return implementor.result(physType, builder.toBlock());
  }

  private String buildSOQL(SalesforceRel.Implementor implementor) {
    StringBuilder soql = new StringBuilder();

    // SELECT clause
    soql.append("SELECT ");
    if (implementor.selectClause != null) {
      soql.append(implementor.selectClause);
    } else {
      // Default to all fields
      soql.append(getAllFields(implementor));
    }

    // FROM clause
    soql.append(" FROM ").append(implementor.sObjectType);

    // WHERE clause
    if (implementor.whereClause != null) {
      soql.append(" WHERE ").append(implementor.whereClause);
    }

    // ORDER BY clause
    if (implementor.orderByClause != null) {
      soql.append(" ORDER BY ").append(implementor.orderByClause);
    }

    // LIMIT clause
    if (implementor.limitValue != null) {
      soql.append(" LIMIT ").append(implementor.limitValue);
    }

    // OFFSET clause - Note: Salesforce requires LIMIT when using OFFSET
    if (implementor.offsetValue != null) {
      if (implementor.limitValue == null) {
        // Salesforce requires LIMIT with OFFSET, use max value
        soql.append(" LIMIT 2000");
      }
      soql.append(" OFFSET ").append(implementor.offsetValue);
    }

    return soql.toString();
  }

  private String getAllFields(SalesforceRel.Implementor implementor) {
    return String.join(", ", selectFields(implementor));
  }

  /** SOQL field names in the order the result row carries them. */
  private List<String> selectFields(SalesforceRel.Implementor implementor) {
    if (implementor.selectFields != null) {
      return implementor.selectFields;
    }
    List<String> fields = new ArrayList<>();
    for (RelDataTypeField field : getRowType().getFieldList()) {
      fields.add(field.getName());
    }
    return fields;
  }
}
