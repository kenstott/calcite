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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The GraphQL filter written for a SQL condition.
 */
class GraphQLFilterTranslationTest {
  private static final List<String> COLUMNS = Arrays.asList("A", "B");

  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
  private final RexBuilder rex = new RexBuilder(typeFactory);
  private final RelDataType integer =
      typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);

  /** Writes filters with each column's lower-cased name as its GraphQL field. */
  private final GraphQLRel.Implementor implementor = new GraphQLRel.Implementor() {
    @Override String graphQLFieldName(String sqlFieldName) {
      return sqlFieldName.toLowerCase(java.util.Locale.ROOT);
    }
  };

  private RexNode column(int index) {
    return rex.makeInputRef(integer, index);
  }

  private RexNode number(int value) {
    return rex.makeExactLiteral(BigDecimal.valueOf(value));
  }

  private RexNode aEquals1() {
    return rex.makeCall(SqlStdOperatorTable.EQUALS, column(0), number(1));
  }

  private RexNode bGreaterThan2() {
    return rex.makeCall(SqlStdOperatorTable.GREATER_THAN, column(1), number(2));
  }

  private String filterFor(RexNode condition) {
    return implementor.convertRexNodeToGraphQLFilter(condition, COLUMNS);
  }

  @Test void andIsWrittenAsAnd() {
    assertEquals("{ _and: [{ a: { _eq: 1 } },{ b: { _gt: 2 } }]}",
        filterFor(rex.makeCall(SqlStdOperatorTable.AND, aEquals1(), bGreaterThan2())));
  }

  @Test void orIsWrittenAsOr() {
    assertEquals("{ _or: [{ a: { _eq: 1 } },{ b: { _gt: 2 } }]}",
        filterFor(rex.makeCall(SqlStdOperatorTable.OR, aEquals1(), bGreaterThan2())));
  }

  @Test void notNegatesItsOnlyOperand() {
    assertEquals("{ _not: { a: { _eq: 1 } } }",
        filterFor(rex.makeCall(SqlStdOperatorTable.NOT, aEquals1())));
  }

  @Test void nestedConditionsKeepTheirShape() {
    RexNode or = rex.makeCall(SqlStdOperatorTable.OR, aEquals1(), bGreaterThan2());
    RexNode not = rex.makeCall(SqlStdOperatorTable.NOT, bGreaterThan2());
    assertEquals("{ _and: [{ _or: [{ a: { _eq: 1 } },{ b: { _gt: 2 } }]},"
            + "{ _not: { b: { _gt: 2 } } }]}",
        filterFor(rex.makeCall(SqlStdOperatorTable.AND, or, not)));
  }

  @Test void nullTestsAreWrittenAsIsNull() {
    assertEquals("{ a: { _is_null: true } }",
        filterFor(rex.makeCall(SqlStdOperatorTable.IS_NULL, column(0))));
    assertEquals("{ a: { _is_null: false } }",
        filterFor(rex.makeCall(SqlStdOperatorTable.IS_NOT_NULL, column(0))));
  }

  @Test void aConditionWithNoGraphQLFormIsRefused() {
    RexNode like =
        rex.makeCall(SqlStdOperatorTable.LIKE, column(0), rex.makeLiteral("x%"));
    assertThrows(IllegalStateException.class, () -> filterFor(like));
  }
}
