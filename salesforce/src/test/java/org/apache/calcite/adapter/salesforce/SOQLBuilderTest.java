/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.salesforce;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link SOQLBuilder}.
 */
class SOQLBuilderTest {

  private static final List<String> FIELDS = Arrays.asList("Id", "AccountId");

  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
  private final RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR, 18);

  private RexNode accountId() {
    return rexBuilder.makeInputRef(varchar, 1);
  }

  @Test void comparisonWithLiteral() {
    RexNode condition =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, accountId(),
            rexBuilder.makeLiteral("001gK00001Za2LCQAZ"));
    assertThat(SOQLBuilder.buildWhereClause(condition, FIELDS),
        equalTo("AccountId = '001gK00001Za2LCQAZ'"));
  }

  @Test void stringLiteralIsEscaped() {
    RexNode condition =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, accountId(),
            rexBuilder.makeLiteral("O'Brien\\"));
    assertThat(SOQLBuilder.buildWhereClause(condition, FIELDS),
        equalTo("AccountId = 'O\\'Brien\\\\'"));
  }

  @Test void stringLiteralWithTemplateMarkIsNotPushedDown() {
    RexNode condition =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, accountId(),
            rexBuilder.makeCharLiteral(
                new NlsString("a" + SOQLBuilder.MARK + "b", "UTF-16LE", SqlCollation.IMPLICIT)));
    assertThrows(UnsupportedOperationException.class,
        () -> SOQLBuilder.buildWhereClause(condition, FIELDS));
  }

  @Test void bindParameterIsBoundAtExecution() {
    RexNode condition =
        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, accountId(), param(varchar, 0));
    assertThat(bind(condition, "O'Brien"), equalTo("AccountId = 'O\\'Brien'"));
  }

  @Test void bindParametersNestedInOr() {
    RexNode condition =
        rexBuilder.makeCall(SqlStdOperatorTable.OR,
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, accountId(), param(varchar, 0)),
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, accountId(), param(varchar, 1)));
    assertThat(bind(condition, "a", "b"),
        equalTo("(AccountId = 'a' OR AccountId = 'b')"));
  }

  /** A comparison with a null parameter is UNKNOWN, which a filter treats as false. */
  @Test void nullBindParameterMatchesNoRow() {
    RexNode condition =
        rexBuilder.makeCall(SqlStdOperatorTable.OR,
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, accountId(), param(varchar, 0)),
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, accountId(), param(varchar, 1)));
    assertThat(bind(condition, "a", null),
        equalTo("(AccountId = 'a' OR Id = null)"));
  }

  /** NOT of UNKNOWN is UNKNOWN, so below a NOT the comparison must become true. */
  @Test void nullBindParameterBelowNotMatchesNoRow() {
    RexNode condition =
        rexBuilder.makeCall(SqlStdOperatorTable.NOT,
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, accountId(), param(varchar, 0)));
    assertThat(bind(condition, new Object[] {null}), equalTo("NOT (Id != null)"));
    assertThat(bind(condition, "a"), equalTo("NOT (AccountId = 'a')"));
  }

  @Test void bindParameterTypes() {
    RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType decimal = typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2);
    RelDataType date = typeFactory.createSqlType(SqlTypeName.DATE);
    RelDataType timestamp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    RelDataType bool = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    assertThat(bind(greaterThan(integer, 0), 42), equalTo("Id > 42"));
    assertThat(bind(greaterThan(decimal, 0), new BigDecimal("1E+3")), equalTo("Id > 1000"));
    assertThat(bind(greaterThan(date, 0), 19723), equalTo("Id > 2024-01-01"));
    assertThat(bind(greaterThan(timestamp, 0), 1704067200123L),
        equalTo("Id > 2024-01-01T00:00:00.123Z"));
    assertThat(bind(greaterThan(bool, 0), true), equalTo("Id > true"));
  }

  @Test void bindParameterOfUnsupportedTypeIsNotPushedDown() {
    RelDataType time = typeFactory.createSqlType(SqlTypeName.TIME);
    assertThrows(UnsupportedOperationException.class,
        () -> SOQLBuilder.buildWhereClause(greaterThan(time, 0), FIELDS));
  }

  /** A parameter that is not an operand of a comparison has no SOQL form. */
  @Test void bareBindParameterIsNotPushedDown() {
    RelDataType bool = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    RexNode condition =
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, param(bool, 0));
    assertThrows(UnsupportedOperationException.class,
        () -> SOQLBuilder.buildWhereClause(condition, FIELDS));
  }

  private RexNode param(RelDataType type, int index) {
    return rexBuilder.makeDynamicParam(type, index);
  }

  private RexNode greaterThan(RelDataType type, int index) {
    return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
        rexBuilder.makeInputRef(type, 0), param(type, index));
  }

  private static String bind(RexNode condition, Object... values) {
    return SOQLBuilder.bind(SOQLBuilder.buildWhereClause(condition, FIELDS),
        new DataContext() {
          @Override public SchemaPlus getRootSchema() {
            throw new UnsupportedOperationException();
          }

          @Override public JavaTypeFactory getTypeFactory() {
            throw new UnsupportedOperationException();
          }

          @Override public QueryProvider getQueryProvider() {
            throw new UnsupportedOperationException();
          }

          @Override public Object get(String name) {
            return values[Integer.parseInt(name.substring(1))];
          }
        });
  }
}
