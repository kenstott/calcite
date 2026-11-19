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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests the capability flags of {@link TrinoSqlDialect}, which decide what
 * the JDBC adapter pushes to Trino (REQ-2257). The pushed SQL itself is
 * checked in {@code RelToSqlConverterTest} and run against a Trino server in
 * {@code TrinoDialectLiveTest}.
 */
class TrinoSqlDialectTest {
  private static final SqlDialect DIALECT = TrinoSqlDialect.DEFAULT;
  private static final SqlTypeFactoryImpl TYPES =
      new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  private static final RelDataType VARCHAR =
      TYPES.createSqlType(SqlTypeName.VARCHAR);
  private static final RelDataType INTEGER =
      TYPES.createSqlType(SqlTypeName.INTEGER);
  private static final RelDataType DOUBLE =
      TYPES.createSqlType(SqlTypeName.DOUBLE);
  private static final RelDataType TIMESTAMP =
      TYPES.createSqlType(SqlTypeName.TIMESTAMP);
  private static final RelDataType SYMBOL =
      TYPES.createSqlType(SqlTypeName.SYMBOL);

  private static boolean supports(SqlOperator op, RelDataType... params) {
    final List<RelDataType> types = ImmutableList.copyOf(params);
    return DIALECT.supportsFunction(op, VARCHAR, types);
  }

  @Test void testFunctionsTrinoRunsWithCalcitesAnswer() {
    assertThat(supports(SqlStdOperatorTable.REPLACE, VARCHAR, VARCHAR, VARCHAR),
        is(true));
    assertThat(supports(SqlStdOperatorTable.ATAN, DOUBLE), is(true));
    assertThat(supports(SqlStdOperatorTable.SIGN, DOUBLE), is(true));
    assertThat(supports(SqlStdOperatorTable.TRUNCATE, DOUBLE), is(true));
    assertThat(supports(SqlStdOperatorTable.UPPER, VARCHAR), is(true));
    assertThat(supports(SqlStdOperatorTable.SUBSTRING, VARCHAR, INTEGER),
        is(true));
    assertThat(supports(SqlStdOperatorTable.EXTRACT, SYMBOL, TIMESTAMP),
        is(true));
    assertThat(supports(SqlStdOperatorTable.FLOOR, TIMESTAMP, SYMBOL),
        is(true));
    assertThat(supports(SqlStdOperatorTable.CEIL, DOUBLE), is(true));
    assertThat(supports(SqlLibraryOperators.STRPOS, VARCHAR, VARCHAR),
        is(true));
    assertThat(
        supports(SqlLibraryOperators.LPAD, VARCHAR, INTEGER, VARCHAR),
        is(true));
    assertThat(supports(SqlLibraryOperators.REGEXP_LIKE, VARCHAR, VARCHAR),
        is(true));
  }

  /** Calls Trino lacks or answers differently stay in Calcite. */
  @Test void testFunctionsKeptInCalcite() {
    // Trino has no COT.
    assertThat(supports(SqlStdOperatorTable.COT, DOUBLE), is(false));
    // Trino evaluates the current time in the client JVM's zone.
    assertThat(supports(SqlStdOperatorTable.CURRENT_TIMESTAMP), is(false));
    assertThat(supports(SqlStdOperatorTable.LOCALTIMESTAMP), is(false));
    assertThat(supports(SqlStdOperatorTable.CURRENT_DATE), is(false));
    // CEIL of a datetime has no Trino form.
    assertThat(supports(SqlStdOperatorTable.CEIL, TIMESTAMP, SYMBOL),
        is(false));
    // Trino's LPAD requires the padding string.
    assertThat(supports(SqlLibraryOperators.LPAD, VARCHAR, INTEGER),
        is(false));
    // Trino's REGEXP_LIKE has no match-parameter argument.
    assertThat(
        supports(SqlLibraryOperators.REGEXP_LIKE, VARCHAR, VARCHAR, VARCHAR),
        is(false));
    // PostgreSQL's SPLIT_PART returns '' past the last part, Trino's NULL.
    assertThat(
        supports(SqlLibraryOperators.SPLIT_PART, VARCHAR, VARCHAR, INTEGER),
        is(false));
    // PostgreSQL's REGEXP_REPLACE replaces the first match, Trino's all.
    assertThat(
        supports(SqlLibraryOperators.REGEXP_REPLACE_PG_3, VARCHAR, VARCHAR,
            VARCHAR),
        is(false));
    assertThat(supports(SqlStdOperatorTable.SIMILAR_TO, VARCHAR, VARCHAR),
        is(false));
    assertThat(supports(SqlLibraryOperators.LEFT, VARCHAR, INTEGER),
        is(false));
    assertThat(supports(SqlLibraryOperators.MD5, VARCHAR), is(false));
  }

  @Test void testAggregateFunctions() {
    for (SqlKind kind
        : ImmutableList.of(SqlKind.COUNT, SqlKind.SUM, SqlKind.SUM0,
            SqlKind.AVG, SqlKind.MIN, SqlKind.MAX, SqlKind.ANY_VALUE,
            SqlKind.STDDEV_POP, SqlKind.STDDEV_SAMP, SqlKind.VAR_POP,
            SqlKind.VAR_SAMP, SqlKind.LITERAL_AGG)) {
      assertThat(kind.toString(), DIALECT.supportsAggregateFunction(kind),
          is(true));
    }
    for (SqlKind kind
        : ImmutableList.of(SqlKind.BIT_AND, SqlKind.BIT_OR,
            SqlKind.ARRAY_AGG, SqlKind.LISTAGG, SqlKind.COLLECT)) {
      assertThat(kind.toString(), DIALECT.supportsAggregateFunction(kind),
          is(false));
    }
  }

  /** Trino has no SEMI, ANTI or ASOF join syntax. */
  @Test void testJoinTypes() {
    for (JoinRelType type
        : ImmutableList.of(JoinRelType.INNER, JoinRelType.LEFT,
            JoinRelType.RIGHT, JoinRelType.FULL)) {
      assertThat(type.toString(), DIALECT.supportsJoinType(type), is(true));
    }
    for (JoinRelType type
        : ImmutableList.of(JoinRelType.SEMI, JoinRelType.ANTI,
            JoinRelType.ASOF, JoinRelType.LEFT_ASOF)) {
      assertThat(type.toString(), DIALECT.supportsJoinType(type), is(false));
    }
  }

  @Test void testTextComparisonsArePushed() {
    assertThat(DIALECT.getTextComparisonPushdown(),
        is(SqlDialect.TextComparisonPushdown.ALL));
  }

  @Test void testDecimalPrecisionIsTrinos() {
    assertThat(DIALECT.getTypeSystem().getMaxPrecision(SqlTypeName.DECIMAL),
        is(38));
  }
}
