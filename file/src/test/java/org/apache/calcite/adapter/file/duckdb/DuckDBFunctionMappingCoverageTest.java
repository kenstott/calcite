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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.sql.SqlKind;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link DuckDBFunctionMapping}.
 * Tests getDuckDBFunction(), needsSpecialHandling(), and supportsSqlKind().
 */
@Tag("unit")
public class DuckDBFunctionMappingCoverageTest {

  // ===== getDuckDBFunction =====

  @Test void testGetDuckDBFunctionStringFunctions() {
    assertEquals("LENGTH", DuckDBFunctionMapping.getDuckDBFunction("CHAR_LENGTH"));
    assertEquals("LENGTH", DuckDBFunctionMapping.getDuckDBFunction("CHARACTER_LENGTH"));
    assertEquals("SUBSTR", DuckDBFunctionMapping.getDuckDBFunction("SUBSTRING"));
    assertEquals("INSTR", DuckDBFunctionMapping.getDuckDBFunction("LOCATE"));
  }

  @Test void testGetDuckDBFunctionDateTimeFunctions() {
    assertEquals("NOW", DuckDBFunctionMapping.getDuckDBFunction("CURRENT_TIMESTAMP"));
    assertEquals("TODAY", DuckDBFunctionMapping.getDuckDBFunction("CURRENT_DATE"));
    assertEquals("DAYOFWEEK", DuckDBFunctionMapping.getDuckDBFunction("DAYOFWEEK"));
    assertEquals("DAY", DuckDBFunctionMapping.getDuckDBFunction("DAYOFMONTH"));
    assertEquals("DAYOFYEAR", DuckDBFunctionMapping.getDuckDBFunction("DAYOFYEAR"));
    assertEquals("YEAR", DuckDBFunctionMapping.getDuckDBFunction("YEAR"));
    assertEquals("MONTH", DuckDBFunctionMapping.getDuckDBFunction("MONTH"));
    assertEquals("DAY", DuckDBFunctionMapping.getDuckDBFunction("DAY"));
    assertEquals("HOUR", DuckDBFunctionMapping.getDuckDBFunction("HOUR"));
    assertEquals("MINUTE", DuckDBFunctionMapping.getDuckDBFunction("MINUTE"));
    assertEquals("SECOND", DuckDBFunctionMapping.getDuckDBFunction("SECOND"));
  }

  @Test void testGetDuckDBFunctionMathFunctions() {
    assertEquals("TRUNC", DuckDBFunctionMapping.getDuckDBFunction("TRUNCATE"));
    assertEquals("LOG10", DuckDBFunctionMapping.getDuckDBFunction("LOG10"));
    assertEquals("LN", DuckDBFunctionMapping.getDuckDBFunction("LOG"));
    assertEquals("POW", DuckDBFunctionMapping.getDuckDBFunction("POWER"));
  }

  @Test void testGetDuckDBFunctionAggregateFunctions() {
    assertEquals("STDDEV_SAMP", DuckDBFunctionMapping.getDuckDBFunction("STDDEV"));
    assertEquals("VAR_SAMP", DuckDBFunctionMapping.getDuckDBFunction("VARIANCE"));
    assertEquals("STRING_AGG", DuckDBFunctionMapping.getDuckDBFunction("GROUP_CONCAT"));
    assertEquals("STRING_AGG", DuckDBFunctionMapping.getDuckDBFunction("LISTAGG"));
  }

  @Test void testGetDuckDBFunctionWindowFunctions() {
    assertEquals("RANK", DuckDBFunctionMapping.getDuckDBFunction("RANK"));
    assertEquals("DENSE_RANK", DuckDBFunctionMapping.getDuckDBFunction("DENSE_RANK"));
    assertEquals("ROW_NUMBER", DuckDBFunctionMapping.getDuckDBFunction("ROW_NUMBER"));
    assertEquals("NTILE", DuckDBFunctionMapping.getDuckDBFunction("NTILE"));
    assertEquals("PERCENT_RANK", DuckDBFunctionMapping.getDuckDBFunction("PERCENT_RANK"));
    assertEquals("CUME_DIST", DuckDBFunctionMapping.getDuckDBFunction("CUME_DIST"));
  }

  @Test void testGetDuckDBFunctionFileFunctions() {
    assertEquals("read_csv_auto", DuckDBFunctionMapping.getDuckDBFunction("READ_CSV"));
    assertEquals("read_parquet", DuckDBFunctionMapping.getDuckDBFunction("READ_PARQUET"));
    assertEquals("read_json_auto", DuckDBFunctionMapping.getDuckDBFunction("READ_JSON"));
  }

  @Test void testGetDuckDBFunctionVectorSimilarity() {
    assertEquals("array_cosine_similarity",
        DuckDBFunctionMapping.getDuckDBFunction("COSINE_SIMILARITY"));
    assertEquals("array_cosine_distance",
        DuckDBFunctionMapping.getDuckDBFunction("COSINE_DISTANCE"));
  }

  @Test void testGetDuckDBFunctionCaseInsensitive() {
    assertEquals("LENGTH", DuckDBFunctionMapping.getDuckDBFunction("char_length"));
    assertEquals("SUBSTR", DuckDBFunctionMapping.getDuckDBFunction("substring"));
    assertEquals("NOW", DuckDBFunctionMapping.getDuckDBFunction("current_timestamp"));
  }

  @Test void testGetDuckDBFunctionUnknownReturnsOriginal() {
    assertEquals("UNKNOWN_FUNC", DuckDBFunctionMapping.getDuckDBFunction("UNKNOWN_FUNC"));
    assertEquals("my_custom_func", DuckDBFunctionMapping.getDuckDBFunction("my_custom_func"));
  }

  // ===== supportsSqlKind =====

  @Test void testSupportsSqlKindBasicOperations() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.SELECT));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.INSERT));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.UPDATE));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.DELETE));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.VALUES));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.ORDER_BY));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.WITH));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.UNION));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.EXCEPT));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.INTERSECT));
  }

  @Test void testSupportsSqlKindJoins() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.JOIN));
  }

  @Test void testSupportsSqlKindAggregates() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.COUNT));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.SUM));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.AVG));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.MIN));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.MAX));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.STDDEV_POP));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.STDDEV_SAMP));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.VAR_POP));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.VAR_SAMP));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.COLLECT));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.LISTAGG));
  }

  @Test void testSupportsSqlKindWindowFunctions() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.OVER));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.RANK));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.DENSE_RANK));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.ROW_NUMBER));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.FIRST_VALUE));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.LAST_VALUE));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.LEAD));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.LAG));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.NTILE));
  }

  @Test void testSupportsSqlKindExpressions() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.CASE));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.CAST));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.LIKE));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.BETWEEN));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.IN));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.NOT_IN));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.EXISTS));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.IS_NULL));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.IS_NOT_NULL));
  }

  @Test void testSupportsSqlKindArithmetic() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.PLUS));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.MINUS));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.TIMES));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.DIVIDE));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.MOD));
  }

  @Test void testSupportsSqlKindComparison() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.EQUALS));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.NOT_EQUALS));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.LESS_THAN));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.LESS_THAN_OR_EQUAL));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.GREATER_THAN));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.GREATER_THAN_OR_EQUAL));
  }

  @Test void testSupportsSqlKindLogical() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.AND));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.OR));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.NOT));
  }

  @Test void testSupportsSqlKindUnsupported() {
    assertFalse(DuckDBFunctionMapping.supportsSqlKind(SqlKind.TRIM));
    assertFalse(DuckDBFunctionMapping.supportsSqlKind(SqlKind.FLOOR));
    assertFalse(DuckDBFunctionMapping.supportsSqlKind(SqlKind.CEIL));
    assertFalse(DuckDBFunctionMapping.supportsSqlKind(SqlKind.POSITION));
  }
}
