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
 * Tests for {@link DuckDBFunctionMapping} covering function mapping,
 * SQL kind support detection, and special handling cases.
 */
@Tag("unit")
class DuckDBFunctionMappingTest {

  // --- String function mappings ---

  @Test void testCharLengthMapsToLength() {
    assertEquals("LENGTH", DuckDBFunctionMapping.getDuckDBFunction("CHAR_LENGTH"));
  }

  @Test void testCharacterLengthMapsToLength() {
    assertEquals("LENGTH", DuckDBFunctionMapping.getDuckDBFunction("CHARACTER_LENGTH"));
  }

  @Test void testSubstringMapsToSubstr() {
    assertEquals("SUBSTR", DuckDBFunctionMapping.getDuckDBFunction("SUBSTRING"));
  }

  @Test void testLocateMapsToInstr() {
    assertEquals("INSTR", DuckDBFunctionMapping.getDuckDBFunction("LOCATE"));
  }

  // --- Date/Time function mappings ---

  @Test void testCurrentTimestampMapsToNow() {
    assertEquals("NOW", DuckDBFunctionMapping.getDuckDBFunction("CURRENT_TIMESTAMP"));
  }

  @Test void testCurrentDateMapsToToday() {
    assertEquals("TODAY", DuckDBFunctionMapping.getDuckDBFunction("CURRENT_DATE"));
  }

  @Test void testDayOfMonthMapsToDay() {
    assertEquals("DAY", DuckDBFunctionMapping.getDuckDBFunction("DAYOFMONTH"));
  }

  @Test void testYearMapsToYear() {
    assertEquals("YEAR", DuckDBFunctionMapping.getDuckDBFunction("YEAR"));
  }

  @Test void testMonthMapsToMonth() {
    assertEquals("MONTH", DuckDBFunctionMapping.getDuckDBFunction("MONTH"));
  }

  @Test void testHourMapsToHour() {
    assertEquals("HOUR", DuckDBFunctionMapping.getDuckDBFunction("HOUR"));
  }

  @Test void testMinuteMapsToMinute() {
    assertEquals("MINUTE", DuckDBFunctionMapping.getDuckDBFunction("MINUTE"));
  }

  @Test void testSecondMapsToSecond() {
    assertEquals("SECOND", DuckDBFunctionMapping.getDuckDBFunction("SECOND"));
  }

  // --- Math function mappings ---

  @Test void testTruncateMapsToTrunc() {
    assertEquals("TRUNC", DuckDBFunctionMapping.getDuckDBFunction("TRUNCATE"));
  }

  @Test void testLog10MapsToLog10() {
    assertEquals("LOG10", DuckDBFunctionMapping.getDuckDBFunction("LOG10"));
  }

  @Test void testLogMapsToLn() {
    assertEquals("LN", DuckDBFunctionMapping.getDuckDBFunction("LOG"));
  }

  @Test void testPowerMapsToPow() {
    assertEquals("POW", DuckDBFunctionMapping.getDuckDBFunction("POWER"));
  }

  // --- Aggregate function mappings ---

  @Test void testStddevMapsToStddevSamp() {
    assertEquals("STDDEV_SAMP", DuckDBFunctionMapping.getDuckDBFunction("STDDEV"));
  }

  @Test void testVarianceMapsToVarSamp() {
    assertEquals("VAR_SAMP", DuckDBFunctionMapping.getDuckDBFunction("VARIANCE"));
  }

  @Test void testGroupConcatMapsToStringAgg() {
    assertEquals("STRING_AGG", DuckDBFunctionMapping.getDuckDBFunction("GROUP_CONCAT"));
  }

  @Test void testListaggMapsToStringAgg() {
    assertEquals("STRING_AGG", DuckDBFunctionMapping.getDuckDBFunction("LISTAGG"));
  }

  // --- Window function mappings ---

  @Test void testRankMapsToRank() {
    assertEquals("RANK", DuckDBFunctionMapping.getDuckDBFunction("RANK"));
  }

  @Test void testDenseRankMapsToDenseRank() {
    assertEquals("DENSE_RANK", DuckDBFunctionMapping.getDuckDBFunction("DENSE_RANK"));
  }

  @Test void testRowNumberMapsToRowNumber() {
    assertEquals("ROW_NUMBER", DuckDBFunctionMapping.getDuckDBFunction("ROW_NUMBER"));
  }

  @Test void testNtileMapsToNtile() {
    assertEquals("NTILE", DuckDBFunctionMapping.getDuckDBFunction("NTILE"));
  }

  @Test void testPercentRankMapsToPercentRank() {
    assertEquals("PERCENT_RANK", DuckDBFunctionMapping.getDuckDBFunction("PERCENT_RANK"));
  }

  @Test void testCumeDistMapsToCumeDist() {
    assertEquals("CUME_DIST", DuckDBFunctionMapping.getDuckDBFunction("CUME_DIST"));
  }

  // --- DuckDB-specific file functions ---

  @Test void testReadCsvMapsToReadCsvAuto() {
    assertEquals("read_csv_auto", DuckDBFunctionMapping.getDuckDBFunction("READ_CSV"));
  }

  @Test void testReadParquetMapsToReadParquet() {
    assertEquals("read_parquet", DuckDBFunctionMapping.getDuckDBFunction("READ_PARQUET"));
  }

  @Test void testReadJsonMapsToReadJsonAuto() {
    assertEquals("read_json_auto", DuckDBFunctionMapping.getDuckDBFunction("READ_JSON"));
  }

  // --- Vector similarity functions ---

  @Test void testCosineSimilarityMapsToArrayCosineSimilarity() {
    assertEquals("array_cosine_similarity",
        DuckDBFunctionMapping.getDuckDBFunction("COSINE_SIMILARITY"));
  }

  @Test void testCosineDistanceMapsToArrayCosineDistance() {
    assertEquals("array_cosine_distance",
        DuckDBFunctionMapping.getDuckDBFunction("COSINE_DISTANCE"));
  }

  // --- Unknown function passthrough ---

  @Test void testUnknownFunctionPassesThrough() {
    assertEquals("UNKNOWN_FUNC", DuckDBFunctionMapping.getDuckDBFunction("UNKNOWN_FUNC"));
  }

  @Test void testCaseInsensitiveLookup() {
    assertEquals("LENGTH", DuckDBFunctionMapping.getDuckDBFunction("char_length"));
    assertEquals("LENGTH", DuckDBFunctionMapping.getDuckDBFunction("Char_Length"));
  }

  // --- supportsSqlKind tests ---

  @Test void testSupportsSelect() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.SELECT));
  }

  @Test void testSupportsJoin() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.JOIN));
  }

  @Test void testSupportsCount() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.COUNT));
  }

  @Test void testSupportsSum() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.SUM));
  }

  @Test void testSupportsAvg() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.AVG));
  }

  @Test void testSupportsMin() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.MIN));
  }

  @Test void testSupportsMax() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.MAX));
  }

  @Test void testSupportsOver() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.OVER));
  }

  @Test void testSupportsCast() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.CAST));
  }

  @Test void testSupportsLike() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.LIKE));
  }

  @Test void testSupportsBetween() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.BETWEEN));
  }

  @Test void testSupportsIn() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.IN));
  }

  @Test void testSupportsNotIn() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.NOT_IN));
  }

  @Test void testSupportsExists() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.EXISTS));
  }

  @Test void testSupportsIsNull() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.IS_NULL));
  }

  @Test void testSupportsIsNotNull() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.IS_NOT_NULL));
  }

  @Test void testSupportsArithmetic() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.PLUS));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.MINUS));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.TIMES));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.DIVIDE));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.MOD));
  }

  @Test void testSupportsComparison() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.EQUALS));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.NOT_EQUALS));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.LESS_THAN));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.LESS_THAN_OR_EQUAL));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.GREATER_THAN));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.GREATER_THAN_OR_EQUAL));
  }

  @Test void testSupportsLogical() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.AND));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.OR));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.NOT));
  }

  @Test void testSupportsSetOps() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.UNION));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.EXCEPT));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.INTERSECT));
  }

  @Test void testSupportsWindowFunctions() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.RANK));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.DENSE_RANK));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.ROW_NUMBER));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.FIRST_VALUE));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.LAST_VALUE));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.LEAD));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.LAG));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.NTILE));
  }

  @Test void testDoesNotSupportOther() {
    assertFalse(DuckDBFunctionMapping.supportsSqlKind(SqlKind.OTHER));
  }

  @Test void testSupportsWith() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.WITH));
  }

  @Test void testSupportsCase() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.CASE));
  }

  @Test void testSupportsOrderBy() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.ORDER_BY));
  }

  @Test void testSupportsInsertUpdateDelete() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.INSERT));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.UPDATE));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.DELETE));
  }

  @Test void testSupportsValues() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.VALUES));
  }

  @Test void testSupportsStddevVariance() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.STDDEV_POP));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.STDDEV_SAMP));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.VAR_POP));
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.VAR_SAMP));
  }

  @Test void testSupportsCollect() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.COLLECT));
  }

  @Test void testSupportsListagg() {
    assertTrue(DuckDBFunctionMapping.supportsSqlKind(SqlKind.LISTAGG));
  }

  @Test void testPercentRankNotInSupportsSqlKind() {
    // PERCENT_RANK is in the FUNCTION_MAP but not in the supportsSqlKind switch
    assertFalse(DuckDBFunctionMapping.supportsSqlKind(SqlKind.PERCENT_RANK));
  }
}
