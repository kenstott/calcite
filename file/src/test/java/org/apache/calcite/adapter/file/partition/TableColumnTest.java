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
package org.apache.calcite.adapter.file.partition;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for TableColumn including expression-based computed columns.
 */
@Tag("unit")
public class TableColumnTest {

  @Test void testBasicColumnWithoutExpression() {
    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn("name", "string", true, "Test column");

    assertEquals("name", column.getName());
    assertEquals("string", column.getType());
    assertTrue(column.isNullable());
    assertEquals("Test column", column.getComment());
    assertNull(column.getExpression());
    assertFalse(column.hasExpression());
    assertFalse(column.isComputed());
    assertFalse(column.isVectorType());
  }

  @Test void testExpressionColumn() {
    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn(
            "year", "integer", true, "Extracted year", "EXTRACT(YEAR FROM date)");

    assertEquals("year", column.getName());
    assertEquals("integer", column.getType());
    assertTrue(column.isNullable());
    assertEquals("Extracted year", column.getComment());
    assertEquals("EXTRACT(YEAR FROM date)", column.getExpression());
    assertTrue(column.hasExpression());
    assertTrue(column.isComputed());
    assertFalse(column.isVectorType());
  }

  @Test void testEmbeddingExpression() {
    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn(
            "text_embedding", "array<double>", true,
            "Semantic embedding", "embed(text)::FLOAT[384]");

    assertEquals("text_embedding", column.getName());
    assertEquals("array<double>", column.getType());
    assertTrue(column.isNullable());
    assertEquals("Semantic embedding", column.getComment());
    assertEquals("embed(text)::FLOAT[384]", column.getExpression());
    assertTrue(column.hasExpression());
    assertTrue(column.isComputed());
    assertTrue(column.isVectorType());
  }

  @Test void testStringExpression() {
    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn(
            "state_fips", "varchar", false,
            "State FIPS code", "SUBSTR(geoid, 1, 2)");

    assertEquals("SUBSTR(geoid, 1, 2)", column.getExpression());
    assertTrue(column.hasExpression());
    assertTrue(column.isComputed());
    assertFalse(column.isVectorType());
  }

  @Test void testNumericExpression() {
    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn(
            "total_price", "double", true,
            "Total price", "price * quantity");

    assertEquals("price * quantity", column.getExpression());
    assertTrue(column.hasExpression());
    assertTrue(column.isComputed());
    assertFalse(column.isVectorType());
  }

  @Test void testGeospatialExpression() {
    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn(
            "h3_index", "varchar", true,
            "H3 hexagonal cell", "h3_latlng_to_cell(lat, lon, 7)");

    assertEquals("h3_latlng_to_cell(lat, lon, 7)", column.getExpression());
    assertTrue(column.hasExpression());
    assertTrue(column.isComputed());
    assertFalse(column.isVectorType());
  }

  @Test void testNullExpression() {
    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn(
            "name", "string", true, "Test column", null);

    assertNull(column.getExpression());
    assertFalse(column.hasExpression());
    assertFalse(column.isComputed());
  }

  @Test void testEmptyExpression() {
    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn(
            "name", "string", true, "Test column", "");

    assertEquals("", column.getExpression());
    assertFalse(column.hasExpression());  // Empty strings are not considered expressions
    assertFalse(column.isComputed());
  }

  @Test void testWhitespaceOnlyExpression() {
    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn(
            "name", "string", true, "Test column", "   ");

    assertEquals("   ", column.getExpression());
    assertFalse(column.hasExpression());  // Whitespace-only strings are not considered expressions
    assertFalse(column.isComputed());
  }

  @Test void testIsVectorType() {
    PartitionedTableConfig.TableColumn arrayDoubleCol =
        new PartitionedTableConfig.TableColumn("vec", "array<double>", true, "Vector");
    assertTrue(arrayDoubleCol.isVectorType());

    PartitionedTableConfig.TableColumn arrayIntCol =
        new PartitionedTableConfig.TableColumn("arr", "array<int>", true, "Array");
    assertTrue(arrayIntCol.isVectorType());

    PartitionedTableConfig.TableColumn stringCol =
        new PartitionedTableConfig.TableColumn("str", "string", true, "String");
    assertFalse(stringCol.isVectorType());

    PartitionedTableConfig.TableColumn intCol =
        new PartitionedTableConfig.TableColumn("num", "int", false, "Number");
    assertFalse(intCol.isVectorType());
  }

  @Test void testComplexExpression() {
    String complexExpr = "CASE WHEN length(text) > 100 THEN embed(text)::FLOAT[384] ELSE NULL END";
    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn(
            "conditional_embedding", "array<double>", true,
            "Conditional embedding", complexExpr);

    assertEquals(complexExpr, column.getExpression());
    assertTrue(column.hasExpression());
    assertTrue(column.isComputed());
    assertTrue(column.isVectorType());
  }

  @Test void testWindowFunctionExpression() {
    String windowExpr = "(value - lag(value, 12) OVER (ORDER BY date)) / lag(value, 12)";
    PartitionedTableConfig.TableColumn column =
        new PartitionedTableConfig.TableColumn(
            "yoy_change", "double", true,
            "Year-over-year change", windowExpr);

    assertEquals(windowExpr, column.getExpression());
    assertTrue(column.hasExpression());
    assertTrue(column.isComputed());
    assertFalse(column.isVectorType());
  }
}
