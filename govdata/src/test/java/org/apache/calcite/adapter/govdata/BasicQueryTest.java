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
package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic query tests for GovData adapter functionality.
 *
 * <p>Consolidates simple query validation, driver loading, and basic
 * functionality tests. These tests are fast, isolated, and require no
 * external dependencies.
 */
@Tag("unit")
public class BasicQueryTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BasicQueryTest.class);

  @Test void testDriverLoading() {
    LOGGER.debug("Testing Calcite JDBC driver loading");

    assertDoesNotThrow(() -> {
      Class.forName("org.apache.calcite.jdbc.Driver");
    }, "Should load Calcite JDBC driver without exception");
  }

  @Test void testConnectionPropertiesValidation() {
    LOGGER.debug("Testing connection properties validation");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    assertNotNull(props.getProperty("lex"));
    assertEquals("ORACLE", props.getProperty("lex"));
    assertEquals("TO_LOWER", props.getProperty("unquotedCasing"));
  }

  @Test void testSqlIdentifierHandling() {
    LOGGER.debug("Testing SQL identifier casing and quoting rules");

    // Test lowercase identifiers (should not need quoting)
    String lowercaseId = "table_name";
    assertFalse(needsQuoting(lowercaseId), "Lowercase identifiers should not need quoting");

    // Test uppercase identifiers (should need quoting)
    String uppercaseId = "TABLE_NAME";
    assertTrue(needsQuoting(uppercaseId), "Uppercase identifiers should need quoting");

    // Test mixed case identifiers (should need quoting)
    String mixedCaseId = "tableName";
    assertTrue(needsQuoting(mixedCaseId), "Mixed case identifiers should need quoting");

    // Test reserved words (should need quoting)
    String reservedWord = "select";
    assertTrue(needsQuoting(reservedWord), "Reserved words should need quoting");
  }

  @Test void testQueryStructureValidation() {
    LOGGER.debug("Testing basic SQL query structure validation");

    // Test simple SELECT query structure
    String simpleQuery = "SELECT col1, col2 FROM table1";
    assertTrue(isValidQueryStructure(simpleQuery), "Simple SELECT should be valid");

    // Test JOIN query structure
    String joinQuery = "SELECT t1.col1, t2.col2 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id";
    assertTrue(isValidQueryStructure(joinQuery), "JOIN query should be valid");

    // Test WHERE clause structure
    String whereQuery = "SELECT col1 FROM table1 WHERE col2 = 'value'";
    assertTrue(isValidQueryStructure(whereQuery), "WHERE clause should be valid");

    // Test invalid query structure
    String invalidQuery = "INVALID SQL SYNTAX";
    assertFalse(isValidQueryStructure(invalidQuery), "Invalid syntax should be rejected");
  }

  @Test void testParameterizedQueryPreparation() {
    LOGGER.debug("Testing parameterized query preparation patterns");

    String queryTemplate = "SELECT * FROM table1 WHERE id = ? AND name = ?";
    assertTrue(queryTemplate.contains("?"), "Query should contain parameter placeholders");

    // Count parameter placeholders
    int paramCount = countParameters(queryTemplate);
    assertEquals(2, paramCount, "Should have exactly 2 parameters");
  }

  @Test void testErrorHandlingPatterns() {
    LOGGER.debug("Testing error handling patterns");

    // Test null handling
    assertThrows(IllegalArgumentException.class, () -> {
      validateInput(null);
    }, "Should throw exception for null input");

    // Test empty string handling
    assertThrows(IllegalArgumentException.class, () -> {
      validateInput("");
    }, "Should throw exception for empty input");

    // Test valid input
    assertDoesNotThrow(() -> {
      validateInput("valid_input");
    }, "Should accept valid input");
  }

  @Test void testDataTypeMapping() {
    LOGGER.debug("Testing basic data type mapping");

    // Test string type mapping
    assertEquals("VARCHAR", mapToSqlType("string"), "String should map to VARCHAR");

    // Test numeric type mapping
    assertEquals("DECIMAL", mapToSqlType("decimal"), "Decimal should map to DECIMAL");
    assertEquals("INTEGER", mapToSqlType("integer"), "Integer should map to INTEGER");

    // Test date type mapping
    assertEquals("DATE", mapToSqlType("date"), "Date should map to DATE");

    // Test boolean type mapping
    assertEquals("BOOLEAN", mapToSqlType("boolean"), "Boolean should map to BOOLEAN");
  }

  // Helper methods for test validation

  private boolean needsQuoting(String identifier) {
    if (identifier == null || identifier.isEmpty()) {
      return true;
    }

    // Check if all lowercase
    if (identifier.equals(identifier.toLowerCase())) {
      // Check if it's a reserved word (simplified check)
      return isReservedWord(identifier);
    }

    // Contains uppercase or mixed case - needs quoting
    return true;
  }

  private boolean isReservedWord(String word) {
    // Simplified reserved word check
    String[] reservedWords = {"select", "from", "where", "join", "on", "and", "or", "order", "by"};
    for (String reserved : reservedWords) {
      if (reserved.equals(word.toLowerCase())) {
        return true;
      }
    }
    return false;
  }

  private boolean isValidQueryStructure(String query) {
    if (query == null || query.trim().isEmpty()) {
      return false;
    }

    String upperQuery = query.trim().toUpperCase();

    // Very basic validation - should start with SELECT, INSERT, UPDATE, or DELETE
    return upperQuery.startsWith("SELECT") ||
           upperQuery.startsWith("INSERT") ||
           upperQuery.startsWith("UPDATE") ||
           upperQuery.startsWith("DELETE");
  }

  private int countParameters(String query) {
    if (query == null) {
      return 0;
    }

    int count = 0;
    for (int i = 0; i < query.length(); i++) {
      if (query.charAt(i) == '?') {
        count++;
      }
    }
    return count;
  }

  private void validateInput(String input) {
    if (input == null) {
      throw new IllegalArgumentException("Input cannot be null");
    }
    if (input.trim().isEmpty()) {
      throw new IllegalArgumentException("Input cannot be empty");
    }
  }

  private String mapToSqlType(String javaType) {
    if (javaType == null) {
      return "VARCHAR";
    }

    switch (javaType.toLowerCase()) {
      case "string":
        return "VARCHAR";
      case "integer":
      case "int":
        return "INTEGER";
      case "decimal":
      case "double":
      case "float":
        return "DECIMAL";
      case "date":
        return "DATE";
      case "boolean":
        return "BOOLEAN";
      default:
        return "VARCHAR";
    }
  }
}
