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
package org.apache.calcite.adapter.file.format.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DirectParquetWriter}.
 *
 * <p>Exercises all public and package-private methods using mock ResultSet
 * and ResultSetMetaData objects. Each test focuses on a single concern:
 * schema mapping, value writing, null handling, metadata, and edge cases.</p>
 */
@Tag("unit")
class DirectParquetWriterCoverageTest {

  @TempDir
  java.nio.file.Path tempDir;

  // -------------------------------------------------------------------
  // Helper: create a mock ResultSet with a single row and given columns
  // -------------------------------------------------------------------

  /**
   * Builds a mock ResultSet that returns one row then stops.
   * Each column is described by a ColumnDef.
   */
  private ResultSet buildMockResultSet(ColumnDef... columns) throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData rsmd = mock(ResultSetMetaData.class);
    when(rs.getMetaData()).thenReturn(rsmd);
    when(rsmd.getColumnCount()).thenReturn(columns.length);

    // First call to next() returns true, second returns false
    when(rs.next()).thenReturn(true, false);

    for (int i = 0; i < columns.length; i++) {
      int col = i + 1; // JDBC columns are 1-based
      ColumnDef def = columns[i];
      when(rsmd.getColumnName(col)).thenReturn(def.name);
      when(rsmd.getColumnType(col)).thenReturn(def.sqlType);
      when(rsmd.isNullable(col)).thenReturn(
          def.nullable ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls);
      when(rsmd.getPrecision(col)).thenReturn(def.precision);
      when(rsmd.getScale(col)).thenReturn(def.scale);

      // Wire up value retrieval based on type
      if (def.value == null) {
        when(rs.getObject(col)).thenReturn(null);
        when(rs.wasNull()).thenReturn(true);
      } else {
        when(rs.getObject(col)).thenReturn(def.value);
        when(rs.wasNull()).thenReturn(false);
        wireValue(rs, col, def);
      }
    }
    return rs;
  }

  /**
   * Builds a mock ResultSet that has zero rows (empty).
   */
  private ResultSet buildEmptyMockResultSet(ColumnDef... columns) throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData rsmd = mock(ResultSetMetaData.class);
    when(rs.getMetaData()).thenReturn(rsmd);
    when(rsmd.getColumnCount()).thenReturn(columns.length);

    // next() always returns false
    when(rs.next()).thenReturn(false);

    for (int i = 0; i < columns.length; i++) {
      int col = i + 1;
      ColumnDef def = columns[i];
      when(rsmd.getColumnName(col)).thenReturn(def.name);
      when(rsmd.getColumnType(col)).thenReturn(def.sqlType);
      when(rsmd.isNullable(col)).thenReturn(
          def.nullable ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls);
      when(rsmd.getPrecision(col)).thenReturn(def.precision);
      when(rsmd.getScale(col)).thenReturn(def.scale);
    }
    return rs;
  }

  /**
   * Builds a mock ResultSet with multiple rows.
   */
  private ResultSet buildMultiRowMockResultSet(ColumnDef[] columns, Object[][] rows)
      throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData rsmd = mock(ResultSetMetaData.class);
    when(rs.getMetaData()).thenReturn(rsmd);
    when(rsmd.getColumnCount()).thenReturn(columns.length);

    // Build the sequence of next() return values
    Boolean[] nextReturns = new Boolean[rows.length + 1];
    for (int r = 0; r < rows.length; r++) {
      nextReturns[r] = true;
    }
    nextReturns[rows.length] = false;
    when(rs.next()).thenReturn(nextReturns[0],
        java.util.Arrays.copyOfRange(nextReturns, 1, nextReturns.length));

    for (int i = 0; i < columns.length; i++) {
      int col = i + 1;
      ColumnDef def = columns[i];
      when(rsmd.getColumnName(col)).thenReturn(def.name);
      when(rsmd.getColumnType(col)).thenReturn(def.sqlType);
      when(rsmd.isNullable(col)).thenReturn(
          def.nullable ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls);
      when(rsmd.getPrecision(col)).thenReturn(def.precision);
      when(rsmd.getScale(col)).thenReturn(def.scale);

      // Wire up sequential return values for each column
      Object[] colValues = new Object[rows.length];
      for (int r = 0; r < rows.length; r++) {
        colValues[r] = rows[r][i];
      }
      wireMultiRowValues(rs, col, def, colValues);
    }
    return rs;
  }

  private void wireValue(ResultSet rs, int col, ColumnDef def) throws SQLException {
    switch (def.sqlType) {
    case Types.BOOLEAN:
      when(rs.getBoolean(col)).thenReturn((Boolean) def.value);
      break;
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.INTEGER:
      when(rs.getInt(col)).thenReturn(((Number) def.value).intValue());
      break;
    case Types.BIGINT:
      when(rs.getLong(col)).thenReturn(((Number) def.value).longValue());
      break;
    case Types.FLOAT:
    case Types.REAL:
      when(rs.getFloat(col)).thenReturn(((Number) def.value).floatValue());
      break;
    case Types.DOUBLE:
      when(rs.getDouble(col)).thenReturn(((Number) def.value).doubleValue());
      break;
    case Types.DECIMAL:
    case Types.NUMERIC:
      when(rs.getBigDecimal(col)).thenReturn((BigDecimal) def.value);
      break;
    case Types.DATE:
      when(rs.getDate(col)).thenReturn((java.sql.Date) def.value);
      break;
    case Types.TIME:
      when(rs.getObject(col)).thenReturn(def.value);
      break;
    case Types.TIMESTAMP:
    case Types.TIMESTAMP_WITH_TIMEZONE:
      when(rs.getTimestamp(col)).thenReturn((Timestamp) def.value);
      break;
    default:
      // VARCHAR and other string-like types
      when(rs.getString(col)).thenReturn(def.value.toString());
      break;
    }
  }

  @SuppressWarnings("unchecked")
  private void wireMultiRowValues(ResultSet rs, int col, ColumnDef def, Object[] values)
      throws SQLException {
    switch (def.sqlType) {
    case Types.INTEGER:
      Integer[] intVals = new Integer[values.length];
      for (int i = 0; i < values.length; i++) {
        intVals[i] = values[i] == null ? 0 : ((Number) values[i]).intValue();
      }
      if (intVals.length == 1) {
        when(rs.getInt(col)).thenReturn(intVals[0]);
      } else {
        when(rs.getInt(col)).thenReturn(intVals[0],
            java.util.Arrays.copyOfRange(intVals, 1, intVals.length));
      }
      // Wire getObject for null checks
      if (values.length == 1) {
        when(rs.getObject(col)).thenReturn(values[0]);
      } else {
        when(rs.getObject(col)).thenReturn(values[0],
            java.util.Arrays.copyOfRange(values, 1, values.length));
      }
      break;
    default:
      // For VARCHAR/string types
      String[] strVals = new String[values.length];
      for (int i = 0; i < values.length; i++) {
        strVals[i] = values[i] == null ? null : values[i].toString();
      }
      if (strVals.length == 1) {
        when(rs.getString(col)).thenReturn(strVals[0]);
      } else {
        when(rs.getString(col)).thenReturn(strVals[0],
            java.util.Arrays.copyOfRange(strVals, 1, strVals.length));
      }
      if (values.length == 1) {
        when(rs.getObject(col)).thenReturn(values[0]);
      } else {
        when(rs.getObject(col)).thenReturn(values[0],
            java.util.Arrays.copyOfRange(values, 1, values.length));
      }
      break;
    }
  }

  // -------------------------------------------------------------------
  // Column definition helper
  // -------------------------------------------------------------------

  private static class ColumnDef {
    final String name;
    final int sqlType;
    final boolean nullable;
    final Object value;
    final int precision;
    final int scale;

    ColumnDef(String name, int sqlType, boolean nullable, Object value) {
      this(name, sqlType, nullable, value, 0, 0);
    }

    ColumnDef(String name, int sqlType, boolean nullable, Object value,
        int precision, int scale) {
      this.name = name;
      this.sqlType = sqlType;
      this.nullable = nullable;
      this.value = value;
      this.precision = precision;
      this.scale = scale;
    }
  }

  // -------------------------------------------------------------------
  // Schema field creation tests (createParquetField)
  // -------------------------------------------------------------------

  @Test void testSchemaFieldInteger() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_int.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("int_col", Types.INTEGER, true, 42));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("int_col");
    assertNotNull(field, "int_col field should exist in schema");
    assertEquals(PrimitiveType.PrimitiveTypeName.INT32,
        field.asPrimitiveType().getPrimitiveTypeName());
    assertEquals(Type.Repetition.OPTIONAL, field.getRepetition());
  }

  @Test void testSchemaFieldRequiredInteger() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_req_int.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("id", Types.INTEGER, false, 1));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("id");
    assertEquals(Type.Repetition.REQUIRED, field.getRepetition());
  }

  @Test void testSchemaFieldBigint() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_bigint.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("big_col", Types.BIGINT, true, 9999999999L));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("big_col");
    assertEquals(PrimitiveType.PrimitiveTypeName.INT64,
        field.asPrimitiveType().getPrimitiveTypeName());
  }

  @Test void testSchemaFieldFloat() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_float.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("float_col", Types.FLOAT, true, 3.14f));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("float_col");
    assertEquals(PrimitiveType.PrimitiveTypeName.FLOAT,
        field.asPrimitiveType().getPrimitiveTypeName());
  }

  @Test void testSchemaFieldReal() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_real.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("real_col", Types.REAL, true, 2.5f));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("real_col");
    assertEquals(PrimitiveType.PrimitiveTypeName.FLOAT,
        field.asPrimitiveType().getPrimitiveTypeName());
  }

  @Test void testSchemaFieldDouble() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_double.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("double_col", Types.DOUBLE, true, 2.71828));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("double_col");
    assertEquals(PrimitiveType.PrimitiveTypeName.DOUBLE,
        field.asPrimitiveType().getPrimitiveTypeName());
  }

  @Test void testSchemaFieldBoolean() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_bool.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("bool_col", Types.BOOLEAN, true, true));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("bool_col");
    assertEquals(PrimitiveType.PrimitiveTypeName.BOOLEAN,
        field.asPrimitiveType().getPrimitiveTypeName());
  }

  @Test void testSchemaFieldVarchar() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_varchar.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("name", Types.VARCHAR, true, "hello"));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("name");
    assertEquals(PrimitiveType.PrimitiveTypeName.BINARY,
        field.asPrimitiveType().getPrimitiveTypeName());
    assertTrue(field.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation,
        "VARCHAR should map to STRING logical type");
  }

  @Test void testSchemaFieldDate() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_date.parquet");
    ResultSet rs =
        buildMockResultSet(
            new ColumnDef("date_col", Types.DATE, true,
            java.sql.Date.valueOf("2024-01-15")));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("date_col");
    assertEquals(PrimitiveType.PrimitiveTypeName.INT32,
        field.asPrimitiveType().getPrimitiveTypeName());
    assertTrue(field.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation,
        "DATE should map to DATE logical type");
  }

  @Test void testSchemaFieldTimestamp() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_ts.parquet");
    ResultSet rs =
        buildMockResultSet(
            new ColumnDef("ts_col", Types.TIMESTAMP, true,
            Timestamp.valueOf("2024-01-15 10:30:00")));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("ts_col");
    assertEquals(PrimitiveType.PrimitiveTypeName.INT64,
        field.asPrimitiveType().getPrimitiveTypeName());
    assertTrue(field.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation,
        "TIMESTAMP should map to TIMESTAMP logical type");
  }

  @Test void testSchemaFieldTimestampWithTimezone() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_ts_tz.parquet");
    ResultSet rs =
        buildMockResultSet(
            new ColumnDef("ts_tz_col", Types.TIMESTAMP_WITH_TIMEZONE, true,
            Timestamp.valueOf("2024-06-15 14:00:00")));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("ts_tz_col");
    assertEquals(PrimitiveType.PrimitiveTypeName.INT64,
        field.asPrimitiveType().getPrimitiveTypeName());
    LogicalTypeAnnotation annotation = field.getLogicalTypeAnnotation();
    assertTrue(annotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation,
        "TIMESTAMP_WITH_TIMEZONE should map to TIMESTAMP logical type");
  }

  @Test void testSchemaFieldDecimal() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_decimal.parquet");
    ResultSet rs =
        buildMockResultSet(
            new ColumnDef("price", Types.DECIMAL, true,
            new BigDecimal("19.99"), 10, 2));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("price");
    assertEquals(PrimitiveType.PrimitiveTypeName.BINARY,
        field.asPrimitiveType().getPrimitiveTypeName());
    assertTrue(field.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation,
        "DECIMAL should map to DECIMAL logical type");
    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
        (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) field.getLogicalTypeAnnotation();
    assertEquals(10, decimalType.getPrecision());
    assertEquals(2, decimalType.getScale());
  }

  @Test void testSchemaFieldDecimalDefaultPrecision() throws Exception {
    // When precision <= 0, it should default to 38
    java.nio.file.Path parquetFile = tempDir.resolve("schema_decimal_default.parquet");
    ResultSet rs =
        buildMockResultSet(
            new ColumnDef("amount", Types.DECIMAL, true,
            new BigDecimal("100.50"), 0, 2));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("amount");
    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
        (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) field.getLogicalTypeAnnotation();
    assertEquals(38, decimalType.getPrecision(),
        "Precision should default to 38 when reported as 0");
    assertEquals(2, decimalType.getScale());
  }

  @Test void testSchemaFieldNumeric() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_numeric.parquet");
    ResultSet rs =
        buildMockResultSet(
            new ColumnDef("numeric_col", Types.NUMERIC, true,
            new BigDecimal("123.456"), 12, 3));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("numeric_col");
    assertTrue(field.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation,
        "NUMERIC should map to DECIMAL logical type");
  }

  @Test void testSchemaFieldTinyint() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_tinyint.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("tiny_col", Types.TINYINT, true, 7));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("tiny_col");
    assertEquals(PrimitiveType.PrimitiveTypeName.INT32,
        field.asPrimitiveType().getPrimitiveTypeName());
  }

  @Test void testSchemaFieldSmallint() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_smallint.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("small_col", Types.SMALLINT, true, 256));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("small_col");
    assertEquals(PrimitiveType.PrimitiveTypeName.INT32,
        field.asPrimitiveType().getPrimitiveTypeName());
  }

  @Test void testSchemaFieldTime() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_time.parquet");
    ResultSet rs =
        buildMockResultSet(
            new ColumnDef("time_col", Types.TIME, true,
            java.time.LocalTime.of(14, 30, 0)));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("time_col");
    assertEquals(PrimitiveType.PrimitiveTypeName.INT32,
        field.asPrimitiveType().getPrimitiveTypeName());
    assertTrue(field.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation,
        "TIME should map to TIME logical type");
  }

  @Test void testSchemaFieldBlobFallsBackToString() throws Exception {
    // BLOB / BINARY / other unknown types should fall through to BINARY+STRING
    java.nio.file.Path parquetFile = tempDir.resolve("schema_blob.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("blob_col", Types.BLOB, true, "binary_data"));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    Type field = schema.getType("blob_col");
    assertEquals(PrimitiveType.PrimitiveTypeName.BINARY,
        field.asPrimitiveType().getPrimitiveTypeName());
    assertTrue(field.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation,
        "Unknown types should default to STRING");
  }

  // -------------------------------------------------------------------
  // Value writing tests (addValueToGroup)
  // -------------------------------------------------------------------

  @Test void testWriteIntegerValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("val_int.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("count", Types.INTEGER, true, 42));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    assertTrue(Files.exists(parquetFile));
    assertTrue(Files.size(parquetFile) > 0);

    // Verify value using DuckDB
    verifyWithDuckDB(parquetFile, "SELECT count FROM read_parquet('{path}')",
        duckRs -> {
          assertTrue(duckRs.next());
          assertEquals(42, duckRs.getInt("count"));
          assertFalse(duckRs.next());
        });
  }

  @Test void testWriteBigintValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("val_bigint.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("big_val", Types.BIGINT, true, 9876543210L));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    verifyWithDuckDB(parquetFile, "SELECT big_val FROM read_parquet('{path}')",
        duckRs -> {
          assertTrue(duckRs.next());
          assertEquals(9876543210L, duckRs.getLong("big_val"));
        });
  }

  @Test void testWriteDoubleValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("val_double.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("amount", Types.DOUBLE, true, 3.14159));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    verifyWithDuckDB(parquetFile, "SELECT amount FROM read_parquet('{path}')",
        duckRs -> {
          assertTrue(duckRs.next());
          assertEquals(3.14159, duckRs.getDouble("amount"), 0.00001);
        });
  }

  @Test void testWriteFloatValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("val_float.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("weight", Types.FLOAT, true, 1.5f));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    verifyWithDuckDB(parquetFile, "SELECT weight FROM read_parquet('{path}')",
        duckRs -> {
          assertTrue(duckRs.next());
          assertEquals(1.5f, duckRs.getFloat("weight"), 0.001f);
        });
  }

  @Test void testWriteBooleanValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("val_bool.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("active", Types.BOOLEAN, true, true));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    verifyWithDuckDB(parquetFile, "SELECT active FROM read_parquet('{path}')",
        duckRs -> {
          assertTrue(duckRs.next());
          assertTrue(duckRs.getBoolean("active"));
        });
  }

  @Test void testWriteVarcharValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("val_varchar.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("label", Types.VARCHAR, true, "test_string"));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    verifyWithDuckDB(parquetFile, "SELECT label FROM read_parquet('{path}')",
        duckRs -> {
          assertTrue(duckRs.next());
          assertEquals("test_string", duckRs.getString("label"));
        });
  }

  @Test void testWriteDateValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("val_date.parquet");
    java.sql.Date dateVal = java.sql.Date.valueOf("2024-01-15");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("created", Types.DATE, true, dateVal));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    verifyWithDuckDB(parquetFile, "SELECT created FROM read_parquet('{path}')",
        duckRs -> {
          assertTrue(duckRs.next());
          java.sql.Date readDate = duckRs.getDate("created");
          assertNotNull(readDate);
          // Verify using epoch days to avoid timezone display issues
          java.time.LocalDate expected = java.time.LocalDate.of(2024, 1, 15);
          java.time.LocalDate actual = readDate.toLocalDate();
          assertEquals(expected.toEpochDay(), actual.toEpochDay());
        });
  }

  @Test void testWriteTimestampValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("val_ts.parquet");
    Timestamp tsVal = Timestamp.valueOf("2024-06-15 10:30:45");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("updated", Types.TIMESTAMP, true, tsVal));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    assertTrue(Files.exists(parquetFile));
    assertTrue(Files.size(parquetFile) > 0);
  }

  @Test void testWriteTimestampWithTimezoneValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("val_ts_tz.parquet");
    Timestamp tsVal = Timestamp.valueOf("2024-06-15 14:00:00");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("event_time", Types.TIMESTAMP_WITH_TIMEZONE, true, tsVal));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    assertTrue(Files.exists(parquetFile));
    assertTrue(Files.size(parquetFile) > 0);
  }

  @Test void testWriteDecimalValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("val_decimal.parquet");
    ResultSet rs =
        buildMockResultSet(
            new ColumnDef("price", Types.DECIMAL, true,
            new BigDecimal("49.99"), 10, 2));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    assertTrue(Files.exists(parquetFile));
    assertTrue(Files.size(parquetFile) > 0);
  }

  @Test void testWriteTimeLocalTimeValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("val_time_lt.parquet");
    ResultSet rs =
        buildMockResultSet(
            new ColumnDef("start_time", Types.TIME, true,
            java.time.LocalTime.of(9, 30, 0)));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    assertTrue(Files.exists(parquetFile));
    assertTrue(Files.size(parquetFile) > 0);
  }

  @Test void testWriteTimeSqlTimeValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("val_time_st.parquet");
    ResultSet rs =
        buildMockResultSet(
            new ColumnDef("end_time", Types.TIME, true,
            java.sql.Time.valueOf("17:45:00")));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    assertTrue(Files.exists(parquetFile));
    assertTrue(Files.size(parquetFile) > 0);
  }

  // -------------------------------------------------------------------
  // Null handling tests
  // -------------------------------------------------------------------

  @Test void testNullIntegerValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("null_int.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("nullable_id", Types.INTEGER, true, null));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    verifyWithDuckDB(parquetFile,
        "SELECT nullable_id FROM read_parquet('{path}')",
        duckRs -> {
          assertTrue(duckRs.next());
          duckRs.getInt("nullable_id");
          assertTrue(duckRs.wasNull(), "NULL integer should be preserved");
        });
  }

  @Test void testNullVarcharValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("null_varchar.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("nullable_name", Types.VARCHAR, true, null));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    verifyWithDuckDB(parquetFile,
        "SELECT nullable_name FROM read_parquet('{path}')",
        duckRs -> {
          assertTrue(duckRs.next());
          String val = duckRs.getString("nullable_name");
          assertTrue(duckRs.wasNull() || val == null, "NULL varchar should be preserved");
        });
  }

  @Test void testNullTimestampValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("null_ts.parquet");
    ColumnDef tsDef = new ColumnDef("nullable_ts", Types.TIMESTAMP, true, null);
    ResultSet rs = buildMockResultSet(tsDef);

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    assertTrue(Files.exists(parquetFile));
    assertTrue(Files.size(parquetFile) > 0);
  }

  @Test void testNullDateValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("null_date.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("nullable_date", Types.DATE, true, null));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    assertTrue(Files.exists(parquetFile));
    assertTrue(Files.size(parquetFile) > 0);
  }

  @Test void testNullBooleanValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("null_bool.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("nullable_flag", Types.BOOLEAN, true, null));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    assertTrue(Files.exists(parquetFile));
    assertTrue(Files.size(parquetFile) > 0);
  }

  @Test void testNullDecimalValue() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("null_decimal.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("nullable_price", Types.DECIMAL, true, null, 10, 2));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    assertTrue(Files.exists(parquetFile));
    assertTrue(Files.size(parquetFile) > 0);
  }

  @Test void testMixedNullAndNonNullValues() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("mixed_nulls.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("id", Types.INTEGER, false, 1),
        new ColumnDef("name", Types.VARCHAR, true, null),
        new ColumnDef("score", Types.DOUBLE, true, 95.5));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    verifyWithDuckDB(parquetFile,
        "SELECT id, name, score FROM read_parquet('{path}')",
        duckRs -> {
          assertTrue(duckRs.next());
          assertEquals(1, duckRs.getInt("id"));
          String name = duckRs.getString("name");
          assertTrue(duckRs.wasNull() || name == null);
          assertEquals(95.5, duckRs.getDouble("score"), 0.001);
        });
  }

  // -------------------------------------------------------------------
  // Metadata tests
  // -------------------------------------------------------------------

  @Test void testTableCommentMetadata() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("meta_table.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("val", Types.INTEGER, true, 1));

    DirectParquetWriter.writeResultSetToParquet(
        rs, new Path(parquetFile.toString()),
        "This is the table comment", null);

    // Verify metadata was written by reading parquet footer
    Map<String, String> metadata = readParquetKeyValueMetadata(parquetFile);
    assertEquals("This is the table comment", metadata.get("table_comment"));
  }

  @Test void testColumnCommentsMetadata() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("meta_columns.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("product_id", Types.INTEGER, true, 1),
        new ColumnDef("product_name", Types.VARCHAR, true, "Widget"));

    Map<String, String> columnComments = new HashMap<String, String>();
    columnComments.put("product_id", "Primary key");
    columnComments.put("product_name", "Display name");

    DirectParquetWriter.writeResultSetToParquet(
        rs, new Path(parquetFile.toString()),
        "Products table", columnComments);

    Map<String, String> metadata = readParquetKeyValueMetadata(parquetFile);
    assertEquals("Products table", metadata.get("table_comment"));
    assertNotNull(metadata.get("column_comments"),
        "column_comments metadata key should be present");
    // The column_comments value is a JSON string
    assertTrue(metadata.get("column_comments").contains("Primary key"));
    assertTrue(metadata.get("column_comments").contains("Display name"));
  }

  @Test void testNullTableCommentOmitted() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("meta_null_comment.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("val", Types.INTEGER, true, 1));

    DirectParquetWriter.writeResultSetToParquet(
        rs, new Path(parquetFile.toString()), null, null);

    Map<String, String> metadata = readParquetKeyValueMetadata(parquetFile);
    assertFalse(metadata.containsKey("table_comment"),
        "Null table comment should not be stored");
  }

  @Test void testEmptyTableCommentOmitted() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("meta_empty_comment.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("val", Types.INTEGER, true, 1));

    DirectParquetWriter.writeResultSetToParquet(
        rs, new Path(parquetFile.toString()), "", null);

    Map<String, String> metadata = readParquetKeyValueMetadata(parquetFile);
    assertFalse(metadata.containsKey("table_comment"),
        "Empty table comment should not be stored");
  }

  @Test void testEmptyColumnCommentsOmitted() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("meta_empty_cols.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("val", Types.INTEGER, true, 1));

    Map<String, String> emptyComments = new HashMap<String, String>();

    DirectParquetWriter.writeResultSetToParquet(
        rs, new Path(parquetFile.toString()), null, emptyComments);

    Map<String, String> metadata = readParquetKeyValueMetadata(parquetFile);
    assertFalse(metadata.containsKey("column_comments"),
        "Empty column comments should not be stored");
  }

  @Test void testNullColumnCommentsOmitted() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("meta_null_cols.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("val", Types.INTEGER, true, 1));

    DirectParquetWriter.writeResultSetToParquet(
        rs, new Path(parquetFile.toString()), "comment", null);

    Map<String, String> metadata = readParquetKeyValueMetadata(parquetFile);
    assertFalse(metadata.containsKey("column_comments"),
        "Null column comments should not be stored");
  }

  @Test void testMetadataMapParameter() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("meta_map.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("id", Types.INTEGER, true, 10));

    Map<String, String> columnComments = new HashMap<String, String>();
    columnComments.put("id", "Identifier column");

    DirectParquetWriter.writeResultSetToParquet(
        rs, new Path(parquetFile.toString()),
        "Test table description", columnComments);

    Map<String, String> metadata = readParquetKeyValueMetadata(parquetFile);
    assertTrue(metadata.containsKey("table_comment"));
    assertTrue(metadata.containsKey("column_comments"));
  }

  // -------------------------------------------------------------------
  // Two-arg convenience method test
  // -------------------------------------------------------------------

  @Test void testTwoArgConvenienceMethod() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("convenience.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("x", Types.INTEGER, true, 99));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    assertTrue(Files.exists(parquetFile));
    assertTrue(Files.size(parquetFile) > 0);

    // Verify no metadata was written (null comments)
    Map<String, String> metadata = readParquetKeyValueMetadata(parquetFile);
    assertFalse(metadata.containsKey("table_comment"));
    assertFalse(metadata.containsKey("column_comments"));
  }

  // -------------------------------------------------------------------
  // Edge case tests
  // -------------------------------------------------------------------

  @Test void testEmptyResultSet() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("empty.parquet");
    ResultSet rs =
        buildEmptyMockResultSet(new ColumnDef("id", Types.INTEGER, true, null),
        new ColumnDef("name", Types.VARCHAR, true, null));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    assertTrue(Files.exists(parquetFile), "Parquet file should still be created for empty result");
    assertTrue(Files.size(parquetFile) > 0, "File should have schema even with no rows");

    // Verify schema is correct and row count is 0
    verifyWithDuckDB(parquetFile,
        "SELECT count(*) AS cnt FROM read_parquet('{path}')",
        duckRs -> {
          assertTrue(duckRs.next());
          assertEquals(0, duckRs.getLong("cnt"));
        });
  }

  @Test void testSingleRowSingleColumn() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("single.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("only_col", Types.VARCHAR, true, "only_value"));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    verifyWithDuckDB(parquetFile,
        "SELECT only_col FROM read_parquet('{path}')",
        duckRs -> {
          assertTrue(duckRs.next());
          assertEquals("only_value", duckRs.getString("only_col"));
          assertFalse(duckRs.next());
        });
  }

  @Test void testMultipleColumns() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("multi_col.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("col_a", Types.INTEGER, true, 1),
        new ColumnDef("col_b", Types.VARCHAR, true, "text"),
        new ColumnDef("col_c", Types.DOUBLE, true, 9.9),
        new ColumnDef("col_d", Types.BOOLEAN, true, false));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    assertEquals(4, schema.getFieldCount(), "Should have 4 columns");

    verifyWithDuckDB(parquetFile,
        "SELECT col_a, col_b, col_c, col_d FROM read_parquet('{path}')",
        duckRs -> {
          assertTrue(duckRs.next());
          assertEquals(1, duckRs.getInt("col_a"));
          assertEquals("text", duckRs.getString("col_b"));
          assertEquals(9.9, duckRs.getDouble("col_c"), 0.001);
          assertFalse(duckRs.getBoolean("col_d"));
        });
  }

  @Test void testMultipleRows() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("multi_row.parquet");

    ColumnDef[] columns = new ColumnDef[] {
        new ColumnDef("id", Types.INTEGER, false, null),
        new ColumnDef("label", Types.VARCHAR, true, null)
    };
    Object[][] rows = new Object[][] {
        {1, "first"},
        {2, "second"},
        {3, "third"}
    };
    ResultSet rs = buildMultiRowMockResultSet(columns, rows);

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    verifyWithDuckDB(parquetFile,
        "SELECT count(*) AS cnt FROM read_parquet('{path}')",
        duckRs -> {
          assertTrue(duckRs.next());
          assertEquals(3, duckRs.getLong("cnt"));
        });
  }

  @Test void testAllTypesInOneRow() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("all_types.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("bool_col", Types.BOOLEAN, true, true),
        new ColumnDef("int_col", Types.INTEGER, true, 42),
        new ColumnDef("bigint_col", Types.BIGINT, true, 123456789L),
        new ColumnDef("float_col", Types.FLOAT, true, 1.5f),
        new ColumnDef("double_col", Types.DOUBLE, true, 2.718),
        new ColumnDef("varchar_col", Types.VARCHAR, true, "hello"),
        new ColumnDef("date_col", Types.DATE, true,
            java.sql.Date.valueOf("2024-03-15")),
        new ColumnDef("ts_col", Types.TIMESTAMP, true,
            Timestamp.valueOf("2024-03-15 12:00:00")),
        new ColumnDef("decimal_col", Types.DECIMAL, true,
            new BigDecimal("99.99"), 10, 2));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    assertEquals(9, schema.getFieldCount(), "All 9 columns should be present");

    assertTrue(Files.exists(parquetFile));
    assertTrue(Files.size(parquetFile) > 0);
  }

  @Test void testSchemaMessageTypeName() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("schema_name.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("x", Types.INTEGER, true, 1));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    MessageType schema = readParquetSchema(parquetFile);
    assertEquals("record", schema.getName(),
        "Schema message type should be named 'record'");
  }

  @Test void testOutputFileExists() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("exists_check.parquet");
    assertFalse(Files.exists(parquetFile), "File should not exist before writing");

    ResultSet rs =
        buildMockResultSet(new ColumnDef("val", Types.INTEGER, true, 1));
    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    assertTrue(Files.exists(parquetFile), "File should exist after writing");
  }

  @Test void testOutputFileNonEmpty() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("nonempty.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("val", Types.VARCHAR, true, "data"));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    assertTrue(Files.size(parquetFile) > 0, "Parquet file should not be empty");
  }

  // -------------------------------------------------------------------
  // DuckDB-based verification of written parquet files
  // -------------------------------------------------------------------

  @Test void testDuckDbVerifiesSchemaTypes() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("duckdb_schema.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("int_col", Types.INTEGER, true, 10),
        new ColumnDef("str_col", Types.VARCHAR, true, "abc"),
        new ColumnDef("bool_col", Types.BOOLEAN, true, false),
        new ColumnDef("big_col", Types.BIGINT, true, 999L));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    verifyWithDuckDB(parquetFile,
        "SELECT typeof(int_col) AS t_int, typeof(str_col) AS t_str, "
        + "typeof(bool_col) AS t_bool, typeof(big_col) AS t_big "
        + "FROM read_parquet('{path}')",
        duckRs -> {
          assertTrue(duckRs.next());
          assertEquals("INTEGER", duckRs.getString("t_int"));
          assertEquals("VARCHAR", duckRs.getString("t_str"));
          assertEquals("BOOLEAN", duckRs.getString("t_bool"));
          assertEquals("BIGINT", duckRs.getString("t_big"));
        });
  }

  @Test void testDuckDbVerifiesRowCount() throws Exception {
    java.nio.file.Path parquetFile = tempDir.resolve("duckdb_rowcount.parquet");
    ResultSet rs =
        buildMockResultSet(new ColumnDef("x", Types.INTEGER, true, 1));

    DirectParquetWriter.writeResultSetToParquet(rs, new Path(parquetFile.toString()));

    verifyWithDuckDB(parquetFile,
        "SELECT count(*) AS cnt FROM read_parquet('{path}')",
        duckRs -> {
          assertTrue(duckRs.next());
          assertEquals(1, duckRs.getLong("cnt"));
        });
  }

  // -------------------------------------------------------------------
  // Helpers: read parquet schema and metadata
  // -------------------------------------------------------------------

  private MessageType readParquetSchema(java.nio.file.Path parquetFile) throws Exception {
    Configuration conf = new Configuration();
    Path hadoopPath = new Path(parquetFile.toString());
    try (ParquetFileReader reader =
        ParquetFileReader.open(HadoopInputFile.fromPath(hadoopPath, conf))) {
      return reader.getFooter().getFileMetaData().getSchema();
    }
  }

  private Map<String, String> readParquetKeyValueMetadata(java.nio.file.Path parquetFile)
      throws Exception {
    Configuration conf = new Configuration();
    Path hadoopPath = new Path(parquetFile.toString());
    try (ParquetFileReader reader =
        ParquetFileReader.open(HadoopInputFile.fromPath(hadoopPath, conf))) {
      Map<String, String> kvMeta =
          reader.getFooter().getFileMetaData().getKeyValueMetaData();
      return kvMeta != null ? kvMeta : Collections.<String, String>emptyMap();
    }
  }

  /**
   * Verifies a parquet file's contents using DuckDB JDBC.
   * The query template should contain {path} which will be replaced
   * with the actual parquet file path.
   */
  private void verifyWithDuckDB(java.nio.file.Path parquetFile,
      String queryTemplate, DuckDbVerifier verifier) throws Exception {
    String query = queryTemplate.replace("{path}", parquetFile.toString());
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      verifier.verify(rs);
    }
  }

  @FunctionalInterface
  private interface DuckDbVerifier {
    void verify(ResultSet rs) throws Exception;
  }
}
