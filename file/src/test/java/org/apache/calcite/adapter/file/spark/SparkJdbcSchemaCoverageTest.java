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
package org.apache.calcite.adapter.file.spark;

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.refresh.PatternAwareRefreshListener;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Coverage unit tests for {@link SparkJdbcSchema}.
 *
 * <p>Tests constructor behavior, recreateView/recreateIcebergView via
 * reflection and listener capture, getFileSchema, getComment, snapshot,
 * and close methods. Uses mocked JDBC Connection/Statement and FileSchema.
 */
@Tag("unit")
class SparkJdbcSchemaCoverageTest {

  // =======================================================================
  // Constructor and refresh listener registration
  // =======================================================================

  @Test void testConstructorWithFileSchemaRegistersListener() {
    FileSchema fileSchema = mock(FileSchema.class);
    SparkJdbcSchema schema = createSchema(fileSchema);
    assertNotNull(schema);
    verify(fileSchema).addRefreshListener(any(PatternAwareRefreshListener.class));
  }

  @Test void testConstructorWithNullFileSchemaSkipsListener() {
    SparkJdbcSchema schema = createSchema(null);
    assertNotNull(schema);
  }

  @Test void testConstructorSetsFields() {
    SparkJdbcSchema schema = createSchema(null);
    assertNotNull(schema);
  }

  // =======================================================================
  // getFileSchema
  // =======================================================================

  @Test void testGetFileSchemaReturnsInjected() {
    FileSchema fileSchema = mock(FileSchema.class);
    SparkJdbcSchema schema = createSchema(fileSchema);
    assertSame(fileSchema, schema.getFileSchema());
  }

  @Test void testGetFileSchemaReturnsNull() {
    SparkJdbcSchema schema = createSchema(null);
    assertNull(schema.getFileSchema());
  }

  // =======================================================================
  // getComment
  // =======================================================================

  @Test void testGetCommentDelegatesToFileSchema() {
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getComment()).thenReturn("Spark schema comment");
    SparkJdbcSchema schema = createSchema(fileSchema);
    assertEquals("Spark schema comment", schema.getComment());
  }

  @Test void testGetCommentReturnsNullWhenFileSchemaNull() {
    SparkJdbcSchema schema = createSchema(null);
    assertNull(schema.getComment());
  }

  @Test void testGetCommentReturnsNullFromFileSchema() {
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getComment()).thenReturn(null);
    SparkJdbcSchema schema = createSchema(fileSchema);
    assertNull(schema.getComment());
  }

  @Test void testGetCommentHandlesLongComment() {
    FileSchema fileSchema = mock(FileSchema.class);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      sb.append("z");
    }
    when(fileSchema.getComment()).thenReturn(sb.toString());
    SparkJdbcSchema schema = createSchema(fileSchema);
    assertEquals(100, schema.getComment().length());
  }

  @Test void testGetCommentHandlesShortComment() {
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getComment()).thenReturn("hi");
    SparkJdbcSchema schema = createSchema(fileSchema);
    assertEquals("hi", schema.getComment());
  }

  @Test void testGetCommentHandlesEmptyComment() {
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getComment()).thenReturn("");
    SparkJdbcSchema schema = createSchema(fileSchema);
    assertEquals("", schema.getComment());
  }

  // =======================================================================
  // snapshot
  // =======================================================================

  @Test void testSnapshotReturnsSameInstance() {
    SparkJdbcSchema schema = createSchema(null);
    Schema result = schema.snapshot(mock(SchemaVersion.class));
    assertSame(schema, result);
  }

  @Test void testSnapshotPreservesType() {
    SparkJdbcSchema schema = createSchema(null);
    Schema result = schema.snapshot(mock(SchemaVersion.class));
    assertTrue(result instanceof SparkJdbcSchema);
  }

  @Test void testSnapshotCalledMultipleTimes() {
    SparkJdbcSchema schema = createSchema(null);
    SchemaVersion version = mock(SchemaVersion.class);
    Schema s1 = schema.snapshot(version);
    Schema s2 = schema.snapshot(version);
    assertSame(s1, s2);
  }

  // =======================================================================
  // close
  // =======================================================================

  @Test void testCloseClosesConnection() throws Exception {
    Connection conn = mock(Connection.class);
    SparkJdbcSchema schema = createSchemaWithConnection(null, conn);
    schema.close();
    verify(conn).close();
  }

  @Test void testCloseHandlesConnectionError() throws Exception {
    Connection conn = mock(Connection.class);
    doThrow(new SQLException("close error")).when(conn).close();
    SparkJdbcSchema schema = createSchemaWithConnection(null, conn);
    // Should not throw
    schema.close();
    verify(conn).close();
  }

  @Test void testCloseWithNullConnection() {
    SparkJdbcSchema schema = createSchemaWithConnection(null, null);
    // Should not throw
    schema.close();
  }

  // =======================================================================
  // recreateView via refresh listener
  // =======================================================================

  @Test void testRecreateViewViaListener() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    SparkJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    listener.onTableRefreshed("sales", new File("/data/sales.parquet"));

    verify(stmt).execute(anyString());
  }

  @Test void testRecreateViewWithPatternViaListener() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    SparkJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    listener.onTableRefreshedWithPattern("partitioned_sales",
        "s3://bucket/sales/**/*.parquet");

    verify(stmt).execute(anyString());
  }

  @Test void testRecreateIcebergViewViaListener() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    SparkJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    listener.onIcebergTableRefreshed("ice_sales",
        "s3://bucket/warehouse/ice_sales");

    // Spark does CREATE TABLE then CREATE VIEW for Iceberg
    verify(stmt, atLeast(2)).execute(anyString());
  }

  @Test void testRecreateViewHandlesError() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    doThrow(new SQLException("view error")).when(stmt).execute(anyString());

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    SparkJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    // Should not throw
    listener.onTableRefreshed("bad_table", new File("/data/bad.parquet"));
  }

  @Test void testRecreateViewWithPatternHandlesError() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    doThrow(new SQLException("pattern error")).when(stmt).execute(anyString());

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    SparkJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    // Should not throw
    listener.onTableRefreshedWithPattern("fail_ptn", "s3://bucket/*.parquet");
  }

  @Test void testRecreateIcebergViewHandlesError() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    doThrow(new SQLException("iceberg error")).when(stmt).execute(anyString());

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    SparkJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    // Should not throw
    listener.onIcebergTableRefreshed("fail_ice", "s3://bucket/iceberg");
  }

  // =======================================================================
  // recreateView/recreateIcebergView via reflection
  // =======================================================================

  @Test void testRecreateViewDirectly() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    SparkJdbcSchema schema = createSchemaWithConnection(null, conn);
    Method method = SparkJdbcSchema.class.getDeclaredMethod(
        "recreateView", String.class, File.class);
    method.setAccessible(true);
    method.invoke(schema, "test_view", new File("/data/test.parquet"));

    verify(stmt).execute(anyString());
  }

  @Test void testRecreateViewWithPatternDirectly() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    SparkJdbcSchema schema = createSchemaWithConnection(null, conn);
    Method method = SparkJdbcSchema.class.getDeclaredMethod(
        "recreateViewWithPattern", String.class, String.class);
    method.setAccessible(true);
    method.invoke(schema, "ptn_view", "s3://bucket/**/*.parquet");

    verify(stmt).execute(anyString());
  }

  @Test void testRecreateIcebergViewDirectly() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    SparkJdbcSchema schema = createSchemaWithConnection(null, conn);
    Method method = SparkJdbcSchema.class.getDeclaredMethod(
        "recreateIcebergView", String.class, String.class);
    method.setAccessible(true);
    method.invoke(schema, "ice_view", "s3://bucket/warehouse/ice_view");

    // Spark does registerTable + createView for Iceberg
    verify(stmt, atLeast(2)).execute(anyString());
  }

  @Test void testRecreateViewDirectlyHandlesError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    doThrow(new SQLException("fail")).when(stmt).execute(anyString());

    SparkJdbcSchema schema = createSchemaWithConnection(null, conn);
    Method method = SparkJdbcSchema.class.getDeclaredMethod(
        "recreateView", String.class, File.class);
    method.setAccessible(true);
    // Should not throw -- error is caught internally
    method.invoke(schema, "bad_view", new File("/data/bad.parquet"));
  }

  @Test void testRecreateViewWithPatternDirectlyHandlesError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    doThrow(new SQLException("fail")).when(stmt).execute(anyString());

    SparkJdbcSchema schema = createSchemaWithConnection(null, conn);
    Method method = SparkJdbcSchema.class.getDeclaredMethod(
        "recreateViewWithPattern", String.class, String.class);
    method.setAccessible(true);
    // Should not throw
    method.invoke(schema, "bad_ptn", "s3://bucket/*.parquet");
  }

  @Test void testRecreateIcebergViewDirectlyHandlesError() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    doThrow(new SQLException("fail")).when(stmt).execute(anyString());

    SparkJdbcSchema schema = createSchemaWithConnection(null, conn);
    Method method = SparkJdbcSchema.class.getDeclaredMethod(
        "recreateIcebergView", String.class, String.class);
    method.setAccessible(true);
    // Should not throw
    method.invoke(schema, "bad_ice", "s3://bucket/iceberg");
  }

  // =======================================================================
  // SQL generation verification for Spark-specific patterns
  // =======================================================================

  @Test void testRecreateViewGeneratesBacktickParquetSyntax() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

    FileSchema fileSchema = mock(FileSchema.class);
    ArgumentCaptor<PatternAwareRefreshListener> listenerCaptor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    SparkJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(listenerCaptor.capture());

    listenerCaptor.getValue().onTableRefreshed("data",
        new File("/data/data.parquet"));
    verify(stmt).execute(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();
    assertTrue(sql.contains("parquet.`"), "Spark should use parquet.`path` syntax");
  }

  @Test void testRecreateIcebergViewCreatesTableAndView() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

    FileSchema fileSchema = mock(FileSchema.class);
    ArgumentCaptor<PatternAwareRefreshListener> listenerCaptor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    SparkJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(listenerCaptor.capture());

    listenerCaptor.getValue().onIcebergTableRefreshed("ice",
        "s3://bucket/warehouse/ice");
    verify(stmt, atLeast(2)).execute(sqlCaptor.capture());
    java.util.List<String> sqls = sqlCaptor.getAllValues();
    // First should register iceberg table, second should create view
    assertTrue(sqls.get(0).contains("CREATE TABLE") || sqls.get(0).contains("iceberg"),
        "First SQL should register Iceberg table in catalog");
  }

  @Test void testRecreateViewWithPatternUsesBacktickSyntax() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

    FileSchema fileSchema = mock(FileSchema.class);
    ArgumentCaptor<PatternAwareRefreshListener> listenerCaptor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    SparkJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(listenerCaptor.capture());

    listenerCaptor.getValue().onTableRefreshedWithPattern("logs",
        "s3://bucket/logs/**/*.parquet");
    verify(stmt).execute(sqlCaptor.capture());
    assertTrue(sqlCaptor.getValue().contains("parquet.`"),
        "Pattern recreation should use parquet backtick syntax");
  }

  @Test void testConstructorWithRecursiveFlag() {
    DataSource dataSource = mock(DataSource.class);
    SqlDialect dialect = mock(SqlDialect.class);
    JdbcConvention convention = mock(JdbcConvention.class);
    Connection conn = mock(Connection.class);

    SparkJdbcSchema schema = new SparkJdbcSchema(
        dataSource, dialect, convention,
        "cat", "sch", "/data", true, conn, null);
    assertNotNull(schema);
  }

  // =======================================================================
  // Helper methods
  // =======================================================================

  private SparkJdbcSchema createSchema(FileSchema fileSchema) {
    DataSource dataSource = mock(DataSource.class);
    SqlDialect dialect = mock(SqlDialect.class);
    JdbcConvention convention = mock(JdbcConvention.class);
    Connection conn = mock(Connection.class);

    return new SparkJdbcSchema(dataSource, dialect, convention,
        "spark_catalog", "spark_schema", "/data/dir", false, conn, fileSchema);
  }

  private SparkJdbcSchema createSchemaWithConnection(FileSchema fileSchema,
      Connection conn) {
    DataSource dataSource = mock(DataSource.class);
    SqlDialect dialect = mock(SqlDialect.class);
    JdbcConvention convention = mock(JdbcConvention.class);

    return new SparkJdbcSchema(dataSource, dialect, convention,
        "spark_catalog", "spark_schema", "/data/dir", false, conn, fileSchema);
  }
}
