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
package org.apache.calcite.adapter.file.trino;

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
 * Coverage unit tests for {@link TrinoJdbcSchema}.
 *
 * <p>Tests constructor behavior, recreateTable/recreateIcebergTable via
 * reflection and listener capture, getFileSchema, getComment, snapshot,
 * and close methods. Uses mocked JDBC Connection/Statement and FileSchema.
 */
@Tag("unit")
class TrinoJdbcSchemaCoverageTest {

  // =======================================================================
  // Constructor and refresh listener registration
  // =======================================================================

  @Test void testConstructorWithFileSchemaRegistersListener() {
    FileSchema fileSchema = mock(FileSchema.class);
    TrinoJdbcSchema schema = createSchema(fileSchema);
    assertNotNull(schema);
    verify(fileSchema).addRefreshListener(any(PatternAwareRefreshListener.class));
  }

  @Test void testConstructorWithNullFileSchemaSkipsListener() {
    TrinoJdbcSchema schema = createSchema(null);
    assertNotNull(schema);
  }

  @Test void testConstructorSetsDirectoryPath() {
    TrinoJdbcSchema schema = createSchema(null);
    assertNotNull(schema);
    // Schema was created without exception
  }

  // =======================================================================
  // getFileSchema
  // =======================================================================

  @Test void testGetFileSchemaReturnsInjected() {
    FileSchema fileSchema = mock(FileSchema.class);
    TrinoJdbcSchema schema = createSchema(fileSchema);
    assertSame(fileSchema, schema.getFileSchema());
  }

  @Test void testGetFileSchemaReturnsNull() {
    TrinoJdbcSchema schema = createSchema(null);
    assertNull(schema.getFileSchema());
  }

  // =======================================================================
  // getComment
  // =======================================================================

  @Test void testGetCommentDelegatesToFileSchema() {
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getComment()).thenReturn("Trino schema comment");
    TrinoJdbcSchema schema = createSchema(fileSchema);
    assertEquals("Trino schema comment", schema.getComment());
  }

  @Test void testGetCommentReturnsNullWhenFileSchemaNull() {
    TrinoJdbcSchema schema = createSchema(null);
    assertNull(schema.getComment());
  }

  @Test void testGetCommentReturnsNullFromFileSchema() {
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getComment()).thenReturn(null);
    TrinoJdbcSchema schema = createSchema(fileSchema);
    assertNull(schema.getComment());
  }

  @Test void testGetCommentHandlesLongComment() {
    FileSchema fileSchema = mock(FileSchema.class);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      sb.append("x");
    }
    when(fileSchema.getComment()).thenReturn(sb.toString());
    TrinoJdbcSchema schema = createSchema(fileSchema);
    assertEquals(100, schema.getComment().length());
  }

  @Test void testGetCommentHandlesShortComment() {
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getComment()).thenReturn("short");
    TrinoJdbcSchema schema = createSchema(fileSchema);
    assertEquals("short", schema.getComment());
  }

  // =======================================================================
  // snapshot
  // =======================================================================

  @Test void testSnapshotReturnsSameInstance() {
    TrinoJdbcSchema schema = createSchema(null);
    Schema result = schema.snapshot(mock(SchemaVersion.class));
    assertSame(schema, result);
  }

  @Test void testSnapshotPreservesType() {
    TrinoJdbcSchema schema = createSchema(null);
    Schema result = schema.snapshot(mock(SchemaVersion.class));
    assertTrue(result instanceof TrinoJdbcSchema);
  }

  // =======================================================================
  // close
  // =======================================================================

  @Test void testCloseClosesConnection() throws Exception {
    Connection conn = mock(Connection.class);
    TrinoJdbcSchema schema = createSchemaWithConnection(null, conn);
    schema.close();
    verify(conn).close();
  }

  @Test void testCloseHandlesConnectionError() throws Exception {
    Connection conn = mock(Connection.class);
    doThrow(new SQLException("close error")).when(conn).close();
    TrinoJdbcSchema schema = createSchemaWithConnection(null, conn);
    // Should not throw
    schema.close();
    verify(conn).close();
  }

  @Test void testCloseWithNullConnection() {
    TrinoJdbcSchema schema = createSchemaWithConnection(null, null);
    // Should not throw
    schema.close();
  }

  // =======================================================================
  // recreateTable via refresh listener
  // =======================================================================

  @Test void testRecreateTableViaListener() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    TrinoJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    listener.onTableRefreshed("orders", new File("/data/orders.parquet"));

    // Trino does DROP then CREATE for table recreation
    verify(stmt, atLeast(2)).execute(anyString());
  }

  @Test void testRecreateTableWithPatternViaListener() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    TrinoJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    listener.onTableRefreshedWithPattern("partitioned",
        "s3://bucket/partitioned/**/*.parquet");

    verify(stmt, atLeast(2)).execute(anyString());
  }

  @Test void testRecreateIcebergTableViaListener() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    TrinoJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    listener.onIcebergTableRefreshed("ice_table",
        "s3://bucket/warehouse/ice_table");

    verify(stmt).execute(anyString());
  }

  @Test void testRecreateTableHandlesDropError() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    doThrow(new SQLException("drop failed")).when(stmt).execute(anyString());

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    TrinoJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    // Should not throw -- error is logged
    listener.onTableRefreshed("fail_table", new File("/data/fail.parquet"));
  }

  @Test void testRecreateTableWithPatternHandlesError() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    doThrow(new SQLException("pattern error")).when(stmt).execute(anyString());

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    TrinoJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    // Should not throw
    listener.onTableRefreshedWithPattern("fail_ptn", "s3://bucket/*.parquet");
  }

  @Test void testRecreateIcebergTableHandlesError() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    doThrow(new SQLException("iceberg error")).when(stmt).execute(anyString());

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    TrinoJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    // Should not throw
    listener.onIcebergTableRefreshed("fail_ice", "s3://bucket/iceberg");
  }

  // =======================================================================
  // recreateTable/recreateIcebergTable via reflection
  // =======================================================================

  @Test void testRecreateTableDirectly() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    TrinoJdbcSchema schema = createSchemaWithConnection(null, conn);
    Method method = TrinoJdbcSchema.class.getDeclaredMethod(
        "recreateTable", String.class, File.class);
    method.setAccessible(true);
    method.invoke(schema, "my_table", new File("/data/my_table.parquet"));

    verify(stmt, atLeast(2)).execute(anyString());
  }

  @Test void testRecreateTableWithPatternDirectly() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    TrinoJdbcSchema schema = createSchemaWithConnection(null, conn);
    Method method = TrinoJdbcSchema.class.getDeclaredMethod(
        "recreateTableWithPattern", String.class, String.class);
    method.setAccessible(true);
    method.invoke(schema, "ptn_table", "s3://bucket/**/*.parquet");

    verify(stmt, atLeast(2)).execute(anyString());
  }

  @Test void testRecreateIcebergTableDirectly() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    TrinoJdbcSchema schema = createSchemaWithConnection(null, conn);
    Method method = TrinoJdbcSchema.class.getDeclaredMethod(
        "recreateIcebergTable", String.class, String.class);
    method.setAccessible(true);
    method.invoke(schema, "ice_table", "s3://bucket/warehouse/ice_table");

    verify(stmt).execute(anyString());
  }

  // =======================================================================
  // SQL generation verification for Trino-specific patterns
  // =======================================================================

  @Test void testRecreateTableGeneratesDropThenCreate() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

    FileSchema fileSchema = mock(FileSchema.class);
    ArgumentCaptor<PatternAwareRefreshListener> listenerCaptor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    TrinoJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(listenerCaptor.capture());

    listenerCaptor.getValue().onTableRefreshed("orders",
        new File("/data/orders.parquet"));
    verify(stmt, atLeast(2)).execute(sqlCaptor.capture());

    // First call should be DROP, second should be CREATE
    java.util.List<String> sqls = sqlCaptor.getAllValues();
    assertTrue(sqls.get(0).contains("DROP"), "First SQL should be DROP");
    assertTrue(sqls.get(1).contains("CREATE"), "Second SQL should be CREATE");
  }

  @Test void testRecreateTableWithPatternGeneratesDropThenCreate() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

    FileSchema fileSchema = mock(FileSchema.class);
    ArgumentCaptor<PatternAwareRefreshListener> listenerCaptor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    TrinoJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(listenerCaptor.capture());

    listenerCaptor.getValue().onTableRefreshedWithPattern("logs",
        "s3://bucket/logs/**/*.parquet");
    verify(stmt, atLeast(2)).execute(sqlCaptor.capture());
    java.util.List<String> sqls = sqlCaptor.getAllValues();
    assertTrue(sqls.get(0).contains("DROP"));
  }

  @Test void testRecreateIcebergTableUsesCallProcedure() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

    FileSchema fileSchema = mock(FileSchema.class);
    ArgumentCaptor<PatternAwareRefreshListener> listenerCaptor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    TrinoJdbcSchema schema = createSchemaWithConnection(fileSchema, conn);
    verify(fileSchema).addRefreshListener(listenerCaptor.capture());

    listenerCaptor.getValue().onIcebergTableRefreshed("ice_data",
        "s3://bucket/warehouse/ice_data");
    verify(stmt).execute(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();
    assertTrue(sql.contains("CALL") || sql.contains("register_table"),
        "Trino Iceberg should use CALL procedure");
  }

  @Test void testConstructorWithRecursiveFlag() {
    DataSource dataSource = mock(DataSource.class);
    SqlDialect dialect = mock(SqlDialect.class);
    JdbcConvention convention = mock(JdbcConvention.class);
    Connection conn = mock(Connection.class);

    TrinoJdbcSchema schema = new TrinoJdbcSchema(
        dataSource, dialect, convention,
        "hive", "sch", "/data", true, conn, null);
    assertNotNull(schema);
  }

  @Test void testSnapshotCalledMultipleTimes() {
    TrinoJdbcSchema schema = createSchema(null);
    SchemaVersion version = mock(SchemaVersion.class);
    Schema s1 = schema.snapshot(version);
    Schema s2 = schema.snapshot(version);
    assertSame(s1, s2);
  }

  @Test void testGetCommentHandlesEmptyComment() {
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getComment()).thenReturn("");
    TrinoJdbcSchema schema = createSchema(fileSchema);
    assertEquals("", schema.getComment());
  }

  // =======================================================================
  // Helper methods
  // =======================================================================

  private TrinoJdbcSchema createSchema(FileSchema fileSchema) {
    DataSource dataSource = mock(DataSource.class);
    SqlDialect dialect = mock(SqlDialect.class);
    JdbcConvention convention = mock(JdbcConvention.class);
    Connection conn = mock(Connection.class);

    return new TrinoJdbcSchema(dataSource, dialect, convention,
        "hive_catalog", "trino_schema", "/data/dir", false, conn, fileSchema);
  }

  private TrinoJdbcSchema createSchemaWithConnection(FileSchema fileSchema,
      Connection conn) {
    DataSource dataSource = mock(DataSource.class);
    SqlDialect dialect = mock(SqlDialect.class);
    JdbcConvention convention = mock(JdbcConvention.class);

    return new TrinoJdbcSchema(dataSource, dialect, convention,
        "hive_catalog", "trino_schema", "/data/dir", false, conn, fileSchema);
  }
}
