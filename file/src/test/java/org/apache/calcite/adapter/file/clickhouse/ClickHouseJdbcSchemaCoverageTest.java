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
package org.apache.calcite.adapter.file.clickhouse;

import org.apache.calcite.adapter.file.FileSchema;
import org.apache.calcite.adapter.file.refresh.PatternAwareRefreshListener;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.sql.SqlDialect;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Coverage unit tests for {@link ClickHouseJdbcSchema}.
 *
 * <p>Tests constructor behavior, recreateView/recreateIcebergView via reflection,
 * getFileSchema, getComment, getTable, snapshot, and close methods.
 * Uses mocked JDBC Connection/Statement and FileSchema.
 */
@Tag("unit")
class ClickHouseJdbcSchemaCoverageTest {

  // =======================================================================
  // Constructor and refresh listener registration
  // =======================================================================

  @Test void testConstructorWithFileSchemaRegistersListener() {
    FileSchema fileSchema = mock(FileSchema.class);
    ClickHouseJdbcSchema schema = createSchema(fileSchema, null);
    assertNotNull(schema);
    verify(fileSchema).addRefreshListener(any(PatternAwareRefreshListener.class));
  }

  @Test void testConstructorWithNullFileSchemaSkipsListener() {
    ClickHouseJdbcSchema schema = createSchema(null, null);
    assertNotNull(schema);
    // No fileSchema, so no listener registration -- just verify no exceptions
  }

  @Test void testConstructorWithLocalProcess() {
    FileSchema fileSchema = mock(FileSchema.class);
    Process localProcess = mock(Process.class);
    ClickHouseJdbcSchema schema = createSchema(fileSchema, localProcess);
    assertNotNull(schema);
  }

  // =======================================================================
  // getFileSchema
  // =======================================================================

  @Test void testGetFileSchemaReturnsInjectedSchema() {
    FileSchema fileSchema = mock(FileSchema.class);
    ClickHouseJdbcSchema schema = createSchema(fileSchema, null);
    assertSame(fileSchema, schema.getFileSchema());
  }

  @Test void testGetFileSchemaReturnsNullWhenNotSet() {
    ClickHouseJdbcSchema schema = createSchema(null, null);
    assertNull(schema.getFileSchema());
  }

  // =======================================================================
  // getComment
  // =======================================================================

  @Test void testGetCommentDelegatesToFileSchema() {
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getComment()).thenReturn("Schema comment");
    ClickHouseJdbcSchema schema = createSchema(fileSchema, null);
    assertEquals("Schema comment", schema.getComment());
  }

  @Test void testGetCommentReturnsNullWhenFileSchemaNull() {
    ClickHouseJdbcSchema schema = createSchema(null, null);
    assertNull(schema.getComment());
  }

  @Test void testGetCommentReturnsNullFromFileSchema() {
    FileSchema fileSchema = mock(FileSchema.class);
    when(fileSchema.getComment()).thenReturn(null);
    ClickHouseJdbcSchema schema = createSchema(fileSchema, null);
    assertNull(schema.getComment());
  }

  @Test void testGetCommentHandlesLongComment() {
    FileSchema fileSchema = mock(FileSchema.class);
    StringBuilder longComment = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      longComment.append("a");
    }
    when(fileSchema.getComment()).thenReturn(longComment.toString());
    ClickHouseJdbcSchema schema = createSchema(fileSchema, null);
    assertEquals(longComment.toString(), schema.getComment());
  }

  // =======================================================================
  // snapshot
  // =======================================================================

  @Test void testSnapshotReturnsSameInstance() {
    ClickHouseJdbcSchema schema = createSchema(null, null);
    Schema result = schema.snapshot(mock(SchemaVersion.class));
    assertSame(schema, result);
  }

  @Test void testSnapshotPreservesType() {
    ClickHouseJdbcSchema schema = createSchema(null, null);
    Schema result = schema.snapshot(mock(SchemaVersion.class));
    assertTrue(result instanceof ClickHouseJdbcSchema);
  }

  // =======================================================================
  // close
  // =======================================================================

  @Test void testCloseWithLocalProcessDestroysProcess() throws Exception {
    Process localProcess = mock(Process.class);
    when(localProcess.waitFor(5, TimeUnit.SECONDS)).thenReturn(true);
    Connection conn = mock(Connection.class);

    ClickHouseJdbcSchema schema = createSchemaWithConnection(null, localProcess, conn);
    schema.close();

    verify(localProcess).destroy();
  }

  @Test void testCloseWithLocalProcessForceDestroyOnTimeout() throws Exception {
    Process localProcess = mock(Process.class);
    when(localProcess.waitFor(5, TimeUnit.SECONDS)).thenReturn(false);
    Connection conn = mock(Connection.class);

    ClickHouseJdbcSchema schema = createSchemaWithConnection(null, localProcess, conn);
    schema.close();

    verify(localProcess).destroy();
    verify(localProcess).destroyForcibly();
  }

  @Test void testCloseWithLocalProcessInterrupted() throws Exception {
    Process localProcess = mock(Process.class);
    when(localProcess.waitFor(5, TimeUnit.SECONDS))
        .thenThrow(new InterruptedException("test"));
    Connection conn = mock(Connection.class);

    ClickHouseJdbcSchema schema = createSchemaWithConnection(null, localProcess, conn);
    schema.close();

    verify(localProcess).destroyForcibly();
    // Thread interrupt flag should be set
    assertTrue(Thread.interrupted());
  }

  @Test void testCloseWithNullLocalProcess() throws Exception {
    Connection conn = mock(Connection.class);
    ClickHouseJdbcSchema schema = createSchemaWithConnection(null, null, conn);
    schema.close();
    verify(conn).close();
  }

  @Test void testCloseWithPersistentConnectionError() throws Exception {
    Connection conn = mock(Connection.class);
    doThrow(new SQLException("close failed")).when(conn).close();

    ClickHouseJdbcSchema schema = createSchemaWithConnection(null, null, conn);
    // Should not throw -- error is logged
    schema.close();
    verify(conn).close();
  }

  @Test void testCloseWithNullConnection() {
    ClickHouseJdbcSchema schema = createSchemaWithConnection(null, null, null);
    // Should not throw with null connection
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
    ClickHouseJdbcSchema schema = createSchemaWithConnection(fileSchema, null, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    File parquetFile = new File("/data/output.parquet");
    listener.onTableRefreshed("my_table", parquetFile);

    verify(stmt).execute(anyString());
  }

  @Test void testRecreateViewWithPatternViaListener() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    ClickHouseJdbcSchema schema = createSchemaWithConnection(fileSchema, null, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    listener.onTableRefreshedWithPattern("partitioned_table",
        "s3://bucket/data/**/*.parquet");

    verify(stmt).execute(anyString());
  }

  @Test void testRecreateIcebergViewViaListener() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    ClickHouseJdbcSchema schema = createSchemaWithConnection(fileSchema, null, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    listener.onIcebergTableRefreshed("iceberg_table",
        "s3://bucket/warehouse/iceberg_table");

    verify(stmt).execute(anyString());
  }

  @Test void testRecreateViewHandlesSQLException() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    doThrow(new SQLException("execution error")).when(stmt).execute(anyString());

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    ClickHouseJdbcSchema schema = createSchemaWithConnection(fileSchema, null, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    // Should not throw -- error is logged
    listener.onTableRefreshed("bad_table", new File("/data/output.parquet"));
  }

  @Test void testRecreateViewWithPatternHandlesError() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    doThrow(new SQLException("pattern error")).when(stmt).execute(anyString());

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    ClickHouseJdbcSchema schema = createSchemaWithConnection(fileSchema, null, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    // Should not throw
    listener.onTableRefreshedWithPattern("fail_table", "s3://bucket/*.parquet");
  }

  @Test void testRecreateIcebergViewHandlesError() throws Exception {
    FileSchema fileSchema = mock(FileSchema.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);
    doThrow(new SQLException("iceberg error")).when(stmt).execute(anyString());

    ArgumentCaptor<PatternAwareRefreshListener> captor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    ClickHouseJdbcSchema schema = createSchemaWithConnection(fileSchema, null, conn);
    verify(fileSchema).addRefreshListener(captor.capture());

    PatternAwareRefreshListener listener = captor.getValue();
    // Should not throw
    listener.onIcebergTableRefreshed("fail_iceberg", "s3://bucket/iceberg");
  }

  // =======================================================================
  // recreateView/recreateIcebergView via reflection (direct method testing)
  // =======================================================================

  @Test void testRecreateViewDirectly() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ClickHouseJdbcSchema schema = createSchemaWithConnection(null, null, conn);
    Method method =
        ClickHouseJdbcSchema.class.getDeclaredMethod("recreateView", String.class, File.class);
    method.setAccessible(true);
    method.invoke(schema, "test_table", new File("/data/test.parquet"));

    verify(stmt).execute(anyString());
  }

  @Test void testRecreateViewWithPatternDirectly() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ClickHouseJdbcSchema schema = createSchemaWithConnection(null, null, conn);
    Method method =
        ClickHouseJdbcSchema.class.getDeclaredMethod("recreateViewWithPattern", String.class, String.class);
    method.setAccessible(true);
    method.invoke(schema, "ptn_table", "s3://bucket/**/*.parquet");

    verify(stmt).execute(anyString());
  }

  @Test void testRecreateIcebergViewDirectly() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ClickHouseJdbcSchema schema = createSchemaWithConnection(null, null, conn);
    Method method =
        ClickHouseJdbcSchema.class.getDeclaredMethod("recreateIcebergView", String.class, String.class);
    method.setAccessible(true);
    method.invoke(schema, "ice_table", "s3://bucket/warehouse/ice_table");

    verify(stmt).execute(anyString());
  }

  // =======================================================================
  // SQL generation for ClickHouseDialect-based view creation
  // =======================================================================

  @Test void testRecreateViewGeneratesCorrectSqlForS3Path() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

    FileSchema fileSchema = mock(FileSchema.class);
    ArgumentCaptor<PatternAwareRefreshListener> listenerCaptor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    ClickHouseJdbcSchema schema = createSchemaWithConnection(fileSchema, null, conn);
    verify(fileSchema).addRefreshListener(listenerCaptor.capture());

    listenerCaptor.getValue().onTableRefreshed("sales",
        new File("/data/sales.parquet"));
    verify(stmt).execute(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();
    assertTrue(sql.contains("sales"), "SQL should reference table name");
    assertTrue(sql.contains("VIEW") || sql.contains("view"),
        "SQL should create a VIEW");
  }

  @Test void testRecreateIcebergViewGeneratesIcebergSql() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

    FileSchema fileSchema = mock(FileSchema.class);
    ArgumentCaptor<PatternAwareRefreshListener> listenerCaptor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    ClickHouseJdbcSchema schema = createSchemaWithConnection(fileSchema, null, conn);
    verify(fileSchema).addRefreshListener(listenerCaptor.capture());

    listenerCaptor.getValue().onIcebergTableRefreshed("ice",
        "s3://bucket/warehouse/ice");
    verify(stmt).execute(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();
    assertTrue(sql.contains("iceberg"), "SQL should reference iceberg function");
  }

  @Test void testRecreateViewWithPatternContainsGlob() throws Exception {
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    when(conn.createStatement()).thenReturn(stmt);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

    FileSchema fileSchema = mock(FileSchema.class);
    ArgumentCaptor<PatternAwareRefreshListener> listenerCaptor =
        ArgumentCaptor.forClass(PatternAwareRefreshListener.class);
    ClickHouseJdbcSchema schema = createSchemaWithConnection(fileSchema, null, conn);
    verify(fileSchema).addRefreshListener(listenerCaptor.capture());

    listenerCaptor.getValue().onTableRefreshedWithPattern("logs",
        "s3://bucket/logs/**/*.parquet");
    verify(stmt).execute(sqlCaptor.capture());
    String sql = sqlCaptor.getValue();
    assertTrue(sql.contains("s3://bucket/logs/**/*.parquet"),
        "SQL should include the glob pattern");
  }

  @Test void testConstructorWithRecursiveFlag() {
    DataSource dataSource = mock(DataSource.class);
    SqlDialect dialect = mock(SqlDialect.class);
    JdbcConvention convention = mock(JdbcConvention.class);
    Connection conn = mock(Connection.class);

    ClickHouseJdbcSchema schema =
        new ClickHouseJdbcSchema(dataSource, dialect, convention,
        "cat", "sch", "/data", true, conn, null, null);
    assertNotNull(schema);
  }

  // =======================================================================
  // Helper methods
  // =======================================================================

  private ClickHouseJdbcSchema createSchema(FileSchema fileSchema, Process localProcess) {
    DataSource dataSource = mock(DataSource.class);
    SqlDialect dialect = mock(SqlDialect.class);
    JdbcConvention convention = mock(JdbcConvention.class);
    Connection conn = mock(Connection.class);

    return new ClickHouseJdbcSchema(dataSource, dialect, convention,
        "test_catalog", "test_schema", "/data/dir", false, conn,
        fileSchema, localProcess);
  }

  private ClickHouseJdbcSchema createSchemaWithConnection(FileSchema fileSchema,
      Process localProcess, Connection conn) {
    DataSource dataSource = mock(DataSource.class);
    SqlDialect dialect = mock(SqlDialect.class);
    JdbcConvention convention = mock(JdbcConvention.class);

    return new ClickHouseJdbcSchema(dataSource, dialect, convention,
        "test_catalog", "test_schema", "/data/dir", false, conn,
        fileSchema, localProcess);
  }
}
