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
package org.apache.calcite.adapter.file;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Coverage unit tests for {@link FilePreparedStatementWrapper}
 * and {@link FileConnectionWrapper}.
 *
 * <p>Tests delegation of all methods using mocked underlying
 * PreparedStatement and Connection objects.
 */
@Tag("unit")
class FileWrappersCoverageTest {

  // ============================================================
  // FilePreparedStatementWrapper tests
  // ============================================================

  private PreparedStatement mockPrepStmt;
  private FilePreparedStatementWrapper prepWrapper;

  @BeforeEach
  void setUp() throws SQLException {
    mockPrepStmt = mock(PreparedStatement.class);
    prepWrapper = new FilePreparedStatementWrapper(mockPrepStmt);
  }

  @Test void testPreparedExecuteQueryWrapsResultSet() throws SQLException {
    ResultSet mockRs = mock(ResultSet.class);
    when(mockPrepStmt.executeQuery()).thenReturn(mockRs);

    ResultSet result = prepWrapper.executeQuery();
    assertNotNull(result);
    assertTrue(result instanceof FileResultSetWrapperImpl,
        "executeQuery should return FileResultSetWrapperImpl");
  }

  @Test void testPreparedExecuteUpdate() throws SQLException {
    when(mockPrepStmt.executeUpdate()).thenReturn(5);
    assertEquals(5, prepWrapper.executeUpdate());
    verify(mockPrepStmt).executeUpdate();
  }

  @Test void testPreparedSetNull() throws SQLException {
    prepWrapper.setNull(1, java.sql.Types.VARCHAR);
    verify(mockPrepStmt).setNull(1, java.sql.Types.VARCHAR);
  }

  @Test void testPreparedSetBoolean() throws SQLException {
    prepWrapper.setBoolean(1, true);
    verify(mockPrepStmt).setBoolean(1, true);
  }

  @Test void testPreparedSetByte() throws SQLException {
    prepWrapper.setByte(1, (byte) 42);
    verify(mockPrepStmt).setByte(1, (byte) 42);
  }

  @Test void testPreparedSetShort() throws SQLException {
    prepWrapper.setShort(1, (short) 100);
    verify(mockPrepStmt).setShort(1, (short) 100);
  }

  @Test void testPreparedSetInt() throws SQLException {
    prepWrapper.setInt(1, 42);
    verify(mockPrepStmt).setInt(1, 42);
  }

  @Test void testPreparedSetLong() throws SQLException {
    prepWrapper.setLong(1, 999L);
    verify(mockPrepStmt).setLong(1, 999L);
  }

  @Test void testPreparedSetFloat() throws SQLException {
    prepWrapper.setFloat(1, 3.14f);
    verify(mockPrepStmt).setFloat(1, 3.14f);
  }

  @Test void testPreparedSetDouble() throws SQLException {
    prepWrapper.setDouble(1, 2.718);
    verify(mockPrepStmt).setDouble(1, 2.718);
  }

  @Test void testPreparedSetBigDecimal() throws SQLException {
    BigDecimal bd = new BigDecimal("123.45");
    prepWrapper.setBigDecimal(1, bd);
    verify(mockPrepStmt).setBigDecimal(1, bd);
  }

  @Test void testPreparedSetString() throws SQLException {
    prepWrapper.setString(1, "hello");
    verify(mockPrepStmt).setString(1, "hello");
  }

  @Test void testPreparedSetBytes() throws SQLException {
    byte[] bytes = new byte[]{1, 2, 3};
    prepWrapper.setBytes(1, bytes);
    verify(mockPrepStmt).setBytes(1, bytes);
  }

  @Test void testPreparedSetDate() throws SQLException {
    Date date = new Date(System.currentTimeMillis());
    prepWrapper.setDate(1, date);
    verify(mockPrepStmt).setDate(1, date);
  }

  @Test void testPreparedSetTime() throws SQLException {
    Time time = new Time(System.currentTimeMillis());
    prepWrapper.setTime(1, time);
    verify(mockPrepStmt).setTime(1, time);
  }

  @Test void testPreparedSetTimestamp() throws SQLException {
    Timestamp ts = new Timestamp(System.currentTimeMillis());
    prepWrapper.setTimestamp(1, ts);
    verify(mockPrepStmt).setTimestamp(1, ts);
  }

  @Test void testPreparedSetAsciiStreamIntLength() throws SQLException {
    InputStream is = mock(InputStream.class);
    prepWrapper.setAsciiStream(1, is, 100);
    verify(mockPrepStmt).setAsciiStream(1, is, 100);
  }

  @SuppressWarnings("deprecation")
  @Test void testPreparedSetUnicodeStream() throws SQLException {
    InputStream is = mock(InputStream.class);
    prepWrapper.setUnicodeStream(1, is, 100);
    verify(mockPrepStmt).setUnicodeStream(1, is, 100);
  }

  @Test void testPreparedSetBinaryStreamIntLength() throws SQLException {
    InputStream is = mock(InputStream.class);
    prepWrapper.setBinaryStream(1, is, 100);
    verify(mockPrepStmt).setBinaryStream(1, is, 100);
  }

  @Test void testPreparedClearParameters() throws SQLException {
    prepWrapper.clearParameters();
    verify(mockPrepStmt).clearParameters();
  }

  @Test void testPreparedSetObjectWithType() throws SQLException {
    prepWrapper.setObject(1, "val", java.sql.Types.VARCHAR);
    verify(mockPrepStmt).setObject(1, "val", java.sql.Types.VARCHAR);
  }

  @Test void testPreparedSetObject() throws SQLException {
    prepWrapper.setObject(1, "val");
    verify(mockPrepStmt).setObject(1, "val");
  }

  @Test void testPreparedExecute() throws SQLException {
    when(mockPrepStmt.execute()).thenReturn(true);
    assertTrue(prepWrapper.execute());
    verify(mockPrepStmt).execute();
  }

  @Test void testPreparedAddBatch() throws SQLException {
    prepWrapper.addBatch();
    verify(mockPrepStmt).addBatch();
  }

  @Test void testPreparedSetCharacterStreamIntLength() throws SQLException {
    Reader reader = mock(Reader.class);
    prepWrapper.setCharacterStream(1, reader, 50);
    verify(mockPrepStmt).setCharacterStream(1, reader, 50);
  }

  @Test void testPreparedSetRef() throws SQLException {
    Ref ref = mock(Ref.class);
    prepWrapper.setRef(1, ref);
    verify(mockPrepStmt).setRef(1, ref);
  }

  @Test void testPreparedSetBlob() throws SQLException {
    Blob blob = mock(Blob.class);
    prepWrapper.setBlob(1, blob);
    verify(mockPrepStmt).setBlob(1, blob);
  }

  @Test void testPreparedSetClob() throws SQLException {
    Clob clob = mock(Clob.class);
    prepWrapper.setClob(1, clob);
    verify(mockPrepStmt).setClob(1, clob);
  }

  @Test void testPreparedSetArray() throws SQLException {
    Array array = mock(Array.class);
    prepWrapper.setArray(1, array);
    verify(mockPrepStmt).setArray(1, array);
  }

  @Test void testPreparedGetMetaData() throws SQLException {
    ResultSetMetaData rsmd = mock(ResultSetMetaData.class);
    when(mockPrepStmt.getMetaData()).thenReturn(rsmd);
    assertEquals(rsmd, prepWrapper.getMetaData());
  }

  @Test void testPreparedSetDateWithCalendar() throws SQLException {
    Date date = new Date(System.currentTimeMillis());
    Calendar cal = Calendar.getInstance();
    prepWrapper.setDate(1, date, cal);
    verify(mockPrepStmt).setDate(1, date, cal);
  }

  @Test void testPreparedSetTimeWithCalendar() throws SQLException {
    Time time = new Time(System.currentTimeMillis());
    Calendar cal = Calendar.getInstance();
    prepWrapper.setTime(1, time, cal);
    verify(mockPrepStmt).setTime(1, time, cal);
  }

  @Test void testPreparedSetTimestampWithCalendar() throws SQLException {
    Timestamp ts = new Timestamp(System.currentTimeMillis());
    Calendar cal = Calendar.getInstance();
    prepWrapper.setTimestamp(1, ts, cal);
    verify(mockPrepStmt).setTimestamp(1, ts, cal);
  }

  @Test void testPreparedSetNullWithTypeName() throws SQLException {
    prepWrapper.setNull(1, java.sql.Types.VARCHAR, "VARCHAR");
    verify(mockPrepStmt).setNull(1, java.sql.Types.VARCHAR, "VARCHAR");
  }

  @Test void testPreparedSetURL() throws Exception {
    URL url = new URI("http://example.com").toURL();
    prepWrapper.setURL(1, url);
    verify(mockPrepStmt).setURL(1, url);
  }

  @Test void testPreparedGetParameterMetaData() throws SQLException {
    ParameterMetaData pmd = mock(ParameterMetaData.class);
    when(mockPrepStmt.getParameterMetaData()).thenReturn(pmd);
    assertEquals(pmd, prepWrapper.getParameterMetaData());
  }

  @Test void testPreparedSetRowId() throws SQLException {
    RowId rowId = mock(RowId.class);
    prepWrapper.setRowId(1, rowId);
    verify(mockPrepStmt).setRowId(1, rowId);
  }

  @Test void testPreparedSetNString() throws SQLException {
    prepWrapper.setNString(1, "nvalue");
    verify(mockPrepStmt).setNString(1, "nvalue");
  }

  @Test void testPreparedSetNCharacterStream() throws SQLException {
    Reader reader = mock(Reader.class);
    prepWrapper.setNCharacterStream(1, reader, 100L);
    verify(mockPrepStmt).setNCharacterStream(1, reader, 100L);
  }

  @Test void testPreparedSetNClob() throws SQLException {
    NClob nclob = mock(NClob.class);
    prepWrapper.setNClob(1, nclob);
    verify(mockPrepStmt).setNClob(1, nclob);
  }

  @Test void testPreparedSetClobReaderLong() throws SQLException {
    Reader reader = mock(Reader.class);
    prepWrapper.setClob(1, reader, 200L);
    verify(mockPrepStmt).setClob(1, reader, 200L);
  }

  @Test void testPreparedSetBlobInputStreamLong() throws SQLException {
    InputStream is = mock(InputStream.class);
    prepWrapper.setBlob(1, is, 300L);
    verify(mockPrepStmt).setBlob(1, is, 300L);
  }

  @Test void testPreparedSetNClobReaderLong() throws SQLException {
    Reader reader = mock(Reader.class);
    prepWrapper.setNClob(1, reader, 400L);
    verify(mockPrepStmt).setNClob(1, reader, 400L);
  }

  @Test void testPreparedSetSQLXML() throws SQLException {
    SQLXML sqlxml = mock(SQLXML.class);
    prepWrapper.setSQLXML(1, sqlxml);
    verify(mockPrepStmt).setSQLXML(1, sqlxml);
  }

  @Test void testPreparedSetObjectWithTypeAndScale() throws SQLException {
    prepWrapper.setObject(1, "val", java.sql.Types.DECIMAL, 2);
    verify(mockPrepStmt).setObject(1, "val", java.sql.Types.DECIMAL, 2);
  }

  @Test void testPreparedSetAsciiStreamLong() throws SQLException {
    InputStream is = mock(InputStream.class);
    prepWrapper.setAsciiStream(1, is, 500L);
    verify(mockPrepStmt).setAsciiStream(1, is, 500L);
  }

  @Test void testPreparedSetBinaryStreamLong() throws SQLException {
    InputStream is = mock(InputStream.class);
    prepWrapper.setBinaryStream(1, is, 600L);
    verify(mockPrepStmt).setBinaryStream(1, is, 600L);
  }

  @Test void testPreparedSetCharacterStreamLong() throws SQLException {
    Reader reader = mock(Reader.class);
    prepWrapper.setCharacterStream(1, reader, 700L);
    verify(mockPrepStmt).setCharacterStream(1, reader, 700L);
  }

  @Test void testPreparedSetAsciiStreamNoLength() throws SQLException {
    InputStream is = mock(InputStream.class);
    prepWrapper.setAsciiStream(1, is);
    verify(mockPrepStmt).setAsciiStream(1, is);
  }

  @Test void testPreparedSetBinaryStreamNoLength() throws SQLException {
    InputStream is = mock(InputStream.class);
    prepWrapper.setBinaryStream(1, is);
    verify(mockPrepStmt).setBinaryStream(1, is);
  }

  @Test void testPreparedSetCharacterStreamNoLength() throws SQLException {
    Reader reader = mock(Reader.class);
    prepWrapper.setCharacterStream(1, reader);
    verify(mockPrepStmt).setCharacterStream(1, reader);
  }

  @Test void testPreparedSetNCharacterStreamNoLength() throws SQLException {
    Reader reader = mock(Reader.class);
    prepWrapper.setNCharacterStream(1, reader);
    verify(mockPrepStmt).setNCharacterStream(1, reader);
  }

  @Test void testPreparedSetClobReader() throws SQLException {
    Reader reader = mock(Reader.class);
    prepWrapper.setClob(1, reader);
    verify(mockPrepStmt).setClob(1, reader);
  }

  @Test void testPreparedSetBlobInputStream() throws SQLException {
    InputStream is = mock(InputStream.class);
    prepWrapper.setBlob(1, is);
    verify(mockPrepStmt).setBlob(1, is);
  }

  @Test void testPreparedSetNClobReader() throws SQLException {
    Reader reader = mock(Reader.class);
    prepWrapper.setNClob(1, reader);
    verify(mockPrepStmt).setNClob(1, reader);
  }

  // ============================================================
  // FileConnectionWrapper tests
  // ============================================================

  @Test void testConnectionCreateStatement() throws SQLException {
    Connection mockConn = mock(Connection.class);
    Statement mockStmt = mock(Statement.class);
    when(mockConn.createStatement()).thenReturn(mockStmt);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    Statement result = wrapper.createStatement();
    assertTrue(result instanceof FileStatementWrapper,
        "createStatement should return FileStatementWrapper");
  }

  @Test void testConnectionCreateStatementWithTypeAndConcurrency() throws SQLException {
    Connection mockConn = mock(Connection.class);
    Statement mockStmt = mock(Statement.class);
    when(mockConn.createStatement(anyInt(), anyInt())).thenReturn(mockStmt);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    Statement result = wrapper.createStatement(
        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    assertTrue(result instanceof FileStatementWrapper);
  }

  @Test void testConnectionCreateStatementWithHoldability() throws SQLException {
    Connection mockConn = mock(Connection.class);
    Statement mockStmt = mock(Statement.class);
    when(mockConn.createStatement(anyInt(), anyInt(), anyInt())).thenReturn(mockStmt);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    Statement result = wrapper.createStatement(
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
    assertTrue(result instanceof FileStatementWrapper);
  }

  @Test void testConnectionPrepareStatement() throws SQLException {
    Connection mockConn = mock(Connection.class);
    PreparedStatement mockPS = mock(PreparedStatement.class);
    when(mockConn.prepareStatement(anyString())).thenReturn(mockPS);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    PreparedStatement result = wrapper.prepareStatement("SELECT 1");
    assertTrue(result instanceof FilePreparedStatementWrapper);
  }

  @Test void testConnectionPrepareStatementWithTypeAndConcurrency() throws SQLException {
    Connection mockConn = mock(Connection.class);
    PreparedStatement mockPS = mock(PreparedStatement.class);
    when(mockConn.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(mockPS);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    PreparedStatement result = wrapper.prepareStatement("SELECT 1",
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    assertTrue(result instanceof FilePreparedStatementWrapper);
  }

  @Test void testConnectionPrepareStatementWithHoldability() throws SQLException {
    Connection mockConn = mock(Connection.class);
    PreparedStatement mockPS = mock(PreparedStatement.class);
    when(mockConn.prepareStatement(anyString(), anyInt(), anyInt(), anyInt())).thenReturn(mockPS);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    PreparedStatement result = wrapper.prepareStatement("SELECT 1",
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
    assertTrue(result instanceof FilePreparedStatementWrapper);
  }

  @Test void testConnectionPrepareStatementAutoGeneratedKeys() throws SQLException {
    Connection mockConn = mock(Connection.class);
    PreparedStatement mockPS = mock(PreparedStatement.class);
    when(mockConn.prepareStatement(anyString(), anyInt())).thenReturn(mockPS);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    PreparedStatement result = wrapper.prepareStatement("INSERT INTO t VALUES (1)",
        Statement.RETURN_GENERATED_KEYS);
    assertTrue(result instanceof FilePreparedStatementWrapper);
  }

  @Test void testConnectionPrepareStatementColumnIndexes() throws SQLException {
    Connection mockConn = mock(Connection.class);
    PreparedStatement mockPS = mock(PreparedStatement.class);
    when(mockConn.prepareStatement(anyString(), any(int[].class))).thenReturn(mockPS);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    PreparedStatement result = wrapper.prepareStatement("INSERT INTO t VALUES (1)",
        new int[]{1});
    assertTrue(result instanceof FilePreparedStatementWrapper);
  }

  @Test void testConnectionPrepareStatementColumnNames() throws SQLException {
    Connection mockConn = mock(Connection.class);
    PreparedStatement mockPS = mock(PreparedStatement.class);
    when(mockConn.prepareStatement(anyString(), any(String[].class))).thenReturn(mockPS);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    PreparedStatement result = wrapper.prepareStatement("INSERT INTO t VALUES (1)",
        new String[]{"id"});
    assertTrue(result instanceof FilePreparedStatementWrapper);
  }

  @Test void testConnectionPrepareCall() throws SQLException {
    Connection mockConn = mock(Connection.class);
    CallableStatement mockCS = mock(CallableStatement.class);
    when(mockConn.prepareCall(anyString())).thenReturn(mockCS);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    assertEquals(mockCS, wrapper.prepareCall("CALL proc()"));
  }

  @Test void testConnectionPrepareCallWithTypeAndConcurrency() throws SQLException {
    Connection mockConn = mock(Connection.class);
    CallableStatement mockCS = mock(CallableStatement.class);
    when(mockConn.prepareCall(anyString(), anyInt(), anyInt())).thenReturn(mockCS);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    assertEquals(mockCS, wrapper.prepareCall("CALL proc()",
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
  }

  @Test void testConnectionPrepareCallWithHoldability() throws SQLException {
    Connection mockConn = mock(Connection.class);
    CallableStatement mockCS = mock(CallableStatement.class);
    when(mockConn.prepareCall(anyString(), anyInt(), anyInt(), anyInt())).thenReturn(mockCS);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    assertEquals(mockCS, wrapper.prepareCall("CALL proc()",
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT));
  }

  @Test void testConnectionNativeSQL() throws SQLException {
    Connection mockConn = mock(Connection.class);
    when(mockConn.nativeSQL("SELECT 1")).thenReturn("SELECT 1");

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    assertEquals("SELECT 1", wrapper.nativeSQL("SELECT 1"));
  }

  @Test void testConnectionAutoCommit() throws SQLException {
    Connection mockConn = mock(Connection.class);
    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);

    wrapper.setAutoCommit(true);
    verify(mockConn).setAutoCommit(true);

    when(mockConn.getAutoCommit()).thenReturn(true);
    assertTrue(wrapper.getAutoCommit());
  }

  @Test void testConnectionCommitAndRollback() throws SQLException {
    Connection mockConn = mock(Connection.class);
    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);

    wrapper.commit();
    verify(mockConn).commit();

    wrapper.rollback();
    verify(mockConn).rollback();
  }

  @Test void testConnectionClose() throws SQLException {
    Connection mockConn = mock(Connection.class);
    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);

    wrapper.close();
    verify(mockConn).close();

    when(mockConn.isClosed()).thenReturn(true);
    assertTrue(wrapper.isClosed());
  }

  @Test void testConnectionGetMetaData() throws SQLException {
    Connection mockConn = mock(Connection.class);
    DatabaseMetaData dbmd = mock(DatabaseMetaData.class);
    when(mockConn.getMetaData()).thenReturn(dbmd);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    assertEquals(dbmd, wrapper.getMetaData());
  }

  @Test void testConnectionReadOnly() throws SQLException {
    Connection mockConn = mock(Connection.class);
    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);

    wrapper.setReadOnly(true);
    verify(mockConn).setReadOnly(true);

    when(mockConn.isReadOnly()).thenReturn(true);
    assertTrue(wrapper.isReadOnly());
  }

  @Test void testConnectionCatalog() throws SQLException {
    Connection mockConn = mock(Connection.class);
    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);

    wrapper.setCatalog("mycat");
    verify(mockConn).setCatalog("mycat");

    when(mockConn.getCatalog()).thenReturn("mycat");
    assertEquals("mycat", wrapper.getCatalog());
  }

  @Test void testConnectionTransactionIsolation() throws SQLException {
    Connection mockConn = mock(Connection.class);
    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);

    wrapper.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    verify(mockConn).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

    when(mockConn.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_READ_COMMITTED);
    assertEquals(Connection.TRANSACTION_READ_COMMITTED, wrapper.getTransactionIsolation());
  }

  @Test void testConnectionWarnings() throws SQLException {
    Connection mockConn = mock(Connection.class);
    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);

    when(mockConn.getWarnings()).thenReturn(null);
    assertNull(wrapper.getWarnings());

    wrapper.clearWarnings();
    verify(mockConn).clearWarnings();
  }

  @Test void testConnectionTypeMap() throws SQLException {
    Connection mockConn = mock(Connection.class);
    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);

    Map<String, Class<?>> typeMap = new HashMap<String, Class<?>>();
    when(mockConn.getTypeMap()).thenReturn(typeMap);
    assertEquals(typeMap, wrapper.getTypeMap());

    wrapper.setTypeMap(typeMap);
    verify(mockConn).setTypeMap(typeMap);
  }

  @Test void testConnectionHoldability() throws SQLException {
    Connection mockConn = mock(Connection.class);
    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);

    wrapper.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
    verify(mockConn).setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);

    when(mockConn.getHoldability()).thenReturn(ResultSet.HOLD_CURSORS_OVER_COMMIT);
    assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, wrapper.getHoldability());
  }

  @Test void testConnectionSavepoints() throws SQLException {
    Connection mockConn = mock(Connection.class);
    Savepoint sp = mock(Savepoint.class);
    when(mockConn.setSavepoint()).thenReturn(sp);
    when(mockConn.setSavepoint("sp1")).thenReturn(sp);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    assertEquals(sp, wrapper.setSavepoint());
    assertEquals(sp, wrapper.setSavepoint("sp1"));

    wrapper.rollback(sp);
    verify(mockConn).rollback(sp);

    wrapper.releaseSavepoint(sp);
    verify(mockConn).releaseSavepoint(sp);
  }

  @Test void testConnectionCreateClob() throws SQLException {
    Connection mockConn = mock(Connection.class);
    Clob clob = mock(Clob.class);
    when(mockConn.createClob()).thenReturn(clob);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    assertEquals(clob, wrapper.createClob());
  }

  @Test void testConnectionCreateBlob() throws SQLException {
    Connection mockConn = mock(Connection.class);
    Blob blob = mock(Blob.class);
    when(mockConn.createBlob()).thenReturn(blob);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    assertEquals(blob, wrapper.createBlob());
  }

  @Test void testConnectionCreateNClob() throws SQLException {
    Connection mockConn = mock(Connection.class);
    NClob nclob = mock(NClob.class);
    when(mockConn.createNClob()).thenReturn(nclob);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    assertEquals(nclob, wrapper.createNClob());
  }

  @Test void testConnectionCreateSQLXML() throws SQLException {
    Connection mockConn = mock(Connection.class);
    SQLXML sqlxml = mock(SQLXML.class);
    when(mockConn.createSQLXML()).thenReturn(sqlxml);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    assertEquals(sqlxml, wrapper.createSQLXML());
  }

  @Test void testConnectionIsValid() throws SQLException {
    Connection mockConn = mock(Connection.class);
    when(mockConn.isValid(5)).thenReturn(true);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    assertTrue(wrapper.isValid(5));
  }

  @Test void testConnectionClientInfoString() throws Exception {
    Connection mockConn = mock(Connection.class);
    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);

    wrapper.setClientInfo("key", "value");
    verify(mockConn).setClientInfo("key", "value");

    when(mockConn.getClientInfo("key")).thenReturn("value");
    assertEquals("value", wrapper.getClientInfo("key"));
  }

  @Test void testConnectionClientInfoProperties() throws Exception {
    Connection mockConn = mock(Connection.class);
    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);

    Properties props = new Properties();
    wrapper.setClientInfo(props);
    verify(mockConn).setClientInfo(props);

    when(mockConn.getClientInfo()).thenReturn(props);
    assertEquals(props, wrapper.getClientInfo());
  }

  @Test void testConnectionCreateArrayOf() throws SQLException {
    Connection mockConn = mock(Connection.class);
    Array array = mock(Array.class);
    Object[] elements = new Object[]{"a", "b"};
    when(mockConn.createArrayOf("VARCHAR", elements)).thenReturn(array);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    assertEquals(array, wrapper.createArrayOf("VARCHAR", elements));
  }

  @Test void testConnectionCreateStruct() throws SQLException {
    Connection mockConn = mock(Connection.class);
    Struct struct = mock(Struct.class);
    Object[] attrs = new Object[]{1, "name"};
    when(mockConn.createStruct("MY_TYPE", attrs)).thenReturn(struct);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    assertEquals(struct, wrapper.createStruct("MY_TYPE", attrs));
  }

  @Test void testConnectionSchema() throws SQLException {
    Connection mockConn = mock(Connection.class);
    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);

    wrapper.setSchema("myschema");
    verify(mockConn).setSchema("myschema");

    when(mockConn.getSchema()).thenReturn("myschema");
    assertEquals("myschema", wrapper.getSchema());
  }

  @Test void testConnectionAbort() throws SQLException {
    Connection mockConn = mock(Connection.class);
    Executor executor = mock(Executor.class);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    wrapper.abort(executor);
    verify(mockConn).abort(executor);
  }

  @Test void testConnectionNetworkTimeout() throws SQLException {
    Connection mockConn = mock(Connection.class);
    Executor executor = mock(Executor.class);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    wrapper.setNetworkTimeout(executor, 5000);
    verify(mockConn).setNetworkTimeout(executor, 5000);

    when(mockConn.getNetworkTimeout()).thenReturn(5000);
    assertEquals(5000, wrapper.getNetworkTimeout());
  }

  @Test void testConnectionUnwrap() throws SQLException {
    Connection mockConn = mock(Connection.class);
    when(mockConn.unwrap(Connection.class)).thenReturn(mockConn);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    assertEquals(mockConn, wrapper.unwrap(Connection.class));
  }

  @Test void testConnectionIsWrapperFor() throws SQLException {
    Connection mockConn = mock(Connection.class);
    when(mockConn.isWrapperFor(Connection.class)).thenReturn(true);

    FileConnectionWrapper wrapper = new FileConnectionWrapper(mockConn);
    assertTrue(wrapper.isWrapperFor(Connection.class));
  }
}
