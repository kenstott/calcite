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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLWarning;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link FileStatementWrapper} delegation methods.
 *
 * <p>Verifies that every Statement interface method delegates to the
 * wrapped delegate, and that ResultSet-returning methods are wrapped.
 */
@Tag("unit")
public class FileStatementWrapperTest {

  private Statement delegate;
  private FileStatementWrapper wrapper;

  @BeforeEach
  public void setUp() {
    delegate = mock(Statement.class);
    wrapper = new FileStatementWrapper(delegate);
  }

  // === ResultSet-wrapping methods ===

  @Test public void testExecuteQueryWrapsResultSet() throws Exception {
    ResultSet rs = mock(ResultSet.class);
    when(delegate.executeQuery(anyString())).thenReturn(rs);

    ResultSet result = wrapper.executeQuery("SELECT 1");
    assertNotNull(result);
    assertInstanceOf(FileResultSetWrapperImpl.class, result);
    verify(delegate).executeQuery("SELECT 1");
  }

  @Test public void testGetResultSetWrapsResultSet() throws Exception {
    ResultSet rs = mock(ResultSet.class);
    when(delegate.getResultSet()).thenReturn(rs);

    ResultSet result = wrapper.getResultSet();
    assertNotNull(result);
    assertInstanceOf(FileResultSetWrapperImpl.class, result);
    verify(delegate).getResultSet();
  }

  @Test public void testGetResultSetNullReturnsNull() throws Exception {
    when(delegate.getResultSet()).thenReturn(null);

    ResultSet result = wrapper.getResultSet();
    assertNull(result);
  }

  @Test public void testGetGeneratedKeysWrapsResultSet() throws Exception {
    ResultSet rs = mock(ResultSet.class);
    when(delegate.getGeneratedKeys()).thenReturn(rs);

    ResultSet result = wrapper.getGeneratedKeys();
    assertNotNull(result);
    assertInstanceOf(FileResultSetWrapperImpl.class, result);
    verify(delegate).getGeneratedKeys();
  }

  @Test public void testGetGeneratedKeysNullReturnsNull() throws Exception {
    when(delegate.getGeneratedKeys()).thenReturn(null);

    ResultSet result = wrapper.getGeneratedKeys();
    assertNull(result);
  }

  // === Delegating methods ===

  @Test public void testExecuteUpdate() throws Exception {
    when(delegate.executeUpdate(anyString())).thenReturn(1);
    int result = wrapper.executeUpdate("UPDATE t SET x=1");
    assertTrue(result == 1);
    verify(delegate).executeUpdate("UPDATE t SET x=1");
  }

  @Test public void testClose() throws Exception {
    doNothing().when(delegate).close();
    wrapper.close();
    verify(delegate).close();
  }

  @Test public void testGetMaxFieldSize() throws Exception {
    when(delegate.getMaxFieldSize()).thenReturn(512);
    int result = wrapper.getMaxFieldSize();
    assertTrue(result == 512);
    verify(delegate).getMaxFieldSize();
  }

  @Test public void testSetMaxFieldSize() throws Exception {
    doNothing().when(delegate).setMaxFieldSize(anyInt());
    wrapper.setMaxFieldSize(256);
    verify(delegate).setMaxFieldSize(256);
  }

  @Test public void testGetMaxRows() throws Exception {
    when(delegate.getMaxRows()).thenReturn(100);
    int result = wrapper.getMaxRows();
    assertTrue(result == 100);
    verify(delegate).getMaxRows();
  }

  @Test public void testSetMaxRows() throws Exception {
    doNothing().when(delegate).setMaxRows(anyInt());
    wrapper.setMaxRows(50);
    verify(delegate).setMaxRows(50);
  }

  @Test public void testSetEscapeProcessing() throws Exception {
    doNothing().when(delegate).setEscapeProcessing(anyBoolean());
    wrapper.setEscapeProcessing(true);
    verify(delegate).setEscapeProcessing(true);
  }

  @Test public void testGetQueryTimeout() throws Exception {
    when(delegate.getQueryTimeout()).thenReturn(30);
    int result = wrapper.getQueryTimeout();
    assertTrue(result == 30);
    verify(delegate).getQueryTimeout();
  }

  @Test public void testSetQueryTimeout() throws Exception {
    doNothing().when(delegate).setQueryTimeout(anyInt());
    wrapper.setQueryTimeout(60);
    verify(delegate).setQueryTimeout(60);
  }

  @Test public void testCancel() throws Exception {
    doNothing().when(delegate).cancel();
    wrapper.cancel();
    verify(delegate).cancel();
  }

  @Test public void testGetWarnings() throws Exception {
    SQLWarning warning = mock(SQLWarning.class);
    when(delegate.getWarnings()).thenReturn(warning);
    SQLWarning result = wrapper.getWarnings();
    assertSame(warning, result);
    verify(delegate).getWarnings();
  }

  @Test public void testClearWarnings() throws Exception {
    doNothing().when(delegate).clearWarnings();
    wrapper.clearWarnings();
    verify(delegate).clearWarnings();
  }

  @Test public void testSetCursorName() throws Exception {
    doNothing().when(delegate).setCursorName(anyString());
    wrapper.setCursorName("cursor1");
    verify(delegate).setCursorName("cursor1");
  }

  @Test public void testExecuteString() throws Exception {
    when(delegate.execute(anyString())).thenReturn(true);
    boolean result = wrapper.execute("SELECT 1");
    assertTrue(result);
    verify(delegate).execute("SELECT 1");
  }

  @Test public void testGetUpdateCount() throws Exception {
    when(delegate.getUpdateCount()).thenReturn(5);
    int result = wrapper.getUpdateCount();
    assertTrue(result == 5);
    verify(delegate).getUpdateCount();
  }

  @Test public void testGetMoreResults() throws Exception {
    when(delegate.getMoreResults()).thenReturn(false);
    boolean result = wrapper.getMoreResults();
    assertFalse(result);
    verify(delegate).getMoreResults();
  }

  @Test public void testSetFetchDirection() throws Exception {
    doNothing().when(delegate).setFetchDirection(anyInt());
    wrapper.setFetchDirection(ResultSet.FETCH_FORWARD);
    verify(delegate).setFetchDirection(ResultSet.FETCH_FORWARD);
  }

  @Test public void testGetFetchDirection() throws Exception {
    when(delegate.getFetchDirection()).thenReturn(ResultSet.FETCH_FORWARD);
    int result = wrapper.getFetchDirection();
    assertTrue(result == ResultSet.FETCH_FORWARD);
    verify(delegate).getFetchDirection();
  }

  @Test public void testSetFetchSize() throws Exception {
    doNothing().when(delegate).setFetchSize(anyInt());
    wrapper.setFetchSize(100);
    verify(delegate).setFetchSize(100);
  }

  @Test public void testGetFetchSize() throws Exception {
    when(delegate.getFetchSize()).thenReturn(200);
    int result = wrapper.getFetchSize();
    assertTrue(result == 200);
    verify(delegate).getFetchSize();
  }

  @Test public void testGetResultSetConcurrency() throws Exception {
    when(delegate.getResultSetConcurrency()).thenReturn(ResultSet.CONCUR_READ_ONLY);
    int result = wrapper.getResultSetConcurrency();
    assertTrue(result == ResultSet.CONCUR_READ_ONLY);
    verify(delegate).getResultSetConcurrency();
  }

  @Test public void testGetResultSetType() throws Exception {
    when(delegate.getResultSetType()).thenReturn(ResultSet.TYPE_FORWARD_ONLY);
    int result = wrapper.getResultSetType();
    assertTrue(result == ResultSet.TYPE_FORWARD_ONLY);
    verify(delegate).getResultSetType();
  }

  @Test public void testAddBatch() throws Exception {
    doNothing().when(delegate).addBatch(anyString());
    wrapper.addBatch("INSERT INTO t VALUES(1)");
    verify(delegate).addBatch("INSERT INTO t VALUES(1)");
  }

  @Test public void testClearBatch() throws Exception {
    doNothing().when(delegate).clearBatch();
    wrapper.clearBatch();
    verify(delegate).clearBatch();
  }

  @Test public void testExecuteBatch() throws Exception {
    int[] counts = {1, 2};
    when(delegate.executeBatch()).thenReturn(counts);
    int[] result = wrapper.executeBatch();
    assertSame(counts, result);
    verify(delegate).executeBatch();
  }

  @Test public void testGetConnection() throws Exception {
    Connection conn = mock(Connection.class);
    when(delegate.getConnection()).thenReturn(conn);
    Connection result = wrapper.getConnection();
    assertSame(conn, result);
    verify(delegate).getConnection();
  }

  @Test public void testGetMoreResultsWithCurrent() throws Exception {
    when(delegate.getMoreResults(anyInt())).thenReturn(false);
    boolean result = wrapper.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
    assertFalse(result);
    verify(delegate).getMoreResults(Statement.CLOSE_CURRENT_RESULT);
  }

  @Test public void testExecuteUpdateWithAutoGeneratedKeys() throws Exception {
    when(delegate.executeUpdate(anyString(), anyInt())).thenReturn(1);
    int result = wrapper.executeUpdate("INSERT INTO t VALUES(1)", Statement.RETURN_GENERATED_KEYS);
    assertTrue(result == 1);
    verify(delegate).executeUpdate("INSERT INTO t VALUES(1)", Statement.RETURN_GENERATED_KEYS);
  }

  @Test public void testExecuteUpdateWithColumnIndexes() throws Exception {
    int[] cols = {1};
    when(delegate.executeUpdate(anyString(), any(int[].class))).thenReturn(1);
    int result = wrapper.executeUpdate("INSERT INTO t VALUES(1)", cols);
    assertTrue(result == 1);
    verify(delegate).executeUpdate("INSERT INTO t VALUES(1)", cols);
  }

  @Test public void testExecuteUpdateWithColumnNames() throws Exception {
    String[] names = {"id"};
    when(delegate.executeUpdate(anyString(), any(String[].class))).thenReturn(1);
    int result = wrapper.executeUpdate("INSERT INTO t VALUES(1)", names);
    assertTrue(result == 1);
    verify(delegate).executeUpdate("INSERT INTO t VALUES(1)", names);
  }

  @Test public void testExecuteWithAutoGeneratedKeys() throws Exception {
    when(delegate.execute(anyString(), anyInt())).thenReturn(false);
    boolean result = wrapper.execute("INSERT INTO t VALUES(1)", Statement.RETURN_GENERATED_KEYS);
    assertFalse(result);
    verify(delegate).execute("INSERT INTO t VALUES(1)", Statement.RETURN_GENERATED_KEYS);
  }

  @Test public void testExecuteWithColumnIndexes() throws Exception {
    int[] cols = {1};
    when(delegate.execute(anyString(), any(int[].class))).thenReturn(false);
    boolean result = wrapper.execute("INSERT INTO t VALUES(1)", cols);
    assertFalse(result);
    verify(delegate).execute("INSERT INTO t VALUES(1)", cols);
  }

  @Test public void testExecuteWithColumnNames() throws Exception {
    String[] names = {"id"};
    when(delegate.execute(anyString(), any(String[].class))).thenReturn(false);
    boolean result = wrapper.execute("INSERT INTO t VALUES(1)", names);
    assertFalse(result);
    verify(delegate).execute("INSERT INTO t VALUES(1)", names);
  }

  @Test public void testGetResultSetHoldability() throws Exception {
    when(delegate.getResultSetHoldability()).thenReturn(ResultSet.HOLD_CURSORS_OVER_COMMIT);
    int result = wrapper.getResultSetHoldability();
    assertTrue(result == ResultSet.HOLD_CURSORS_OVER_COMMIT);
    verify(delegate).getResultSetHoldability();
  }

  @Test public void testIsClosed() throws Exception {
    when(delegate.isClosed()).thenReturn(false);
    boolean result = wrapper.isClosed();
    assertFalse(result);
    verify(delegate).isClosed();
  }

  @Test public void testSetPoolable() throws Exception {
    doNothing().when(delegate).setPoolable(anyBoolean());
    wrapper.setPoolable(true);
    verify(delegate).setPoolable(true);
  }

  @Test public void testIsPoolable() throws Exception {
    when(delegate.isPoolable()).thenReturn(true);
    boolean result = wrapper.isPoolable();
    assertTrue(result);
    verify(delegate).isPoolable();
  }

  @Test public void testCloseOnCompletion() throws Exception {
    doNothing().when(delegate).closeOnCompletion();
    wrapper.closeOnCompletion();
    verify(delegate).closeOnCompletion();
  }

  @Test public void testIsCloseOnCompletion() throws Exception {
    when(delegate.isCloseOnCompletion()).thenReturn(true);
    boolean result = wrapper.isCloseOnCompletion();
    assertTrue(result);
    verify(delegate).isCloseOnCompletion();
  }

  @Test public void testUnwrap() throws Exception {
    when(delegate.unwrap(Statement.class)).thenReturn(delegate);
    Statement result = wrapper.unwrap(Statement.class);
    assertSame(delegate, result);
    verify(delegate).unwrap(Statement.class);
  }

  @Test public void testIsWrapperFor() throws Exception {
    when(delegate.isWrapperFor(Statement.class)).thenReturn(true);
    boolean result = wrapper.isWrapperFor(Statement.class);
    assertTrue(result);
    verify(delegate).isWrapperFor(Statement.class);
  }
}
