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

import org.mockito.Mockito;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
 * Coverage tests for {@link FileResultSetWrapperImpl}.
 */
@Tag("unit")
class FileResultSetWrapperImplCoverageTest {

  private ResultSet delegate;
  private ResultSetMetaData metaData;
  private FileResultSetWrapperImpl wrapper;

  @BeforeEach
  void setUp() throws SQLException {
    delegate = mock(ResultSet.class);
    metaData = mock(ResultSetMetaData.class);
    when(delegate.getMetaData()).thenReturn(metaData);
    wrapper = new FileResultSetWrapperImpl(delegate);
  }

  // ========== TIME methods with compensation ==========

  @Test void testGetTimeByIndexReturnsNull() throws SQLException {
    when(delegate.getTime(1)).thenReturn(null);
    assertNull(wrapper.getTime(1));
  }

  @Test void testGetTimeByIndexCompensates() throws SQLException {
    Time original = new Time(44156000L);
    when(delegate.getTime(1)).thenReturn(original);
    Time result = wrapper.getTime(1);
    assertNotNull(result);
    // compensateTimeForTimezone should adjust the value
  }

  @Test void testGetTimeByLabelReturnsNull() throws SQLException {
    when(delegate.getTime("col")).thenReturn(null);
    assertNull(wrapper.getTime("col"));
  }

  @Test void testGetTimeByLabelCompensates() throws SQLException {
    Time original = new Time(44156000L);
    when(delegate.getTime("col")).thenReturn(original);
    Time result = wrapper.getTime("col");
    assertNotNull(result);
  }

  @Test void testGetTimeByIndexWithCalReturnsNull() throws SQLException {
    Calendar cal = Calendar.getInstance();
    when(delegate.getTime(1, cal)).thenReturn(null);
    assertNull(wrapper.getTime(1, cal));
  }

  @Test void testGetTimeByIndexWithCalCompensates() throws SQLException {
    Calendar cal = Calendar.getInstance();
    Time original = new Time(44156000L);
    when(delegate.getTime(1, cal)).thenReturn(original);
    Time result = wrapper.getTime(1, cal);
    assertNotNull(result);
  }

  @Test void testGetTimeByLabelWithCalReturnsNull() throws SQLException {
    Calendar cal = Calendar.getInstance();
    when(delegate.getTime("col", cal)).thenReturn(null);
    assertNull(wrapper.getTime("col", cal));
  }

  @Test void testGetTimeByLabelWithCalCompensates() throws SQLException {
    Calendar cal = Calendar.getInstance();
    Time original = new Time(44156000L);
    when(delegate.getTime("col", cal)).thenReturn(original);
    Time result = wrapper.getTime("col", cal);
    assertNotNull(result);
  }

  // ========== getObject with TIME column type checks ==========

  @Test void testGetObjectByIndexTimeColumnWithTimeObject() throws SQLException {
    Time original = new Time(44156000L);
    when(delegate.getObject(1)).thenReturn(original);
    when(metaData.getColumnType(1)).thenReturn(Types.TIME);
    Object result = wrapper.getObject(1);
    assertNotNull(result);
    assertTrue(result instanceof Time);
  }

  @Test void testGetObjectByIndexTimeColumnWithIntegerObject() throws SQLException {
    when(delegate.getObject(1)).thenReturn(Integer.valueOf(26156000));
    when(metaData.getColumnType(1)).thenReturn(Types.TIME);
    Object result = wrapper.getObject(1);
    assertEquals(26156000, result);
  }

  @Test void testGetObjectByIndexNonTimeColumn() throws SQLException {
    when(delegate.getObject(1)).thenReturn("hello");
    when(metaData.getColumnType(1)).thenReturn(Types.VARCHAR);
    Object result = wrapper.getObject(1);
    assertEquals("hello", result);
  }

  @Test void testGetObjectByLabelTimeColumnWithTimeObject() throws SQLException {
    Time original = new Time(44156000L);
    when(delegate.getObject("col")).thenReturn(original);
    when(delegate.findColumn("col")).thenReturn(1);
    when(metaData.getColumnType(1)).thenReturn(Types.TIME);
    Object result = wrapper.getObject("col");
    assertNotNull(result);
    assertTrue(result instanceof Time);
  }

  @Test void testGetObjectByLabelTimeColumnWithInteger() throws SQLException {
    when(delegate.getObject("col")).thenReturn(Integer.valueOf(26156000));
    when(delegate.findColumn("col")).thenReturn(1);
    when(metaData.getColumnType(1)).thenReturn(Types.TIME);
    Object result = wrapper.getObject("col");
    assertEquals(26156000, result);
  }

  @Test void testGetObjectByLabelNonTimeColumn() throws SQLException {
    when(delegate.getObject("col")).thenReturn("world");
    when(delegate.findColumn("col")).thenReturn(1);
    when(metaData.getColumnType(1)).thenReturn(Types.VARCHAR);
    Object result = wrapper.getObject("col");
    assertEquals("world", result);
  }

  // ========== Basic delegation methods ==========

  @Test void testNext() throws SQLException {
    when(delegate.next()).thenReturn(true);
    assertTrue(wrapper.next());
    verify(delegate).next();
  }

  @Test void testClose() throws SQLException {
    wrapper.close();
    verify(delegate).close();
  }

  @Test void testWasNull() throws SQLException {
    when(delegate.wasNull()).thenReturn(true);
    assertTrue(wrapper.wasNull());
  }

  @Test void testGetStringByIndex() throws SQLException {
    when(delegate.getString(1)).thenReturn("test");
    assertEquals("test", wrapper.getString(1));
  }

  @Test void testGetBooleanByIndex() throws SQLException {
    when(delegate.getBoolean(1)).thenReturn(true);
    assertTrue(wrapper.getBoolean(1));
  }

  @Test void testGetByteByIndex() throws SQLException {
    when(delegate.getByte(1)).thenReturn((byte) 42);
    assertEquals((byte) 42, wrapper.getByte(1));
  }

  @Test void testGetShortByIndex() throws SQLException {
    when(delegate.getShort(1)).thenReturn((short) 100);
    assertEquals((short) 100, wrapper.getShort(1));
  }

  @Test void testGetIntByIndex() throws SQLException {
    when(delegate.getInt(1)).thenReturn(42);
    assertEquals(42, wrapper.getInt(1));
  }

  @Test void testGetLongByIndex() throws SQLException {
    when(delegate.getLong(1)).thenReturn(1000L);
    assertEquals(1000L, wrapper.getLong(1));
  }

  @Test void testGetFloatByIndex() throws SQLException {
    when(delegate.getFloat(1)).thenReturn(3.14f);
    assertEquals(3.14f, wrapper.getFloat(1), 0.001f);
  }

  @Test void testGetDoubleByIndex() throws SQLException {
    when(delegate.getDouble(1)).thenReturn(2.718);
    assertEquals(2.718, wrapper.getDouble(1), 0.001);
  }

  @SuppressWarnings("deprecation")
  @Test void testGetBigDecimalByIndexWithScale() throws SQLException {
    BigDecimal bd = new BigDecimal("123.45");
    when(delegate.getBigDecimal(1, 2)).thenReturn(bd);
    assertEquals(bd, wrapper.getBigDecimal(1, 2));
  }

  @Test void testGetBytesByIndex() throws SQLException {
    byte[] bytes = new byte[]{1, 2, 3};
    when(delegate.getBytes(1)).thenReturn(bytes);
    assertArrayEquals(bytes, wrapper.getBytes(1));
  }

  @Test void testGetDateByIndex() throws SQLException {
    Date date = new Date(1000L);
    when(delegate.getDate(1)).thenReturn(date);
    assertEquals(date, wrapper.getDate(1));
  }

  @Test void testGetTimestampByIndex() throws SQLException {
    Timestamp ts = new Timestamp(1000L);
    when(delegate.getTimestamp(1)).thenReturn(ts);
    assertEquals(ts, wrapper.getTimestamp(1));
  }

  @Test void testGetAsciiStreamByIndex() throws SQLException {
    InputStream is = mock(InputStream.class);
    when(delegate.getAsciiStream(1)).thenReturn(is);
    assertEquals(is, wrapper.getAsciiStream(1));
  }

  @SuppressWarnings("deprecation")
  @Test void testGetUnicodeStreamByIndex() throws SQLException {
    InputStream is = mock(InputStream.class);
    when(delegate.getUnicodeStream(1)).thenReturn(is);
    assertEquals(is, wrapper.getUnicodeStream(1));
  }

  @Test void testGetBinaryStreamByIndex() throws SQLException {
    InputStream is = mock(InputStream.class);
    when(delegate.getBinaryStream(1)).thenReturn(is);
    assertEquals(is, wrapper.getBinaryStream(1));
  }

  // ========== String label variants ==========

  @Test void testGetStringByLabel() throws SQLException {
    when(delegate.getString("col")).thenReturn("value");
    assertEquals("value", wrapper.getString("col"));
  }

  @Test void testGetBooleanByLabel() throws SQLException {
    when(delegate.getBoolean("col")).thenReturn(false);
    assertFalse(wrapper.getBoolean("col"));
  }

  @Test void testGetByteByLabel() throws SQLException {
    when(delegate.getByte("col")).thenReturn((byte) 1);
    assertEquals((byte) 1, wrapper.getByte("col"));
  }

  @Test void testGetShortByLabel() throws SQLException {
    when(delegate.getShort("col")).thenReturn((short) 10);
    assertEquals((short) 10, wrapper.getShort("col"));
  }

  @Test void testGetIntByLabel() throws SQLException {
    when(delegate.getInt("col")).thenReturn(99);
    assertEquals(99, wrapper.getInt("col"));
  }

  @Test void testGetLongByLabel() throws SQLException {
    when(delegate.getLong("col")).thenReturn(999L);
    assertEquals(999L, wrapper.getLong("col"));
  }

  @Test void testGetFloatByLabel() throws SQLException {
    when(delegate.getFloat("col")).thenReturn(1.5f);
    assertEquals(1.5f, wrapper.getFloat("col"), 0.001f);
  }

  @Test void testGetDoubleByLabel() throws SQLException {
    when(delegate.getDouble("col")).thenReturn(2.5);
    assertEquals(2.5, wrapper.getDouble("col"), 0.001);
  }

  @SuppressWarnings("deprecation")
  @Test void testGetBigDecimalByLabelWithScale() throws SQLException {
    BigDecimal bd = new BigDecimal("99.99");
    when(delegate.getBigDecimal("col", 2)).thenReturn(bd);
    assertEquals(bd, wrapper.getBigDecimal("col", 2));
  }

  @Test void testGetBytesByLabel() throws SQLException {
    byte[] bytes = new byte[]{4, 5};
    when(delegate.getBytes("col")).thenReturn(bytes);
    assertArrayEquals(bytes, wrapper.getBytes("col"));
  }

  @Test void testGetDateByLabel() throws SQLException {
    Date date = new Date(2000L);
    when(delegate.getDate("col")).thenReturn(date);
    assertEquals(date, wrapper.getDate("col"));
  }

  @Test void testGetTimestampByLabel() throws SQLException {
    Timestamp ts = new Timestamp(3000L);
    when(delegate.getTimestamp("col")).thenReturn(ts);
    assertEquals(ts, wrapper.getTimestamp("col"));
  }

  @Test void testGetAsciiStreamByLabel() throws SQLException {
    InputStream is = mock(InputStream.class);
    when(delegate.getAsciiStream("col")).thenReturn(is);
    assertEquals(is, wrapper.getAsciiStream("col"));
  }

  @SuppressWarnings("deprecation")
  @Test void testGetUnicodeStreamByLabel() throws SQLException {
    InputStream is = mock(InputStream.class);
    when(delegate.getUnicodeStream("col")).thenReturn(is);
    assertEquals(is, wrapper.getUnicodeStream("col"));
  }

  @Test void testGetBinaryStreamByLabel() throws SQLException {
    InputStream is = mock(InputStream.class);
    when(delegate.getBinaryStream("col")).thenReturn(is);
    assertEquals(is, wrapper.getBinaryStream("col"));
  }

  // ========== Misc ResultSet methods ==========

  @Test void testGetWarnings() throws SQLException {
    SQLWarning warning = new SQLWarning("test");
    when(delegate.getWarnings()).thenReturn(warning);
    assertEquals(warning, wrapper.getWarnings());
  }

  @Test void testClearWarnings() throws SQLException {
    wrapper.clearWarnings();
    verify(delegate).clearWarnings();
  }

  @Test void testGetCursorName() throws SQLException {
    when(delegate.getCursorName()).thenReturn("cursor1");
    assertEquals("cursor1", wrapper.getCursorName());
  }

  @Test void testGetMetaData() throws SQLException {
    assertEquals(metaData, wrapper.getMetaData());
  }

  @Test void testFindColumn() throws SQLException {
    when(delegate.findColumn("col")).thenReturn(3);
    assertEquals(3, wrapper.findColumn("col"));
  }

  @Test void testGetCharacterStreamByIndex() throws SQLException {
    Reader reader = mock(Reader.class);
    when(delegate.getCharacterStream(1)).thenReturn(reader);
    assertEquals(reader, wrapper.getCharacterStream(1));
  }

  @Test void testGetCharacterStreamByLabel() throws SQLException {
    Reader reader = mock(Reader.class);
    when(delegate.getCharacterStream("col")).thenReturn(reader);
    assertEquals(reader, wrapper.getCharacterStream("col"));
  }

  @Test void testGetBigDecimalByIndex() throws SQLException {
    BigDecimal bd = new BigDecimal("1234.56");
    when(delegate.getBigDecimal(1)).thenReturn(bd);
    assertEquals(bd, wrapper.getBigDecimal(1));
  }

  @Test void testGetBigDecimalByLabel() throws SQLException {
    BigDecimal bd = new BigDecimal("7890.12");
    when(delegate.getBigDecimal("col")).thenReturn(bd);
    assertEquals(bd, wrapper.getBigDecimal("col"));
  }

  // ========== Navigation methods ==========

  @Test void testIsBeforeFirst() throws SQLException {
    when(delegate.isBeforeFirst()).thenReturn(true);
    assertTrue(wrapper.isBeforeFirst());
  }

  @Test void testIsAfterLast() throws SQLException {
    when(delegate.isAfterLast()).thenReturn(false);
    assertFalse(wrapper.isAfterLast());
  }

  @Test void testIsFirst() throws SQLException {
    when(delegate.isFirst()).thenReturn(true);
    assertTrue(wrapper.isFirst());
  }

  @Test void testIsLast() throws SQLException {
    when(delegate.isLast()).thenReturn(false);
    assertFalse(wrapper.isLast());
  }

  @Test void testBeforeFirst() throws SQLException {
    wrapper.beforeFirst();
    verify(delegate).beforeFirst();
  }

  @Test void testAfterLast() throws SQLException {
    wrapper.afterLast();
    verify(delegate).afterLast();
  }

  @Test void testFirst() throws SQLException {
    when(delegate.first()).thenReturn(true);
    assertTrue(wrapper.first());
  }

  @Test void testLast() throws SQLException {
    when(delegate.last()).thenReturn(true);
    assertTrue(wrapper.last());
  }

  @Test void testGetRow() throws SQLException {
    when(delegate.getRow()).thenReturn(5);
    assertEquals(5, wrapper.getRow());
  }

  @Test void testAbsolute() throws SQLException {
    when(delegate.absolute(3)).thenReturn(true);
    assertTrue(wrapper.absolute(3));
  }

  @Test void testRelative() throws SQLException {
    when(delegate.relative(2)).thenReturn(true);
    assertTrue(wrapper.relative(2));
  }

  @Test void testPrevious() throws SQLException {
    when(delegate.previous()).thenReturn(false);
    assertFalse(wrapper.previous());
  }

  @Test void testSetFetchDirection() throws SQLException {
    wrapper.setFetchDirection(ResultSet.FETCH_FORWARD);
    verify(delegate).setFetchDirection(ResultSet.FETCH_FORWARD);
  }

  @Test void testGetFetchDirection() throws SQLException {
    when(delegate.getFetchDirection()).thenReturn(ResultSet.FETCH_FORWARD);
    assertEquals(ResultSet.FETCH_FORWARD, wrapper.getFetchDirection());
  }

  @Test void testSetFetchSize() throws SQLException {
    wrapper.setFetchSize(100);
    verify(delegate).setFetchSize(100);
  }

  @Test void testGetFetchSize() throws SQLException {
    when(delegate.getFetchSize()).thenReturn(50);
    assertEquals(50, wrapper.getFetchSize());
  }

  @Test void testGetType() throws SQLException {
    when(delegate.getType()).thenReturn(ResultSet.TYPE_FORWARD_ONLY);
    assertEquals(ResultSet.TYPE_FORWARD_ONLY, wrapper.getType());
  }

  @Test void testGetConcurrency() throws SQLException {
    when(delegate.getConcurrency()).thenReturn(ResultSet.CONCUR_READ_ONLY);
    assertEquals(ResultSet.CONCUR_READ_ONLY, wrapper.getConcurrency());
  }

  @Test void testRowUpdated() throws SQLException {
    when(delegate.rowUpdated()).thenReturn(false);
    assertFalse(wrapper.rowUpdated());
  }

  @Test void testRowInserted() throws SQLException {
    when(delegate.rowInserted()).thenReturn(false);
    assertFalse(wrapper.rowInserted());
  }

  @Test void testRowDeleted() throws SQLException {
    when(delegate.rowDeleted()).thenReturn(false);
    assertFalse(wrapper.rowDeleted());
  }

  // ========== Update methods ==========

  @Test void testUpdateNullByIndex() throws SQLException {
    wrapper.updateNull(1);
    verify(delegate).updateNull(1);
  }

  @Test void testUpdateBooleanByIndex() throws SQLException {
    wrapper.updateBoolean(1, true);
    verify(delegate).updateBoolean(1, true);
  }

  @Test void testUpdateByteByIndex() throws SQLException {
    wrapper.updateByte(1, (byte) 1);
    verify(delegate).updateByte(1, (byte) 1);
  }

  @Test void testUpdateShortByIndex() throws SQLException {
    wrapper.updateShort(1, (short) 1);
    verify(delegate).updateShort(1, (short) 1);
  }

  @Test void testUpdateIntByIndex() throws SQLException {
    wrapper.updateInt(1, 42);
    verify(delegate).updateInt(1, 42);
  }

  @Test void testUpdateLongByIndex() throws SQLException {
    wrapper.updateLong(1, 100L);
    verify(delegate).updateLong(1, 100L);
  }

  @Test void testUpdateFloatByIndex() throws SQLException {
    wrapper.updateFloat(1, 1.0f);
    verify(delegate).updateFloat(1, 1.0f);
  }

  @Test void testUpdateDoubleByIndex() throws SQLException {
    wrapper.updateDouble(1, 2.0);
    verify(delegate).updateDouble(1, 2.0);
  }

  @Test void testUpdateBigDecimalByIndex() throws SQLException {
    BigDecimal bd = new BigDecimal("1.23");
    wrapper.updateBigDecimal(1, bd);
    verify(delegate).updateBigDecimal(1, bd);
  }

  @Test void testUpdateStringByIndex() throws SQLException {
    wrapper.updateString(1, "val");
    verify(delegate).updateString(1, "val");
  }

  @Test void testUpdateBytesByIndex() throws SQLException {
    byte[] b = new byte[]{1};
    wrapper.updateBytes(1, b);
    verify(delegate).updateBytes(1, b);
  }

  @Test void testUpdateDateByIndex() throws SQLException {
    Date d = new Date(1000L);
    wrapper.updateDate(1, d);
    verify(delegate).updateDate(1, d);
  }

  @Test void testUpdateTimeByIndex() throws SQLException {
    Time t = new Time(1000L);
    wrapper.updateTime(1, t);
    verify(delegate).updateTime(1, t);
  }

  @Test void testUpdateTimestampByIndex() throws SQLException {
    Timestamp ts = new Timestamp(1000L);
    wrapper.updateTimestamp(1, ts);
    verify(delegate).updateTimestamp(1, ts);
  }

  @Test void testUpdateAsciiStreamByIndex() throws SQLException {
    InputStream is = mock(InputStream.class);
    wrapper.updateAsciiStream(1, is, 100);
    verify(delegate).updateAsciiStream(1, is, 100);
  }

  @Test void testUpdateBinaryStreamByIndex() throws SQLException {
    InputStream is = mock(InputStream.class);
    wrapper.updateBinaryStream(1, is, 100);
    verify(delegate).updateBinaryStream(1, is, 100);
  }

  @Test void testUpdateCharacterStreamByIndex() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateCharacterStream(1, r, 100);
    verify(delegate).updateCharacterStream(1, r, 100);
  }

  @Test void testUpdateObjectByIndexWithScale() throws SQLException {
    wrapper.updateObject(1, "obj", 2);
    verify(delegate).updateObject(1, "obj", 2);
  }

  @Test void testUpdateObjectByIndex() throws SQLException {
    wrapper.updateObject(1, "obj");
    verify(delegate).updateObject(1, "obj");
  }

  // ========== Update by label ==========

  @Test void testUpdateNullByLabel() throws SQLException {
    wrapper.updateNull("col");
    verify(delegate).updateNull("col");
  }

  @Test void testUpdateBooleanByLabel() throws SQLException {
    wrapper.updateBoolean("col", true);
    verify(delegate).updateBoolean("col", true);
  }

  @Test void testUpdateByteByLabel() throws SQLException {
    wrapper.updateByte("col", (byte) 1);
    verify(delegate).updateByte("col", (byte) 1);
  }

  @Test void testUpdateShortByLabel() throws SQLException {
    wrapper.updateShort("col", (short) 1);
    verify(delegate).updateShort("col", (short) 1);
  }

  @Test void testUpdateIntByLabel() throws SQLException {
    wrapper.updateInt("col", 42);
    verify(delegate).updateInt("col", 42);
  }

  @Test void testUpdateLongByLabel() throws SQLException {
    wrapper.updateLong("col", 100L);
    verify(delegate).updateLong("col", 100L);
  }

  @Test void testUpdateFloatByLabel() throws SQLException {
    wrapper.updateFloat("col", 1.0f);
    verify(delegate).updateFloat("col", 1.0f);
  }

  @Test void testUpdateDoubleByLabel() throws SQLException {
    wrapper.updateDouble("col", 2.0);
    verify(delegate).updateDouble("col", 2.0);
  }

  @Test void testUpdateBigDecimalByLabel() throws SQLException {
    BigDecimal bd = new BigDecimal("4.56");
    wrapper.updateBigDecimal("col", bd);
    verify(delegate).updateBigDecimal("col", bd);
  }

  @Test void testUpdateStringByLabel() throws SQLException {
    wrapper.updateString("col", "val");
    verify(delegate).updateString("col", "val");
  }

  @Test void testUpdateBytesByLabel() throws SQLException {
    byte[] b = new byte[]{2};
    wrapper.updateBytes("col", b);
    verify(delegate).updateBytes("col", b);
  }

  @Test void testUpdateDateByLabel() throws SQLException {
    Date d = new Date(2000L);
    wrapper.updateDate("col", d);
    verify(delegate).updateDate("col", d);
  }

  @Test void testUpdateTimeByLabel() throws SQLException {
    Time t = new Time(2000L);
    wrapper.updateTime("col", t);
    verify(delegate).updateTime("col", t);
  }

  @Test void testUpdateTimestampByLabel() throws SQLException {
    Timestamp ts = new Timestamp(2000L);
    wrapper.updateTimestamp("col", ts);
    verify(delegate).updateTimestamp("col", ts);
  }

  @Test void testUpdateAsciiStreamByLabel() throws SQLException {
    InputStream is = mock(InputStream.class);
    wrapper.updateAsciiStream("col", is, 100);
    verify(delegate).updateAsciiStream("col", is, 100);
  }

  @Test void testUpdateBinaryStreamByLabel() throws SQLException {
    InputStream is = mock(InputStream.class);
    wrapper.updateBinaryStream("col", is, 100);
    verify(delegate).updateBinaryStream("col", is, 100);
  }

  @Test void testUpdateCharacterStreamByLabel() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateCharacterStream("col", r, 100);
    verify(delegate).updateCharacterStream("col", r, 100);
  }

  @Test void testUpdateObjectByLabelWithScale() throws SQLException {
    wrapper.updateObject("col", "obj", 2);
    verify(delegate).updateObject("col", "obj", 2);
  }

  @Test void testUpdateObjectByLabel() throws SQLException {
    wrapper.updateObject("col", "obj");
    verify(delegate).updateObject("col", "obj");
  }

  // ========== Row manipulation ==========

  @Test void testInsertRow() throws SQLException {
    wrapper.insertRow();
    verify(delegate).insertRow();
  }

  @Test void testUpdateRow() throws SQLException {
    wrapper.updateRow();
    verify(delegate).updateRow();
  }

  @Test void testDeleteRow() throws SQLException {
    wrapper.deleteRow();
    verify(delegate).deleteRow();
  }

  @Test void testRefreshRow() throws SQLException {
    wrapper.refreshRow();
    verify(delegate).refreshRow();
  }

  @Test void testCancelRowUpdates() throws SQLException {
    wrapper.cancelRowUpdates();
    verify(delegate).cancelRowUpdates();
  }

  @Test void testMoveToInsertRow() throws SQLException {
    wrapper.moveToInsertRow();
    verify(delegate).moveToInsertRow();
  }

  @Test void testMoveToCurrentRow() throws SQLException {
    wrapper.moveToCurrentRow();
    verify(delegate).moveToCurrentRow();
  }

  @Test void testGetStatement() throws SQLException {
    Statement stmt = mock(Statement.class);
    when(delegate.getStatement()).thenReturn(stmt);
    assertEquals(stmt, wrapper.getStatement());
  }

  // ========== Typed object getters ==========

  @Test void testGetObjectByIndexWithMap() throws SQLException {
    Map<String, Class<?>> map = new HashMap<String, Class<?>>();
    when(delegate.getObject(1, map)).thenReturn("val");
    assertEquals("val", wrapper.getObject(1, map));
  }

  @Test void testGetRefByIndex() throws SQLException {
    Ref ref = mock(Ref.class);
    when(delegate.getRef(1)).thenReturn(ref);
    assertEquals(ref, wrapper.getRef(1));
  }

  @Test void testGetBlobByIndex() throws SQLException {
    Blob blob = mock(Blob.class);
    when(delegate.getBlob(1)).thenReturn(blob);
    assertEquals(blob, wrapper.getBlob(1));
  }

  @Test void testGetClobByIndex() throws SQLException {
    Clob clob = mock(Clob.class);
    when(delegate.getClob(1)).thenReturn(clob);
    assertEquals(clob, wrapper.getClob(1));
  }

  @Test void testGetArrayByIndex() throws SQLException {
    Array array = mock(Array.class);
    when(delegate.getArray(1)).thenReturn(array);
    assertEquals(array, wrapper.getArray(1));
  }

  @Test void testGetObjectByLabelWithMap() throws SQLException {
    Map<String, Class<?>> map = new HashMap<String, Class<?>>();
    when(delegate.getObject("col", map)).thenReturn("val");
    assertEquals("val", wrapper.getObject("col", map));
  }

  @Test void testGetRefByLabel() throws SQLException {
    Ref ref = mock(Ref.class);
    when(delegate.getRef("col")).thenReturn(ref);
    assertEquals(ref, wrapper.getRef("col"));
  }

  @Test void testGetBlobByLabel() throws SQLException {
    Blob blob = mock(Blob.class);
    when(delegate.getBlob("col")).thenReturn(blob);
    assertEquals(blob, wrapper.getBlob("col"));
  }

  @Test void testGetClobByLabel() throws SQLException {
    Clob clob = mock(Clob.class);
    when(delegate.getClob("col")).thenReturn(clob);
    assertEquals(clob, wrapper.getClob("col"));
  }

  @Test void testGetArrayByLabel() throws SQLException {
    Array array = mock(Array.class);
    when(delegate.getArray("col")).thenReturn(array);
    assertEquals(array, wrapper.getArray("col"));
  }

  // ========== Date/Timestamp with Calendar ==========

  @Test void testGetDateByIndexWithCal() throws SQLException {
    Calendar cal = Calendar.getInstance();
    Date date = new Date(1000L);
    when(delegate.getDate(1, cal)).thenReturn(date);
    assertEquals(date, wrapper.getDate(1, cal));
  }

  @Test void testGetDateByLabelWithCal() throws SQLException {
    Calendar cal = Calendar.getInstance();
    Date date = new Date(2000L);
    when(delegate.getDate("col", cal)).thenReturn(date);
    assertEquals(date, wrapper.getDate("col", cal));
  }

  @Test void testGetTimestampByIndexWithCal() throws SQLException {
    Calendar cal = Calendar.getInstance();
    Timestamp ts = new Timestamp(1000L);
    when(delegate.getTimestamp(1, cal)).thenReturn(ts);
    assertEquals(ts, wrapper.getTimestamp(1, cal));
  }

  @Test void testGetTimestampByLabelWithCal() throws SQLException {
    Calendar cal = Calendar.getInstance();
    Timestamp ts = new Timestamp(2000L);
    when(delegate.getTimestamp("col", cal)).thenReturn(ts);
    assertEquals(ts, wrapper.getTimestamp("col", cal));
  }

  // ========== URL getters ==========

  @Test void testGetURLByIndex() throws SQLException {
    when(delegate.getURL(1)).thenReturn(null);
    assertNull(wrapper.getURL(1));
  }

  @Test void testGetURLByLabel() throws SQLException {
    when(delegate.getURL("col")).thenReturn(null);
    assertNull(wrapper.getURL("col"));
  }

  // ========== Ref/Blob/Clob update methods ==========

  @Test void testUpdateRefByIndex() throws SQLException {
    Ref ref = mock(Ref.class);
    wrapper.updateRef(1, ref);
    verify(delegate).updateRef(1, ref);
  }

  @Test void testUpdateRefByLabel() throws SQLException {
    Ref ref = mock(Ref.class);
    wrapper.updateRef("col", ref);
    verify(delegate).updateRef("col", ref);
  }

  @Test void testUpdateBlobByIndex() throws SQLException {
    Blob blob = mock(Blob.class);
    wrapper.updateBlob(1, blob);
    verify(delegate).updateBlob(1, blob);
  }

  @Test void testUpdateBlobByLabel() throws SQLException {
    Blob blob = mock(Blob.class);
    wrapper.updateBlob("col", blob);
    verify(delegate).updateBlob("col", blob);
  }

  @Test void testUpdateClobByIndex() throws SQLException {
    Clob clob = mock(Clob.class);
    wrapper.updateClob(1, clob);
    verify(delegate).updateClob(1, clob);
  }

  @Test void testUpdateClobByLabel() throws SQLException {
    Clob clob = mock(Clob.class);
    wrapper.updateClob("col", clob);
    verify(delegate).updateClob("col", clob);
  }

  @Test void testUpdateArrayByIndex() throws SQLException {
    Array array = mock(Array.class);
    wrapper.updateArray(1, array);
    verify(delegate).updateArray(1, array);
  }

  @Test void testUpdateArrayByLabel() throws SQLException {
    Array array = mock(Array.class);
    wrapper.updateArray("col", array);
    verify(delegate).updateArray("col", array);
  }

  // ========== RowId methods ==========

  @Test void testGetRowIdByIndex() throws SQLException {
    RowId rowId = mock(RowId.class);
    when(delegate.getRowId(1)).thenReturn(rowId);
    assertEquals(rowId, wrapper.getRowId(1));
  }

  @Test void testGetRowIdByLabel() throws SQLException {
    RowId rowId = mock(RowId.class);
    when(delegate.getRowId("col")).thenReturn(rowId);
    assertEquals(rowId, wrapper.getRowId("col"));
  }

  @Test void testUpdateRowIdByIndex() throws SQLException {
    RowId rowId = mock(RowId.class);
    wrapper.updateRowId(1, rowId);
    verify(delegate).updateRowId(1, rowId);
  }

  @Test void testUpdateRowIdByLabel() throws SQLException {
    RowId rowId = mock(RowId.class);
    wrapper.updateRowId("col", rowId);
    verify(delegate).updateRowId("col", rowId);
  }

  @Test void testGetHoldability() throws SQLException {
    when(delegate.getHoldability()).thenReturn(ResultSet.HOLD_CURSORS_OVER_COMMIT);
    assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, wrapper.getHoldability());
  }

  @Test void testIsClosed() throws SQLException {
    when(delegate.isClosed()).thenReturn(false);
    assertFalse(wrapper.isClosed());
  }

  // ========== NString, NClob, SQLXML methods ==========

  @Test void testUpdateNStringByIndex() throws SQLException {
    wrapper.updateNString(1, "nval");
    verify(delegate).updateNString(1, "nval");
  }

  @Test void testUpdateNStringByLabel() throws SQLException {
    wrapper.updateNString("col", "nval");
    verify(delegate).updateNString("col", "nval");
  }

  @Test void testUpdateNClobByIndex() throws SQLException {
    NClob nclob = mock(NClob.class);
    wrapper.updateNClob(1, nclob);
    verify(delegate).updateNClob(1, nclob);
  }

  @Test void testUpdateNClobByLabel() throws SQLException {
    NClob nclob = mock(NClob.class);
    wrapper.updateNClob("col", nclob);
    verify(delegate).updateNClob("col", nclob);
  }

  @Test void testGetNClobByIndex() throws SQLException {
    NClob nclob = mock(NClob.class);
    when(delegate.getNClob(1)).thenReturn(nclob);
    assertEquals(nclob, wrapper.getNClob(1));
  }

  @Test void testGetNClobByLabel() throws SQLException {
    NClob nclob = mock(NClob.class);
    when(delegate.getNClob("col")).thenReturn(nclob);
    assertEquals(nclob, wrapper.getNClob("col"));
  }

  @Test void testGetSQLXMLByIndex() throws SQLException {
    SQLXML xml = mock(SQLXML.class);
    when(delegate.getSQLXML(1)).thenReturn(xml);
    assertEquals(xml, wrapper.getSQLXML(1));
  }

  @Test void testGetSQLXMLByLabel() throws SQLException {
    SQLXML xml = mock(SQLXML.class);
    when(delegate.getSQLXML("col")).thenReturn(xml);
    assertEquals(xml, wrapper.getSQLXML("col"));
  }

  @Test void testUpdateSQLXMLByIndex() throws SQLException {
    SQLXML xml = mock(SQLXML.class);
    wrapper.updateSQLXML(1, xml);
    verify(delegate).updateSQLXML(1, xml);
  }

  @Test void testUpdateSQLXMLByLabel() throws SQLException {
    SQLXML xml = mock(SQLXML.class);
    wrapper.updateSQLXML("col", xml);
    verify(delegate).updateSQLXML("col", xml);
  }

  @Test void testGetNStringByIndex() throws SQLException {
    when(delegate.getNString(1)).thenReturn("nstr");
    assertEquals("nstr", wrapper.getNString(1));
  }

  @Test void testGetNStringByLabel() throws SQLException {
    when(delegate.getNString("col")).thenReturn("nstr");
    assertEquals("nstr", wrapper.getNString("col"));
  }

  @Test void testGetNCharacterStreamByIndex() throws SQLException {
    Reader reader = mock(Reader.class);
    when(delegate.getNCharacterStream(1)).thenReturn(reader);
    assertEquals(reader, wrapper.getNCharacterStream(1));
  }

  @Test void testGetNCharacterStreamByLabel() throws SQLException {
    Reader reader = mock(Reader.class);
    when(delegate.getNCharacterStream("col")).thenReturn(reader);
    assertEquals(reader, wrapper.getNCharacterStream("col"));
  }

  // ========== Long-form update streams ==========

  @Test void testUpdateNCharacterStreamByIndexLong() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateNCharacterStream(1, r, 100L);
    verify(delegate).updateNCharacterStream(1, r, 100L);
  }

  @Test void testUpdateNCharacterStreamByLabelLong() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateNCharacterStream("col", r, 100L);
    verify(delegate).updateNCharacterStream("col", r, 100L);
  }

  @Test void testUpdateAsciiStreamByIndexLong() throws SQLException {
    InputStream is = mock(InputStream.class);
    wrapper.updateAsciiStream(1, is, 100L);
    verify(delegate).updateAsciiStream(1, is, 100L);
  }

  @Test void testUpdateBinaryStreamByIndexLong() throws SQLException {
    InputStream is = mock(InputStream.class);
    wrapper.updateBinaryStream(1, is, 100L);
    verify(delegate).updateBinaryStream(1, is, 100L);
  }

  @Test void testUpdateCharacterStreamByIndexLong() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateCharacterStream(1, r, 100L);
    verify(delegate).updateCharacterStream(1, r, 100L);
  }

  @Test void testUpdateAsciiStreamByLabelLong() throws SQLException {
    InputStream is = mock(InputStream.class);
    wrapper.updateAsciiStream("col", is, 100L);
    verify(delegate).updateAsciiStream("col", is, 100L);
  }

  @Test void testUpdateBinaryStreamByLabelLong() throws SQLException {
    InputStream is = mock(InputStream.class);
    wrapper.updateBinaryStream("col", is, 100L);
    verify(delegate).updateBinaryStream("col", is, 100L);
  }

  @Test void testUpdateCharacterStreamByLabelLong() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateCharacterStream("col", r, 100L);
    verify(delegate).updateCharacterStream("col", r, 100L);
  }

  // ========== Blob/Clob/NClob stream updates ==========

  @Test void testUpdateBlobByIndexStream() throws SQLException {
    InputStream is = mock(InputStream.class);
    wrapper.updateBlob(1, is, 100L);
    verify(delegate).updateBlob(1, is, 100L);
  }

  @Test void testUpdateBlobByLabelStream() throws SQLException {
    InputStream is = mock(InputStream.class);
    wrapper.updateBlob("col", is, 100L);
    verify(delegate).updateBlob("col", is, 100L);
  }

  @Test void testUpdateClobByIndexReader() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateClob(1, r, 100L);
    verify(delegate).updateClob(1, r, 100L);
  }

  @Test void testUpdateClobByLabelReader() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateClob("col", r, 100L);
    verify(delegate).updateClob("col", r, 100L);
  }

  @Test void testUpdateNClobByIndexReader() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateNClob(1, r, 100L);
    verify(delegate).updateNClob(1, r, 100L);
  }

  @Test void testUpdateNClobByLabelReader() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateNClob("col", r, 100L);
    verify(delegate).updateNClob("col", r, 100L);
  }

  // ========== No-length stream updates ==========

  @Test void testUpdateNCharacterStreamByIndexNoLength() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateNCharacterStream(1, r);
    verify(delegate).updateNCharacterStream(1, r);
  }

  @Test void testUpdateNCharacterStreamByLabelNoLength() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateNCharacterStream("col", r);
    verify(delegate).updateNCharacterStream("col", r);
  }

  @Test void testUpdateAsciiStreamByIndexNoLength() throws SQLException {
    InputStream is = mock(InputStream.class);
    wrapper.updateAsciiStream(1, is);
    verify(delegate).updateAsciiStream(1, is);
  }

  @Test void testUpdateBinaryStreamByIndexNoLength() throws SQLException {
    InputStream is = mock(InputStream.class);
    wrapper.updateBinaryStream(1, is);
    verify(delegate).updateBinaryStream(1, is);
  }

  @Test void testUpdateCharacterStreamByIndexNoLength() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateCharacterStream(1, r);
    verify(delegate).updateCharacterStream(1, r);
  }

  @Test void testUpdateAsciiStreamByLabelNoLength() throws SQLException {
    InputStream is = mock(InputStream.class);
    wrapper.updateAsciiStream("col", is);
    verify(delegate).updateAsciiStream("col", is);
  }

  @Test void testUpdateBinaryStreamByLabelNoLength() throws SQLException {
    InputStream is = mock(InputStream.class);
    wrapper.updateBinaryStream("col", is);
    verify(delegate).updateBinaryStream("col", is);
  }

  @Test void testUpdateCharacterStreamByLabelNoLength() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateCharacterStream("col", r);
    verify(delegate).updateCharacterStream("col", r);
  }

  @Test void testUpdateBlobByIndexStreamNoLength() throws SQLException {
    InputStream is = mock(InputStream.class);
    wrapper.updateBlob(1, is);
    verify(delegate).updateBlob(1, is);
  }

  @Test void testUpdateBlobByLabelStreamNoLength() throws SQLException {
    InputStream is = mock(InputStream.class);
    wrapper.updateBlob("col", is);
    verify(delegate).updateBlob("col", is);
  }

  @Test void testUpdateClobByIndexReaderNoLength() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateClob(1, r);
    verify(delegate).updateClob(1, r);
  }

  @Test void testUpdateClobByLabelReaderNoLength() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateClob("col", r);
    verify(delegate).updateClob("col", r);
  }

  @Test void testUpdateNClobByIndexReaderNoLength() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateNClob(1, r);
    verify(delegate).updateNClob(1, r);
  }

  @Test void testUpdateNClobByLabelReaderNoLength() throws SQLException {
    Reader r = mock(Reader.class);
    wrapper.updateNClob("col", r);
    verify(delegate).updateNClob("col", r);
  }

  // ========== Generic object getters ==========

  @Test void testGetObjectByIndexWithType() throws SQLException {
    when(delegate.getObject(1, String.class)).thenReturn("typed");
    assertEquals("typed", wrapper.getObject(1, String.class));
  }

  @Test void testGetObjectByLabelWithType() throws SQLException {
    when(delegate.getObject("col", Integer.class)).thenReturn(42);
    assertEquals(Integer.valueOf(42), wrapper.getObject("col", Integer.class));
  }

  // ========== Wrapper methods ==========

  @Test void testUnwrap() throws SQLException {
    when(delegate.unwrap(ResultSet.class)).thenReturn(delegate);
    assertEquals(delegate, wrapper.unwrap(ResultSet.class));
  }

  @Test void testIsWrapperFor() throws SQLException {
    when(delegate.isWrapperFor(ResultSet.class)).thenReturn(true);
    assertTrue(wrapper.isWrapperFor(ResultSet.class));
  }
}
