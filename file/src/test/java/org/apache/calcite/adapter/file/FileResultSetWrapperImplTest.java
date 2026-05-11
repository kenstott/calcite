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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link FileResultSetWrapperImpl}.
 *
 * <p>Verifies that the wrapper correctly intercepts getTime() and getObject()
 * calls to apply timezone compensation, and that all other ResultSet methods
 * are properly delegated to the underlying ResultSet.</p>
 */
@Tag("unit")
public class FileResultSetWrapperImplTest {

  @Mock
  private ResultSet mockResultSet;

  @Mock
  private ResultSetMetaData mockMetaData;

  private FileResultSetWrapperImpl wrapper;

  @BeforeEach
  public void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    wrapper = new FileResultSetWrapperImpl(mockResultSet);
    when(mockResultSet.getMetaData()).thenReturn(mockMetaData);
  }

  // -----------------------------------------------------------------------
  // getTime(int) tests
  // -----------------------------------------------------------------------

  @Test
  public void testGetTimeByIndexAppliesCompensation() throws SQLException {
    Time originalTime = new Time(44156000L); // some raw millis
    when(mockResultSet.getTime(1)).thenReturn(originalTime);

    Time result = wrapper.getTime(1);

    Time expected = FileResultSetWrapper.compensateTimeForTimezone(originalTime);
    assertEquals(expected.getTime(), result.getTime());
    verify(mockResultSet).getTime(1);
  }

  @Test
  public void testGetTimeByIndexReturnsNullWhenNull() throws SQLException {
    when(mockResultSet.getTime(1)).thenReturn(null);

    Time result = wrapper.getTime(1);

    assertNull(result);
    verify(mockResultSet).getTime(1);
  }

  // -----------------------------------------------------------------------
  // getTime(String) tests
  // -----------------------------------------------------------------------

  @Test
  public void testGetTimeByLabelAppliesCompensation() throws SQLException {
    Time originalTime = new Time(26156000L);
    when(mockResultSet.getTime("start_time")).thenReturn(originalTime);

    Time result = wrapper.getTime("start_time");

    Time expected = FileResultSetWrapper.compensateTimeForTimezone(originalTime);
    assertEquals(expected.getTime(), result.getTime());
    verify(mockResultSet).getTime("start_time");
  }

  @Test
  public void testGetTimeByLabelReturnsNullWhenNull() throws SQLException {
    when(mockResultSet.getTime("start_time")).thenReturn(null);

    Time result = wrapper.getTime("start_time");

    assertNull(result);
    verify(mockResultSet).getTime("start_time");
  }

  // -----------------------------------------------------------------------
  // getTime(int, Calendar) tests
  // -----------------------------------------------------------------------

  @Test
  public void testGetTimeByIndexWithCalendarAppliesCompensation() throws SQLException {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    Time originalTime = new Time(36000000L);
    when(mockResultSet.getTime(2, cal)).thenReturn(originalTime);

    Time result = wrapper.getTime(2, cal);

    Time expected = FileResultSetWrapper.compensateTimeForTimezone(originalTime);
    assertEquals(expected.getTime(), result.getTime());
    verify(mockResultSet).getTime(2, cal);
  }

  @Test
  public void testGetTimeByIndexWithCalendarReturnsNullWhenNull() throws SQLException {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    when(mockResultSet.getTime(2, cal)).thenReturn(null);

    Time result = wrapper.getTime(2, cal);

    assertNull(result);
    verify(mockResultSet).getTime(2, cal);
  }

  // -----------------------------------------------------------------------
  // getTime(String, Calendar) tests
  // -----------------------------------------------------------------------

  @Test
  public void testGetTimeByLabelWithCalendarAppliesCompensation() throws SQLException {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
    Time originalTime = new Time(50400000L);
    when(mockResultSet.getTime("end_time", cal)).thenReturn(originalTime);

    Time result = wrapper.getTime("end_time", cal);

    Time expected = FileResultSetWrapper.compensateTimeForTimezone(originalTime);
    assertEquals(expected.getTime(), result.getTime());
    verify(mockResultSet).getTime("end_time", cal);
  }

  @Test
  public void testGetTimeByLabelWithCalendarReturnsNullWhenNull() throws SQLException {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
    when(mockResultSet.getTime("end_time", cal)).thenReturn(null);

    Time result = wrapper.getTime("end_time", cal);

    assertNull(result);
    verify(mockResultSet).getTime("end_time", cal);
  }

  // -----------------------------------------------------------------------
  // getObject(int) tests - TIME column type
  // -----------------------------------------------------------------------

  @Test
  public void testGetObjectByIndexTimeColumnWithTimeObjectAppliesCompensation()
      throws SQLException {
    Time originalTime = new Time(44156000L);
    when(mockResultSet.getObject(1)).thenReturn(originalTime);
    when(mockMetaData.getColumnType(1)).thenReturn(Types.TIME);

    Object result = wrapper.getObject(1);

    assertTrue(result instanceof Time);
    Time expected = FileResultSetWrapper.compensateTimeForTimezone(originalTime);
    assertEquals(expected.getTime(), ((Time) result).getTime());
  }

  @Test
  public void testGetObjectByIndexTimeColumnWithIntegerPassesThrough() throws SQLException {
    Integer millisSinceMidnight = 26156000;
    when(mockResultSet.getObject(1)).thenReturn(millisSinceMidnight);
    when(mockMetaData.getColumnType(1)).thenReturn(Types.TIME);

    Object result = wrapper.getObject(1);

    assertTrue(result instanceof Integer);
    assertEquals(millisSinceMidnight, result);
  }

  @Test
  public void testGetObjectByIndexTimeColumnWithNullPassesThrough() throws SQLException {
    when(mockResultSet.getObject(1)).thenReturn(null);
    when(mockMetaData.getColumnType(1)).thenReturn(Types.TIME);

    Object result = wrapper.getObject(1);

    assertNull(result);
  }

  @Test
  public void testGetObjectByIndexNonTimeColumnPassesThrough() throws SQLException {
    String stringValue = "hello";
    when(mockResultSet.getObject(1)).thenReturn(stringValue);
    when(mockMetaData.getColumnType(1)).thenReturn(Types.VARCHAR);

    Object result = wrapper.getObject(1);

    assertEquals("hello", result);
  }

  @Test
  public void testGetObjectByIndexIntegerColumnPassesThrough() throws SQLException {
    Integer intValue = 42;
    when(mockResultSet.getObject(2)).thenReturn(intValue);
    when(mockMetaData.getColumnType(2)).thenReturn(Types.INTEGER);

    Object result = wrapper.getObject(2);

    assertEquals(42, result);
  }

  @Test
  public void testGetObjectByIndexTimestampColumnPassesThrough() throws SQLException {
    Timestamp tsValue = new Timestamp(1609459200000L);
    when(mockResultSet.getObject(3)).thenReturn(tsValue);
    when(mockMetaData.getColumnType(3)).thenReturn(Types.TIMESTAMP);

    Object result = wrapper.getObject(3);

    assertEquals(tsValue, result);
  }

  // -----------------------------------------------------------------------
  // getObject(String) tests - TIME column type
  // -----------------------------------------------------------------------

  @Test
  public void testGetObjectByLabelTimeColumnWithTimeObjectAppliesCompensation()
      throws SQLException {
    Time originalTime = new Time(44156000L);
    when(mockResultSet.getObject("event_time")).thenReturn(originalTime);
    when(mockResultSet.findColumn("event_time")).thenReturn(3);
    when(mockMetaData.getColumnType(3)).thenReturn(Types.TIME);

    Object result = wrapper.getObject("event_time");

    assertTrue(result instanceof Time);
    Time expected = FileResultSetWrapper.compensateTimeForTimezone(originalTime);
    assertEquals(expected.getTime(), ((Time) result).getTime());
  }

  @Test
  public void testGetObjectByLabelTimeColumnWithIntegerPassesThrough() throws SQLException {
    Integer millisSinceMidnight = 26156000;
    when(mockResultSet.getObject("event_time")).thenReturn(millisSinceMidnight);
    when(mockResultSet.findColumn("event_time")).thenReturn(3);
    when(mockMetaData.getColumnType(3)).thenReturn(Types.TIME);

    Object result = wrapper.getObject("event_time");

    assertTrue(result instanceof Integer);
    assertEquals(millisSinceMidnight, result);
  }

  @Test
  public void testGetObjectByLabelTimeColumnWithNullPassesThrough() throws SQLException {
    when(mockResultSet.getObject("event_time")).thenReturn(null);
    when(mockResultSet.findColumn("event_time")).thenReturn(3);
    when(mockMetaData.getColumnType(3)).thenReturn(Types.TIME);

    Object result = wrapper.getObject("event_time");

    assertNull(result);
  }

  @Test
  public void testGetObjectByLabelNonTimeColumnPassesThrough() throws SQLException {
    Double doubleValue = 3.14;
    when(mockResultSet.getObject("amount")).thenReturn(doubleValue);
    when(mockResultSet.findColumn("amount")).thenReturn(5);
    when(mockMetaData.getColumnType(5)).thenReturn(Types.DOUBLE);

    Object result = wrapper.getObject("amount");

    assertEquals(3.14, result);
  }

  // -----------------------------------------------------------------------
  // getTime compensation correctness test
  // -----------------------------------------------------------------------

  @Test
  public void testCompensationMatchesStaticUtilityMethod() throws SQLException {
    // Verify that the wrapper's compensation produces the same result as
    // calling the static method directly
    Time originalTime = new Time(29756000L);
    when(mockResultSet.getTime(1)).thenReturn(originalTime);

    Time wrapperResult = wrapper.getTime(1);
    Time directResult = FileResultSetWrapper.compensateTimeForTimezone(originalTime);

    assertEquals(directResult.getTime(), wrapperResult.getTime());
  }

  @Test
  public void testGetTimeZeroMillisCompensation() throws SQLException {
    Time zeroTime = new Time(0L);
    when(mockResultSet.getTime(1)).thenReturn(zeroTime);

    Time result = wrapper.getTime(1);

    Time expected = FileResultSetWrapper.compensateTimeForTimezone(zeroTime);
    assertEquals(expected.getTime(), result.getTime());
  }

  // -----------------------------------------------------------------------
  // Delegation tests - verify pass-through behavior
  // -----------------------------------------------------------------------

  @Test
  public void testNextDelegates() throws SQLException {
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);

    assertTrue(wrapper.next());
    assertFalse(wrapper.next());
    verify(mockResultSet, times(2)).next();
  }

  @Test
  public void testCloseDelegates() throws SQLException {
    wrapper.close();
    verify(mockResultSet).close();
  }

  @Test
  public void testWasNullDelegates() throws SQLException {
    when(mockResultSet.wasNull()).thenReturn(true);
    assertTrue(wrapper.wasNull());
    verify(mockResultSet).wasNull();
  }

  @Test
  public void testGetStringByIndexDelegates() throws SQLException {
    when(mockResultSet.getString(1)).thenReturn("test");
    assertEquals("test", wrapper.getString(1));
    verify(mockResultSet).getString(1);
  }

  @Test
  public void testGetStringByLabelDelegates() throws SQLException {
    when(mockResultSet.getString("name")).thenReturn("Alice");
    assertEquals("Alice", wrapper.getString("name"));
    verify(mockResultSet).getString("name");
  }

  @Test
  public void testGetIntByIndexDelegates() throws SQLException {
    when(mockResultSet.getInt(1)).thenReturn(42);
    assertEquals(42, wrapper.getInt(1));
    verify(mockResultSet).getInt(1);
  }

  @Test
  public void testGetIntByLabelDelegates() throws SQLException {
    when(mockResultSet.getInt("age")).thenReturn(30);
    assertEquals(30, wrapper.getInt("age"));
    verify(mockResultSet).getInt("age");
  }

  @Test
  public void testGetDoubleByIndexDelegates() throws SQLException {
    when(mockResultSet.getDouble(1)).thenReturn(3.14);
    assertEquals(3.14, wrapper.getDouble(1), 0.001);
    verify(mockResultSet).getDouble(1);
  }

  @Test
  public void testGetDoubleByLabelDelegates() throws SQLException {
    when(mockResultSet.getDouble("price")).thenReturn(99.99);
    assertEquals(99.99, wrapper.getDouble("price"), 0.001);
    verify(mockResultSet).getDouble("price");
  }

  @Test
  public void testGetLongByIndexDelegates() throws SQLException {
    when(mockResultSet.getLong(1)).thenReturn(123456789L);
    assertEquals(123456789L, wrapper.getLong(1));
    verify(mockResultSet).getLong(1);
  }

  @Test
  public void testGetBooleanByIndexDelegates() throws SQLException {
    when(mockResultSet.getBoolean(1)).thenReturn(true);
    assertTrue(wrapper.getBoolean(1));
    verify(mockResultSet).getBoolean(1);
  }

  @Test
  public void testGetFloatByIndexDelegates() throws SQLException {
    when(mockResultSet.getFloat(1)).thenReturn(1.5f);
    assertEquals(1.5f, wrapper.getFloat(1), 0.001f);
    verify(mockResultSet).getFloat(1);
  }

  @Test
  public void testGetDateByIndexDelegates() throws SQLException {
    Date date = new Date(1609459200000L);
    when(mockResultSet.getDate(1)).thenReturn(date);
    assertEquals(date, wrapper.getDate(1));
    verify(mockResultSet).getDate(1);
  }

  @Test
  public void testGetDateByLabelDelegates() throws SQLException {
    Date date = new Date(1609459200000L);
    when(mockResultSet.getDate("hire_date")).thenReturn(date);
    assertEquals(date, wrapper.getDate("hire_date"));
    verify(mockResultSet).getDate("hire_date");
  }

  @Test
  public void testGetTimestampByIndexDelegates() throws SQLException {
    Timestamp ts = new Timestamp(1609459200000L);
    when(mockResultSet.getTimestamp(1)).thenReturn(ts);
    assertEquals(ts, wrapper.getTimestamp(1));
    verify(mockResultSet).getTimestamp(1);
  }

  @Test
  public void testGetTimestampByLabelDelegates() throws SQLException {
    Timestamp ts = new Timestamp(1609459200000L);
    when(mockResultSet.getTimestamp("created_at")).thenReturn(ts);
    assertEquals(ts, wrapper.getTimestamp("created_at"));
    verify(mockResultSet).getTimestamp("created_at");
  }

  @Test
  public void testGetDateWithCalendarDelegates() throws SQLException {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    Date date = new Date(1609459200000L);
    when(mockResultSet.getDate(1, cal)).thenReturn(date);
    assertEquals(date, wrapper.getDate(1, cal));
    verify(mockResultSet).getDate(1, cal);
  }

  @Test
  public void testGetTimestampWithCalendarDelegates() throws SQLException {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    Timestamp ts = new Timestamp(1609459200000L);
    when(mockResultSet.getTimestamp(1, cal)).thenReturn(ts);
    assertEquals(ts, wrapper.getTimestamp(1, cal));
    verify(mockResultSet).getTimestamp(1, cal);
  }

  @Test
  public void testFindColumnDelegates() throws SQLException {
    when(mockResultSet.findColumn("empno")).thenReturn(1);
    assertEquals(1, wrapper.findColumn("empno"));
    verify(mockResultSet).findColumn("empno");
  }

  @Test
  public void testGetMetaDataDelegates() throws SQLException {
    ResultSetMetaData metaData = mock(ResultSetMetaData.class);
    when(mockResultSet.getMetaData()).thenReturn(metaData);
    assertEquals(metaData, wrapper.getMetaData());
  }

  @Test
  public void testGetByteByIndexDelegates() throws SQLException {
    when(mockResultSet.getByte(1)).thenReturn((byte) 7);
    assertEquals((byte) 7, wrapper.getByte(1));
    verify(mockResultSet).getByte(1);
  }

  @Test
  public void testGetShortByIndexDelegates() throws SQLException {
    when(mockResultSet.getShort(1)).thenReturn((short) 100);
    assertEquals((short) 100, wrapper.getShort(1));
    verify(mockResultSet).getShort(1);
  }

  @Test
  public void testGetRowDelegates() throws SQLException {
    when(mockResultSet.getRow()).thenReturn(5);
    assertEquals(5, wrapper.getRow());
    verify(mockResultSet).getRow();
  }

  @Test
  public void testIsClosedDelegates() throws SQLException {
    when(mockResultSet.isClosed()).thenReturn(false);
    assertFalse(wrapper.isClosed());
    verify(mockResultSet).isClosed();
  }

  @Test
  public void testGetTypeDelegates() throws SQLException {
    when(mockResultSet.getType()).thenReturn(ResultSet.TYPE_FORWARD_ONLY);
    assertEquals(ResultSet.TYPE_FORWARD_ONLY, wrapper.getType());
    verify(mockResultSet).getType();
  }

  @Test
  public void testGetConcurrencyDelegates() throws SQLException {
    when(mockResultSet.getConcurrency()).thenReturn(ResultSet.CONCUR_READ_ONLY);
    assertEquals(ResultSet.CONCUR_READ_ONLY, wrapper.getConcurrency());
    verify(mockResultSet).getConcurrency();
  }

  @Test
  public void testGetFetchSizeDelegates() throws SQLException {
    when(mockResultSet.getFetchSize()).thenReturn(100);
    assertEquals(100, wrapper.getFetchSize());
    verify(mockResultSet).getFetchSize();
  }

  @Test
  public void testSetFetchSizeDelegates() throws SQLException {
    wrapper.setFetchSize(200);
    verify(mockResultSet).setFetchSize(200);
  }

  @Test
  public void testGetFetchDirectionDelegates() throws SQLException {
    when(mockResultSet.getFetchDirection()).thenReturn(ResultSet.FETCH_FORWARD);
    assertEquals(ResultSet.FETCH_FORWARD, wrapper.getFetchDirection());
    verify(mockResultSet).getFetchDirection();
  }

  @Test
  public void testSetFetchDirectionDelegates() throws SQLException {
    wrapper.setFetchDirection(ResultSet.FETCH_REVERSE);
    verify(mockResultSet).setFetchDirection(ResultSet.FETCH_REVERSE);
  }

  @Test
  public void testAbsoluteDelegates() throws SQLException {
    when(mockResultSet.absolute(3)).thenReturn(true);
    assertTrue(wrapper.absolute(3));
    verify(mockResultSet).absolute(3);
  }

  @Test
  public void testRelativeDelegates() throws SQLException {
    when(mockResultSet.relative(2)).thenReturn(true);
    assertTrue(wrapper.relative(2));
    verify(mockResultSet).relative(2);
  }

  @Test
  public void testPreviousDelegates() throws SQLException {
    when(mockResultSet.previous()).thenReturn(true);
    assertTrue(wrapper.previous());
    verify(mockResultSet).previous();
  }

  @Test
  public void testFirstDelegates() throws SQLException {
    when(mockResultSet.first()).thenReturn(true);
    assertTrue(wrapper.first());
    verify(mockResultSet).first();
  }

  @Test
  public void testLastDelegates() throws SQLException {
    when(mockResultSet.last()).thenReturn(true);
    assertTrue(wrapper.last());
    verify(mockResultSet).last();
  }

  @Test
  public void testIsBeforeFirstDelegates() throws SQLException {
    when(mockResultSet.isBeforeFirst()).thenReturn(true);
    assertTrue(wrapper.isBeforeFirst());
    verify(mockResultSet).isBeforeFirst();
  }

  @Test
  public void testIsAfterLastDelegates() throws SQLException {
    when(mockResultSet.isAfterLast()).thenReturn(false);
    assertFalse(wrapper.isAfterLast());
    verify(mockResultSet).isAfterLast();
  }

  @Test
  public void testIsFirstDelegates() throws SQLException {
    when(mockResultSet.isFirst()).thenReturn(true);
    assertTrue(wrapper.isFirst());
    verify(mockResultSet).isFirst();
  }

  @Test
  public void testIsLastDelegates() throws SQLException {
    when(mockResultSet.isLast()).thenReturn(false);
    assertFalse(wrapper.isLast());
    verify(mockResultSet).isLast();
  }

  @Test
  public void testBeforeFirstDelegates() throws SQLException {
    wrapper.beforeFirst();
    verify(mockResultSet).beforeFirst();
  }

  @Test
  public void testAfterLastDelegates() throws SQLException {
    wrapper.afterLast();
    verify(mockResultSet).afterLast();
  }

  @Test
  public void testGetStatementDelegates() throws SQLException {
    when(mockResultSet.getStatement()).thenReturn(null);
    assertNull(wrapper.getStatement());
    verify(mockResultSet).getStatement();
  }

  @Test
  public void testGetHoldabilityDelegates() throws SQLException {
    when(mockResultSet.getHoldability()).thenReturn(ResultSet.HOLD_CURSORS_OVER_COMMIT);
    assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, wrapper.getHoldability());
    verify(mockResultSet).getHoldability();
  }

  @Test
  public void testGetWarningsDelegates() throws SQLException {
    when(mockResultSet.getWarnings()).thenReturn(null);
    assertNull(wrapper.getWarnings());
    verify(mockResultSet).getWarnings();
  }

  @Test
  public void testClearWarningsDelegates() throws SQLException {
    wrapper.clearWarnings();
    verify(mockResultSet).clearWarnings();
  }

  @Test
  public void testGetCursorNameDelegates() throws SQLException {
    when(mockResultSet.getCursorName()).thenReturn("cursor1");
    assertEquals("cursor1", wrapper.getCursorName());
    verify(mockResultSet).getCursorName();
  }

  @Test
  public void testUnwrapDelegates() throws SQLException {
    when(mockResultSet.unwrap(ResultSet.class)).thenReturn(mockResultSet);
    assertEquals(mockResultSet, wrapper.unwrap(ResultSet.class));
    verify(mockResultSet).unwrap(ResultSet.class);
  }

  @Test
  public void testIsWrapperForDelegates() throws SQLException {
    when(mockResultSet.isWrapperFor(ResultSet.class)).thenReturn(true);
    assertTrue(wrapper.isWrapperFor(ResultSet.class));
    verify(mockResultSet).isWrapperFor(ResultSet.class);
  }

  // -----------------------------------------------------------------------
  // getObject with Map and Class type delegation tests
  // -----------------------------------------------------------------------

  @Test
  public void testGetObjectByIndexWithMapDelegates() throws SQLException {
    java.util.Map<String, Class<?>> map = new java.util.HashMap<>();
    when(mockResultSet.getObject(1, map)).thenReturn("value");
    assertEquals("value", wrapper.getObject(1, map));
    verify(mockResultSet).getObject(1, map);
  }

  @Test
  public void testGetObjectByLabelWithMapDelegates() throws SQLException {
    java.util.Map<String, Class<?>> map = new java.util.HashMap<>();
    when(mockResultSet.getObject("col", map)).thenReturn("value");
    assertEquals("value", wrapper.getObject("col", map));
    verify(mockResultSet).getObject("col", map);
  }

  @Test
  public void testGetObjectByIndexWithClassDelegates() throws SQLException {
    when(mockResultSet.getObject(1, String.class)).thenReturn("hello");
    assertEquals("hello", wrapper.getObject(1, String.class));
    verify(mockResultSet).getObject(1, String.class);
  }

  @Test
  public void testGetObjectByLabelWithClassDelegates() throws SQLException {
    when(mockResultSet.getObject("col", Integer.class)).thenReturn(99);
    assertEquals(99, wrapper.getObject("col", Integer.class));
    verify(mockResultSet).getObject("col", Integer.class);
  }

  // -----------------------------------------------------------------------
  // Edge case: TIME column with unexpected object type
  // -----------------------------------------------------------------------

  @Test
  public void testGetObjectByIndexTimeColumnWithStringPassesThrough() throws SQLException {
    // If a TIME column somehow returns a String, it should pass through unchanged
    when(mockResultSet.getObject(1)).thenReturn("07:15:56");
    when(mockMetaData.getColumnType(1)).thenReturn(Types.TIME);

    Object result = wrapper.getObject(1);

    assertEquals("07:15:56", result);
  }

  @Test
  public void testGetObjectByLabelTimeColumnWithStringPassesThrough() throws SQLException {
    when(mockResultSet.getObject("t")).thenReturn("12:30:00");
    when(mockResultSet.findColumn("t")).thenReturn(1);
    when(mockMetaData.getColumnType(1)).thenReturn(Types.TIME);

    Object result = wrapper.getObject("t");

    assertEquals("12:30:00", result);
  }

  @Test
  public void testGetObjectByIndexTimeColumnWithLongPassesThrough() throws SQLException {
    // Long is not Integer, so it falls through without compensation
    Long longValue = 26156000L;
    when(mockResultSet.getObject(1)).thenReturn(longValue);
    when(mockMetaData.getColumnType(1)).thenReturn(Types.TIME);

    Object result = wrapper.getObject(1);

    assertEquals(longValue, result);
  }

  // -----------------------------------------------------------------------
  // Row mutation delegation tests
  // -----------------------------------------------------------------------

  @Test
  public void testRowUpdatedDelegates() throws SQLException {
    when(mockResultSet.rowUpdated()).thenReturn(false);
    assertFalse(wrapper.rowUpdated());
    verify(mockResultSet).rowUpdated();
  }

  @Test
  public void testRowInsertedDelegates() throws SQLException {
    when(mockResultSet.rowInserted()).thenReturn(false);
    assertFalse(wrapper.rowInserted());
    verify(mockResultSet).rowInserted();
  }

  @Test
  public void testRowDeletedDelegates() throws SQLException {
    when(mockResultSet.rowDeleted()).thenReturn(false);
    assertFalse(wrapper.rowDeleted());
    verify(mockResultSet).rowDeleted();
  }

  @Test
  public void testInsertRowDelegates() throws SQLException {
    wrapper.insertRow();
    verify(mockResultSet).insertRow();
  }

  @Test
  public void testUpdateRowDelegates() throws SQLException {
    wrapper.updateRow();
    verify(mockResultSet).updateRow();
  }

  @Test
  public void testDeleteRowDelegates() throws SQLException {
    wrapper.deleteRow();
    verify(mockResultSet).deleteRow();
  }

  @Test
  public void testRefreshRowDelegates() throws SQLException {
    wrapper.refreshRow();
    verify(mockResultSet).refreshRow();
  }

  @Test
  public void testCancelRowUpdatesDelegates() throws SQLException {
    wrapper.cancelRowUpdates();
    verify(mockResultSet).cancelRowUpdates();
  }

  @Test
  public void testMoveToInsertRowDelegates() throws SQLException {
    wrapper.moveToInsertRow();
    verify(mockResultSet).moveToInsertRow();
  }

  @Test
  public void testMoveToCurrentRowDelegates() throws SQLException {
    wrapper.moveToCurrentRow();
    verify(mockResultSet).moveToCurrentRow();
  }
}
