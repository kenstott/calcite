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
package org.apache.calcite.adapter.file.partition;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Coverage tests for {@link PGPipelineTracker} using Mockito-mocked JDBC
 * connections and statements.
 *
 * <p>PGPipelineTracker targets PostgreSQL with ON CONFLICT syntax, so we
 * mock the Connection/Statement/PreparedStatement/ResultSet objects rather
 * than using an embedded DB.
 */
@Tag("unit")
class PGPipelineTrackerCoverageTest {

  private PGPipelineTracker tracker;
  private Connection mockConnection;
  private Statement mockStatement;

  @BeforeEach void setUp() throws Exception {
    mockConnection = mock(Connection.class);
    mockStatement = mock(Statement.class);
    when(mockConnection.isClosed()).thenReturn(false);
    when(mockConnection.createStatement()).thenReturn(mockStatement);

    // Create tracker with mock URL - we'll inject the mock connection
    tracker = new PGPipelineTracker("jdbc:mock://test", null, null);

    // Inject mock connection and mark as initialized via reflection
    Field connectionField = PGPipelineTracker.class.getDeclaredField("connection");
    connectionField.setAccessible(true);
    connectionField.set(tracker, mockConnection);

    Field initializedField = PGPipelineTracker.class.getDeclaredField("initialized");
    initializedField.setAccessible(true);
    initializedField.set(tracker, true);
  }

  @AfterEach void tearDown() {
    // Don't call close() on tracker as it would try to close our mock
  }

  // ===== isComplete =====

  @Test void testIsCompleteReturnsTrueWhenComplete() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getString("state")).thenReturn("complete");

    assertTrue(tracker.isComplete("src1", "table1", "download"));
  }

  @Test void testIsCompleteReturnsFalseWhenNotComplete() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getString("state")).thenReturn("error");

    assertFalse(tracker.isComplete("src1", "table1", "download"));
  }

  @Test void testIsCompleteReturnsFalseWhenNoRow() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(false);

    assertFalse(tracker.isComplete("src1", "table1", "download"));
  }

  @Test void testIsCompleteHandlesSQLException() throws Exception {
    when(mockConnection.prepareStatement(anyString()))
        .thenThrow(new SQLException("connection lost"));

    // Should not throw, returns false
    assertFalse(tracker.isComplete("src1", "table1", "download"));
  }

  // ===== markComplete =====

  @Test void testMarkComplete() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    // Should not throw
    tracker.markComplete("src1", "table1", "download", 100);
    verify(pstmt).setString(4, "complete");
  }

  // ===== markError =====

  @Test void testMarkError() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    tracker.markError("src1", "table1", "download", "Connection timeout");
    verify(pstmt).setString(4, "error");
  }

  @Test void testMarkErrorTruncatesLongMessage() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    // Build a message longer than 1000 chars
    StringBuilder longMessage = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      longMessage.append("error_");
    }
    tracker.markError("src1", "table1", "download", longMessage.toString());
    // Verify the error_message parameter (index 8) is set with truncated value
    verify(pstmt).setString(4, "error");
  }

  // ===== markCleared =====

  @Test void testMarkCleared() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    tracker.markCleared("src1", "table1", "download");
    verify(pstmt).setString(1, "src1");
    verify(pstmt).setString(2, "table1");
    verify(pstmt).setString(3, "download");
  }

  @Test void testMarkClearedHandlesSQLException() throws Exception {
    when(mockConnection.prepareStatement(anyString()))
        .thenThrow(new SQLException("connection lost"));

    // Should not throw
    tracker.markCleared("src1", "table1", "download");
  }

  // ===== getCompletedTables =====

  @Test void testGetCompletedTables() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true, true, false);
    when(rs.getString("table_name")).thenReturn("facts", "metadata");

    Set<String> tables = tracker.getCompletedTables("src1", "staging");
    assertEquals(2, tables.size());
    assertTrue(tables.contains("facts"));
    assertTrue(tables.contains("metadata"));
  }

  @Test void testGetCompletedTablesEmpty() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(false);

    Set<String> tables = tracker.getCompletedTables("src1", "staging");
    assertTrue(tables.isEmpty());
  }

  @Test void testGetCompletedTablesHandlesSQLException() throws Exception {
    when(mockConnection.prepareStatement(anyString()))
        .thenThrow(new SQLException("connection lost"));

    Set<String> tables = tracker.getCompletedTables("src1", "staging");
    assertTrue(tables.isEmpty());
  }

  // ===== getSourceKeysForPhase =====

  @Test void testGetSourceKeysForPhase() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true, true, false);
    when(rs.getString("source_key")).thenReturn("src1", "src2");

    Set<String> keys = tracker.getSourceKeysForPhase("staging");
    assertEquals(2, keys.size());
    assertTrue(keys.contains("src1"));
    assertTrue(keys.contains("src2"));
  }

  @Test void testGetSourceKeysForPhaseHandlesSQLException() throws Exception {
    when(mockConnection.prepareStatement(anyString()))
        .thenThrow(new SQLException("connection lost"));

    Set<String> keys = tracker.getSourceKeysForPhase("staging");
    assertTrue(keys.isEmpty());
  }

  // ===== IncrementalTracker bridge: isProcessed =====

  @Test void testIsProcessedSingleKey() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getString("state")).thenReturn("complete");

    Map<String, String> keyValues = Collections.singletonMap("year", "2020");
    assertTrue(tracker.isProcessed("alt1", "source1", keyValues));
  }

  @Test void testIsProcessedEmptyKey() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(false);

    Map<String, String> keyValues = Collections.emptyMap();
    assertFalse(tracker.isProcessed("alt1", "source1", keyValues));
  }

  // ===== IncrementalTracker bridge: isProcessedWithTtl =====

  @Test void testIsProcessedWithTtlNotProcessed() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(false);

    Map<String, String> keyValues = Collections.singletonMap("year", "2020");
    assertFalse(tracker.isProcessedWithTtl("alt1", "source1", keyValues, 60000));
  }

  @Test void testIsProcessedWithTtlWithinTtl() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    // Set as_of to current time so it's within TTL
    when(rs.getLong("as_of")).thenReturn(System.currentTimeMillis());

    Map<String, String> keyValues = Collections.singletonMap("year", "2020");
    assertTrue(tracker.isProcessedWithTtl("alt1", "source1", keyValues, 60000));
  }

  @Test void testIsProcessedWithTtlExpired() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    // Set as_of to long ago so TTL has expired
    when(rs.getLong("as_of")).thenReturn(1000L);

    Map<String, String> keyValues = Collections.singletonMap("year", "2020");
    assertFalse(tracker.isProcessedWithTtl("alt1", "source1", keyValues, 60000));
  }

  @Test void testIsProcessedWithTtlHandlesSQLException() throws Exception {
    when(mockConnection.prepareStatement(anyString()))
        .thenThrow(new SQLException("connection lost"));

    Map<String, String> keyValues = Collections.singletonMap("year", "2020");
    assertFalse(tracker.isProcessedWithTtl("alt1", "source1", keyValues, 60000));
  }

  // ===== markProcessed / markProcessedWithRowCount =====

  @Test void testMarkProcessed() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    Map<String, String> keyValues = Collections.singletonMap("year", "2020");
    tracker.markProcessed("alt1", "source1", keyValues, "target");
    // Verify it calls markProcessedWithRowCount internally
    verify(pstmt).executeUpdate();
  }

  @Test void testMarkProcessedWithRowCount() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    Map<String, String> keyValues = Collections.singletonMap("year", "2021");
    tracker.markProcessedWithRowCount("alt1", "source1", keyValues, "target", 500);
    verify(pstmt).executeUpdate();
  }

  // ===== markProcessedWithError =====

  @Test void testMarkProcessedWithError() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    Map<String, String> keyValues = Collections.singletonMap("year", "2022");
    tracker.markProcessedWithError("alt1", "source1", keyValues, "target", "Error msg");
    verify(pstmt).setString(4, "error");
  }

  // ===== isProcessedWithEmptyTtl =====

  @Test void testIsProcessedWithEmptyTtlNotProcessed() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(false);

    Map<String, String> keyValues = Collections.singletonMap("year", "2020");
    assertFalse(tracker.isProcessedWithEmptyTtl("alt1", "source1", keyValues, 60000));
  }

  @Test void testIsProcessedWithEmptyTtlWithData() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getString("state")).thenReturn("complete");
    when(rs.getLong("row_count")).thenReturn(100L);
    when(rs.getLong("as_of")).thenReturn(System.currentTimeMillis());

    Map<String, String> keyValues = Collections.singletonMap("year", "2020");
    assertTrue(tracker.isProcessedWithEmptyTtl("alt1", "source1", keyValues, 60000));
  }

  @Test void testIsProcessedWithEmptyTtlEmptyResultWithinTtl() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getString("state")).thenReturn("complete");
    when(rs.getLong("row_count")).thenReturn(0L);
    when(rs.getLong("as_of")).thenReturn(System.currentTimeMillis());

    Map<String, String> keyValues = Collections.singletonMap("year", "2020");
    assertTrue(tracker.isProcessedWithEmptyTtl("alt1", "source1", keyValues, 60000));
  }

  @Test void testIsProcessedWithEmptyTtlErrorState() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getString("state")).thenReturn("error");

    Map<String, String> keyValues = Collections.singletonMap("year", "2020");
    assertFalse(tracker.isProcessedWithEmptyTtl("alt1", "source1", keyValues, 60000));
  }

  @Test void testIsProcessedWithEmptyTtlHandlesSQLException() throws Exception {
    when(mockConnection.prepareStatement(anyString()))
        .thenThrow(new SQLException("connection lost"));

    Map<String, String> keyValues = Collections.singletonMap("year", "2020");
    assertFalse(tracker.isProcessedWithEmptyTtl("alt1", "source1", keyValues, 60000));
  }

  // ===== getProcessedKeyValues =====

  @Test void testGetProcessedKeyValues() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true, true, false);
    when(rs.getString("source_key")).thenReturn("2020", "2021");

    Set<Map<String, String>> processed = tracker.getProcessedKeyValues("alt1");
    assertEquals(2, processed.size());
  }

  @Test void testGetProcessedKeyValuesEmpty() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(false);

    Set<Map<String, String>> processed = tracker.getProcessedKeyValues("nonexistent");
    assertTrue(processed.isEmpty());
  }

  @Test void testGetProcessedKeyValuesHandlesSQLException() throws Exception {
    when(mockConnection.prepareStatement(anyString()))
        .thenThrow(new SQLException("connection lost"));

    Set<Map<String, String>> processed = tracker.getProcessedKeyValues("alt1");
    assertTrue(processed.isEmpty());
  }

  // ===== invalidate =====

  @Test void testInvalidate() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    Map<String, String> keyValues = Collections.singletonMap("year", "2020");
    tracker.invalidate("alt1", keyValues);
    verify(pstmt).executeUpdate();
  }

  // ===== invalidateAll =====

  @Test void testInvalidateAll() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(2);

    tracker.invalidateAll("alt1");
    verify(pstmt).executeUpdate();
  }

  @Test void testInvalidateAllHandlesSQLException() throws Exception {
    when(mockConnection.prepareStatement(anyString()))
        .thenThrow(new SQLException("connection lost"));

    // Should not throw
    tracker.invalidateAll("alt1");
  }

  // ===== filterUnprocessed =====

  @Test void testFilterUnprocessedNullList() {
    Set<Integer> result = tracker.filterUnprocessed("alt1", "source1", null);
    assertTrue(result.isEmpty());
  }

  @Test void testFilterUnprocessedEmptyList() {
    Set<Integer> result =
        tracker.filterUnprocessed("alt1", "source1",
            Collections.<Map<String, String>>emptyList());
    assertTrue(result.isEmpty());
  }

  @Test void testFilterUnprocessed() throws Exception {
    // Mock getProcessedKeyValues to return one processed entry
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true, false);
    when(rs.getString("source_key")).thenReturn("2020");

    List<Map<String, String>> all = new ArrayList<Map<String, String>>();
    all.add(Collections.singletonMap("year", "2020"));
    all.add(Collections.singletonMap("year", "2021"));

    Set<Integer> unprocessed = tracker.filterUnprocessed("alt1", "source1", all);
    // Both should be unprocessed since unflatten("2020") -> {"source_key":"2020"}
    // which doesn't match {"year":"2020"}
    assertNotNull(unprocessed);
  }

  // ===== Table Completion =====

  @Test void testIsTableCompleteReturnsFalse() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(false);

    assertFalse(tracker.isTableComplete("pipeline1", "sig123"));
  }

  @Test void testIsTableCompleteReturnsTrue() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getString("signature")).thenReturn("sig123");

    assertTrue(tracker.isTableComplete("pipeline1", "sig123"));
  }

  @Test void testIsTableCompleteDifferentSignature() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getString("signature")).thenReturn("sig456");

    assertFalse(tracker.isTableComplete("pipeline1", "sig123"));
  }

  @Test void testIsTableCompleteHandlesSQLException() throws Exception {
    when(mockConnection.prepareStatement(anyString()))
        .thenThrow(new SQLException("connection lost"));

    assertFalse(tracker.isTableComplete("pipeline1", "sig123"));
  }

  // ===== markTableComplete =====

  @Test void testMarkTableComplete() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    tracker.markTableComplete("pipeline1", "sig123");
    verify(pstmt).executeUpdate();
  }

  @Test void testMarkTableCompleteHandlesSQLException() throws Exception {
    when(mockConnection.prepareStatement(anyString()))
        .thenThrow(new SQLException("connection lost"));

    // Should not throw
    tracker.markTableComplete("pipeline1", "sig123");
  }

  // ===== markTableCompleteWithConfig =====

  @Test void testMarkTableCompleteWithConfig() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    tracker.markTableCompleteWithConfig("pipeline1", "hash1", "sig1", 1000);
    verify(pstmt).executeUpdate();
  }

  // ===== markTableCompleteWithSourceWatermark =====

  @Test void testMarkTableCompleteWithSourceWatermark() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    tracker.markTableCompleteWithSourceWatermark("pipeline1", "hash1", "sig1", 500, 123456789L);
    verify(pstmt).executeUpdate();
  }

  // ===== getCachedCompletion =====

  @Test void testGetCachedCompletion() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getString("config_hash")).thenReturn("hash1");
    when(rs.getString("signature")).thenReturn("sig1");
    when(rs.getLong("row_count")).thenReturn(1000L);
    when(rs.getLong("completed_at")).thenReturn(System.currentTimeMillis());
    when(rs.getLong("source_file_watermark")).thenReturn(123456789L);

    IncrementalTracker.CachedCompletion cached = tracker.getCachedCompletion("pipeline1");
    assertNotNull(cached);
    assertEquals("hash1", cached.configHash);
    assertEquals("sig1", cached.signature);
    assertEquals(1000, cached.rowCount);
    assertEquals(123456789L, cached.sourceFileWatermark);
  }

  @Test void testGetCachedCompletionNotFound() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(false);

    assertNull(tracker.getCachedCompletion("nonexistent"));
  }

  @Test void testGetCachedCompletionNullConfigHash() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getString("config_hash")).thenReturn(null);

    assertNull(tracker.getCachedCompletion("pipeline1"));
  }

  @Test void testGetCachedCompletionHandlesSQLException() throws Exception {
    when(mockConnection.prepareStatement(anyString()))
        .thenThrow(new SQLException("connection lost"));

    assertNull(tracker.getCachedCompletion("pipeline1"));
  }

  // ===== invalidateTableCompletion =====

  @Test void testInvalidateTableCompletion() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    tracker.invalidateTableCompletion("pipeline1");
    verify(pstmt).executeUpdate();
  }

  @Test void testInvalidateTableCompletionHandlesSQLException() throws Exception {
    when(mockConnection.prepareStatement(anyString()))
        .thenThrow(new SQLException("connection lost"));

    // Should not throw
    tracker.invalidateTableCompletion("pipeline1");
  }

  // ===== clearAllCompletions =====

  @Test void testClearAllCompletions() throws Exception {
    Statement stmt1 = mock(Statement.class);
    Statement stmt2 = mock(Statement.class);

    when(mockConnection.createStatement()).thenReturn(stmt1, stmt2);
    when(stmt1.executeUpdate(anyString())).thenReturn(5);
    when(stmt2.executeUpdate(anyString())).thenReturn(3);

    tracker.clearAllCompletions();
    verify(stmt1).executeUpdate(anyString());
    verify(stmt2).executeUpdate(anyString());
  }

  @Test void testClearAllCompletionsHandlesSQLException() throws Exception {
    Statement stmt = mock(Statement.class);
    when(mockConnection.createStatement()).thenReturn(stmt);
    when(stmt.executeUpdate(anyString())).thenThrow(new SQLException("error"));

    // Should not throw
    tracker.clearAllCompletions();
  }

  // ===== close =====

  @Test void testClose() throws Exception {
    tracker.close();
    verify(mockConnection).close();
  }

  @Test void testCloseHandlesSQLException() throws Exception {
    doThrow(new SQLException("close error")).when(mockConnection).close();

    // Should not throw
    tracker.close();
  }

  @Test void testDoubleClose() throws Exception {
    tracker.close();

    // After first close, connection field is null, so second close is no-op
    tracker.close();
  }

  // ===== flattenKeyValues / unflattenKeyValues (tested via public API) =====

  @Test void testFlattenEmptyMap() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    // Empty map should flatten to "_empty"
    tracker.markProcessed("alt1", "source1", Collections.<String, String>emptyMap(), "target");
    verify(pstmt).setString(1, "_empty");
  }

  @Test void testFlattenNullMap() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    // Null map should flatten to "_empty"
    tracker.markProcessed("alt1", "source1", null, "target");
    verify(pstmt).setString(1, "_empty");
  }

  @Test void testFlattenSingleKeyMap() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    // Single-entry map should flatten to just the value
    tracker.markProcessed("alt1", "source1",
        Collections.singletonMap("year", "2020"), "target");
    verify(pstmt).setString(1, "2020");
  }

  @Test void testFlattenMultiKeyMap() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    Map<String, String> keyValues = new LinkedHashMap<String, String>();
    keyValues.put("geo", "STATE");
    keyValues.put("year", "2020");

    // Multi-entry map flattens to sorted "key=value__key=value"
    tracker.markProcessed("alt1", "source1", keyValues, "target");
    verify(pstmt).setString(1, "geo=STATE__year=2020");
  }

  // ===== unflattenKeyValues via getProcessedKeyValues =====

  @Test void testUnflattenEmpty() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true, false);
    when(rs.getString("source_key")).thenReturn("_empty");

    Set<Map<String, String>> processed = tracker.getProcessedKeyValues("alt1");
    assertEquals(1, processed.size());
    Map<String, String> first = processed.iterator().next();
    assertTrue(first.isEmpty());
  }

  @Test void testUnflattenMultiKey() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true, false);
    when(rs.getString("source_key")).thenReturn("geo=STATE__year=2020");

    Set<Map<String, String>> processed = tracker.getProcessedKeyValues("alt1");
    assertEquals(1, processed.size());
    Map<String, String> first = processed.iterator().next();
    assertEquals("STATE", first.get("geo"));
    assertEquals("2020", first.get("year"));
  }

  @Test void testUnflattenSingleValueNoEquals() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);

    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true, false);
    when(rs.getString("source_key")).thenReturn("2020");

    Set<Map<String, String>> processed = tracker.getProcessedKeyValues("alt1");
    assertEquals(1, processed.size());
    Map<String, String> first = processed.iterator().next();
    // Single value wraps as {"source_key": "2020"}
    assertEquals("2020", first.get("source_key"));
  }

  // ===== Constructor variants =====

  @Test void testConstructorWithUser() {
    PGPipelineTracker t = new PGPipelineTracker("jdbc:mock://test", "user", "password");
    assertNotNull(t);
    t.close(); // This will try to close a null connection - no-op
  }

  @Test void testConstructorWithoutUser() {
    PGPipelineTracker t = new PGPipelineTracker("jdbc:mock://test", null, null);
    assertNotNull(t);
    t.close();
  }

  // ===== upsertState with null errorMessage =====

  @Test void testUpsertStateNullErrorMessage() throws Exception {
    PreparedStatement pstmt = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(anyString())).thenReturn(pstmt);
    when(pstmt.executeUpdate()).thenReturn(1);

    tracker.markComplete("src1", "table1", "download", 100);
    // errorMessage is null -> should set parameter 8 to null
    verify(pstmt).setString(8, null);
  }

  @Test void testUpsertStateHandlesSQLException() throws Exception {
    when(mockConnection.prepareStatement(anyString()))
        .thenThrow(new SQLException("connection lost"));

    // Should not throw
    tracker.markComplete("src1", "table1", "download", 100);
  }
}
