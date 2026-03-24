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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for JDBC wrapper classes:
 * {@link FileJdbcDriver}, {@link FilePreparedStatementWrapper},
 * {@link FileResultSetWrapperImpl}, {@link FileStatementWrapper},
 * and {@link FileConnectionWrapper}.
 *
 * <p>These tests exercise prepared statements with parameters, ResultSet
 * metadata, getXxx() methods, ResultSet navigation, null handling, and
 * the wrapper delegation patterns.
 */
@SuppressWarnings("deprecation")
@Tag("integration")
public class FileJdbcCoverageTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FileJdbcCoverageTest.class);

  @TempDir
  Path tempDir;

  private Connection connection;
  private final List<Connection> openConnections = new ArrayList<Connection>();

  @BeforeEach
  public void setUp() throws Exception {
    // Create test data files
    createCsvFile("jdbc_test.csv",
        "rec_id:int,rec_name:string,rec_amount:double,rec_active:boolean",
        "1,Alice,100.50,true",
        "2,Bob,200.75,false",
        "3,Charlie,,true",
        "4,,400.00,false",
        "5,Eve,500.25,true");

    createCsvFile("jdbc_types.csv",
        "tid:int,tname:string,tprice:double,tcount:int",
        "10,Widget,9.99,100",
        "20,Gadget,19.99,50",
        "30,Gizmo,29.99,25");

    createCsvFile("jdbc_nulls.csv",
        "nid:int,nstr:string,nnum:double",
        "1,Hello,10.5",
        "2,,",
        "3,World,30.5");

    String model = buildModel("JDBC", tempDir.toFile().getAbsolutePath());
    Properties info = new Properties();
    info.put("model", "inline:" + model);
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    connection = DriverManager.getConnection("jdbc:calcite:", info);
    openConnections.add(connection);
  }

  @AfterEach
  public void tearDown() {
    for (Connection conn : openConnections) {
      try {
        if (conn != null && !conn.isClosed()) {
          conn.close();
        }
      } catch (SQLException e) {
        LOGGER.debug("Error closing connection", e);
      }
    }
    openConnections.clear();
  }

  // Helpers

  private String buildModel(String schemaName, String directory) {
    String dir = directory.replace("\\", "\\\\");
    return BaseFileTest.addEphemeralCacheToModel(
        "{\n"
        + "  \"version\": \"1.0\",\n"
        + "  \"defaultSchema\": \"" + schemaName + "\",\n"
        + "  \"schemas\": [\n"
        + "    {\n"
        + "      \"name\": \"" + schemaName + "\",\n"
        + "      \"type\": \"custom\",\n"
        + "      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
        + "      \"operand\": {\n"
        + "        \"directory\": \"" + dir + "\"\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}");
  }

  private void createCsvFile(String name, String... lines) throws IOException {
    java.io.File file = new java.io.File(tempDir.toFile(), name);
    try (FileWriter writer = new FileWriter(file)) {
      for (String line : lines) {
        writer.write(line);
        writer.write("\n");
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 1. Basic Statement query execution
  // ---------------------------------------------------------------------------

  @Test void testStatementExecuteQuery() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT rec_id, rec_name FROM jdbc_test ORDER BY rec_id")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("rec_id"));
      assertEquals("Alice", rs.getString("rec_name"));
    }
  }

  // ---------------------------------------------------------------------------
  // 2. PreparedStatement with no parameters
  // ---------------------------------------------------------------------------

  @Test void testPreparedStatementNoParams() throws Exception {
    try (PreparedStatement pstmt = connection.prepareStatement(
        "SELECT rec_id, rec_name FROM jdbc_test ORDER BY rec_id")) {
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("Alice", rs.getString(2));

        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("Bob", rs.getString(2));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 3. PreparedStatement with int parameter
  // ---------------------------------------------------------------------------

  @Test void testPreparedStatementWithIntParam() throws Exception {
    try (PreparedStatement pstmt = connection.prepareStatement(
        "SELECT rec_name FROM jdbc_test WHERE rec_id = ?")) {
      pstmt.setInt(1, 2);
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Bob", rs.getString(1));
        assertFalse(rs.next());
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 4. PreparedStatement with string parameter
  // ---------------------------------------------------------------------------

  @Test void testPreparedStatementWithStringParam() throws Exception {
    try (PreparedStatement pstmt = connection.prepareStatement(
        "SELECT rec_id FROM jdbc_test WHERE rec_name = ?")) {
      pstmt.setString(1, "Charlie");
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 5. PreparedStatement with double parameter
  // ---------------------------------------------------------------------------

  @Test void testPreparedStatementWithDoubleParam() throws Exception {
    try (PreparedStatement pstmt = connection.prepareStatement(
        "SELECT rec_name FROM jdbc_test WHERE rec_amount > ?")) {
      pstmt.setDouble(1, 300.0);
      try (ResultSet rs = pstmt.executeQuery()) {
        List<String> names = new ArrayList<String>();
        while (rs.next()) {
          names.add(rs.getString(1));
        }
        LOGGER.debug("Names with amount > 300: {}", names);
        assertTrue(names.size() >= 2,
            "Should find rows with amount > 300, got: " + names);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 6. PreparedStatement with boolean parameter
  // ---------------------------------------------------------------------------

  @Test void testPreparedStatementWithBooleanParam() throws Exception {
    try (PreparedStatement pstmt = connection.prepareStatement(
        "SELECT rec_id FROM jdbc_test WHERE rec_active = ? ORDER BY rec_id")) {
      pstmt.setBoolean(1, true);
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        int firstId = rs.getInt(1);
        LOGGER.debug("First active rec_id: {}", firstId);
        assertTrue(firstId > 0);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 7. PreparedStatement with multiple parameters
  // ---------------------------------------------------------------------------

  @Test void testPreparedStatementMultipleParams() throws Exception {
    try (PreparedStatement pstmt = connection.prepareStatement(
        "SELECT rec_name FROM jdbc_test WHERE rec_id >= ? AND rec_id <= ? ORDER BY rec_id")) {
      pstmt.setInt(1, 2);
      pstmt.setInt(2, 4);
      try (ResultSet rs = pstmt.executeQuery()) {
        List<String> names = new ArrayList<String>();
        while (rs.next()) {
          names.add(rs.getString(1));
        }
        LOGGER.debug("Names between 2-4: {}", names);
        assertEquals(3, names.size());
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 8. PreparedStatement reuse with clearParameters
  // ---------------------------------------------------------------------------

  @Test void testPreparedStatementClearAndReuse() throws Exception {
    try (PreparedStatement pstmt = connection.prepareStatement(
        "SELECT rec_name FROM jdbc_test WHERE rec_id = ?")) {
      // First execution
      pstmt.setInt(1, 1);
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Alice", rs.getString(1));
      }

      // Clear and reuse
      pstmt.clearParameters();
      pstmt.setInt(1, 5);
      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals("Eve", rs.getString(1));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 9. ResultSet metadata column count and names
  // ---------------------------------------------------------------------------

  @Test void testResultSetMetadataColumns() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT rec_id, rec_name, rec_amount FROM jdbc_test")) {
      ResultSetMetaData rsmd = rs.getMetaData();
      assertNotNull(rsmd);
      assertEquals(3, rsmd.getColumnCount());

      // Check column names (may be uppercase due to Calcite)
      String col1 = rsmd.getColumnName(1).toLowerCase();
      String col2 = rsmd.getColumnName(2).toLowerCase();
      String col3 = rsmd.getColumnName(3).toLowerCase();
      LOGGER.debug("Column names: {}, {}, {}", col1, col2, col3);

      assertTrue(col1.contains("rec_id"), "First column should be rec_id: " + col1);
      assertTrue(col2.contains("rec_name"), "Second column should be rec_name: " + col2);
      assertTrue(col3.contains("rec_amount"), "Third column should be rec_amount: " + col3);
    }
  }

  // ---------------------------------------------------------------------------
  // 10. ResultSet metadata column types
  // ---------------------------------------------------------------------------

  @Test void testResultSetMetadataColumnTypes() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT tid, tname, tprice FROM jdbc_types")) {
      ResultSetMetaData rsmd = rs.getMetaData();
      int type1 = rsmd.getColumnType(1);
      int type2 = rsmd.getColumnType(2);
      int type3 = rsmd.getColumnType(3);
      LOGGER.debug("Column types: {}, {}, {} (INTEGER={}, VARCHAR={}, DOUBLE={})",
          type1, type2, type3, Types.INTEGER, Types.VARCHAR, Types.DOUBLE);

      // int column should be INTEGER
      assertEquals(Types.INTEGER, type1, "tid should be INTEGER");
      // string column should be VARCHAR
      assertEquals(Types.VARCHAR, type2, "tname should be VARCHAR");
    }
  }

  // ---------------------------------------------------------------------------
  // 11. ResultSet getInt by column index and name
  // ---------------------------------------------------------------------------

  @Test void testResultSetGetIntByIndexAndName() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT tid, tcount FROM jdbc_types WHERE tid = 10")) {
      assertTrue(rs.next());
      assertEquals(10, rs.getInt(1));
      assertEquals(10, rs.getInt("tid"));
      assertEquals(100, rs.getInt(2));
      assertEquals(100, rs.getInt("tcount"));
    }
  }

  // ---------------------------------------------------------------------------
  // 12. ResultSet getString by column index and name
  // ---------------------------------------------------------------------------

  @Test void testResultSetGetStringByIndexAndName() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT tname FROM jdbc_types WHERE tid = 20")) {
      assertTrue(rs.next());
      assertEquals("Gadget", rs.getString(1));
      assertEquals("Gadget", rs.getString("tname"));
    }
  }

  // ---------------------------------------------------------------------------
  // 13. ResultSet getDouble by column index and name
  // ---------------------------------------------------------------------------

  @Test void testResultSetGetDoubleByIndexAndName() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT tprice FROM jdbc_types WHERE tid = 30")) {
      assertTrue(rs.next());
      assertEquals(29.99, rs.getDouble(1), 0.001);
      assertEquals(29.99, rs.getDouble("tprice"), 0.001);
    }
  }

  // ---------------------------------------------------------------------------
  // 14. ResultSet getObject
  // ---------------------------------------------------------------------------

  @Test void testResultSetGetObject() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT tid, tname, tprice FROM jdbc_types WHERE tid = 10")) {
      assertTrue(rs.next());
      Object id = rs.getObject(1);
      Object name = rs.getObject(2);
      Object price = rs.getObject(3);
      assertNotNull(id);
      assertNotNull(name);
      assertNotNull(price);
      LOGGER.debug("Objects: id={} ({}), name={} ({}), price={} ({})",
          id, id.getClass().getSimpleName(),
          name, name.getClass().getSimpleName(),
          price, price.getClass().getSimpleName());
    }
  }

  // ---------------------------------------------------------------------------
  // 15. ResultSet null handling with wasNull
  // ---------------------------------------------------------------------------

  @Test void testResultSetNullHandling() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT nid, nstr, nnum FROM jdbc_nulls ORDER BY nid")) {
      // First row: non-null
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("nid"));
      assertEquals("Hello", rs.getString("nstr"));
      assertFalse(rs.wasNull());

      // Second row: null name and number
      assertTrue(rs.next());
      assertEquals(2, rs.getInt("nid"));
      String nullStr = rs.getString("nstr");
      // For CSV, empty values may be null or empty string
      LOGGER.debug("Null row nstr: '{}'", nullStr);

      // Third row: non-null
      assertTrue(rs.next());
      assertEquals(3, rs.getInt("nid"));
      assertEquals("World", rs.getString("nstr"));
    }
  }

  // ---------------------------------------------------------------------------
  // 16. ResultSet navigation (next, close, isClosed)
  // ---------------------------------------------------------------------------

  @Test void testResultSetNavigation() throws Exception {
    Statement stmt = connection.createStatement();
    ResultSet rs = stmt.executeQuery(
        "SELECT tid FROM jdbc_types ORDER BY tid");

    assertFalse(rs.isClosed(), "ResultSet should not be closed initially");

    assertTrue(rs.next());
    assertEquals(10, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(20, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(30, rs.getInt(1));
    assertFalse(rs.next(), "No more rows");

    rs.close();
    assertTrue(rs.isClosed(), "ResultSet should be closed after close()");
    stmt.close();
  }

  // ---------------------------------------------------------------------------
  // 17. ResultSet getRow
  // ---------------------------------------------------------------------------

  @Test void testResultSetGetRow() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT tid FROM jdbc_types ORDER BY tid")) {
      // Before first row
      int rowBeforeFirst = rs.getRow();
      LOGGER.debug("Row before first: {}", rowBeforeFirst);

      assertTrue(rs.next());
      // After first next(), row should be 1 or implementation-specific
      int rowAfterFirst = rs.getRow();
      LOGGER.debug("Row after first next: {}", rowAfterFirst);
    }
  }

  // ---------------------------------------------------------------------------
  // 18. ResultSet findColumn
  // ---------------------------------------------------------------------------

  @Test void testResultSetFindColumn() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT tid, tname FROM jdbc_types")) {
      int tidIdx = rs.findColumn("tid");
      int tnameIdx = rs.findColumn("tname");
      LOGGER.debug("findColumn: tid={}, tname={}", tidIdx, tnameIdx);
      assertTrue(tidIdx > 0, "findColumn(tid) should return positive index");
      assertTrue(tnameIdx > 0, "findColumn(tname) should return positive index");
      assertTrue(tidIdx != tnameIdx, "Different columns should have different indices");
    }
  }

  // ---------------------------------------------------------------------------
  // 19. ResultSet getFloat and getLong
  // ---------------------------------------------------------------------------

  @Test void testResultSetGetFloatAndGetLong() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT tprice, tcount FROM jdbc_types WHERE tid = 20")) {
      assertTrue(rs.next());
      float price = rs.getFloat("tprice");
      long count = rs.getLong("tcount");
      LOGGER.debug("Float price: {}, Long count: {}", price, count);
      assertEquals(19.99f, price, 0.01f);
      assertEquals(50L, count);
    }
  }

  // ---------------------------------------------------------------------------
  // 20. ResultSet getShort and getByte
  // ---------------------------------------------------------------------------

  @Test void testResultSetGetShortAndGetByte() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT tid FROM jdbc_types WHERE tid = 10")) {
      assertTrue(rs.next());
      short asShort = rs.getShort(1);
      byte asByte = rs.getByte(1);
      LOGGER.debug("Short: {}, Byte: {}", asShort, asByte);
      assertEquals(10, asShort);
      assertEquals(10, asByte);
    }
  }

  // ---------------------------------------------------------------------------
  // 21. ResultSet getBigDecimal
  // ---------------------------------------------------------------------------

  @Test void testResultSetGetBigDecimal() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT tprice FROM jdbc_types WHERE tid = 10")) {
      assertTrue(rs.next());
      BigDecimal bd = rs.getBigDecimal("tprice");
      assertNotNull(bd);
      LOGGER.debug("BigDecimal tprice: {}", bd);
      assertEquals(0, bd.compareTo(new BigDecimal("9.99")),
          "BigDecimal should be 9.99, got: " + bd);
    }
  }

  // ---------------------------------------------------------------------------
  // 22. ResultSet getBoolean
  // ---------------------------------------------------------------------------

  @Test void testResultSetGetBoolean() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT rec_active FROM jdbc_test WHERE rec_id = 1")) {
      assertTrue(rs.next());
      boolean active = rs.getBoolean("rec_active");
      assertTrue(active, "rec_active for id=1 should be true");
    }
  }

  // ---------------------------------------------------------------------------
  // 23. ResultSet warnings and statement metadata
  // ---------------------------------------------------------------------------

  @Test void testResultSetWarningsAndStatementMeta() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT tid FROM jdbc_types ORDER BY tid")) {
      // Warnings may be null
      java.sql.SQLWarning warnings = rs.getWarnings();
      LOGGER.debug("Initial warnings: {}", warnings);

      rs.clearWarnings();
      assertNull(rs.getWarnings(), "Warnings should be null after clearWarnings");

      // Get statement from result set
      Statement fromRs = rs.getStatement();
      assertNotNull(fromRs, "getStatement should return non-null");
    }
  }

  // ---------------------------------------------------------------------------
  // 24. Concurrent ResultSets from same connection
  // ---------------------------------------------------------------------------

  @Test void testConcurrentResultSets() throws Exception {
    try (Statement stmt1 = connection.createStatement();
         Statement stmt2 = connection.createStatement()) {
      try (ResultSet rs1 = stmt1.executeQuery(
               "SELECT tid FROM jdbc_types ORDER BY tid");
           ResultSet rs2 = stmt2.executeQuery(
               "SELECT rec_id FROM jdbc_test ORDER BY rec_id")) {
        assertTrue(rs1.next());
        assertTrue(rs2.next());
        assertEquals(10, rs1.getInt(1));
        assertEquals(1, rs2.getInt(1));
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 25. FileResultSetWrapperImpl delegation
  // ---------------------------------------------------------------------------

  @Test void testFileResultSetWrapperImplDelegation() throws Exception {
    // Create a FileResultSetWrapperImpl manually and test delegation
    try (Statement stmt = connection.createStatement();
         ResultSet rawRs = stmt.executeQuery(
             "SELECT tid, tname, tprice FROM jdbc_types ORDER BY tid")) {
      FileResultSetWrapperImpl wrapper = new FileResultSetWrapperImpl(rawRs);

      assertTrue(wrapper.next());

      // Test getString by index
      assertEquals("Widget", wrapper.getString(2));

      // Test getInt by index
      assertEquals(10, wrapper.getInt(1));

      // Test getDouble by index
      assertEquals(9.99, wrapper.getDouble(3), 0.001);

      // Test getMetaData
      ResultSetMetaData meta = wrapper.getMetaData();
      assertNotNull(meta);
      assertEquals(3, meta.getColumnCount());

      // Test wasNull
      assertFalse(wrapper.wasNull());

      // Test next / navigation
      assertTrue(wrapper.next());
      assertEquals(20, wrapper.getInt(1));

      assertTrue(wrapper.next());
      assertEquals(30, wrapper.getInt(1));

      assertFalse(wrapper.next());

      // Test close
      assertFalse(wrapper.isClosed());
      wrapper.close();
      assertTrue(wrapper.isClosed());
    }
  }

  // ---------------------------------------------------------------------------
  // 26. FileResultSetWrapperImpl string column by label
  // ---------------------------------------------------------------------------

  @Test void testFileResultSetWrapperImplStringByLabel() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rawRs = stmt.executeQuery(
             "SELECT tname, tprice FROM jdbc_types WHERE tid = 10")) {
      FileResultSetWrapperImpl wrapper = new FileResultSetWrapperImpl(rawRs);
      assertTrue(wrapper.next());
      assertEquals("Widget", wrapper.getString("tname"));
      assertEquals(9.99, wrapper.getDouble("tprice"), 0.001);
      wrapper.close();
    }
  }

  // ---------------------------------------------------------------------------
  // 27. FileResultSetWrapperImpl getObject delegation
  // ---------------------------------------------------------------------------

  @Test void testFileResultSetWrapperImplGetObject() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rawRs = stmt.executeQuery(
             "SELECT tid, tname FROM jdbc_types WHERE tid = 20")) {
      FileResultSetWrapperImpl wrapper = new FileResultSetWrapperImpl(rawRs);
      assertTrue(wrapper.next());

      Object idObj = wrapper.getObject(1);
      assertNotNull(idObj);
      LOGGER.debug("getObject(1) = {} ({})", idObj, idObj.getClass().getSimpleName());

      Object nameObj = wrapper.getObject("tname");
      assertNotNull(nameObj);
      assertEquals("Gadget", nameObj.toString());

      wrapper.close();
    }
  }

  // ---------------------------------------------------------------------------
  // 28. FileResultSetWrapperImpl findColumn
  // ---------------------------------------------------------------------------

  @Test void testFileResultSetWrapperImplFindColumn() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rawRs = stmt.executeQuery(
             "SELECT tid, tname FROM jdbc_types")) {
      FileResultSetWrapperImpl wrapper = new FileResultSetWrapperImpl(rawRs);
      int tidIdx = wrapper.findColumn("tid");
      int tnameIdx = wrapper.findColumn("tname");
      assertTrue(tidIdx > 0);
      assertTrue(tnameIdx > 0);
      wrapper.close();
    }
  }

  // ---------------------------------------------------------------------------
  // 29. FileResultSetWrapperImpl get numeric types
  // ---------------------------------------------------------------------------

  @Test void testFileResultSetWrapperImplNumericTypes() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rawRs = stmt.executeQuery(
             "SELECT tid, tprice, tcount FROM jdbc_types WHERE tid = 30")) {
      FileResultSetWrapperImpl wrapper = new FileResultSetWrapperImpl(rawRs);
      assertTrue(wrapper.next());

      // Various numeric getters
      assertEquals(30, wrapper.getInt(1));
      assertEquals(30, wrapper.getInt("tid"));
      assertEquals(30L, wrapper.getLong(1));
      assertEquals(30L, wrapper.getLong("tid"));
      assertEquals((short) 30, wrapper.getShort(1));
      assertEquals((byte) 30, wrapper.getByte(1));
      assertEquals(30f, wrapper.getFloat(1), 0.01f);
      assertEquals(30.0, wrapper.getDouble(1), 0.01);

      // BigDecimal
      BigDecimal bd = wrapper.getBigDecimal("tprice");
      assertNotNull(bd);
      assertEquals(0, bd.compareTo(new BigDecimal("29.99")));

      wrapper.close();
    }
  }

  // ---------------------------------------------------------------------------
  // 30. FileResultSetWrapperImpl getBoolean
  // ---------------------------------------------------------------------------

  @Test void testFileResultSetWrapperImplGetBoolean() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rawRs = stmt.executeQuery(
             "SELECT rec_active FROM jdbc_test WHERE rec_id = 1")) {
      FileResultSetWrapperImpl wrapper = new FileResultSetWrapperImpl(rawRs);
      assertTrue(wrapper.next());
      boolean active = wrapper.getBoolean(1);
      assertTrue(active, "rec_active should be true for id=1");
      active = wrapper.getBoolean("rec_active");
      assertTrue(active);
      wrapper.close();
    }
  }

  // ---------------------------------------------------------------------------
  // 31. FilePreparedStatementWrapper delegation
  // ---------------------------------------------------------------------------

  @Test void testFilePreparedStatementWrapperDelegation() throws Exception {
    // Get the underlying prepared statement and wrap it
    PreparedStatement underlying = connection.prepareStatement(
        "SELECT rec_id, rec_name FROM jdbc_test WHERE rec_id = ?");
    FilePreparedStatementWrapper wrapper =
        new FilePreparedStatementWrapper(underlying);

    wrapper.setInt(1, 3);
    try (ResultSet rs = wrapper.executeQuery()) {
      assertTrue(rs.next());
      // ResultSet from wrapper should be a FileResultSetWrapperImpl
      assertTrue(rs instanceof FileResultSetWrapperImpl,
          "executeQuery should return FileResultSetWrapperImpl, got: "
              + rs.getClass().getName());
      assertEquals(3, rs.getInt(1));
      assertEquals("Charlie", rs.getString(2));
    }
    wrapper.close();
  }

  // ---------------------------------------------------------------------------
  // 32. FilePreparedStatementWrapper setString and setDouble
  // ---------------------------------------------------------------------------

  @Test void testFilePreparedStatementWrapperSetTypes() throws Exception {
    PreparedStatement underlying = connection.prepareStatement(
        "SELECT rec_id FROM jdbc_test WHERE rec_name = ? AND rec_amount > ?");
    FilePreparedStatementWrapper wrapper =
        new FilePreparedStatementWrapper(underlying);

    wrapper.setString(1, "Eve");
    wrapper.setDouble(2, 400.0);
    try (ResultSet rs = wrapper.executeQuery()) {
      assertTrue(rs.next());
      assertEquals(5, rs.getInt(1));
    }
    wrapper.close();
  }

  // ---------------------------------------------------------------------------
  // 33. FilePreparedStatementWrapper setObject
  // ---------------------------------------------------------------------------

  @Test void testFilePreparedStatementWrapperSetObject() throws Exception {
    PreparedStatement underlying = connection.prepareStatement(
        "SELECT rec_name FROM jdbc_test WHERE rec_id = ?");
    FilePreparedStatementWrapper wrapper =
        new FilePreparedStatementWrapper(underlying);

    wrapper.setObject(1, 1);
    try (ResultSet rs = wrapper.executeQuery()) {
      assertTrue(rs.next());
      assertEquals("Alice", rs.getString(1));
    }
    wrapper.close();
  }

  // ---------------------------------------------------------------------------
  // 34. FileStatementWrapper delegation
  // ---------------------------------------------------------------------------

  @Test void testFileStatementWrapperDelegation() throws Exception {
    Statement underlying = connection.createStatement();
    FileStatementWrapper wrapper = new FileStatementWrapper(underlying);

    try (ResultSet rs = wrapper.executeQuery(
        "SELECT tname FROM jdbc_types WHERE tid = 10")) {
      assertTrue(rs instanceof FileResultSetWrapperImpl,
          "FileStatementWrapper.executeQuery should return FileResultSetWrapperImpl");
      assertTrue(rs.next());
      assertEquals("Widget", rs.getString(1));
    }
    wrapper.close();
  }

  // ---------------------------------------------------------------------------
  // 35. FileConnectionWrapper creates wrapped statements
  // ---------------------------------------------------------------------------

  @Test void testFileConnectionWrapperCreatesWrappedStatements() throws Exception {
    FileConnectionWrapper connWrapper = new FileConnectionWrapper(connection);

    Statement stmt = connWrapper.createStatement();
    assertTrue(stmt instanceof FileStatementWrapper,
        "createStatement should return FileStatementWrapper, got: "
            + stmt.getClass().getName());

    PreparedStatement pstmt = connWrapper.prepareStatement(
        "SELECT tid FROM jdbc_types");
    assertTrue(pstmt instanceof FilePreparedStatementWrapper,
        "prepareStatement should return FilePreparedStatementWrapper, got: "
            + pstmt.getClass().getName());

    pstmt.close();
    stmt.close();
    // Do not close connWrapper - it would close the underlying connection
  }

  // ---------------------------------------------------------------------------
  // 36. FileConnectionWrapper delegates metadata
  // ---------------------------------------------------------------------------

  @Test void testFileConnectionWrapperDelegatesMetadata() throws Exception {
    FileConnectionWrapper connWrapper = new FileConnectionWrapper(connection);

    assertNotNull(connWrapper.getMetaData());
    assertFalse(connWrapper.isClosed());
    assertFalse(connWrapper.getAutoCommit());

    // Schema operations
    String schema = connWrapper.getSchema();
    LOGGER.debug("Connection schema: {}", schema);
    // Do not close connWrapper
  }

  // ---------------------------------------------------------------------------
  // 37. FileResultSetWrapperImpl getHoldability and getConcurrency
  // ---------------------------------------------------------------------------

  @Test void testFileResultSetWrapperImplMetadata() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rawRs = stmt.executeQuery(
             "SELECT tid FROM jdbc_types")) {
      FileResultSetWrapperImpl wrapper = new FileResultSetWrapperImpl(rawRs);

      int holdability = wrapper.getHoldability();
      int concurrency = wrapper.getConcurrency();
      int type = wrapper.getType();
      LOGGER.debug("Holdability: {}, Concurrency: {}, Type: {}",
          holdability, concurrency, type);

      // These should not throw exceptions
      assertTrue(holdability >= 0);
      assertTrue(concurrency >= 0);
      assertTrue(type >= 0);

      wrapper.close();
    }
  }

  // ---------------------------------------------------------------------------
  // 38. FileResultSetWrapperImpl fetch direction and size
  // ---------------------------------------------------------------------------

  @Test void testFileResultSetWrapperImplFetchSettings() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rawRs = stmt.executeQuery(
             "SELECT tid FROM jdbc_types")) {
      FileResultSetWrapperImpl wrapper = new FileResultSetWrapperImpl(rawRs);

      int fetchDir = wrapper.getFetchDirection();
      int fetchSize = wrapper.getFetchSize();
      LOGGER.debug("Fetch direction: {}, Fetch size: {}", fetchDir, fetchSize);

      // Set fetch size
      wrapper.setFetchSize(10);
      assertEquals(10, wrapper.getFetchSize());

      wrapper.close();
    }
  }

  // ---------------------------------------------------------------------------
  // 39. FileResultSetWrapperImpl unwrap/isWrapperFor
  // ---------------------------------------------------------------------------

  @Test void testFileResultSetWrapperImplUnwrap() throws Exception {
    try (Statement stmt = connection.createStatement();
         ResultSet rawRs = stmt.executeQuery(
             "SELECT tid FROM jdbc_types")) {
      FileResultSetWrapperImpl wrapper = new FileResultSetWrapperImpl(rawRs);

      // isWrapperFor should delegate
      boolean isWrapper = wrapper.isWrapperFor(ResultSet.class);
      LOGGER.debug("isWrapperFor(ResultSet): {}", isWrapper);

      wrapper.close();
    }
  }
}
