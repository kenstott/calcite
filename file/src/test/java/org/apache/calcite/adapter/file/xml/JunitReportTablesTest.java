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
package org.apache.calcite.adapter.file.xml;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JUnit XML reports read as the tables junit__runs, junit__cases and junit__results, through
 * SQL on a file schema.
 */
@Tag("unit")
class JunitReportTablesTest {
  private static final double TOLERANCE = 1e-14;

  @TempDir Path tempDir;

  @Test void pytestReportGivesItsRunCasesAndResults() throws Exception {
    File dir = reports("pytest-1.xml");

    assertEquals(
        Arrays.asList("run_key=pytest-1.xml; name=pytest tests; hostname=ci-runner-1; tests=4; "
            + "failures=1; errors=1; skipped=1; properties={\"build\":\"1234\",\"branch\":\"main\"}"),
        rows(dir, "SELECT run_key, name, hostname, tests, failures, errors, skipped, properties "
            + "FROM \"junit__runs\""));
    // The report's 10:15:30.123456+00:00, in UTC to the millisecond.
    assertEquals(Arrays.asList("run_key=pytest-1.xml"),
        rows(dir, "SELECT run_key FROM \"junit__runs\" "
            + "WHERE run_timestamp = TIMESTAMP '2026-11-13 10:15:30.123'"));
    assertSeconds(dir, "SELECT time_seconds FROM \"junit__runs\"", 0.125);
    assertTrue(single(dir, "SELECT file_path FROM \"junit__runs\"")
        .endsWith("pytest-1.xml"));

    assertEquals(
        Arrays.asList(
            "case_key=tests.test_math::test_add; classname=tests.test_math; name=test_add; "
                + "file=tests/test_math.py; line=3",
            "case_key=tests.test_math::test_db; classname=tests.test_math; name=test_db; "
                + "file=tests/test_math.py; line=11",
            "case_key=tests.test_math::test_div; classname=tests.test_math; name=test_div; "
                + "file=tests/test_math.py; line=7",
            "case_key=tests.test_math::test_slow; classname=tests.test_math; name=test_slow; "
                + "file=tests/test_math.py; line=19"),
        rows(dir, "SELECT case_key, classname, name, file, line FROM \"junit__cases\" "
            + "ORDER BY case_key"));

    assertEquals(
        Arrays.asList(
            "run_key=pytest-1.xml; case_key=tests.test_math::test_add; outcome=passed; "
                + "message=null; type=null",
            "run_key=pytest-1.xml; case_key=tests.test_math::test_db; outcome=error; "
                + "message=failed on setup with \"ConnectionError: refused\"; type=null",
            "run_key=pytest-1.xml; case_key=tests.test_math::test_div; outcome=failed; "
                + "message=assert 1 == 2; type=null",
            "run_key=pytest-1.xml; case_key=tests.test_math::test_slow; outcome=skipped; "
                + "message=too slow; type=pytest.skip"),
        rows(dir, "SELECT run_key, case_key, outcome, message, type FROM \"junit__results\" "
            + "ORDER BY case_key"));
    assertTrue(single(dir, "SELECT text FROM \"junit__results\" "
        + "WHERE case_key = 'tests.test_math::test_div'")
        .endsWith("tests/test_math.py:8: AssertionError"));
    assertEquals("tests/test_math.py:20: too slow", single(dir, "SELECT text FROM "
        + "\"junit__results\" WHERE case_key = 'tests.test_math::test_slow'"));
    assertSeconds(dir, "SELECT duration_seconds FROM \"junit__results\" "
        + "WHERE case_key = 'tests.test_math::test_div'", 0.002);
  }

  @Test void vitestReportGivesItsRunCasesAndResults() throws Exception {
    File dir = reports("vitest.xml");

    assertEquals(
        Arrays.asList("run_key=vitest.xml; name=vitest tests; hostname=dev-laptop; tests=3; "
            + "failures=1; errors=0; skipped=1; properties={}"),
        rows(dir, "SELECT run_key, name, hostname, tests, failures, errors, skipped, properties "
            + "FROM \"junit__runs\""));
    assertSeconds(dir, "SELECT time_seconds FROM \"junit__runs\"", 0.512);
    assertEquals(
        Arrays.asList(
            "case_key=src/utils.test.ts::utils > formats a date; outcome=passed; message=null; "
                + "type=null; text=null",
            "case_key=src/utils.test.ts::utils > parses a number; outcome=failed; "
                + "message=expected 2 to be 3 // Object.is equality; type=AssertionError; "
                + "text=AssertionError: expected 2 to be 3 // Object.is equality\n"
                + " ❯ src/utils.test.ts:12:21",
            "case_key=src/utils.test.ts::utils > todo case; outcome=skipped; message=null; "
                + "type=null; text=null"),
        rows(dir, "SELECT case_key, outcome, message, type, text FROM \"junit__results\" "
            + "ORDER BY case_key"));
    assertEquals(
        Arrays.asList(
            "classname=src/utils.test.ts; name=utils > formats a date; file=null",
            "classname=src/utils.test.ts; name=utils > parses a number; file=null",
            "classname=src/utils.test.ts; name=utils > todo case; file=null"),
        rows(dir, "SELECT classname, name, file FROM \"junit__cases\" ORDER BY name"));
  }

  @Test void playwrightReportGivesItsRunCasesAndResults() throws Exception {
    File dir = reports("playwright.xml");

    assertEquals(
        Arrays.asList("run_key=playwright.xml; name=; hostname=chromium; tests=2; failures=1; "
            + "errors=0; skipped=0"),
        rows(dir, "SELECT run_key, name, hostname, tests, failures, errors, skipped "
            + "FROM \"junit__runs\""));
    assertEquals(
        Arrays.asList(
            "case_key=login.spec.ts::login › rejects a bad password; outcome=failed; "
                + "message=login.spec.ts:14:5 rejects a bad password; type=FAILURE",
            "case_key=login.spec.ts::login › signs in; outcome=passed; message=null; type=null"),
        rows(dir, "SELECT case_key, outcome, message, type FROM \"junit__results\" "
            + "ORDER BY case_key"));
    assertTrue(single(dir, "SELECT text FROM \"junit__results\" WHERE outcome = 'failed'")
        .endsWith("Error: expect(received).toBe(expected)"));
    assertSeconds(dir, "SELECT duration_seconds FROM \"junit__results\" "
        + "WHERE outcome = 'failed'", 1.7);
    assertEquals("2",
        single(dir, "SELECT CAST(COUNT(*) AS VARCHAR) FROM \"junit__cases\""));
  }

  @Test void surefireReportGivesItsRunCasesAndResults() throws Exception {
    File dir = reports("TEST-com.example.CalculatorTest.xml");

    assertEquals(
        Arrays.asList("run_key=TEST-com.example.CalculatorTest.xml; "
            + "name=com.example.CalculatorTest; hostname=null; tests=3; failures=1; errors=0; "
            + "skipped=1; properties={\"java.version\":\"17.0.9\",\"os.name\":\"Linux\"}"),
        rows(dir, "SELECT run_key, name, hostname, tests, failures, errors, skipped, properties "
            + "FROM \"junit__runs\""));
    assertEquals(Arrays.asList("ts=null"),
        rows(dir, "SELECT run_timestamp AS ts FROM \"junit__runs\""));
    assertEquals(
        Arrays.asList(
            "case_key=com.example.CalculatorTest::adds; outcome=passed; message=null; type=null",
            "case_key=com.example.CalculatorTest::divides; outcome=failed; "
                + "message=expected: <2> but was: <3>; type=org.opentest4j.AssertionFailedError",
            "case_key=com.example.CalculatorTest::subtracts; outcome=skipped; message=disabled; "
                + "type=null"),
        rows(dir, "SELECT case_key, outcome, message, type FROM \"junit__results\" "
            + "ORDER BY case_key"));
    assertTrue(single(dir, "SELECT text FROM \"junit__results\" WHERE outcome = 'failed'")
        .endsWith("at com.example.CalculatorTest.divides(CalculatorTest.java:15)"));
  }

  @Test void twoReportsInOneDirectoryGiveTwoRunsAndSharedCases() throws Exception {
    File dir = reports("pytest-1.xml", "pytest-2.xml");

    assertEquals(
        Arrays.asList("run_key=pytest-1.xml; hostname=ci-runner-1; failures=1",
            "run_key=pytest-2.xml; hostname=ci-runner-2; failures=0"),
        rows(dir, "SELECT run_key, hostname, failures FROM \"junit__runs\" ORDER BY run_key"));
    assertEquals("4", single(dir, "SELECT CAST(COUNT(*) AS VARCHAR) FROM \"junit__cases\""));
    assertEquals("8", single(dir, "SELECT CAST(COUNT(*) AS VARCHAR) FROM \"junit__results\""));
    assertEquals(
        Arrays.asList("run_key=pytest-1.xml; outcome=failed", "run_key=pytest-2.xml; outcome=passed"),
        rows(dir, "SELECT run_key, outcome FROM \"junit__results\" "
            + "WHERE case_key = 'tests.test_math::test_div' ORDER BY run_key"));
  }

  @Test void resultsJoinToCases() throws Exception {
    File dir = reports("pytest-1.xml", "pytest-2.xml");

    assertEquals(
        Arrays.asList("run_key=pytest-1.xml; file=tests/test_math.py; line=7; outcome=failed",
            "run_key=pytest-2.xml; file=tests/test_math.py; line=7; outcome=passed"),
        rows(dir, "SELECT r.run_key, c.file, c.line, r.outcome "
            + "FROM \"junit__results\" r JOIN \"junit__cases\" c ON r.case_key = c.case_key "
            + "WHERE c.name = 'test_div' ORDER BY r.run_key"));
  }

  @Test void reportsInSubdirectoriesAreKeyedByTheirPath() throws Exception {
    File dir = tempDir.resolve("data").toFile();
    copy("pytest-1.xml", new File(dir, "reports/unit/pytest.xml"));
    copy("vitest.xml", new File(dir, "reports/ui/vitest.xml"));

    assertEquals(
        Arrays.asList("run_key=reports/ui/vitest.xml", "run_key=reports/unit/pytest.xml"),
        rows(dir, "SELECT run_key FROM \"junit__runs\" ORDER BY run_key", true));
  }

  @Test void malformedReportFailsNamingTheFile() throws Exception {
    File dir = reports("pytest-1.xml", "malformed.xml");

    Exception e = assertThrows(Exception.class,
        () -> rows(dir, "SELECT run_key FROM \"junit__runs\""));
    assertTrue(messages(e).contains("Malformed JUnit report")
            && messages(e).contains("malformed.xml"),
        () -> "the failure names the file: " + messages(e));
  }

  @Test void tablesHaveTheirColumnTypes() throws Exception {
    File dir = reports("pytest-1.xml");

    assertEquals(
        Arrays.asList("run_key VARCHAR", "file_path VARCHAR", "name VARCHAR",
            "run_timestamp TIMESTAMP", "hostname VARCHAR", "tests INTEGER", "failures INTEGER",
            "errors INTEGER", "skipped INTEGER", "time_seconds DOUBLE", "properties VARCHAR"),
        columns(dir, "junit__runs"));
    assertEquals(
        Arrays.asList("case_key VARCHAR", "classname VARCHAR", "name VARCHAR", "file VARCHAR",
            "line INTEGER"),
        columns(dir, "junit__cases"));
    assertEquals(
        Arrays.asList("run_key VARCHAR", "case_key VARCHAR", "outcome VARCHAR",
            "duration_seconds DOUBLE", "message VARCHAR", "type VARCHAR", "text VARCHAR"),
        columns(dir, "junit__results"));
  }

  /** A directory schema reads no table from an XML file that is not a JUnit report. */
  @Test void nonJunitXmlIsReadAsBefore() throws Exception {
    File dir = reports("catalog.xml", "pytest-1.xml");

    List<String> tables = new ArrayList<>();
    try (Connection connection = connect(dir, false);
         ResultSet resultSet =
             connection.getMetaData().getTables(null, "files", "%", null)) {
      while (resultSet.next()) {
        tables.add(resultSet.getString("TABLE_NAME"));
      }
    }
    tables.sort(null);
    assertEquals(Arrays.asList("junit__cases", "junit__results", "junit__runs"), tables);
  }

  private File reports(String... fixtures) throws IOException {
    File dir = tempDir.resolve("data").toFile();
    for (String fixture : fixtures) {
      copy(fixture, new File(dir, fixture));
    }
    return dir;
  }

  private void copy(String fixture, File target) throws IOException {
    Files.createDirectories(target.getParentFile().toPath());
    try (InputStream in = getClass().getResourceAsStream("/junit-reports/" + fixture)) {
      if (in == null) {
        throw new IOException("No fixture " + fixture);
      }
      Files.copy(in, target.toPath());
    }
  }

  private static List<String> columns(File dir, String table) throws SQLException {
    List<String> columns = new ArrayList<>();
    try (Connection connection = connect(dir, false);
         ResultSet resultSet =
             connection.getMetaData().getColumns(null, "files", table, "%")) {
      while (resultSet.next()) {
        columns.add(resultSet.getString("COLUMN_NAME") + " "
            + java.sql.JDBCType.valueOf(resultSet.getInt("DATA_TYPE")).getName());
      }
    }
    return columns;
  }

  private static String messages(Throwable e) {
    StringBuilder all = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause()) {
      all.append(t.getMessage()).append('\n');
    }
    return all.toString();
  }

  private static String single(File dir, String sql) throws SQLException {
    List<String> values = new ArrayList<>();
    try (Connection connection = connect(dir, false);
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        values.add(resultSet.getString(1));
      }
    }
    assertEquals(1, values.size(), () -> sql + " gives one row: " + values);
    return values.get(0);
  }

  private static void assertSeconds(File dir, String sql, double expected) throws SQLException {
    try (Connection connection = connect(dir, false);
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      assertTrue(resultSet.next(), sql);
      assertEquals(expected, resultSet.getDouble(1), Math.abs(expected) * TOLERANCE, sql);
    }
  }

  private static List<String> rows(File dir, String sql) throws SQLException {
    return rows(dir, sql, false);
  }

  private static List<String> rows(File dir, String sql, boolean recursive) throws SQLException {
    List<String> rows = new ArrayList<>();
    try (Connection connection = connect(dir, recursive);
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      ResultSetMetaData metaData = resultSet.getMetaData();
      while (resultSet.next()) {
        StringBuilder row = new StringBuilder();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          if (i > 1) {
            row.append("; ");
          }
          row.append(metaData.getColumnLabel(i)).append('=').append(resultSet.getString(i));
        }
        rows.add(row.toString());
      }
    }
    return rows;
  }

  private static Connection connect(File dir, boolean recursive) throws SQLException {
    String path = dir.getAbsolutePath().replace("\\", "\\\\");
    String work = new File(dir.getParentFile(), "work").getAbsolutePath()
        .replace("\\", "\\\\");
    String model = "{\"version\": \"1.0\", \"defaultSchema\": \"files\", \"schemas\": [{"
        + "\"name\": \"files\", \"type\": \"custom\", "
        + "\"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\", "
        + "\"operand\": {\"directory\": \"" + path + "\", \"baseDirectory\": \"" + work + "\", "
        + "\"ephemeralCache\": true, "
        + "\"recursive\": " + recursive + "}}]}";
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    info.setProperty("model", "inline:" + model);
    return DriverManager.getConnection("jdbc:calcite:", info);
  }
}
