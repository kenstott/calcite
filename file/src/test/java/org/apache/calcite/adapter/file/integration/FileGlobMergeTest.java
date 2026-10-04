/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.file.integration;

import org.apache.calcite.adapter.file.BaseFileTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** REQ-788: a table whose url is a glob merges its matched files into one logical table. */
@Tag("unit")
@SuppressWarnings("deprecation")
public class FileGlobMergeTest extends BaseFileTest {

  private File tempDir;

  @BeforeEach public void setUp() throws Exception {
    tempDir = Files.createTempDirectory("file-glob-merge").toFile();
  }

  @AfterEach public void tearDown() {
    if (tempDir != null && tempDir.exists()) {
      for (File f : tempDir.listFiles()) {
        f.delete();
      }
      tempDir.delete();
    }
  }

  private String model(String tableDef) {
    return "{"
        + "\n  \"version\": \"1.0\","
        + "\n  \"defaultSchema\": \"G\","
        + "\n  \"schemas\": [{"
        + "\n    \"name\": \"G\","
        + "\n    \"type\": \"custom\","
        + "\n    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "\n    \"operand\": {"
        + "\n      \"directory\": \"" + tempDir.getAbsolutePath().replace("\\", "\\\\") + "\","
        + (getExecutionEngine() != null ? "\n      \"executionEngine\": \"" + getExecutionEngine() + "\"," : "")
        + "\n      \"tableNameCasing\": \"LOWER\","
        + "\n      \"columnNameCasing\": \"LOWER\","
        + "\n      \"tables\": [" + tableDef + "]"
        + "\n    }"
        + "\n  }]"
        + "\n}";
  }

  private String tableDef(String extra) {
    return "{\"name\": \"orders\", \"url\": \""
        + new File(tempDir, "*.csv").getAbsolutePath().replace("\\", "\\\\") + "\"" + extra + "}";
  }

  @Test void testGlobMergesMatchedCsvFilesIntoOneTable() throws Exception {
    Files.writeString(new File(tempDir, "a.csv").toPath(), "id,amount\n1,10\n2,20\n");
    Files.writeString(new File(tempDir, "b.csv").toPath(), "id,amount\n3,30\n");
    String m = addEphemeralCacheToModel(model(tableDef("")));
    try (Connection c = DriverManager.getConnection("jdbc:calcite:model=inline:" + m);
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM \"G\".\"orders\"")) {
      rs.next();
      assertEquals(3, rs.getInt(1));
    }
  }

  @Test void testSourceFileColumnCarriesEachRowsPath() throws Exception {
    Files.writeString(new File(tempDir, "a.csv").toPath(), "id,amount\n1,10\n");
    Files.writeString(new File(tempDir, "b.csv").toPath(), "id,amount\n2,20\n");
    String m = addEphemeralCacheToModel(model(tableDef(", \"sourceFileColumn\": \"_source_file\"")));
    try (Connection c = DriverManager.getConnection("jdbc:calcite:model=inline:" + m);
         Statement st = c.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT \"id\", \"_source_file\" FROM \"G\".\"orders\" ORDER BY \"id\"")) {
      TreeSet<String> files = new TreeSet<>();
      while (rs.next()) {
        files.add(new File(rs.getString(2)).getName());
      }
      assertTrue(files.contains("a.csv"), "a.csv in " + files);
      assertTrue(files.contains("b.csv"), "b.csv in " + files);
    }
  }

  @Test void testDifferingColumnsRefusedByName() throws Exception {
    Files.writeString(new File(tempDir, "a.csv").toPath(), "id,amount\n1,10\n");
    Files.writeString(new File(tempDir, "b.csv").toPath(), "id,amount,extra\n2,20,x\n");
    String m = addEphemeralCacheToModel(model(tableDef("")));
    try (Connection c = DriverManager.getConnection("jdbc:calcite:model=inline:" + m);
         Statement st = c.createStatement()) {
      st.executeQuery("SELECT COUNT(*) FROM \"G\".\"orders\"");
      fail("expected a column-set mismatch to be refused by name");
    } catch (Exception e) {
      StringBuilder chain = new StringBuilder();
      for (Throwable t = e; t != null; t = t.getCause()) {
        chain.append(t.getMessage()).append(" | ");
      }
      String msg = chain.toString();
      assertTrue(msg.contains("b.csv") && msg.contains("extra"),
          "error should name b.csv and the extra column: " + msg);
    }
  }
}
