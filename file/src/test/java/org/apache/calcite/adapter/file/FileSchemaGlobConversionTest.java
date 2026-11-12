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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.lookup.LikePattern;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * The schema's glob decides which source files are converted and which conversions become
 * tables: a convertible file (xlsx, html, docx, ...) the glob does not match is neither
 * converted nor registered, and a conversion left by an earlier run does not leak through.
 */
@Tag("unit")
public class FileSchemaGlobConversionTest {

  private static final List<String> DROP_FILES =
      Arrays.asList("budget_2026.xlsx", "building_permits.parquet",
          "complaints_2026_08.csv", "complaints_2026_09.csv", "inspections.json");

  private static final String SHEET_TABLE = "budget_2026__budget";

  @TempDir Path dataDir;
  @TempDir Path operatingDir;

  @Test void globOnCsvListsOnlyTheCsvTablesAndNeitherConvertsNorServesTheSheet()
      throws Exception {
    copyDropFiles();
    try (Connection conn = connection()) {
      SchemaPlus root = conn.unwrap(CalciteConnection.class).getRootSchema();
      Schema schema = factorySchema(root, "g", "complaints*.csv");
      root.add("g", schema);

      assertEquals(new TreeSet<>(Arrays.asList("complaints_2026_08", "complaints_2026_09")),
          tableNames(schema));
      assertThrows(SQLException.class, () -> count(conn, "g", SHEET_TABLE));
      assertFalse(hasConversionOf("budget_2026"),
          "the xlsx the glob does not match must not be converted");
    }
  }

  @Test void globOnXlsxListsTheSheetTableOnly() throws Exception {
    copyDropFiles();
    try (Connection conn = connection()) {
      SchemaPlus root = conn.unwrap(CalciteConnection.class).getRootSchema();
      Schema schema = factorySchema(root, "g", "*.xlsx");
      root.add("g", schema);

      assertEquals(new TreeSet<>(Arrays.asList(SHEET_TABLE)), tableNames(schema));
      assertEquals(30, count(conn, "g", SHEET_TABLE));
    }
  }

  @Test void noGlobListsAllFiveFilesAsBefore() throws Exception {
    copyDropFiles();
    try (Connection conn = connection()) {
      SchemaPlus root = conn.unwrap(CalciteConnection.class).getRootSchema();
      Schema schema = factorySchema(root, "g", null);
      root.add("g", schema);

      assertEquals(
          new TreeSet<>(
              Arrays.asList(SHEET_TABLE, "building_permits", "complaints_2026_08",
              "complaints_2026_09", "inspections")),
          tableNames(schema));
      assertEquals(30, count(conn, "g", SHEET_TABLE));
    }
  }

  @Test void conversionFromAnEarlierRunWithoutTheGlobDoesNotLeak() throws Exception {
    copyDropFiles();
    try (Connection conn = connection()) {
      SchemaPlus root = conn.unwrap(CalciteConnection.class).getRootSchema();
      Schema first = factorySchema(root, "first", null);
      assertTrue(tableNames(first).contains(SHEET_TABLE));
      assertTrue(hasConversionOf("budget_2026"),
          "the run without a glob leaves the xlsx conversion in conversions/");
    }
    try (Connection conn = connection()) {
      SchemaPlus root = conn.unwrap(CalciteConnection.class).getRootSchema();
      Schema second = factorySchema(root, "g", "complaints*.csv");
      root.add("g", second);

      assertEquals(new TreeSet<>(Arrays.asList("complaints_2026_08", "complaints_2026_09")),
          tableNames(second));
      assertThrows(SQLException.class, () -> count(conn, "g", SHEET_TABLE));
    }
  }

  /** The schema without a storage provider (storageType unset) honours the glob too. */
  @Test void schemaWithoutStorageProviderHonoursTheGlob() throws Exception {
    copyDropFiles();
    File data = dataDir.toFile();
    FileSchema unfiltered =
        new FileSchema(mock(SchemaPlus.class), "first", data, data, null, null,
            new ExecutionEngineConfig(), false, null, null, null, null, "SMART_CASING",
            "SMART_CASING", null, null, null, null, false);
    assertTrue(tableNames(unfiltered).contains(SHEET_TABLE));

    FileSchema globbed =
        new FileSchema(mock(SchemaPlus.class), "first", data, data, "complaints*.csv", null,
            new ExecutionEngineConfig(), false, null, null, null, null, "SMART_CASING",
            "SMART_CASING", null, null, null, null, false);
    assertFalse(tableNames(globbed).contains(SHEET_TABLE),
        "a stale conversion must not be registered under a glob that excludes its source");
    assertTrue(tableNames(globbed).contains("complaints_2026_08"));
  }

  /**
   * A non-local storage provider (keys, downloaded to the cache, then converted): served here by
   * the local file system under an object-store storage type, so no network is used.
   */
  @Test void nonLocalStorageProviderConversionHonoursTheGlob() throws Exception {
    copyDropFiles();
    File data = dataDir.toFile();

    Set<String> unfiltered = tableNames(remoteSchema(data, null));
    assertTrue(unfiltered.contains(SHEET_TABLE), "no glob converts the xlsx: " + unfiltered);

    Set<String> globbed = tableNames(remoteSchema(data, "complaints*.csv"));
    assertFalse(globbed.contains(SHEET_TABLE),
        "a stale download-cache conversion must not leak: " + globbed);
    assertTrue(globbed.contains("complaints_2026_08"), globbed.toString());

    Set<String> sheetOnly = tableNames(remoteSchema(data, "*.xlsx"));
    assertEquals(new TreeSet<>(Arrays.asList(SHEET_TABLE)), sheetOnly);
  }

  private FileSchema remoteSchema(File data, String glob) {
    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("_storageProvider", new ObjectStoreShapedLocalProvider());
    return new FileSchema(mock(SchemaPlus.class), "remote", data, operatingDir.toFile(),
        data.getAbsolutePath(), glob, null, new ExecutionEngineConfig(), false, null, null,
        null, null, "SMART_CASING", "SMART_CASING", "s3", storageConfig, null, null, false, null);
  }

  /** The local file system, reported as an object store so its keys take the remote path. */
  private static class ObjectStoreShapedLocalProvider extends LocalFileStorageProvider {
    @Override public String getStorageType() {
      return "s3";
    }
  }

  private Schema factorySchema(SchemaPlus root, String name, String glob) {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", dataDir.toString());
    operand.put("baseDirectory", operatingDir.toString());
    if (glob != null) {
      operand.put("glob", glob);
    }
    return FileSchemaFactory.INSTANCE.create(root, name, operand);
  }

  private boolean hasConversionOf(String sourceBaseName) throws Exception {
    try (java.util.stream.Stream<Path> walk = Files.walk(operatingDir)) {
      return walk.anyMatch(p -> p.getParent() != null
          && "conversions".equals(p.getParent().getFileName().toString())
          && p.getFileName().toString().startsWith(sourceBaseName + "__"));
    }
  }

  private static Set<String> tableNames(Schema schema) {
    return new TreeSet<>(schema.tables().getNames(LikePattern.any()));
  }

  private static long count(Connection conn, String schema, String table) throws SQLException {
    try (Statement stmt = conn.createStatement();
         ResultSet rs =
             stmt.executeQuery("select count(*) from \"" + schema + "\".\"" + table + "\"")) {
      assertTrue(rs.next());
      return rs.getLong(1);
    }
  }

  private void copyDropFiles() throws Exception {
    for (String name : DROP_FILES) {
      try (InputStream in =
               getClass().getClassLoader().getResourceAsStream("glob-conversions/" + name)) {
        assertTrue(in != null, "test resource glob-conversions/" + name);
        Files.copy(in, dataDir.resolve(name), StandardCopyOption.REPLACE_EXISTING);
      }
    }
  }

  private static Connection connection() throws SQLException {
    Properties info = new Properties();
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    return DriverManager.getConnection("jdbc:calcite:", info);
  }
}
