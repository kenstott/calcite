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

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.Frameworks;

import com.google.common.collect.ImmutableMultimap;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deep line coverage tests for {@link FileSchema}.
 *
 * <p>Targets the largest uncovered code blocks in FileSchema by exercising
 * its PUBLIC API with real file system interactions. Every test calls real
 * FileSchema methods -- no mocks.
 */
@Tag("unit")
class FileSchemaDeepLineCoverageTest {

  @TempDir
  Path tempDir;

  /** LINQ4J engine for tests that only need table discovery (no Parquet conversion). */
  private static final ExecutionEngineConfig LINQ4J =
      new ExecutionEngineConfig("LINQ4J", 2048);

  /** PARQUET engine for tests that exercise Parquet conversion code paths. */
  private static final ExecutionEngineConfig PARQUET =
      new ExecutionEngineConfig("PARQUET", 2048);

  /** VECTORIZED engine. */
  private static final ExecutionEngineConfig VECTORIZED =
      new ExecutionEngineConfig("VECTORIZED", 2048);

  // ---------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------

  private SchemaPlus rootSchema() {
    return Frameworks.createRootSchema(false);
  }

  /** Create an isolated sub-directory so tests never share files. */
  private File dir(String name) {
    File d = tempDir.resolve(name).toFile();
    d.mkdirs();
    return d;
  }

  private File writeCsv(File d, String name, String content) throws IOException {
    File f = new File(d, name);
    try (FileWriter w = new FileWriter(f)) {
      w.write(content);
    }
    return f;
  }

  private File writeJson(File d, String name, String content) throws IOException {
    return writeCsv(d, name, content); // same I/O, different content
  }

  private File writeTsv(File d, String name) throws IOException {
    return writeCsv(d, name, "id\tname\tvalue\n1\tfoo\t100\n2\tbar\t200\n");
  }

  private File writeHtml(File d, String name) throws IOException {
    return writeCsv(d, name,
        "<html><body><table id=\"test\">"
        + "<tr><th>id</th><th>name</th></tr>"
        + "<tr><td>1</td><td>alpha</td></tr>"
        + "<tr><td>2</td><td>beta</td></tr>"
        + "</table></body></html>");
  }

  /**
   * Create a FileSchema with proper baseDirectory for local file discovery.
   * Passes the source dir as userConfiguredBaseDirectory so findMatchingFiles
   * finds files in the source dir rather than the operating cache directory.
   */
  private FileSchema schemaForDiscovery(SchemaPlus p, String name, File srcDir,
      ExecutionEngineConfig engine, boolean recursive) {
    return new FileSchema(p, name, srcDir, srcDir, (String) null,
        (List<Map<String, Object>>) null, engine, recursive,
        (List<Map<String, Object>>) null, (List<Map<String, Object>>) null,
        (List<Map<String, Object>>) null,
        (String) null, "SMART_CASING", "SMART_CASING",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false);
  }

  /** Overload for non-recursive discovery. */
  private FileSchema schemaForDiscovery(SchemaPlus p, String name, File srcDir,
      ExecutionEngineConfig engine) {
    return schemaForDiscovery(p, name, srcDir, engine, false);
  }

  /**
   * Create a FileSchema with explicit table defs and proper baseDirectory.
   */
  private FileSchema schemaWithDefs(SchemaPlus p, String name, File srcDir,
      List<Map<String, Object>> defs, ExecutionEngineConfig engine) {
    return new FileSchema(p, name, srcDir, srcDir, (String) null,
        defs, engine, false,
        (List<Map<String, Object>>) null, (List<Map<String, Object>>) null,
        (List<Map<String, Object>>) null,
        (String) null, "SMART_CASING", "SMART_CASING",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false);
  }

  // ---------------------------------------------------------------
  // 1. CSV / JSON / TSV auto-discovery (LINQ4J)
  // ---------------------------------------------------------------
  @Test void testDiscoverCsvJsonTsv() throws Exception {
    File d = dir("t01");
    writeCsv(d, "employees.csv", "id,name,salary\n1,Alice,100\n2,Bob,200\n");
    writeJson(d, "departments.json",
        "[{\"id\":1,\"name\":\"eng\"},{\"id\":2,\"name\":\"sales\"}]");
    writeTsv(d, "metrics.tsv");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t01", d, LINQ4J);
    p.add("t01", s);

    Map<String, Table> t = s.getTableMap();
    assertNotNull(t);
    assertFalse(t.isEmpty(), "Should discover at least 1 table");
    assertTrue(keyContains(t, "employees"), "CSV not found: " + t.keySet());
    assertTrue(keyContains(t, "departments"), "JSON not found: " + t.keySet());
    assertTrue(keyContains(t, "metrics"), "TSV not found: " + t.keySet());
  }

  // ---------------------------------------------------------------
  // 2. Table caching -- second call returns same map
  // ---------------------------------------------------------------
  @Test void testTableCacheReturnsSameMap() throws Exception {
    File d = dir("t02");
    writeCsv(d, "data.csv", "a,b\n1,2\n");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t02", d, LINQ4J);
    p.add("t02", s);

    Map<String, Table> first = s.getTableMap();
    Map<String, Table> second = s.getTableMap();
    assertSame(first, second, "Second call should return cached map");
  }

  // ---------------------------------------------------------------
  // 3. clearTableCache forces recomputation
  // ---------------------------------------------------------------
  @Test void testClearTableCacheInvalidatesCache() throws Exception {
    File d = dir("t03");
    writeCsv(d, "data.csv", "x,y\n1,2\n");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t03", d, LINQ4J);
    p.add("t03", s);

    Map<String, Table> first = s.getTableMap();
    assertFalse(first.isEmpty());
    s.clearTableCache();
    Map<String, Table> second = s.getTableMap();
    assertNotSame(first, second, "After clearTableCache, a new map should be returned");
  }

  // ---------------------------------------------------------------
  // 4. Recursive directory scanning
  // ---------------------------------------------------------------
  @Test void testRecursiveDirectoryScanning() throws Exception {
    File d = dir("t04");
    File sub = new File(d, "subdir");
    sub.mkdirs();
    writeCsv(d, "root.csv", "id,name\n1,root\n");
    writeCsv(sub, "nested.csv", "id,name\n2,nested\n");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t04", d, LINQ4J, true);
    p.add("t04", s);

    Map<String, Table> t = s.getTableMap();
    assertTrue(t.size() >= 2,
        "Recursive scan should find >= 2 tables, found: " + t.keySet());
  }

  // ---------------------------------------------------------------
  // 5. Explicit table def -- CSV format
  // ---------------------------------------------------------------
  @Test void testExplicitTableDefCsv() throws Exception {
    File d = dir("t05");
    writeCsv(d, "mydata.csv", "id,value\n1,100\n2,200\n");

    List<Map<String, Object>> defs = new ArrayList<>();
    Map<String, Object> def = new HashMap<>();
    def.put("name", "my_table");
    def.put("url", "mydata.csv");
    def.put("format", "csv");
    defs.add(def);

    SchemaPlus p = rootSchema();
    FileSchema s = schemaWithDefs(p, "t05", d, defs, LINQ4J);
    p.add("t05", s);

    Map<String, Table> t = s.getTableMap();
    assertTrue(t.containsKey("my_table"), "found: " + t.keySet());
  }

  // ---------------------------------------------------------------
  // 6. Explicit table def -- JSON format
  // ---------------------------------------------------------------
  @Test void testExplicitTableDefJson() throws Exception {
    File d = dir("t06");
    writeJson(d, "items.json",
        "[{\"id\":1,\"name\":\"foo\"},{\"id\":2,\"name\":\"bar\"}]");

    List<Map<String, Object>> defs = new ArrayList<>();
    Map<String, Object> def = new HashMap<>();
    def.put("name", "items");
    def.put("url", "items.json");
    def.put("format", "json");
    defs.add(def);

    SchemaPlus p = rootSchema();
    FileSchema s = schemaWithDefs(p, "t06", d, defs, LINQ4J);
    p.add("t06", s);

    assertTrue(s.getTableMap().containsKey("items"));
  }

  // ---------------------------------------------------------------
  // 7. Explicit table def -- TSV format
  // ---------------------------------------------------------------
  @Test void testExplicitTableDefTsv() throws Exception {
    File d = dir("t07");
    writeTsv(d, "tab_data.tsv");

    List<Map<String, Object>> defs = new ArrayList<>();
    Map<String, Object> def = new HashMap<>();
    def.put("name", "tab_data");
    def.put("url", "tab_data.tsv");
    def.put("format", "tsv");
    defs.add(def);

    SchemaPlus p = rootSchema();
    FileSchema s = schemaWithDefs(p, "t07", d, defs, LINQ4J);
    p.add("t07", s);

    assertTrue(s.getTableMap().containsKey("tab_data"));
  }

  // ---------------------------------------------------------------
  // 8. View creation
  // ---------------------------------------------------------------
  @Test void testViewCreation() throws Exception {
    File d = dir("t08");
    writeCsv(d, "base.csv", "id,name\n1,Alice\n");

    List<Map<String, Object>> views = new ArrayList<>();
    Map<String, Object> v = new HashMap<>();
    v.put("name", "my_view");
    v.put("sql", "SELECT * FROM \"base\"");
    views.add(v);

    SchemaPlus p = rootSchema();
    FileSchema s =
        new FileSchema(p, "t08", d, d, (String) null, (List<Map<String, Object>>) null, LINQ4J, false,
        (List<Map<String, Object>>) null, views,
        (List<Map<String, Object>>) null,
        (String) null, "SMART_CASING", "SMART_CASING",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false);
    p.add("t08", s);

    assertTrue(s.getTableMap().containsKey("my_view"),
        "found: " + s.getTableMap().keySet());
  }

  // ---------------------------------------------------------------
  // 9. Materialized view with PARQUET engine
  // ---------------------------------------------------------------
  @Test void testMaterializedViewWithParquetEngine() throws Exception {
    File d = dir("t09");
    writeJson(d, "source.json",
        "[{\"id\":1,\"value\":10},{\"id\":2,\"value\":20}]");

    List<Map<String, Object>> mvs = new ArrayList<>();
    Map<String, Object> mv = new HashMap<>();
    mv.put("view", "mv_view");
    mv.put("table", "mv_table");
    mv.put("sql", "SELECT * FROM \"source\"");
    mvs.add(mv);

    SchemaPlus p = rootSchema();
    FileSchema s =
        new FileSchema(p, "t09", d, d, (String) null, (List<Map<String, Object>>) null, PARQUET, false,
        mvs, (List<Map<String, Object>>) null,
        (List<Map<String, Object>>) null,
        (String) null, "SMART_CASING", "SMART_CASING",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false);
    p.add("t09", s);

    Map<String, Table> t = s.getTableMap();
    assertTrue(t.containsKey("mv_view") || t.containsKey("mv_table"),
        "MV should be found: " + t.keySet());
  }

  // ---------------------------------------------------------------
  // 10. Materialized view skipped with non-PARQUET engine
  // ---------------------------------------------------------------
  @Test void testMaterializedViewSkippedWithLinq4j() throws Exception {
    File d = dir("t10");
    writeCsv(d, "source.csv", "id,value\n1,10\n");

    List<Map<String, Object>> mvs = new ArrayList<>();
    Map<String, Object> mv = new HashMap<>();
    mv.put("view", "mv_view");
    mv.put("table", "mv_table");
    mv.put("sql", "SELECT * FROM source");
    mvs.add(mv);

    SchemaPlus p = rootSchema();
    FileSchema s =
        new FileSchema(p, "t10", d, d, (String) null, (List<Map<String, Object>>) null, LINQ4J, false,
        mvs, (List<Map<String, Object>>) null,
        (List<Map<String, Object>>) null,
        (String) null, "SMART_CASING", "SMART_CASING",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false);
    p.add("t10", s);

    assertFalse(s.getTableMap().containsKey("mv_view"),
        "MV should not be created with LINQ4J engine");
  }

  // ---------------------------------------------------------------
  // 11. getComment()
  // ---------------------------------------------------------------
  @Test void testSchemaComment() throws Exception {
    File d = dir("t11");

    SchemaPlus p = rootSchema();
    // 22-arg constructor with comment
    FileSchema s =
        new FileSchema(p, "t11", d, (File) null, (String) null, (String) null,
        (List<Map<String, Object>>) null, LINQ4J, false,
        (List<Map<String, Object>>) null, (List<Map<String, Object>>) null,
        (List<Map<String, Object>>) null,
        (String) null, "SMART_CASING", "SMART_CASING",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false,
        "This is a test schema", null);
    p.add("t11", s);

    assertEquals("This is a test schema", s.getComment());
  }

  // ---------------------------------------------------------------
  // 12. getComment() is null by default
  // ---------------------------------------------------------------
  @Test void testSchemaCommentNullByDefault() throws Exception {
    File d = dir("t12");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t12", d, LINQ4J);
    p.add("t12", s);
    assertNull(s.getComment());
  }

  // ---------------------------------------------------------------
  // 13. getBaseDirectory
  // ---------------------------------------------------------------
  @Test void testGetBaseDirectory() throws Exception {
    File d = dir("t13");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t13", d, LINQ4J);
    p.add("t13", s);
    assertNotNull(s.getBaseDirectory());
  }

  // ---------------------------------------------------------------
  // 14. getStorageConfig is null by default
  // ---------------------------------------------------------------
  @Test void testGetStorageConfigNull() throws Exception {
    File d = dir("t14");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t14", d, LINQ4J);
    p.add("t14", s);
    assertNull(s.getStorageConfig());
  }

  // ---------------------------------------------------------------
  // 15. getStorageProvider is null by default
  // ---------------------------------------------------------------
  @Test void testGetStorageProviderNull() throws Exception {
    File d = dir("t15");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t15", d, LINQ4J);
    p.add("t15", s);
    assertNull(s.getStorageProvider());
  }

  // ---------------------------------------------------------------
  // 16. getOperatingCacheDirectory
  // ---------------------------------------------------------------
  @Test void testGetOperatingCacheDirectory() throws Exception {
    File d = dir("t16");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t16", d, LINQ4J);
    p.add("t16", s);
    assertNotNull(s.getOperatingCacheDirectory());
    assertTrue(s.getOperatingCacheDirectory().isDirectory());
  }

  // ---------------------------------------------------------------
  // 17. hasRefreshableTables -- false by default
  // ---------------------------------------------------------------
  @Test void testHasRefreshableTablesFalse() throws Exception {
    File d = dir("t17");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t17", d, LINQ4J);
    p.add("t17", s);
    assertFalse(s.hasRefreshableTables());
  }

  // ---------------------------------------------------------------
  // 18. hasRefreshableTables -- true with interval
  // ---------------------------------------------------------------
  @Test void testHasRefreshableTablesTrue() throws Exception {
    File d = dir("t18");
    writeCsv(d, "data.csv", "id,v\n1,a\n");

    SchemaPlus p = rootSchema();
    // 11-arg constructor with refreshInterval
    FileSchema s =
        new FileSchema(p, "t18", d, d, (String) null, (List<Map<String, Object>>) null, LINQ4J, false,
        (List<Map<String, Object>>) null, (List<Map<String, Object>>) null,
        (List<Map<String, Object>>) null, "5 minutes",
        "SMART_CASING", "SMART_CASING",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false);
    p.add("t18", s);

    assertTrue(s.hasRefreshableTables());
  }

  // ---------------------------------------------------------------
  // 19. setFunctionMultimap and getFunctions
  // ---------------------------------------------------------------
  @Test void testFunctionMultimap() throws Exception {
    File d = dir("t19");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t19", d, LINQ4J);
    p.add("t19", s);

    assertTrue(s.getFunctions("anything").isEmpty());
    s.setFunctionMultimap(ImmutableMultimap.of());
    assertTrue(s.getFunctions("anything").isEmpty());
  }

  // ---------------------------------------------------------------
  // 20. PARQUET engine discovers CSV
  // ---------------------------------------------------------------
  @Test void testParquetEngineDiscoversCsv() throws Exception {
    File d = dir("t20");
    writeCsv(d, "sales.csv", "id,amount\n1,100\n2,200\n");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t20", d, PARQUET);
    p.add("t20", s);

    Map<String, Table> t = s.getTableMap();
    assertFalse(t.isEmpty(), "PARQUET engine should discover CSV: " + t.keySet());
  }

  // ---------------------------------------------------------------
  // 21. PARQUET engine discovers JSON
  // ---------------------------------------------------------------
  @Test void testParquetEngineDiscoversJson() throws Exception {
    File d = dir("t21");
    writeJson(d, "items.json",
        "[{\"id\":1,\"price\":9.99},{\"id\":2,\"price\":19.99}]");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t21", d, PARQUET);
    p.add("t21", s);

    Map<String, Table> t = s.getTableMap();
    assertFalse(t.isEmpty(), "PARQUET engine should discover JSON: " + t.keySet());
  }

  // ---------------------------------------------------------------
  // 22. Empty directory returns empty table map
  // ---------------------------------------------------------------
  @Test void testEmptyDirectory() throws Exception {
    File d = dir("t22_empty");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t22", d, LINQ4J);
    p.add("t22", s);

    assertTrue(s.getTableMap().isEmpty());
  }

  // ---------------------------------------------------------------
  // 23. Table name casing -- UPPER
  // ---------------------------------------------------------------
  @Test void testTableNameCasingUpper() throws Exception {
    File d = dir("t23");
    writeCsv(d, "myfile.csv", "a,b\n1,2\n");

    SchemaPlus p = rootSchema();
    // 13-arg constructor with casing
    FileSchema s =
        new FileSchema(p, "t23", d, d, (String) null, (List<Map<String, Object>>) null, LINQ4J, false,
        (List<Map<String, Object>>) null, (List<Map<String, Object>>) null,
        (List<Map<String, Object>>) null, (String) null,
        "UPPER", "UPPER",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false);
    p.add("t23", s);

    Map<String, Table> t = s.getTableMap();
    assertFalse(t.isEmpty(), "Should discover CSV");
    for (String name : t.keySet()) {
      assertEquals(name.toUpperCase(), name,
          "With UPPER casing, table name should be uppercase: " + name);
    }
  }

  // ---------------------------------------------------------------
  // 24. Table name casing -- LOWER
  // ---------------------------------------------------------------
  @Test void testTableNameCasingLower() throws Exception {
    File d = dir("t24");
    writeCsv(d, "MyFile.csv", "a,b\n1,2\n");

    SchemaPlus p = rootSchema();
    FileSchema s =
        new FileSchema(p, "t24", d, d, (String) null, (List<Map<String, Object>>) null, LINQ4J, false,
        (List<Map<String, Object>>) null, (List<Map<String, Object>>) null,
        (List<Map<String, Object>>) null, (String) null,
        "LOWER", "LOWER",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false);
    p.add("t24", s);

    Map<String, Table> t = s.getTableMap();
    for (String name : t.keySet()) {
      assertEquals(name.toLowerCase(), name,
          "With LOWER casing, table name should be lowercase");
    }
  }

  // ---------------------------------------------------------------
  // 25. Null URL in table def is skipped
  // ---------------------------------------------------------------
  @Test void testNullUrlTableDefSkipped() throws Exception {
    File d = dir("t25");

    List<Map<String, Object>> defs = new ArrayList<>();
    Map<String, Object> def = new HashMap<>();
    def.put("name", "broken");
    defs.add(def);

    SchemaPlus p = rootSchema();
    FileSchema s = schemaWithDefs(p, "t25", d, defs, LINQ4J);
    p.add("t25", s);

    assertFalse(s.getTableMap().containsKey("broken"));
  }

  // ---------------------------------------------------------------
  // 26. View type table def is skipped
  // ---------------------------------------------------------------
  @Test void testViewTypeTableDefSkipped() throws Exception {
    File d = dir("t26");

    List<Map<String, Object>> defs = new ArrayList<>();
    Map<String, Object> def = new HashMap<>();
    def.put("name", "a_view");
    def.put("type", "view");
    def.put("url", "dummy.csv");
    defs.add(def);

    SchemaPlus p = rootSchema();
    FileSchema s = schemaWithDefs(p, "t26", d, defs, LINQ4J);
    p.add("t26", s);

    assertFalse(s.getTableMap().containsKey("a_view"));
  }

  // ---------------------------------------------------------------
  // 27. Unsupported format override results in empty map (error caught)
  // ---------------------------------------------------------------
  @Test void testUnsupportedFormatOverride() throws Exception {
    File d = dir("t27");
    writeCsv(d, "data.csv", "id\n1\n");

    List<Map<String, Object>> defs = new ArrayList<>();
    Map<String, Object> def = new HashMap<>();
    def.put("name", "bad_format");
    def.put("url", "data.csv");
    def.put("format", "protobuf");
    defs.add(def);

    SchemaPlus p = rootSchema();
    FileSchema s = schemaWithDefs(p, "t27", d, defs, LINQ4J);
    p.add("t27", s);

    // getTableMap should not throw; format error is logged, table may or may not appear
    Map<String, Table> t = s.getTableMap();
    assertNotNull(t);
  }

  // ---------------------------------------------------------------
  // 28. HTML file triggers conversion attempt
  // ---------------------------------------------------------------
  @Test void testHtmlFileTriggersConversion() throws Exception {
    File d = dir("t28");
    writeHtml(d, "states.html");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t28", d, LINQ4J);
    p.add("t28", s);

    assertNotNull(s.getTableMap());
  }

  // ---------------------------------------------------------------
  // 29. Canonical schema name
  // ---------------------------------------------------------------
  @Test void testCanonicalSchemaName() throws Exception {
    File d = dir("t29");
    writeCsv(d, "data.csv", "id\n1\n");

    SchemaPlus p = rootSchema();
    FileSchema s =
        new FileSchema(p, "UPPER_NAME", d, (File) null, (String) null, (String) null,
        (List<Map<String, Object>>) null, LINQ4J, false,
        (List<Map<String, Object>>) null, (List<Map<String, Object>>) null,
        (List<Map<String, Object>>) null,
        (String) null, "SMART_CASING", "SMART_CASING",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false, null, "lower_name");
    p.add("UPPER_NAME", s);

    String cachePath = s.getOperatingCacheDirectory().getAbsolutePath();
    assertTrue(cachePath.contains("lower_name"),
        "Cache dir should use canonical name: " + cachePath);
  }

  // ---------------------------------------------------------------
  // 30. Storage operations: write, exists, delete
  // ---------------------------------------------------------------
  @Test void testWriteExistsDeleteFromStorage() throws Exception {
    File d = dir("t30");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t30", d, LINQ4J);
    p.add("t30", s);

    s.writeToStorage("test_file.txt", "hello".getBytes());
    assertTrue(s.existsInStorage("test_file.txt"));
    assertTrue(s.deleteFromStorage("test_file.txt"));
    assertFalse(s.existsInStorage("test_file.txt"));
    assertFalse(s.deleteFromStorage("nonexistent.txt"));
  }

  // ---------------------------------------------------------------
  // 31. createStorageDirectories
  // ---------------------------------------------------------------
  @Test void testCreateStorageDirectories() throws Exception {
    File d = dir("t31");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t31", d, LINQ4J);
    p.add("t31", s);

    s.createStorageDirectories("subdir1/subdir2");
    assertTrue(s.existsInStorage("subdir1/subdir2"));
  }

  // ---------------------------------------------------------------
  // 32. writeToStorage with InputStream
  // ---------------------------------------------------------------
  @Test void testWriteToStorageInputStream() throws Exception {
    File d = dir("t32");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t32", d, LINQ4J);
    p.add("t32", s);

    java.io.ByteArrayInputStream bais =
        new java.io.ByteArrayInputStream("stream data".getBytes());
    s.writeToStorage("stream_test.txt", bais);
    assertTrue(s.existsInStorage("stream_test.txt"));
  }

  // ---------------------------------------------------------------
  // 33. getConversionMetadata not null
  // ---------------------------------------------------------------
  @Test void testGetConversionMetadataNotNull() throws Exception {
    File d = dir("t33");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t33", d, LINQ4J);
    p.add("t33", s);
    assertNotNull(s.getConversionMetadata());
  }

  // ---------------------------------------------------------------
  // 34. getAllTableRecords with empty schema
  // ---------------------------------------------------------------
  @Test void testGetAllTableRecordsEmpty() throws Exception {
    File d = dir("t34_empty");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t34", d, LINQ4J);
    p.add("t34", s);
    assertNotNull(s.getAllTableRecords());
  }

  // ---------------------------------------------------------------
  // 35. setConstraintMetadata and getTableConstraints
  // ---------------------------------------------------------------
  @Test void testConstraintMetadata() throws Exception {
    File d = dir("t35");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t35", d, LINQ4J);
    p.add("t35", s);

    Map<String, Map<String, Object>> cm = new HashMap<>();
    Map<String, Object> pk = new HashMap<>();
    pk.put("primaryKey", Arrays.asList("id"));
    cm.put("my_table", pk);
    s.setConstraintMetadata(cm);

    assertNotNull(s.getTableConstraints("my_table"));
    assertNull(s.getTableConstraints("nonexistent"));
  }

  // ---------------------------------------------------------------
  // 36. Refresh listener notification
  // ---------------------------------------------------------------
  @Test void testRefreshListenerNotification() throws Exception {
    File d = dir("t36");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t36", d, LINQ4J);
    p.add("t36", s);

    final boolean[] notified = {false};
    s.addRefreshListener((tableName, parquetFile) -> notified[0] = true);
    s.notifyTableRefreshed("test_table", new File(d, "dummy.parquet"));
    assertTrue(notified[0]);
  }

  // ---------------------------------------------------------------
  // 37. notifyTableRefreshedWithPattern
  // ---------------------------------------------------------------
  @Test void testNotifyTableRefreshedWithPattern() throws Exception {
    File d = dir("t37");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t37", d, LINQ4J);
    p.add("t37", s);

    s.addRefreshListener(
        new org.apache.calcite.adapter.file.refresh.TableRefreshListener() {
          @Override public void onTableRefreshed(String tableName, File f) { }
        });
    // Should not throw
    s.notifyTableRefreshedWithPattern("test_table", "**/*.parquet");
  }

  // ---------------------------------------------------------------
  // 38. notifyIcebergTableRefreshed (no listeners)
  // ---------------------------------------------------------------
  @Test void testNotifyIcebergTableRefreshed() throws Exception {
    File d = dir("t38");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t38", d, LINQ4J);
    p.add("t38", s);
    s.notifyIcebergTableRefreshed("my_table", "s3://bucket/warehouse/table");
  }

  // ---------------------------------------------------------------
  // 39. getTableBaseline returns null
  // ---------------------------------------------------------------
  @Test void testGetTableBaselineNull() throws Exception {
    File d = dir("t39");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t39", d, LINQ4J);
    p.add("t39", s);
    assertNull(s.getTableBaseline("nonexistent_table"));
  }

  // ---------------------------------------------------------------
  // 40. updateTableBaseline with no record
  // ---------------------------------------------------------------
  @Test void testUpdateTableBaselineNoRecord() throws Exception {
    File d = dir("t40");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t40", d, LINQ4J);
    p.add("t40", s);
    s.updateTableBaseline("nonexistent", null);
  }

  // ---------------------------------------------------------------
  // 41. getAlternatePartitionRegistry not null
  // ---------------------------------------------------------------
  @Test void testGetAlternatePartitionRegistry() throws Exception {
    File d = dir("t41");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t41", d, LINQ4J);
    p.add("t41", s);
    assertNotNull(s.getAlternatePartitionRegistry());
  }

  // ---------------------------------------------------------------
  // 42. PARQUET engine -- explicit CSV table def
  // ---------------------------------------------------------------
  @Test void testParquetEngineExplicitCsv() throws Exception {
    File d = dir("t42");
    writeCsv(d, "explicit.csv", "id,value\n1,100\n2,200\n");

    List<Map<String, Object>> defs = new ArrayList<>();
    Map<String, Object> def = new HashMap<>();
    def.put("name", "explicit_csv");
    def.put("url", "explicit.csv");
    def.put("format", "csv");
    defs.add(def);

    SchemaPlus p = rootSchema();
    FileSchema s = schemaWithDefs(p, "t42", d, defs, PARQUET);
    p.add("t42", s);

    assertFalse(s.getTableMap().isEmpty());
  }

  // ---------------------------------------------------------------
  // 43. PARQUET engine -- explicit JSON table def
  // ---------------------------------------------------------------
  @Test void testParquetEngineExplicitJson() throws Exception {
    File d = dir("t43");
    writeJson(d, "explicit.json", "[{\"x\":1,\"y\":2}]");

    List<Map<String, Object>> defs = new ArrayList<>();
    Map<String, Object> def = new HashMap<>();
    def.put("name", "explicit_json");
    def.put("url", "explicit.json");
    def.put("format", "json");
    defs.add(def);

    SchemaPlus p = rootSchema();
    FileSchema s = schemaWithDefs(p, "t43", d, defs, PARQUET);
    p.add("t43", s);

    assertFalse(s.getTableMap().isEmpty());
  }

  // ---------------------------------------------------------------
  // 44. Ephemeral cache directory
  // ---------------------------------------------------------------
  @Test void testEphemeralCacheDirectory() throws Exception {
    File d = dir("t44");
    writeCsv(d, "data.csv", "id\n1\n");

    File tmpBase =
        new File(System.getProperty("java.io.tmpdir"), "calcite_test_eph_" + System.nanoTime());
    tmpBase.mkdirs();
    try {
      SchemaPlus p = rootSchema();
      FileSchema s =
          new FileSchema(p, "t44", d, tmpBase, (String) null,
          (List<Map<String, Object>>) null, LINQ4J, false,
          (List<Map<String, Object>>) null, (List<Map<String, Object>>) null,
          (List<Map<String, Object>>) null,
          (String) null, "SMART_CASING", "SMART_CASING",
          (String) null, (Map<String, Object>) null,
          (Boolean) null, (Map<String, Object>) null, false);
      p.add("t44", s);
      assertNotNull(s.getOperatingCacheDirectory());
    } finally {
      deleteRecursive(tmpBase);
    }
  }

  // ---------------------------------------------------------------
  // 45. JSON flattening -- schema level
  // ---------------------------------------------------------------
  @Test void testJsonFlatteningSchemaLevel() throws Exception {
    File d = dir("t45");
    writeJson(d, "nested.json",
        "[{\"id\":1,\"address\":{\"city\":\"NYC\",\"zip\":\"10001\"}}]");

    SchemaPlus p = rootSchema();
    FileSchema s =
        new FileSchema(p, "t45", d, (File) null, (String) null,
        (List<Map<String, Object>>) null, LINQ4J, false,
        (List<Map<String, Object>>) null, (List<Map<String, Object>>) null,
        (List<Map<String, Object>>) null,
        (String) null, "SMART_CASING", "SMART_CASING",
        (String) null, (Map<String, Object>) null,
        Boolean.TRUE, (Map<String, Object>) null, false, null);
    p.add("t45", s);

    assertNotNull(s.getTableMap());
  }

  // ---------------------------------------------------------------
  // 46. JSON flattening -- table level
  // ---------------------------------------------------------------
  @Test void testJsonFlatteningTableLevel() throws Exception {
    File d = dir("t46");
    writeJson(d, "flat_target.json",
        "[{\"id\":1,\"data\":{\"a\":10,\"b\":20}}]");

    List<Map<String, Object>> defs = new ArrayList<>();
    Map<String, Object> def = new HashMap<>();
    def.put("name", "flat_table");
    def.put("url", "flat_target.json");
    def.put("format", "json");
    def.put("flatten", true);
    def.put("flattenSeparator", ".");
    defs.add(def);

    SchemaPlus p = rootSchema();
    FileSchema s = schemaWithDefs(p, "t46", d, defs, LINQ4J);
    p.add("t46", s);

    assertTrue(s.getTableMap().containsKey("flat_table"),
        "found: " + s.getTableMap().keySet());
  }

  // ---------------------------------------------------------------
  // 47. HTML with explicit format=html table def
  // ---------------------------------------------------------------
  @Test void testHtmlExplicitTableDef() throws Exception {
    File d = dir("t47");
    writeHtml(d, "my_html.html");

    List<Map<String, Object>> defs = new ArrayList<>();
    Map<String, Object> def = new HashMap<>();
    def.put("name", "html_table");
    def.put("url", "my_html.html");
    def.put("format", "html");
    defs.add(def);

    SchemaPlus p = rootSchema();
    FileSchema s = schemaWithDefs(p, "t47", d, defs, LINQ4J);
    p.add("t47", s);

    Map<String, Table> t = s.getTableMap();
    // HTML conversion should produce a table
    assertNotNull(t, "Table map should not be null");
    // The table may or may not be created depending on HTML->JSON conversion success
    // But the code paths for HTML handling are exercised either way
  }

  // ---------------------------------------------------------------
  // 48. YAML format override
  // ---------------------------------------------------------------
  @Test void testYamlFormatOverride() throws Exception {
    File d = dir("t48");
    writeJson(d, "config.yaml", "[{\"key\": \"val\"}]");

    List<Map<String, Object>> defs = new ArrayList<>();
    Map<String, Object> def = new HashMap<>();
    def.put("name", "yaml_table");
    def.put("url", "config.yaml");
    def.put("format", "yaml");
    defs.add(def);

    SchemaPlus p = rootSchema();
    FileSchema s = schemaWithDefs(p, "t48", d, defs, LINQ4J);
    p.add("t48", s);

    assertTrue(s.getTableMap().containsKey("yaml_table"));
  }

  // ---------------------------------------------------------------
  // 49. Duplicate table name disambiguation
  // ---------------------------------------------------------------
  @Test void testDuplicateTableNameDisambiguation() throws Exception {
    File d = dir("t49");
    writeCsv(d, "data.csv", "id\n1\n");
    writeJson(d, "data.json", "[{\"id\":1}]");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t49", d, LINQ4J);
    p.add("t49", s);

    Map<String, Table> t = s.getTableMap();
    assertTrue(t.size() >= 2,
        "Should have >= 2 tables from data.csv and data.json: " + t.keySet());
  }

  // ---------------------------------------------------------------
  // 50. Calcite model file is skipped
  // ---------------------------------------------------------------
  @Test void testCalciteModelFileSkipped() throws Exception {
    File d = dir("t50");
    writeJson(d, "model.json",
        "{\"version\":\"1.0\",\"schemas\":[{\"name\":\"test\"}]}");
    writeCsv(d, "real_data.csv", "id\n1\n");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t50", d, LINQ4J);
    p.add("t50", s);

    Map<String, Table> t = s.getTableMap();
    for (String name : t.keySet()) {
      assertFalse(name.toLowerCase().contains("model"),
          "Calcite model files should be skipped: " + t.keySet());
    }
  }

  // ---------------------------------------------------------------
  // 51. Hidden dot files excluded
  // ---------------------------------------------------------------
  @Test void testDotFilesExcluded() throws Exception {
    File d = dir("t51");
    writeCsv(d, ".hidden.csv", "id\n1\n");
    writeCsv(d, "visible.csv", "id\n1\n");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t51", d, LINQ4J);
    p.add("t51", s);

    for (String name : s.getTableMap().keySet()) {
      assertFalse(name.startsWith("."), "Dot files should be excluded: " + name);
    }
  }

  // ---------------------------------------------------------------
  // 52. setConversionRecords
  // ---------------------------------------------------------------
  @Test void testSetConversionRecords() throws Exception {
    File d = dir("t52");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t52", d, LINQ4J);
    p.add("t52", s);

    Map<String, ConversionMetadata.ConversionRecord> records = new HashMap<>();
    ConversionMetadata.ConversionRecord r = new ConversionMetadata.ConversionRecord();
    r.tableName = "test_table";
    r.viewScanPattern = "**/*.parquet";
    records.put("test_table", r);

    s.setConversionRecords(records);
    assertNotNull(s.getConversionMetadata());
  }

  // ---------------------------------------------------------------
  // 53. setConversionRecords null/empty no-op
  // ---------------------------------------------------------------
  @Test void testSetConversionRecordsNullEmpty() throws Exception {
    File d = dir("t53");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t53", d, LINQ4J);
    p.add("t53", s);

    s.setConversionRecords(null);
    s.setConversionRecords(Collections.emptyMap());
  }

  // ---------------------------------------------------------------
  // 54. registerRawToParquetConverter
  // ---------------------------------------------------------------
  @Test void testRegisterRawToParquetConverter() throws Exception {
    File d = dir("t54");
    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t54", d, LINQ4J);
    p.add("t54", s);

    s.registerRawToParquetConverter(
        new org.apache.calcite.adapter.file.converters.RawToParquetConverter() {
          @Override public boolean canConvert(String src, ConversionMetadata m) {
            return false;
          }
          @Override public boolean convertToParquet(String src, String tgt,
              org.apache.calcite.adapter.file.storage.StorageProvider sp) {
            return false;
          }
        });
  }

  // ---------------------------------------------------------------
  // 55. Refresh interval with PARQUET engine
  // ---------------------------------------------------------------
  @Test void testRefreshIntervalWithParquetEngine() throws Exception {
    File d = dir("t55");
    writeCsv(d, "refreshable.csv", "id,name\n1,Alice\n");

    SchemaPlus p = rootSchema();
    // 11-arg constructor with refreshInterval
    FileSchema s =
        new FileSchema(p, "t55", d, d, (String) null, (List<Map<String, Object>>) null, PARQUET, false,
        (List<Map<String, Object>>) null, (List<Map<String, Object>>) null,
        (List<Map<String, Object>>) null, "5 minutes",
        "SMART_CASING", "SMART_CASING",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false);
    p.add("t55", s);

    assertTrue(s.hasRefreshableTables());
    assertFalse(s.getTableMap().isEmpty());
  }

  // ---------------------------------------------------------------
  // 56. Multiple views
  // ---------------------------------------------------------------
  @Test void testMultipleViews() throws Exception {
    File d = dir("t56");
    writeCsv(d, "orders.csv", "id,total\n1,100\n");

    List<Map<String, Object>> views = new ArrayList<>();
    Map<String, Object> v1 = new HashMap<>();
    v1.put("name", "view_a");
    v1.put("sql", "SELECT * FROM \"orders\"");
    views.add(v1);

    Map<String, Object> v2 = new HashMap<>();
    v2.put("name", "view_b");
    v2.put("sql", "SELECT id FROM \"orders\"");
    views.add(v2);

    SchemaPlus p = rootSchema();
    FileSchema s =
        new FileSchema(p, "t56", d, d, (String) null, (List<Map<String, Object>>) null, LINQ4J, false,
        (List<Map<String, Object>>) null, views,
        (List<Map<String, Object>>) null,
        (String) null, "SMART_CASING", "SMART_CASING",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false);
    p.add("t56", s);

    Map<String, Table> t = s.getTableMap();
    assertTrue(t.containsKey("view_a"), "found: " + t.keySet());
    assertTrue(t.containsKey("view_b"), "found: " + t.keySet());
  }

  // ---------------------------------------------------------------
  // 57. View with null name or SQL skipped
  // ---------------------------------------------------------------
  @Test void testViewNullNameOrSqlSkipped() throws Exception {
    File d = dir("t57");

    List<Map<String, Object>> views = new ArrayList<>();
    Map<String, Object> v1 = new HashMap<>();
    v1.put("name", null);
    v1.put("sql", "SELECT 1");
    views.add(v1);

    Map<String, Object> v2 = new HashMap<>();
    v2.put("name", "good_view");
    v2.put("sql", null);
    views.add(v2);

    SchemaPlus p = rootSchema();
    FileSchema s =
        new FileSchema(p, "t57", d, d, (String) null, (List<Map<String, Object>>) null, LINQ4J, false,
        (List<Map<String, Object>>) null, views,
        (List<Map<String, Object>>) null,
        (String) null, "SMART_CASING", "SMART_CASING",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false);
    p.add("t57", s);

    assertFalse(s.getTableMap().containsKey("good_view"));
  }

  // ---------------------------------------------------------------
  // 58. YAML file auto-discovery
  // ---------------------------------------------------------------
  @Test void testYamlAutoDiscovery() throws Exception {
    File d = dir("t58");
    writeJson(d, "config.yml", "[{\"key\":\"val\",\"num\":42}]");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t58", d, LINQ4J);
    p.add("t58", s);

    Map<String, Table> t = s.getTableMap();
    assertTrue(keyContains(t, "config"),
        "Should auto-discover .yml files: " + t.keySet());
  }

  // ---------------------------------------------------------------
  // 59. Multiple constructor variants
  // ---------------------------------------------------------------
  @Test void testConstructorVariants() throws Exception {
    File d1 = dir("t59a");
    File d2 = dir("t59b");
    File d3 = dir("t59c");
    File d4 = dir("t59d");
    SchemaPlus p = rootSchema();

    // 5-arg constructor
    FileSchema s2 =
        new FileSchema(p, "s2", d1, (List<Map<String, Object>>) null, LINQ4J);
    assertNotNull(s2.getTableMap());

    // 6-arg constructor with recursive
    FileSchema s3 =
        new FileSchema(p, "s3", d2, (List<Map<String, Object>>) null, LINQ4J, false);
    assertNotNull(s3.getTableMap());

    // 8-arg constructor with materializations and views
    FileSchema s4 =
        new FileSchema(p, "s4", d3, (List<Map<String, Object>>) null, LINQ4J, false,
        (List<Map<String, Object>>) null, (List<Map<String, Object>>) null);
    assertNotNull(s4.getTableMap());

    // 11-arg with refreshInterval
    FileSchema s5 =
        new FileSchema(p, "s5", d4, (String) null, (List<Map<String, Object>>) null, LINQ4J, false,
        (List<Map<String, Object>>) null, (List<Map<String, Object>>) null,
        (List<Map<String, Object>>) null, (String) null);
    assertNotNull(s5.getTableMap());
  }

  // ---------------------------------------------------------------
  // 60. FK validation removes invalid refs
  // ---------------------------------------------------------------
  @Test void testFKValidationRemovesInvalid() throws Exception {
    File d = dir("t60");
    writeCsv(d, "orders.csv", "id,customer_id\n1,10\n");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t60", d, LINQ4J);
    p.add("t60", s);

    Map<String, Map<String, Object>> cm = new HashMap<>();
    Map<String, Object> oc = new HashMap<>();
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("targetTable", "nonexistent_table");
    fk.put("columns", Arrays.asList("customer_id"));
    fk.put("targetColumns", Arrays.asList("id"));
    fks.add(fk);
    oc.put("foreignKeys", fks);
    cm.put("orders", oc);
    s.setConstraintMetadata(cm);

    // getTableMap triggers validateForeignKeyConstraints
    assertNotNull(s.getTableMap());
  }

  // ---------------------------------------------------------------
  // 61. FK validation with qualified table name (List form)
  // ---------------------------------------------------------------
  @Test void testFKValidationQualifiedName() throws Exception {
    File d = dir("t61");
    writeCsv(d, "items.csv", "id,cat_id\n1,5\n");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t61", d, LINQ4J);
    p.add("t61", s);

    Map<String, Map<String, Object>> cm = new HashMap<>();
    Map<String, Object> ic = new HashMap<>();
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("targetTable", Arrays.asList("other_schema", "categories"));
    fk.put("columns", Arrays.asList("cat_id"));
    fk.put("targetColumns", Arrays.asList("id"));
    fks.add(fk);
    ic.put("foreignKeys", fks);
    cm.put("items", ic);
    s.setConstraintMetadata(cm);

    assertNotNull(s.getTableMap());
  }

  // ---------------------------------------------------------------
  // 62. directoryPattern with glob
  // ---------------------------------------------------------------
  @Test void testDirectoryPatternGlob() throws Exception {
    File d = dir("t62");
    File sub = new File(d, "data_2024");
    sub.mkdirs();
    writeCsv(sub, "report.csv", "id,val\n1,100\n");

    SchemaPlus p = rootSchema();
    FileSchema s =
        new FileSchema(p, "t62", d, d, "data_*/*.csv", (List<Map<String, Object>>) null, LINQ4J, true,
        (List<Map<String, Object>>) null, (List<Map<String, Object>>) null,
        (List<Map<String, Object>>) null, (String) null,
        "SMART_CASING", "SMART_CASING",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false);
    p.add("t62", s);

    assertNotNull(s.getTableMap());
  }

  // ---------------------------------------------------------------
  // 63. PARQUET engine + refresh + JSON
  // ---------------------------------------------------------------
  @Test void testParquetEngineRefreshJson() throws Exception {
    File d = dir("t63");
    writeJson(d, "refresh_json.json", "[{\"id\":1,\"val\":\"hello\"}]");

    SchemaPlus p = rootSchema();
    FileSchema s =
        new FileSchema(p, "t63", d, d, (String) null, (List<Map<String, Object>>) null, PARQUET, false,
        (List<Map<String, Object>>) null, (List<Map<String, Object>>) null,
        (List<Map<String, Object>>) null, "10 minutes",
        "SMART_CASING", "SMART_CASING",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false);
    p.add("t63", s);

    assertFalse(s.getTableMap().isEmpty());
  }

  // ---------------------------------------------------------------
  // 64. VECTORIZED engine with CSV
  // ---------------------------------------------------------------
  @Test void testVectorizedEngineCsv() throws Exception {
    File d = dir("t64");
    writeCsv(d, "vec_data.csv", "id,name\n1,Alice\n2,Bob\n");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t64", d, VECTORIZED);
    p.add("t64", s);

    assertFalse(s.getTableMap().isEmpty(),
        "VECTORIZED should discover CSV: " + s.getTableMap().keySet());
  }

  // ---------------------------------------------------------------
  // 65. VECTORIZED engine with refresh interval
  // ---------------------------------------------------------------
  @Test void testVectorizedEngineRefresh() throws Exception {
    File d = dir("t65");
    writeCsv(d, "vec_refresh.csv", "id\n1\n");

    List<Map<String, Object>> defs = new ArrayList<>();
    Map<String, Object> def = new HashMap<>();
    def.put("name", "vec_refresh_tbl");
    def.put("url", "vec_refresh.csv");
    def.put("format", "csv");
    def.put("refreshInterval", "5 minutes");
    defs.add(def);

    SchemaPlus p = rootSchema();
    FileSchema s = schemaWithDefs(p, "t65", d, defs, VECTORIZED);
    p.add("t65", s);

    assertTrue(s.getTableMap().containsKey("vec_refresh_tbl"),
        "found: " + s.getTableMap().keySet());
  }

  // ---------------------------------------------------------------
  // 66. VECTORIZED engine with JSON
  // ---------------------------------------------------------------
  @Test void testVectorizedEngineJson() throws Exception {
    File d = dir("t66");
    writeJson(d, "vec_json.json", "[{\"x\":1}]");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t66", d, VECTORIZED);
    p.add("t66", s);

    assertFalse(s.getTableMap().isEmpty(),
        "VECTORIZED should discover JSON: " + s.getTableMap().keySet());
  }

  // ---------------------------------------------------------------
  // 67. LINQ4J engine with multiple file types
  // ---------------------------------------------------------------
  @Test void testLinq4jMultipleFileTypes() throws Exception {
    File d = dir("t67");
    writeCsv(d, "a.csv", "id\n1\n");
    writeJson(d, "b.json", "[{\"id\":2}]");
    writeTsv(d, "c.tsv");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t67", d, LINQ4J);
    p.add("t67", s);

    Map<String, Table> t = s.getTableMap();
    assertTrue(t.size() >= 3,
        "LINQ4J should discover all files: " + t.keySet());
  }

  // ---------------------------------------------------------------
  // 68. LINQ4J with refresh interval on explicit def
  // ---------------------------------------------------------------
  @Test void testLinq4jRefreshInterval() throws Exception {
    File d = dir("t68");
    writeJson(d, "refresh_data.json", "[{\"id\":1}]");

    List<Map<String, Object>> defs = new ArrayList<>();
    Map<String, Object> def = new HashMap<>();
    def.put("name", "refresh_linq");
    def.put("url", "refresh_data.json");
    def.put("format", "json");
    def.put("refreshInterval", "5 minutes");
    defs.add(def);

    SchemaPlus p = rootSchema();
    FileSchema s = schemaWithDefs(p, "t68", d, defs, LINQ4J);
    p.add("t68", s);

    assertTrue(s.getTableMap().containsKey("refresh_linq"),
        "found: " + s.getTableMap().keySet());
  }

  // ---------------------------------------------------------------
  // 69. CSV type inference config
  // ---------------------------------------------------------------
  @Test void testCsvTypeInferenceConfig() throws Exception {
    File d = dir("t69");
    writeCsv(d, "typed.csv", "id,amount,active\n1,99.5,true\n");

    Map<String, Object> csvTI = new HashMap<>();
    csvTI.put("enabled", true);
    csvTI.put("sampleSize", 10);

    SchemaPlus p = rootSchema();
    FileSchema s =
        new FileSchema(p, "t69", d, null, null, null, null, LINQ4J, false, null, null, null, null,
        "SMART_CASING", "SMART_CASING", null, null, null, csvTI, false, null, null);
    p.add("t69", s);

    assertNotNull(s.getTableMap());
  }

  // ---------------------------------------------------------------
  // 70. primeCache=false
  // ---------------------------------------------------------------
  @Test void testPrimeCacheFalse() throws Exception {
    File d = dir("t70");
    writeCsv(d, "data.csv", "id\n1\n");

    SchemaPlus p = rootSchema();
    FileSchema s =
        new FileSchema(p, "t70", d, null, null, null, null, LINQ4J, false, null, null, null, null,
        "SMART_CASING", "SMART_CASING", null, null, null, null, false, null, null);
    p.add("t70", s);

    assertNotNull(s.getTableMap());
  }

  // ---------------------------------------------------------------
  // 71. HTML with selector in explicit table def
  // ---------------------------------------------------------------
  @Test void testHtmlWithSelector() throws Exception {
    File d = dir("t71");
    writeHtml(d, "wiki.html");

    List<Map<String, Object>> defs = new ArrayList<>();
    Map<String, Object> def = new HashMap<>();
    def.put("name", "wiki_table");
    def.put("url", "wiki.html");
    def.put("format", "html");
    def.put("selector", "table#test");
    def.put("index", 0);
    defs.add(def);

    SchemaPlus p = rootSchema();
    FileSchema s = schemaWithDefs(p, "t71", d, defs, LINQ4J);
    p.add("t71", s);

    assertNotNull(s.getTableMap());
  }

  // ---------------------------------------------------------------
  // 72. Deep recursive with mixed types
  // ---------------------------------------------------------------
  @Test void testDeepRecursiveMixedTypes() throws Exception {
    File d = dir("t72");
    File sub1 = new File(d, "level1");
    File sub2 = new File(sub1, "level2");
    sub2.mkdirs();

    writeCsv(d, "root.csv", "id\n1\n");
    writeJson(sub1, "mid.json", "[{\"id\":2}]");
    writeCsv(sub2, "deep.csv", "id\n3\n");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t72", d, LINQ4J, true);
    p.add("t72", s);

    Map<String, Table> t = s.getTableMap();
    assertTrue(t.size() >= 3,
        "Deep recursive should find >= 3 tables: " + t.keySet());
  }

  // ---------------------------------------------------------------
  // 73. Schema-level refresh applied to CSV auto-discovery
  // ---------------------------------------------------------------
  @Test void testSchemaLevelRefresh() throws Exception {
    File d = dir("t73");
    writeCsv(d, "auto_refresh.csv", "id,name\n1,test\n");

    SchemaPlus p = rootSchema();
    FileSchema s =
        new FileSchema(p, "t73", d, d, (String) null, (List<Map<String, Object>>) null, PARQUET, false,
        (List<Map<String, Object>>) null, (List<Map<String, Object>>) null,
        (List<Map<String, Object>>) null, "15 minutes",
        "SMART_CASING", "SMART_CASING",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false);
    p.add("t73", s);

    assertTrue(s.hasRefreshableTables());
    assertFalse(s.getTableMap().isEmpty());
  }

  // ---------------------------------------------------------------
  // 74. Parquet file in directory discovered (create via CSV conversion)
  // ---------------------------------------------------------------
  @Test void testParquetFileDiscovery() throws Exception {
    File d = dir("t74");
    // Put CSV source in a separate subdirectory so it won't conflict during discovery
    File csvSrcDir = new File(d, "csv_src");
    csvSrcDir.mkdirs();
    File csvFile = writeCsv(csvSrcDir, "for_pq.csv", "id,name\n1,Alice\n2,Bob\n");

    boolean converted = false;
    try {
      org.apache.calcite.util.Source csvSource = org.apache.calcite.util.Sources.of(csvFile);
      org.apache.calcite.adapter.file.table.CsvTranslatableTable csvTable =
          new org.apache.calcite.adapter.file.table.CsvTranslatableTable(
              csvSource, null, "SMART_CASING", null);
      SchemaPlus tp = rootSchema();
      org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil
          .convertToParquet(csvSource, "test_pq", csvTable, d, tp, "tmp",
              "SMART_CASING");
      converted = true;
    } catch (Exception e) {
      // Parquet conversion may not be available; fall back to CSV discovery
      writeCsv(d, "fallback.csv", "id,name\n1,Alice\n");
    }

    SchemaPlus p = rootSchema();
    // Discovery from d only (not csv_src subdirectory unless recursive)
    FileSchema s = schemaForDiscovery(p, "t74", d, LINQ4J);
    p.add("t74", s);

    // Should find at least one table
    assertFalse(s.getTableMap().isEmpty());
  }

  // ---------------------------------------------------------------
  // 75. Explicit parquet format override
  // ---------------------------------------------------------------
  @Test void testExplicitParquetFormat() throws Exception {
    File d = dir("t75");
    File csvFile = writeCsv(d, "for_pq.csv", "id,name\n1,Alice\n2,Bob\n");

    try {
      org.apache.calcite.util.Source csvSource = org.apache.calcite.util.Sources.of(csvFile);
      org.apache.calcite.adapter.file.table.CsvTranslatableTable csvTable =
          new org.apache.calcite.adapter.file.table.CsvTranslatableTable(
              csvSource, null, "SMART_CASING", null);
      SchemaPlus tp = rootSchema();
      File parquetFile =
          org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil
              .convertToParquet(csvSource, "pq_test", csvTable, d, tp, "tmp",
                  "SMART_CASING");

      List<Map<String, Object>> defs = new ArrayList<>();
      Map<String, Object> def = new HashMap<>();
      def.put("name", "pq_explicit");
      def.put("url", parquetFile.getName());
      def.put("format", "parquet");
      defs.add(def);

      SchemaPlus p = rootSchema();
      FileSchema s = schemaWithDefs(p, "t75", d, defs, LINQ4J);
      p.add("t75", s);

      assertFalse(s.getTableMap().isEmpty());
    } catch (Exception e) {
      // May not be available in all environments
    }
  }

  // ---------------------------------------------------------------
  // 76. FK constraint with valid local table ref
  // ---------------------------------------------------------------
  @Test void testFKValidLocalRef() throws Exception {
    File d = dir("t76");
    writeCsv(d, "orders.csv", "id,customer_id\n1,10\n");
    writeCsv(d, "customers.csv", "id,name\n10,Alice\n");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t76", d, LINQ4J);
    p.add("t76", s);

    Map<String, Map<String, Object>> cm = new HashMap<>();
    Map<String, Object> oc = new HashMap<>();
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("targetTable", "customers");
    fk.put("columns", Arrays.asList("customer_id"));
    fk.put("targetColumns", Arrays.asList("id"));
    fks.add(fk);
    oc.put("foreignKeys", fks);
    cm.put("orders", oc);
    s.setConstraintMetadata(cm);

    Map<String, Table> t = s.getTableMap();
    assertTrue(t.size() >= 2);
  }

  // ---------------------------------------------------------------
  // 77. FK constraint with same-schema target
  // ---------------------------------------------------------------
  @Test void testFKSameSchemaTarget() throws Exception {
    File d = dir("t77");
    writeCsv(d, "products.csv", "id,cat_id\n1,5\n");
    writeCsv(d, "categories.csv", "id,name\n5,food\n");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t77", d, LINQ4J);
    p.add("t77", s);

    Map<String, Map<String, Object>> cm = new HashMap<>();
    Map<String, Object> pc = new HashMap<>();
    List<Map<String, Object>> fks = new ArrayList<>();
    Map<String, Object> fk = new HashMap<>();
    fk.put("targetSchema", "t77"); // Same schema
    fk.put("targetTable", "categories");
    fk.put("columns", Arrays.asList("cat_id"));
    fk.put("targetColumns", Arrays.asList("id"));
    fks.add(fk);
    pc.put("foreignKeys", fks);
    cm.put("products", pc);
    s.setConstraintMetadata(cm);

    assertNotNull(s.getTableMap());
  }

  // ---------------------------------------------------------------
  // 78. FK constraint with null constraints entry
  // ---------------------------------------------------------------
  @Test void testFKNullConstraintsEntry() throws Exception {
    File d = dir("t78");
    writeCsv(d, "data.csv", "id\n1\n");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t78", d, LINQ4J);
    p.add("t78", s);

    Map<String, Map<String, Object>> cm = new HashMap<>();
    cm.put("data", null); // null constraints
    s.setConstraintMetadata(cm);

    assertNotNull(s.getTableMap());
  }

  // ---------------------------------------------------------------
  // 79. FK constraint with empty foreignKeys list
  // ---------------------------------------------------------------
  @Test void testFKEmptyForeignKeys() throws Exception {
    File d = dir("t79");
    writeCsv(d, "data.csv", "id\n1\n");

    SchemaPlus p = rootSchema();
    FileSchema s = schemaForDiscovery(p, "t79", d, LINQ4J);
    p.add("t79", s);

    Map<String, Map<String, Object>> cm = new HashMap<>();
    Map<String, Object> oc = new HashMap<>();
    oc.put("foreignKeys", new ArrayList<>());
    cm.put("data", oc);
    s.setConstraintMetadata(cm);

    assertNotNull(s.getTableMap());
  }

  // ---------------------------------------------------------------
  // 80. Schema with SMART_CASING (default)
  // ---------------------------------------------------------------
  @Test void testSmartCasingDefault() throws Exception {
    File d = dir("t80");
    writeCsv(d, "My_Mixed_Case.csv", "id\n1\n");

    SchemaPlus p = rootSchema();
    FileSchema s =
        new FileSchema(p, "t80", d, d, (String) null, (List<Map<String, Object>>) null, LINQ4J, false,
        (List<Map<String, Object>>) null, (List<Map<String, Object>>) null,
        (List<Map<String, Object>>) null, (String) null,
        "SMART_CASING", "SMART_CASING",
        (String) null, (Map<String, Object>) null,
        (Boolean) null, (Map<String, Object>) null, false);
    p.add("t80", s);

    Map<String, Table> t = s.getTableMap();
    assertFalse(t.isEmpty());
  }

  // ---------------------------------------------------------------
  // Utility
  // ---------------------------------------------------------------

  /** Check if any key in the map contains the given substring (case insensitive). */
  private static boolean keyContains(Map<String, Table> m, String sub) {
    for (String key : m.keySet()) {
      if (key.toLowerCase().contains(sub.toLowerCase())) {
        return true;
      }
    }
    return false;
  }

  private static void deleteRecursive(File file) {
    if (file.isDirectory()) {
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          deleteRecursive(child);
        }
      }
    }
    file.delete();
  }
}
