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
package org.apache.calcite.adapter.file.refresh;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Deep coverage tests for {@link RefreshableJsonTable} targeting:
 * <ul>
 *   <li>{@code doRefresh()} with a non-null {@code operatingCacheDirectory}</li>
 *   <li>{@code scan(DataContext)} and the inner enumerator creation</li>
 *   <li>{@code getDataList()} via scan execution</li>
 * </ul>
 */
@Tag("unit")
public class RefreshableJsonTableDeepCoverageTest {

  @TempDir
  java.nio.file.Path tempDir;

  private File createJsonFile(String name, String content) throws IOException {
    File f = new File(tempDir.toFile(), name);
    try (FileWriter w = new FileWriter(f)) {
      w.write(content);
    }
    return f;
  }

  // === doRefresh with operatingCacheDirectory ===

  @Test
  public void testDoRefreshWithOperatingCacheDirectory() throws IOException {
    File jsonFile = createJsonFile("refresh_test.json", "[{\"id\":1,\"name\":\"Alice\"}]");
    File cacheDir = new File(tempDir.toFile(), "cache");
    cacheDir.mkdirs();
    Source source = Sources.of(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "refresh_test", Duration.ofMillis(1), "UNCHANGED", cacheDir);

    // Trigger doRefresh - should initialize ConversionMetadata and check for original source
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  public void testDoRefreshWithOperatingCacheDirAndModifiedFile() throws Exception {
    File jsonFile = createJsonFile("modified.json", "[{\"val\":1}]");
    File cacheDir = new File(tempDir.toFile(), "mod_cache");
    cacheDir.mkdirs();
    Source source = Sources.of(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "modified_table", Duration.ofMillis(1), "UNCHANGED", cacheDir);

    // First refresh
    table.refresh();

    // Wait and modify the file
    Thread.sleep(10);
    try (FileWriter w = new FileWriter(jsonFile)) {
      w.write("[{\"val\":2}]");
    }
    jsonFile.setLastModified(System.currentTimeMillis() + 1000);

    // Wait for refresh interval
    Thread.sleep(10);

    // Second refresh - should detect modification
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  @Test
  public void testDoRefreshWithNonExistentCacheDir() throws IOException {
    File jsonFile = createJsonFile("no_cache.json", "[{\"a\":1}]");
    // Point to a cache directory that doesn't exist
    File cacheDir = new File(tempDir.toFile(), "nonexistent_cache");
    // Do NOT create cacheDir
    Source source = Sources.of(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "no_cache_table", Duration.ofMillis(1), "UNCHANGED", cacheDir);

    // Should handle initialization failure gracefully
    table.refresh();
    assertNotNull(table.getLastRefreshTime());
  }

  // === scan() and getDataList() ===

  @Test
  public void testScanReturnsEnumerable() throws IOException {
    File jsonFile = createJsonFile("scan_test.json",
        "[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]");
    Source source = Sources.of(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "scan_table", null);

    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();

    // Create a minimal DataContext mock
    DataContext dataContext = mock(DataContext.class);
    when(dataContext.getTypeFactory()).thenReturn(typeFactory);

    Enumerable<Object[]> enumerable = table.scan(dataContext);
    assertNotNull(enumerable);

    // Exercise the enumerator - it creates getDataList() internally
    Enumerator<Object[]> enumerator = enumerable.enumerator();
    assertNotNull(enumerator);

    int count = 0;
    while (enumerator.moveNext()) {
      assertNotNull(enumerator.current());
      count++;
    }
    enumerator.close();

    assertTrue(count == 2, "Should scan 2 rows from JSON array");
  }

  @Test
  public void testScanWithRefreshInterval() throws IOException {
    File jsonFile = createJsonFile("scan_refresh.json",
        "[{\"x\":10},{\"x\":20},{\"x\":30}]");
    Source source = Sources.of(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "scan_refresh_table", Duration.ofMinutes(5));

    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    DataContext dataContext = mock(DataContext.class);
    when(dataContext.getTypeFactory()).thenReturn(typeFactory);

    // scan() calls refresh() first, then returns enumerable
    Enumerable<Object[]> enumerable = table.scan(dataContext);
    assertNotNull(enumerable);

    int count = 0;
    Enumerator<Object[]> enumerator = enumerable.enumerator();
    while (enumerator.moveNext()) {
      count++;
    }
    enumerator.close();

    assertTrue(count == 3, "Should scan 3 rows");
  }

  @Test
  public void testScanReturnsEmptyForEmptyArray() throws IOException {
    File jsonFile = createJsonFile("empty_scan.json", "[]");
    Source source = Sources.of(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "empty_table", null);

    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    DataContext dataContext = mock(DataContext.class);
    when(dataContext.getTypeFactory()).thenReturn(typeFactory);

    Enumerable<Object[]> enumerable = table.scan(dataContext);
    assertNotNull(enumerable);

    Enumerator<Object[]> enumerator = enumerable.enumerator();
    int count = 0;
    while (enumerator.moveNext()) {
      count++;
    }
    enumerator.close();

    assertTrue(count == 0, "Empty JSON array should produce 0 rows");
  }

  @Test
  public void testScanCachesDataList() throws IOException {
    File jsonFile = createJsonFile("cached_data.json",
        "[{\"v\":1},{\"v\":2}]");
    Source source = Sources.of(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "cached_table", null);

    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    DataContext dataContext = mock(DataContext.class);
    when(dataContext.getTypeFactory()).thenReturn(typeFactory);

    // Two scans - second call should use cached dataList
    Enumerable<Object[]> e1 = table.scan(dataContext);
    Enumerable<Object[]> e2 = table.scan(dataContext);

    assertNotNull(e1);
    assertNotNull(e2);

    // Both should yield 2 rows
    int c1 = countEnumerator(e1.enumerator());
    int c2 = countEnumerator(e2.enumerator());
    assertTrue(c1 == 2 && c2 == 2);
  }

  private int countEnumerator(Enumerator<Object[]> e) {
    int c = 0;
    while (e.moveNext()) {
      c++;
    }
    e.close();
    return c;
  }

  @Test
  public void testScanWithOperatingCacheDirectory() throws IOException {
    File jsonFile = createJsonFile("with_cache.json",
        "[{\"name\":\"test\",\"score\":99}]");
    File cacheDir = new File(tempDir.toFile(), "scan_cache");
    cacheDir.mkdirs();
    Source source = Sources.of(jsonFile);

    RefreshableJsonTable table = new RefreshableJsonTable(
        source, "cache_scan_table", null, "UNCHANGED", cacheDir);

    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    DataContext dataContext = mock(DataContext.class);
    when(dataContext.getTypeFactory()).thenReturn(typeFactory);

    Enumerable<Object[]> enumerable = table.scan(dataContext);
    assertNotNull(enumerable);

    Enumerator<Object[]> enumerator = enumerable.enumerator();
    assertTrue(enumerator.moveNext());
    enumerator.close();
  }
}
