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
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Verifies that {@link AskAmericaDriver#connect} sets the three system properties required by
 * the engine when {@code ASKAMERICA_DATA_DIR} is supplied.
 *
 * <p>Regression coverage for Bug 1 in GitHub issue #19: the driver previously used
 * a stale catalog path (nested under {@code .aperio}) rather than
 * {@code <dataDir>/.duckdb/catalog.duckdb}.
 *
 * <p>Properties under test:
 * <ul>
 *   <li>{@code govdata.operating.dir.base} — pinned to {@code ASKAMERICA_DATA_DIR}</li>
 *   <li>{@code duckdb.catalog.path} — set to {@code <dataDir>/.duckdb/catalog.duckdb}</li>
 *   <li>{@code duckdb.cache_httpfs.directory} — set to {@code <dataDir>/.duckdb_httpfs_cache}</li>
 * </ul>
 *
 * <p>Pre-existing values for the last two must not be overwritten (idempotent on repeat calls).
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
public class AskAmericaDriverSystemPropertiesTest {

  private static final String PROP_OPERATING_DIR = "govdata.operating.dir.base";
  private static final String PROP_CATALOG_PATH = "duckdb.catalog.path";
  private static final String PROP_CACHE_DIR = "duckdb.cache_httpfs.directory";
  private static final String PROP_DATA_DIR = "ASKAMERICA_DATA_DIR";

  private String savedOperatingDir;
  private String savedCatalogPath;
  private String savedCacheDir;
  private String savedDataDir;

  @BeforeEach
  void saveAndClearProperties() {
    savedOperatingDir = System.getProperty(PROP_OPERATING_DIR);
    savedCatalogPath = System.getProperty(PROP_CATALOG_PATH);
    savedCacheDir = System.getProperty(PROP_CACHE_DIR);
    savedDataDir = System.getProperty(PROP_DATA_DIR);
    System.clearProperty(PROP_OPERATING_DIR);
    System.clearProperty(PROP_CATALOG_PATH);
    System.clearProperty(PROP_CACHE_DIR);
    System.clearProperty(PROP_DATA_DIR);
  }

  @AfterEach
  void restoreProperties() {
    restore(PROP_OPERATING_DIR, savedOperatingDir);
    restore(PROP_CATALOG_PATH, savedCatalogPath);
    restore(PROP_CACHE_DIR, savedCacheDir);
    restore(PROP_DATA_DIR, savedDataDir);
  }

  private static void restore(String key, String value) {
    if (value != null) {
      System.setProperty(key, value);
    } else {
      System.clearProperty(key);
    }
  }

  /** Invokes connect() and swallows the expected connection failure (no real schema). */
  private static void invokeConnect(String dataDir) {
    System.setProperty(PROP_DATA_DIR, dataDir);
    AskAmericaDriver driver = new AskAmericaDriver();
    try {
      driver.connect("jdbc:askamerica:source=ref", new Properties());
    } catch (Exception ignored) {
      // Properties are set before super.connect(), so they survive the failure.
    }
  }

  @Test void connect_setsGovdataOperatingDirToDataDir(@TempDir Path tmpDir) {
    invokeConnect(tmpDir.toString());
    assertEquals(tmpDir.toString(), System.getProperty(PROP_OPERATING_DIR));
  }

  @Test void connect_setsCatalogPathUnderDotDuckdb(@TempDir Path tmpDir) {
    invokeConnect(tmpDir.toString());
    String catalogPath = System.getProperty(PROP_CATALOG_PATH);
    assertNotNull(catalogPath);
    String expected = tmpDir.toAbsolutePath() + "/.duckdb/catalog.duckdb";
    assertEquals(expected, catalogPath,
        "catalog.duckdb must be at <dataDir>/.duckdb/catalog.duckdb, not nested under .aperio");
    assertFalse(catalogPath.contains(".aperio"), "catalog path must not use .aperio");
  }

  @Test void connect_setsCacheHttpfsDirUnderDataDir(@TempDir Path tmpDir) {
    invokeConnect(tmpDir.toString());
    assertEquals(tmpDir + "/.duckdb_httpfs_cache", System.getProperty(PROP_CACHE_DIR));
  }

  @Test void connect_doesNotOverwritePreexistingCatalogPath(@TempDir Path tmpDir) {
    System.setProperty(PROP_CATALOG_PATH, "/custom/path/catalog.duckdb");
    invokeConnect(tmpDir.toString());
    assertEquals("/custom/path/catalog.duckdb", System.getProperty(PROP_CATALOG_PATH),
        "Pre-existing duckdb.catalog.path must not be overwritten");
  }

  @Test void connect_doesNotOverwritePreexistingCacheDir(@TempDir Path tmpDir) {
    System.setProperty(PROP_CACHE_DIR, "/custom/cache");
    invokeConnect(tmpDir.toString());
    assertEquals("/custom/cache", System.getProperty(PROP_CACHE_DIR),
        "Pre-existing duckdb.cache_httpfs.directory must not be overwritten");
  }
}
