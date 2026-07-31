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
import java.sql.Connection;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Verifies that {@link AskAmericaDriver#connect} sets the system properties required by the
 * engine when {@code ASKAMERICA_DATA_DIR} is supplied, and only when the URL is actually one this
 * driver accepts.
 *
 * <p>{@code duckdb.catalog.path} is deliberately not covered here any more: the generated govdata
 * model always sets {@code database_filename} in its operand, which wins over that property in
 * {@code DuckDBJdbcSchemaFactory} regardless, so this driver stopped setting it as dead
 * configuration (it also used to fire as a DriverManager side effect on every unrelated
 * jdbc:duckdb: connection, not just jdbc:askamerica: ones — see {@code connect_ignoresNonAskAmericaUrls} below).
 *
 * <p>Properties under test:
 * <ul>
 *   <li>{@code govdata.operating.dir.base} — pinned to {@code ASKAMERICA_DATA_DIR}</li>
 *   <li>{@code duckdb.cache_httpfs.directory} — set to {@code <dataDir>/.duckdb_httpfs_cache}</li>
 * </ul>
 *
 * <p>Pre-existing values for the cache dir must not be overwritten (idempotent on repeat calls).
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
public class AskAmericaDriverSystemPropertiesTest {

  private static final String PROP_OPERATING_DIR = "govdata.operating.dir.base";
  private static final String PROP_CACHE_DIR = "duckdb.cache_httpfs.directory";
  private static final String PROP_DATA_DIR = "ASKAMERICA_DATA_DIR";

  private String savedOperatingDir;
  private String savedCacheDir;
  private String savedDataDir;

  @BeforeEach
  void saveAndClearProperties() {
    savedOperatingDir = System.getProperty(PROP_OPERATING_DIR);
    savedCacheDir = System.getProperty(PROP_CACHE_DIR);
    savedDataDir = System.getProperty(PROP_DATA_DIR);
    System.clearProperty(PROP_OPERATING_DIR);
    System.clearProperty(PROP_CACHE_DIR);
    System.clearProperty(PROP_DATA_DIR);
  }

  @AfterEach
  void restoreProperties() {
    restore(PROP_OPERATING_DIR, savedOperatingDir);
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

  @Test void connect_setsCacheHttpfsDirUnderDataDir(@TempDir Path tmpDir) {
    invokeConnect(tmpDir.toString());
    assertEquals(tmpDir + "/.duckdb_httpfs_cache", System.getProperty(PROP_CACHE_DIR));
  }

  @Test void connect_doesNotOverwritePreexistingCacheDir(@TempDir Path tmpDir) {
    System.setProperty(PROP_CACHE_DIR, "/custom/cache");
    invokeConnect(tmpDir.toString());
    assertEquals("/custom/cache", System.getProperty(PROP_CACHE_DIR),
        "Pre-existing duckdb.cache_httpfs.directory must not be overwritten");
  }

  /**
   * Regression coverage: {@code connect()} used to run its data-dir pinning unconditionally,
   * before checking whether the URL was even one this driver handles. Since AskAmericaDriver is
   * registered with DriverManager, that meant every plain {@code jdbc:duckdb:} connection opened
   * anywhere in the process — e.g. deep inside a completely unrelated jdbc:govdata: connection's
   * schema creation — silently reset govdata.operating.dir.base as a side effect. A govdata
   * connection that had already baked the old value into its generated model's
   * database_filename kept using the stale catalog path while the property itself moved on.
   */
  @Test void connect_ignoresNonAskAmericaUrls(@TempDir Path tmpDir) throws Exception {
    System.setProperty(PROP_DATA_DIR, tmpDir.toString());
    AskAmericaDriver driver = new AskAmericaDriver();
    Connection result = driver.connect("jdbc:duckdb:", new Properties());
    assertNull(result, "a non-matching URL must return null, per the Driver contract");
    assertNull(System.getProperty(PROP_OPERATING_DIR),
        "a URL this driver does not handle must not pin govdata.operating.dir.base");
  }
}
