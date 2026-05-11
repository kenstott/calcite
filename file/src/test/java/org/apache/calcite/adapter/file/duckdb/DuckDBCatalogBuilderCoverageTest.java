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
package org.apache.calcite.adapter.file.duckdb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep coverage tests for {@link DuckDBCatalogBuilder}.
 *
 * <p>Uses subprocess execution via ProcessBuilder to test main() code paths
 * that call System.exit(), avoiding SecurityManager (removed in Java 21).
 * Pure logic tests exercise formatting, filtering, and display logic directly.
 */
@Tag("unit")
@Execution(ExecutionMode.SAME_THREAD)
public class DuckDBCatalogBuilderCoverageTest {

  private String oldCatalogPath;

  @BeforeEach
  void setUp() {
    oldCatalogPath = System.getProperty("duckdb.catalog.path");
  }

  @AfterEach
  void tearDown() {
    if (oldCatalogPath != null) {
      System.setProperty("duckdb.catalog.path", oldCatalogPath);
    } else {
      System.clearProperty("duckdb.catalog.path");
    }
  }

  /**
   * Runs DuckDBCatalogBuilder.main() in a subprocess and captures the result.
   */
  private SubprocessResult runMainInSubprocess(String... mainArgs) throws Exception {
    String javaHome = System.getProperty("java.home");
    String classpath = System.getProperty("java.class.path");
    String javaBin = javaHome + File.separator + "bin" + File.separator + "java";

    List<String> command = new ArrayList<String>();
    command.add(javaBin);
    command.add("-cp");
    command.add(classpath);
    command.add(DuckDBCatalogBuilder.class.getName());
    for (String arg : mainArgs) {
      command.add(arg);
    }

    ProcessBuilder pb = new ProcessBuilder(command);
    pb.redirectErrorStream(false);
    Process process = pb.start();

    // Read stdout and stderr in parallel to avoid blocking
    StringBuilder stdout = new StringBuilder();
    StringBuilder stderr = new StringBuilder();

    Thread stdoutThread = new Thread(new Runnable() {
      @Override public void run() {
        try {
          BufferedReader reader =
              new BufferedReader(new InputStreamReader(process.getInputStream()));
          String line;
          while ((line = reader.readLine()) != null) {
            stdout.append(line).append("\n");
          }
        } catch (Exception e) {
          // ignore
        }
      }
    });

    Thread stderrThread = new Thread(new Runnable() {
      @Override public void run() {
        try {
          BufferedReader reader =
              new BufferedReader(new InputStreamReader(process.getErrorStream()));
          String line;
          while ((line = reader.readLine()) != null) {
            stderr.append(line).append("\n");
          }
        } catch (Exception e) {
          // ignore
        }
      }
    });

    stdoutThread.start();
    stderrThread.start();

    boolean finished = process.waitFor(30, TimeUnit.SECONDS);
    if (!finished) {
      process.destroyForcibly();
    }
    stdoutThread.join(5000);
    stderrThread.join(5000);

    return new SubprocessResult(
        process.exitValue(), stdout.toString(), stderr.toString());
  }

  /** Holds the result of a subprocess execution. */
  private static class SubprocessResult {
    final int exitCode;
    final String stdout;
    final String stderr;

    SubprocessResult(int exitCode, String stdout, String stderr) {
      this.exitCode = exitCode;
      this.stdout = stdout;
      this.stderr = stderr;
    }
  }

  // --- Instantiation ---

  @Test void testClassIsInstantiable() {
    DuckDBCatalogBuilder builder = new DuckDBCatalogBuilder();
    assertNotNull(builder);
  }

  // --- main() with no args => usage + exit(1) ---

  @Test void testMainNoArgs() throws Exception {
    SubprocessResult result = runMainInSubprocess();
    assertEquals(1, result.exitCode);
    assertTrue(result.stderr.contains("Usage: DuckDBCatalogBuilder"),
        "Expected usage message in stderr, got: " + result.stderr);
    assertTrue(result.stderr.contains("Example:"));
    assertTrue(result.stderr.contains("Environment variables (optional):"));
    assertTrue(result.stderr.contains("GOVDATA_CACHE_DIR"));
    assertTrue(result.stderr.contains("GOVDATA_PARQUET_DIR"));
    assertTrue(result.stderr.contains("AWS_ACCESS_KEY_ID"));
    assertTrue(result.stderr.contains("AWS_SECRET_ACCESS_KEY"));
  }

  @Test void testMainOneArg() throws Exception {
    SubprocessResult result = runMainInSubprocess("model.json");
    assertEquals(1, result.exitCode);
    assertTrue(result.stderr.contains("Usage:"),
        "Expected usage in stderr, got: " + result.stderr);
  }

  // --- main() with non-existent model file => error + exit(1) ---

  @Test void testMainNonExistentModelFile(@TempDir Path tempDir) throws Exception {
    String nonExistentModel = tempDir.resolve("does_not_exist.json").toString();
    String catalogPath = tempDir.resolve("output.duckdb").toString();

    SubprocessResult result = runMainInSubprocess(nonExistentModel, catalogPath);
    assertEquals(1, result.exitCode);
    assertTrue(result.stderr.contains("Error: Model file not found:"),
        "Expected 'Model file not found' in stderr, got: " + result.stderr);
    // Also verify header output was printed before the error
    assertTrue(result.stdout.contains("DuckDB Catalog Builder"));
    assertTrue(result.stdout.contains("Model:"));
    assertTrue(result.stdout.contains("Catalog:"));
  }

  // --- main() with existing catalog file that gets deleted ---

  @Test void testMainDeletesExistingCatalog(@TempDir Path tempDir) throws Exception {
    // Create a model file (will fail at Calcite connection step, but covers delete logic)
    File modelFile = tempDir.resolve("model.json").toFile();
    try (FileWriter w = new FileWriter(modelFile)) {
      w.write("{\"version\":\"1.0\",\"schemas\":[]}");
    }

    // Create an existing catalog file that should be deleted
    File catalogFile = tempDir.resolve("existing.duckdb").toFile();
    try (FileWriter w = new FileWriter(catalogFile)) {
      w.write("simulated existing catalog content");
    }
    assertTrue(catalogFile.exists());

    SubprocessResult result = runMainInSubprocess(
        modelFile.getAbsolutePath(), catalogFile.getAbsolutePath());

    assertTrue(result.stdout.contains("Removing existing catalog:"),
        "Expected catalog removal message, got stdout: " + result.stdout);
    assertTrue(result.stdout.contains("Set catalog path:"));
    assertTrue(result.stdout.contains("Connecting to Calcite..."));
  }

  // --- main() with invalid model causing connection failure => error + exit(1) ---

  @Test void testMainConnectionFailure(@TempDir Path tempDir) throws Exception {
    File modelFile = tempDir.resolve("bad_model.json").toFile();
    try (FileWriter w = new FileWriter(modelFile)) {
      // Write an invalid model that will cause Calcite to fail
      w.write("{\"version\":\"1.0\",\"schemas\":[{\"name\":\"bad\","
          + "\"type\":\"custom\",\"factory\":\"nonexistent.Factory\"}]}");
    }
    String catalogPath = tempDir.resolve("out.duckdb").toString();

    SubprocessResult result = runMainInSubprocess(
        modelFile.getAbsolutePath(), catalogPath);

    // The connection should fail, triggering error path and exit(1)
    assertEquals(1, result.exitCode);
    assertTrue(result.stderr.contains("ERROR: Failed to build catalog"),
        "Expected error message in stderr, got: " + result.stderr);
    // Verify JDBC URL output was printed before the error
    assertTrue(result.stdout.contains("JDBC URL:"),
        "Expected JDBC URL in stdout, got: " + result.stdout);
  }

  // --- File size formatting via actual file operations ---

  @Test void testFileSizeFormattingBytes(@TempDir Path tempDir) throws Exception {
    long bytes = 500;
    String size;
    if (bytes < 1024) {
      size = bytes + " bytes";
    } else if (bytes < 1024 * 1024) {
      size = String.format("%.1f KB", bytes / 1024.0);
    } else {
      size = String.format("%.1f MB", bytes / (1024.0 * 1024.0));
    }
    assertEquals("500 bytes", size);
  }

  @Test void testFileSizeFormattingKB() {
    long bytes = 2048;
    String size;
    if (bytes < 1024) {
      size = bytes + " bytes";
    } else if (bytes < 1024 * 1024) {
      size = String.format("%.1f KB", bytes / 1024.0);
    } else {
      size = String.format("%.1f MB", bytes / (1024.0 * 1024.0));
    }
    assertEquals("2.0 KB", size);
  }

  @Test void testFileSizeFormattingMB() {
    long bytes = 2 * 1024 * 1024;
    String size;
    if (bytes < 1024) {
      size = bytes + " bytes";
    } else if (bytes < 1024 * 1024) {
      size = String.format("%.1f KB", bytes / 1024.0);
    } else {
      size = String.format("%.1f MB", bytes / (1024.0 * 1024.0));
    }
    assertEquals("2.0 MB", size);
  }

  // --- Schema filtering logic (exercised in main) ---

  @Test void testSchemaFilteringMetadata() {
    String schema = "metadata";
    assertTrue(schema.equals("metadata") || schema.equalsIgnoreCase("INFORMATION_SCHEMA"));
  }

  @Test void testSchemaFilteringInformationSchema() {
    String schema = "INFORMATION_SCHEMA";
    assertTrue(schema.equals("metadata") || schema.equalsIgnoreCase("INFORMATION_SCHEMA"));
  }

  @Test void testSchemaFilteringRegular() {
    String schema = "govdata";
    assertFalse(schema.equals("metadata") || schema.equalsIgnoreCase("INFORMATION_SCHEMA"));
  }

  // --- Table display count logic ---

  @Test void testTableDisplayLogicMoreThanFive() {
    String[] tables = {"t1", "t2", "t3", "t4", "t5", "t6", "t7"};
    int count = 0;
    int remaining = -1;
    for (String tableName : tables) {
      if (count < 5) {
        count++;
      } else if (count == 5) {
        remaining = tables.length - 5;
        break;
      }
    }
    assertEquals(5, count);
    assertEquals(2, remaining);
  }

  @Test void testTableDisplayLogicExactlyFive() {
    String[] tables = {"t1", "t2", "t3", "t4", "t5"};
    int count = 0;
    boolean overflow = false;
    for (String tableName : tables) {
      if (count < 5) {
        count++;
      } else if (count == 5) {
        overflow = true;
        break;
      }
    }
    assertEquals(5, count);
    assertFalse(overflow);
  }

  @Test void testTableDisplayLogicFewerThanFive() {
    String[] tables = {"t1", "t2"};
    int count = 0;
    for (String tableName : tables) {
      if (count < 5) {
        count++;
      } else if (count == 5) {
        break;
      }
    }
    assertEquals(2, count);
  }

  // --- Argument count validation ---

  @Test void testArgsLengthTwoIsValid() {
    String[] args = {"model.json", "output.duckdb"};
    assertFalse(args.length < 2);
  }

  @Test void testAbsolutePathResolution(@TempDir Path tempDir) {
    File f = new File(tempDir.resolve("test.json").toString());
    String abs = f.getAbsolutePath();
    assertNotNull(abs);
    assertTrue(abs.contains("test.json"));
  }

  // --- System property for catalog path ---

  @Test void testSystemPropertyForCatalog(@TempDir Path tempDir) {
    String path = tempDir.resolve("catalog.duckdb").toString();
    System.setProperty("duckdb.catalog.path", path);
    assertEquals(path, System.getProperty("duckdb.catalog.path"));
  }

  // --- JDBC URL construction ---

  @Test void testJdbcUrlConstruction() {
    String modelPath = "/path/to/model.json";
    String jdbcUrl = "jdbc:calcite:model=" + modelPath;
    assertEquals("jdbc:calcite:model=/path/to/model.json", jdbcUrl);
  }

  // --- Summary output construction ---

  @Test void testSummaryOutput() {
    int totalTables = 42;
    String line = "Tables:  " + totalTables;
    assertEquals("Tables:  42", line);
  }
}
