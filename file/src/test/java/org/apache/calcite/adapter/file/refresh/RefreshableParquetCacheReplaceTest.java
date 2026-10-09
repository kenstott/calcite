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
package org.apache.calcite.adapter.file.refresh;

import org.apache.calcite.adapter.file.DirectFileSource;
import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.format.parquet.ParquetConversionUtil;
import org.apache.calcite.adapter.file.table.JsonScannableTable;
import org.apache.calcite.adapter.file.table.ParquetScannableTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.util.Source;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A refresh replaces the Parquet cache file of a {@link RefreshableParquetCacheTable} while
 * queries on other threads read it. The cache file must be readable at every moment of a
 * refresh, including one whose replacement the operating system refuses, and a refresh must
 * rebuild it whatever the files' timestamps say.
 */
@Tag("unit")
class RefreshableParquetCacheReplaceTest {

  private static final int REFRESHES = 30;

  @TempDir
  Path tempDir;

  private File sourceFile;
  private File cacheDir;
  private Source source;

  private RefreshableParquetCacheTable createTable(String json) throws Exception {
    File sourceDir = Files.createDirectories(tempDir.resolve("source")).toFile();
    cacheDir = Files.createDirectories(tempDir.resolve("cache")).toFile();
    sourceFile = new File(sourceDir, "data.json");
    writeSource(json);
    // The source the schema gives a scanned file under the PARQUET engine: it reads the file
    // on every open, where Sources.of(File) serves bytes cached by path.
    source = new DirectFileSource(sourceFile);
    File parquetFile =
        ParquetConversionUtil.convertToParquet(source, sourceFile.getName(),
            new JsonScannableTable(source), cacheDir, null, "replace_test", "SMART_CASING");
    return new RefreshableParquetCacheTable(source, parquetFile, cacheDir,
        Duration.ofSeconds(1), false, "SMART_CASING", "SMART_CASING", null,
        ExecutionEngineConfig.ExecutionEngineType.PARQUET, null, "replace_test");
  }

  private void writeSource(String json) throws IOException {
    Files.write(sourceFile.toPath(), json.getBytes(StandardCharsets.UTF_8));
  }

  /** Reads the footer of the cache file the way a newly planned query does. */
  private static RelDataType readRowType(File parquetFile) {
    return new ParquetScannableTable(parquetFile)
        .getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
  }

  @Test void cacheFileIsReadableThroughoutARefresh() throws Exception {
    final RefreshableParquetCacheTable table = createTable("[{\"id\": 0}]");

    final AtomicBoolean done = new AtomicBoolean(false);
    final AtomicInteger reads = new AtomicInteger();
    final AtomicReference<Throwable> readFailure = new AtomicReference<Throwable>();
    Thread reader = new Thread(new Runnable() {
      @Override public void run() {
        while (!done.get() && readFailure.get() == null) {
          try {
            readRowType(table.getParquetFile());
            reads.incrementAndGet();
          } catch (Throwable t) {
            readFailure.compareAndSet(null, t);
          }
        }
      }
    }, "cache-reader");
    reader.start();

    long sourceTime = sourceFile.lastModified();
    try {
      for (int i = 1; i <= REFRESHES && readFailure.get() == null; i++) {
        writeSource("[{\"id\": " + i + "}]");
        sourceTime += 2000L;
        assertTrue(sourceFile.setLastModified(sourceTime), "source timestamp set");

        table.doRefresh();

        // Whether this refresh replaced the file or the operating system refused the
        // replacement (Windows does while another handle has the file open; the refresh is
        // then tried again at the next interval), a complete cache file is in place.
        assertTrue(table.getParquetFile().isFile(), "a cache file is in place after refresh " + i);
        assertEquals("id", readRowType(table.getParquetFile()).getFieldNames().get(0),
            "the cache file is readable after refresh " + i);
      }
    } finally {
      done.set(true);
      reader.join(30000L);
    }

    Throwable failure = readFailure.get();
    assertNull(failure, () -> "the cache file was unreadable during a refresh after "
        + reads.get() + " reads: " + failure + " / cause: " + failure.getCause());
    assertTrue(reads.get() > 0, "the reader thread read the cache file");
  }

  @Test void refreshRebuildsACacheFileNewerThanItsSource() throws Exception {
    RefreshableParquetCacheTable table = createTable("[{\"id\": 1}]");
    File parquetFile = table.getParquetFile();
    assertEquals("id", readRowType(parquetFile).getFieldNames().get(0));

    // The source changes but carries a timestamp older than the cache file,
    // as a file copied into place with its original timestamp does.
    writeSource("[{\"code\": 1}]");
    long olderThanCache = parquetFile.lastModified() - 10000L;
    assertTrue(sourceFile.setLastModified(olderThanCache), "source timestamp set");

    table.doRefresh();

    assertEquals(sourceFile.lastModified(), table.lastModifiedTime, "refresh completed");
    assertEquals("code", readRowType(table.getParquetFile()).getFieldNames().get(0));
  }
}
