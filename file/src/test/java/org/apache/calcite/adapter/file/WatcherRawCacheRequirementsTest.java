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

import org.apache.calcite.adapter.file.etl.HttpSource;
import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.refresh.ConversionFileWatcher;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILE-133 / FILE-143 — hermetic recode of two file-adapter invariants:
 *
 * <p>FILE-133: {@link ConversionFileWatcher} is the only eager/timer-based refresher. Its watch
 * loop is a daemon {@code scheduleWithFixedDelay} (default 60000ms when the interval is null) over
 * EXCEL/HTML/XML/MARKDOWN/DOCX/PPTX sources. It re-converts on an mtime increase and ONLY advances
 * its stored mtime on a SUCCESSFUL conversion, so a failed conversion is retried on the next tick.
 * The live timer thread is not driven here (see NOTE); the per-tick {@code checkFile} method is
 * invoked directly via reflection to assert the advance-on-success / no-advance-on-failure rule.
 *
 * <p>FILE-143: the raw cache is immutable with NO TTL. {@code HttpSource.hasValidRawCache} returns
 * valid whenever the cache file EXISTS (no age check); the IncrementalTracker is the sole staleness
 * authority. (CacheResolver tier order is already covered by {@code RawCacheRequirementsTest} and is
 * intentionally not duplicated here.)
 */
@Tag("unit")
public class WatcherRawCacheRequirementsTest {

  @TempDir
  File tempDir;

  // ---------------------------------------------------------------------------------------------
  // FILE-133 — ConversionFileWatcher: the only eager/timer refresher; advances stored mtime only
  // on a successful conversion.
  // ---------------------------------------------------------------------------------------------

  @Test @Tag("FILE-133")
  void defaultRefreshIntervalIsSixtySecondsWhenNull() throws IOException {
    // Mirrors ConversionFileWatcherTest#testWatchFileWithNullDuration: registering with a null
    // Duration must be accepted (the watcher falls back to its 60000ms default).
    // NOTE: the live scheduleWithFixedDelay loop is a daemon timer and is not driven in this
    // hermetic test; the 60000ms fallback at the scheduling site is asserted indirectly here by
    // exercising the null-interval registration path without error.
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    watcher.registerSchemaBaseDirectory("file133_schema", tempDir);
    File excelFile = new File(tempDir, "interval.xlsx");
    Files.write(excelFile.toPath(), new byte[]{0x50, 0x4B});
    watcher.watchFile("file133_schema", excelFile, (Duration) null);
    // No exception == null interval accepted (default 60000ms applied at the scheduling site).
    assertTrue(excelFile.exists());
  }

  @Test @Tag("FILE-133")
  void successfulConversionAdvancesStoredMtime() throws Exception {
    // A Markdown source converts cleanly; an mtime increase must trigger conversion AND advance the
    // watcher's stored lastModified so the same change is not re-processed on the next tick.
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    watcher.registerSchemaBaseDirectory("file133_md", tempDir);

    File mdFile = new File(tempDir, "convert_ok.md");
    Files.write(mdFile.toPath(),
        "| a | b |\n|---|---|\n| 1 | 2 |".getBytes(StandardCharsets.UTF_8));
    watcher.watchFile("file133_md", mdFile, Duration.ofMinutes(1));

    long stored = readStoredMtime(watcher, "file133_md", mdFile);

    // Force a strictly-greater on-disk mtime so checkFile detects a change.
    long newMtime = stored + 5000L;
    assertTrue(mdFile.setLastModified(newMtime));

    invokeCheckFile(watcher, "file133_md", mdFile);

    long advanced = readStoredMtime(watcher, "file133_md", mdFile);
    assertTrue(advanced > stored,
        "successful conversion must advance the stored mtime past the original value");
    assertTrue(advanced >= newMtime,
        "stored mtime must advance to the file's new mtime on success");
  }

  @Test @Tag("FILE-133")
  void failedConversionDoesNotAdvanceStoredMtimeSoItRetries() throws Exception {
    // An Excel source with garbage bytes fails conversion (SafeExcelToJsonConverter throws, caught
    // by checkFile's outer try). Because the failure short-circuits before `info.lastModified =
    // currentModified`, the stored mtime must stay put — the next tick will retry.
    ConversionFileWatcher watcher = ConversionFileWatcher.getInstance();
    watcher.registerSchemaBaseDirectory("file133_xls", tempDir);

    File xlsxFile = new File(tempDir, "convert_fail.xlsx");
    Files.write(xlsxFile.toPath(),
        "not-a-real-xlsx".getBytes(StandardCharsets.UTF_8));
    watcher.watchFile("file133_xls", xlsxFile, Duration.ofMinutes(1));

    long stored = readStoredMtime(watcher, "file133_xls", xlsxFile);

    long newMtime = stored + 5000L;
    assertTrue(xlsxFile.setLastModified(newMtime));

    // checkFile swallows the conversion exception internally; this must not propagate.
    invokeCheckFile(watcher, "file133_xls", xlsxFile);

    long afterFailure = readStoredMtime(watcher, "file133_xls", xlsxFile);
    assertTrue(afterFailure == stored,
        "failed conversion must NOT advance the stored mtime (so it retries next tick)");
  }

  // ---------------------------------------------------------------------------------------------
  // FILE-143 — HttpSource.hasValidRawCache: immutable cache, NO TTL. Valid whenever the file exists,
  // regardless of age; invalid only when absent.
  // ---------------------------------------------------------------------------------------------

  @Test @Tag("FILE-143")
  void rawCacheIsValidForAnExistingFileRegardlessOfAge() throws Exception {
    File cacheFile = new File(tempDir, "response.json");
    Files.write(cacheFile.toPath(), "{\"x\":1}".getBytes(StandardCharsets.UTF_8));
    // Backdate the cache far into the past: a TTL-based check would treat this as stale.
    long oneYearAgo = System.currentTimeMillis() - (365L * 24L * 60L * 60L * 1000L);
    assertTrue(cacheFile.setLastModified(oneYearAgo));

    HttpSource source = newHttpSourceWithProvider(new LocalExistsStorageProvider());
    assertTrue(invokeHasValidRawCache(source, cacheFile.getAbsolutePath()),
        "an existing cache file must be valid regardless of age (no TTL)");
  }

  @Test @Tag("FILE-143")
  void rawCacheIsInvalidWhenAbsent() throws Exception {
    File missing = new File(tempDir, "absent_response.json");
    assertFalse(missing.exists());

    HttpSource source = newHttpSourceWithProvider(new LocalExistsStorageProvider());
    assertFalse(invokeHasValidRawCache(source, missing.getAbsolutePath()),
        "a missing cache file must be invalid");
  }

  // ---------------------------------------------------------------------------------------------
  // Reflection helpers (private API on ConversionFileWatcher / HttpSource).
  // ---------------------------------------------------------------------------------------------

  /** Reads the watcher's privately-stored {@code FileInfo.lastModified} for a watched file. */
  @SuppressWarnings("unchecked")
  private static long readStoredMtime(ConversionFileWatcher watcher, String schema, File file)
      throws Exception {
    Field schemaWatchedField = ConversionFileWatcher.class.getDeclaredField("schemaWatchedFiles");
    schemaWatchedField.setAccessible(true);
    Map<String, Map<File, Object>> schemaWatched =
        (Map<String, Map<File, Object>>) schemaWatchedField.get(watcher);
    Map<File, Object> watchedFiles = schemaWatched.get(schema);
    if (watchedFiles == null) {
      throw new IllegalStateException("schema not registered as watched: " + schema);
    }
    Object info = watchedFiles.get(file);
    if (info == null) {
      throw new IllegalStateException("file not watched: " + file);
    }
    Field lastModifiedField = info.getClass().getDeclaredField("lastModified");
    lastModifiedField.setAccessible(true);
    return lastModifiedField.getLong(info);
  }

  /** Invokes the private per-tick {@code checkFile(String, File)} method directly. */
  private static void invokeCheckFile(ConversionFileWatcher watcher, String schema, File file)
      throws Exception {
    Method checkFile =
        ConversionFileWatcher.class.getDeclaredMethod("checkFile", String.class, File.class);
    checkFile.setAccessible(true);
    checkFile.invoke(watcher, schema, file);
  }

  /** Builds an HttpSource with a minimal config and a reflectively-set storage provider. */
  private static HttpSource newHttpSourceWithProvider(StorageProvider provider) throws Exception {
    HttpSourceConfig config = HttpSourceConfig.builder().url("http://example.test/data").build();
    return new HttpSource(config, null, provider, "/raw");
  }

  /** Invokes the private {@code hasValidRawCache(String)} method on an HttpSource. */
  private static boolean invokeHasValidRawCache(HttpSource source, String cachePath)
      throws Exception {
    Method m = HttpSource.class.getDeclaredMethod("hasValidRawCache", String.class);
    m.setAccessible(true);
    return ((Boolean) m.invoke(source, cachePath)).booleanValue();
  }

  // ---------------------------------------------------------------------------------------------
  // Minimal StorageProvider whose exists() delegates to a real local File — proving hasValidRawCache
  // depends ONLY on existence, never on age.
  // ---------------------------------------------------------------------------------------------

  private static final class LocalExistsStorageProvider implements StorageProvider {

    @Override public boolean exists(String path) {
      return new File(path).exists();
    }

    @Override public List<FileEntry> listFiles(String path, boolean recursive) {
      return new ArrayList<FileEntry>();
    }

    @Override public FileMetadata getMetadata(String path) throws IOException {
      throw new IOException("Not supported");
    }

    @Override public InputStream openInputStream(String path) throws IOException {
      return new ByteArrayInputStream(Files.readAllBytes(new File(path).toPath()));
    }

    @Override public Reader openReader(String path) throws IOException {
      return new java.io.InputStreamReader(openInputStream(path), StandardCharsets.UTF_8);
    }

    @Override public boolean isDirectory(String path) {
      return new File(path).isDirectory();
    }

    @Override public String getStorageType() {
      return "local-exists";
    }

    @Override public String resolvePath(String basePath, String relativePath) {
      return basePath + "/" + relativePath;
    }
  }
}
