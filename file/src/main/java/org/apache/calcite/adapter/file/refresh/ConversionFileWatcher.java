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
// storage-provider-guard:ignore-file - audited: all filesystem operations here target genuinely-local paths (temp / local cache / spill / local config), not object-store URIs.

import org.apache.calcite.adapter.file.converters.SourceFileConversion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Watches source files that need conversion (Excel, HTML, XML, Markdown,
 * DOCX, PPTX) and re-runs conversion when they change. These are the
 * "complex" source types: a single file can yield many tables. This
 * ensures that the generated JSON files stay up-to-date, which then
 * triggers the normal refresh mechanism for tables based on those JSON
 * files.
 */
public class ConversionFileWatcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConversionFileWatcher.class);

  private static ConversionFileWatcher instance;
  private final ScheduledExecutorService executor;
  // Schema-aware watched files: schema -> file -> info
  private final Map<String, Map<File, FileInfo>> schemaWatchedFiles = new ConcurrentHashMap<>();

  private static class FileInfo {
    final FileType type;
    final String schemaName;
    // The conversion the schema first ran on the file, re-run as it was on every change.
    final SourceFileConversion conversion;
    long lastModified;
    long lastChecked;

    FileInfo(FileType type, String schemaName, SourceFileConversion conversion,
        long lastModified) {
      this.type = type;
      this.schemaName = schemaName;
      this.conversion = conversion;
      this.lastModified = lastModified;
      this.lastChecked = System.currentTimeMillis();
    }
  }

  private enum FileType {
    EXCEL, HTML, XML, MARKDOWN, DOCX, PPTX
  }

  private ConversionFileWatcher() {
    this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "ConversionFileWatcher");
      t.setDaemon(true);
      return t;
    });
  }

  /**
   * Gets the singleton instance of the watcher.
   */
  public static synchronized ConversionFileWatcher getInstance() {
    if (instance == null) {
      instance = new ConversionFileWatcher();
    }
    return instance;
  }

  /**
   * Registers a file to be watched for changes with schema awareness.
   *
   * @param schemaName The schema name (optional)
   * @param file The file to watch
   * @param refreshInterval How often to check for changes
   * @param conversion The conversion the schema ran on the file, re-run on each change
   */
  public void watchFile(String schemaName, File file, Duration refreshInterval,
      SourceFileConversion conversion) {
    if (file == null || !file.exists()) {
      return;
    }

    String name = file.getName().toLowerCase();
    FileType type = null;

    if (name.endsWith(".xlsx") || name.endsWith(".xls")) {
      type = FileType.EXCEL;
    } else if (name.endsWith(".html") || name.endsWith(".htm")) {
      type = FileType.HTML;
    } else if (name.endsWith(".xml")) {
      type = FileType.XML;
    } else if (name.endsWith(".md") || name.endsWith(".markdown")) {
      type = FileType.MARKDOWN;
    } else if (name.endsWith(".docx")) {
      type = FileType.DOCX;
    } else if (name.endsWith(".pptx")) {
      type = FileType.PPTX;
    }

    if (type != null) {
      // Use "default" if no schema name provided
      String effectiveSchema = schemaName != null ? schemaName : "default";
      Map<File, FileInfo> watchedFiles =
          schemaWatchedFiles.computeIfAbsent(effectiveSchema, k -> new ConcurrentHashMap<>());

      FileInfo info = new FileInfo(type, effectiveSchema, conversion, file.lastModified());
      FileInfo existing = watchedFiles.putIfAbsent(file, info);

      if (existing == null) {
        LOGGER.debug("Started watching {} file for schema '{}': {}",
                    type, effectiveSchema, file.getAbsolutePath());

        // Schedule periodic checks
        long intervalMillis = refreshInterval != null ?
            refreshInterval.toMillis() : 60000; // Default 1 minute

        @SuppressWarnings("FutureReturnValueIgnored")
        java.util.concurrent.ScheduledFuture<?> ignored =
            executor.scheduleWithFixedDelay(() -> checkFile(effectiveSchema, file),
                intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
      }
    }
  }

  /**
   * Stops watching a file.
   */
  public void unwatchFile(File file) {
    // Remove from all schemas
    for (Map<File, FileInfo> watchedFiles : schemaWatchedFiles.values()) {
      watchedFiles.remove(file);
    }
  }

  /**
   * Stops watching a file for a specific schema.
   */
  public void unwatchFile(String schemaName, File file) {
    String effectiveSchema = schemaName != null ? schemaName : "default";
    Map<File, FileInfo> watchedFiles = schemaWatchedFiles.get(effectiveSchema);
    if (watchedFiles != null) {
      watchedFiles.remove(file);
    }
  }

  /**
   * Checks if a file has been modified and re-runs conversion if needed.
   */
  private void checkFile(String schemaName, File file) {
    Map<File, FileInfo> watchedFiles = schemaWatchedFiles.get(schemaName);
    if (watchedFiles == null) {
      return;
    }

    FileInfo info = watchedFiles.get(file);
    if (info == null) {
      return; // File no longer being watched
    }

    if (!file.exists()) {
      LOGGER.warn("Watched file no longer exists: {}", file.getAbsolutePath());
      watchedFiles.remove(file);
      return;
    }

    long currentModified = file.lastModified();
    if (currentModified > info.lastModified) {
      LOGGER.info("Detected change in {} file: {}", info.type, file.getName());

      try {
        // Re-run the conversion the schema first ran, so it writes what that one wrote.
        info.conversion.reconvert(file, info.conversion.outputDirectoryOf(file));
        LOGGER.info("Re-converted {} file: {}", info.type, file.getName());

        // Update the last modified time
        info.lastModified = currentModified;

      } catch (Exception e) {
        LOGGER.error("Failed to re-convert {} file {}: {}",
            info.type, file.getName(), e.getMessage(), e);
      }
    }

    info.lastChecked = System.currentTimeMillis();
  }

  /**
   * Shuts down the watcher.
   */
  public void shutdown() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
