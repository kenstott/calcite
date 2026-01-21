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
package org.apache.calcite.adapter.file.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Manages spillover directories for large data processing with proper
 * concurrency control and cleanup.
 */
public class ConcurrentSpilloverManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentSpilloverManager.class);

  // Connection ID to spillover directory mapping
  private static final ConcurrentHashMap<String, Path> CONNECTION_DIRS = new ConcurrentHashMap<>();

  // Base spillover directory (configurable via system property)
  private static final String SPILLOVER_BASE =
      System.getProperty("calcite.spillover.dir",
      System.getProperty("java.io.tmpdir") + File.separator + "calcite_spillover");

  private ConcurrentSpilloverManager() {
    // Utility class should not be instantiated
  }

  /**
   * Get or create a spillover directory for a specific connection.
   * Each connection gets its own subdirectory to avoid conflicts.
   */
  public static Path getSpilloverDirectory(String connectionId) throws IOException {
    return CONNECTION_DIRS.computeIfAbsent(connectionId, id -> {
      try {
        // Create base directory if it doesn't exist
        Path baseDir = Paths.get(SPILLOVER_BASE);
        Files.createDirectories(baseDir);

        // Create connection-specific directory with UUID to ensure uniqueness
        String dirName = "conn_" + id + "_" + UUID.randomUUID().toString();
        Path connDir = baseDir.resolve(dirName);
        Files.createDirectories(connDir);

        // Register shutdown hook for cleanup
        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
              cleanupConnectionDirectory(id);
            }));

        return connDir;
      } catch (IOException e) {
        throw new RuntimeException("Failed to create spillover directory", e);
      }
    });
  }

  /**
   * Create a unique spillover file within the connection's directory.
   */
  public static File createSpilloverFile(String connectionId, String prefix) throws IOException {
    Path spillDir = getSpilloverDirectory(connectionId);
    String fileName = prefix + "_" + System.currentTimeMillis()
        + "_"
        + Thread.currentThread().hashCode() + ".tmp";
    return spillDir.resolve(fileName).toFile();
  }

  /**
   * Clean up spillover directory for a connection.
   */
  public static void cleanupConnectionDirectory(String connectionId) {
    Path dir = CONNECTION_DIRS.remove(connectionId);
    if (dir != null && Files.exists(dir)) {
      try {
        // Delete all files in the directory
        Files.walk(dir)
            .sorted((a, b) -> b.compareTo(a)) // Delete files before directories
            .forEach(path -> {
              try {
                Files.deleteIfExists(path);
              } catch (IOException e) {
                // Log error but continue cleanup
                LOGGER.error("Failed to delete spillover file: {}", path);
              }
            });
      } catch (IOException e) {
        LOGGER.error("Failed to cleanup spillover directory: {}", dir);
      }
    }
  }

  /**
   * Clean up old spillover directories (older than specified hours).
   */
  public static void cleanupOldDirectories(int hoursOld) {
    try {
      Path baseDir = Paths.get(SPILLOVER_BASE);
      if (!Files.exists(baseDir)) {
        return;
      }

      long cutoffTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(hoursOld);

      Files.list(baseDir)
          .filter(Files::isDirectory)
          .filter(dir -> {
            try {
              return Files.getLastModifiedTime(dir).toMillis() < cutoffTime;
            } catch (IOException e) {
              return false;
            }
          })
          .forEach(dir -> {
            try {
              Files.walk(dir)
                  .sorted((a, b) -> b.compareTo(a))
                  .forEach(path -> {
                    try {
                      Files.deleteIfExists(path);
                    } catch (IOException e) {
                      // Ignore
                    }
                  });
            } catch (IOException e) {
              // Ignore
            }
          });

    } catch (IOException e) {
      LOGGER.error("Error during spillover cleanup: {}", e.getMessage());
    }
  }
}
