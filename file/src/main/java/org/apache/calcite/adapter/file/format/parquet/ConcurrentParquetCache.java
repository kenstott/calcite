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
package org.apache.calcite.adapter.file.format.parquet;

import org.apache.calcite.adapter.file.cache.RedisDistributedLock;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread-safe Parquet cache manager that handles concurrent access from multiple
 * JDBC connections.
 */
public class ConcurrentParquetCache {
  // In-process lock map for threads within the same JVM
  private static final ConcurrentHashMap<String, Lock> LOCK_MAP = new ConcurrentHashMap<>();

  private ConcurrentParquetCache() {
    // Utility class should not be instantiated
  }

  /**
   * Clear all cached locks. This should only be used in testing.
   * @deprecated This method creates unrealistic test scenarios and should not be used.
   * In production, multiple instances must synchronize access to shared storage.
   */
  @Deprecated
  public static void clearLocks() {
    // Deprecated: This method creates unrealistic production scenarios
    // In real deployments, multiple instances will share storage and must synchronize properly
    // Tests should use proper cleanup that doesn't interfere with active locks
  }

  // Lock acquisition timeout
  private static final long LOCK_TIMEOUT_SECONDS = 30;

  /**
   * Convert a file to Parquet with proper concurrency control, type inference flag, schema name, and casing.
   * Uses Redis locks if available, otherwise falls back to file system locks.
   * @param sourceFile The source file to convert
   * @param cacheDir The cache directory to use
   * @param typeInferenceEnabled Whether type inference is enabled
   * @param schemaName Optional schema name for schema-specific locking
   * @param casing The casing strategy to use for filename generation
   * @param callback The conversion callback
   */
  public static File convertWithLocking(File sourceFile, File cacheDir, boolean typeInferenceEnabled,
      String schemaName, String casing, ConversionCallback callback) throws Exception {

    // Include schema name in lock key for proper isolation
    String lockKey = (schemaName != null ? schemaName + ":" : "")
        + sourceFile.getAbsolutePath();

    // Try Redis distributed lock first
    RedisDistributedLock redisLock = RedisDistributedLock.createIfAvailable(lockKey);
    if (redisLock != null) {
      try {
        if (redisLock.tryLock(TimeUnit.SECONDS.toMillis(LOCK_TIMEOUT_SECONDS))) {
          return performConversion(sourceFile, cacheDir, typeInferenceEnabled, casing, callback);
        } else {
          throw new IOException("Timeout waiting for Redis lock on: " + sourceFile);
        }
      } finally {
        redisLock.close();
      }
    }

    // Fall back to local locks
    Lock processLock = LOCK_MAP.computeIfAbsent(lockKey, k -> new ReentrantLock());

    boolean acquired = false;
    try {
      // Try to acquire in-process lock with timeout
      acquired = processLock.tryLock(LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (!acquired) {
        throw new IOException("Timeout waiting for lock on: " + sourceFile);
      }

      return performConversionWithFileLock(sourceFile, cacheDir, typeInferenceEnabled, casing, callback);
    } finally {
      if (acquired) {
        processLock.unlock();
      }
      // Clean up lock file periodically (could be done in a background thread)
      cleanupOldLockFiles(cacheDir);
    }
  }

  private static File performConversion(File sourceFile, File cacheDir, boolean typeInferenceEnabled, String casing,
      ConversionCallback callback) throws Exception {
    // Ensure cache directory exists
    if (!cacheDir.exists()) {
      cacheDir.mkdirs();
    }

    File parquetFile = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, typeInferenceEnabled, casing);

    // Double-check if conversion is still needed
    if (!ParquetConversionUtil.needsConversion(sourceFile, parquetFile)) {
      return parquetFile;
    }

    // Perform the actual conversion to a temp file
    File tempFile = new File(parquetFile.getAbsolutePath() + ".tmp."
        + Thread.currentThread().hashCode());

    try {
      callback.convert(tempFile);

      // Atomic rename (on most filesystems)
      Files.move(tempFile.toPath(), parquetFile.toPath(),
          java.nio.file.StandardCopyOption.REPLACE_EXISTING,
          java.nio.file.StandardCopyOption.ATOMIC_MOVE);

      // Don't set cache file timestamp to match source file
      // This allows cache invalidation to work correctly by comparing timestamps

    } finally {
      // Clean up temp file if it still exists
      if (tempFile.exists()) {
        tempFile.delete();
      }
    }

    return parquetFile;
  }

  private static File performConversionWithFileLock(File sourceFile, File cacheDir, boolean typeInferenceEnabled, String casing,
      ConversionCallback callback) throws Exception {
    // Ensure cache directory exists
    if (!cacheDir.exists()) {
      cacheDir.mkdirs();
    }

    File parquetFile = ParquetConversionUtil.getCachedParquetFile(sourceFile, cacheDir, typeInferenceEnabled, casing);
    File lockFile = new File(parquetFile.getAbsolutePath() + ".lock");

    // Use file lock for cross-JVM synchronization
    try (FileChannel channel =
         FileChannel.open(lockFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
         FileLock fileLock = channel.tryLock()) {

      if (fileLock == null) {
        throw new IOException("Could not acquire file lock for: " + parquetFile);
      }

      // Use the common conversion logic
      return performConversion(sourceFile, cacheDir, typeInferenceEnabled, casing, callback);
    }
  }

  /**
   * Clean up stale lock files older than 1 hour.
   */
  private static void cleanupOldLockFiles(File cacheDir) {
    if (!cacheDir.exists()) {
      return;
    }

    long oneHourAgo = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
    File[] lockFiles = cacheDir.listFiles((dir, name) -> name.endsWith(".lock"));

    if (lockFiles != null) {
      for (File lockFile : lockFiles) {
        if (lockFile.lastModified() < oneHourAgo) {
          lockFile.delete();
        }
      }
    }
  }

  /**
   * Callback interface for the actual conversion logic.
   */
  @FunctionalInterface
  public interface ConversionCallback {
    void convert(File targetFile) throws Exception;
  }
}
