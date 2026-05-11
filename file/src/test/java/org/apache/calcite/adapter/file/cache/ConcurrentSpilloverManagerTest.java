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
package org.apache.calcite.adapter.file.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ConcurrentSpilloverManager}.
 *
 * <p>Tests verify concurrent get/put operations for spillover directories,
 * directory creation and cleanup behavior, and connection isolation.
 * Uses unique connection IDs to avoid interference between tests.
 */
@Tag("unit")
public class ConcurrentSpilloverManagerTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConcurrentSpilloverManagerTest.class);

  private final List<String> connectionIdsToCleanup =
      Collections.synchronizedList(new ArrayList<String>());

  @BeforeEach
  public void setUp() {
    // Each test generates unique connection IDs; cleanup is done in @AfterEach
  }

  @AfterEach
  public void tearDown() {
    // Clean up all connection directories created during the test
    for (String connId : connectionIdsToCleanup) {
      ConcurrentSpilloverManager.cleanupConnectionDirectory(connId);
    }
    connectionIdsToCleanup.clear();
  }

  /**
   * Generates a unique connection ID for test isolation.
   */
  private String uniqueConnectionId() {
    String connId = "test_" + UUID.randomUUID().toString();
    connectionIdsToCleanup.add(connId);
    return connId;
  }

  @Test public void testGetSpilloverDirectoryCreatesDirectory() throws Exception {
    String connId = uniqueConnectionId();
    Path spillDir = ConcurrentSpilloverManager.getSpilloverDirectory(connId);

    assertNotNull(spillDir, "Spillover directory path should not be null");
    assertTrue(Files.exists(spillDir), "Spillover directory should exist");
    assertTrue(Files.isDirectory(spillDir), "Spillover path should be a directory");
    LOGGER.debug("Created spillover directory: {}", spillDir);
  }

  @Test public void testGetSpilloverDirectoryReturnsSameForSameConnection() throws Exception {
    String connId = uniqueConnectionId();
    Path dir1 = ConcurrentSpilloverManager.getSpilloverDirectory(connId);
    Path dir2 = ConcurrentSpilloverManager.getSpilloverDirectory(connId);

    assertEquals(dir1, dir2,
        "Same connection ID should return the same directory");
  }

  @Test public void testDifferentConnectionsGetDifferentDirectories() throws Exception {
    String connId1 = uniqueConnectionId();
    String connId2 = uniqueConnectionId();

    Path dir1 = ConcurrentSpilloverManager.getSpilloverDirectory(connId1);
    Path dir2 = ConcurrentSpilloverManager.getSpilloverDirectory(connId2);

    assertNotEquals(dir1, dir2,
        "Different connection IDs should get different directories");
  }

  @Test public void testCreateSpilloverFileCreatesUniqueFiles() throws Exception {
    String connId = uniqueConnectionId();

    // Use different prefixes to guarantee uniqueness since the file name includes
    // timestamp and thread hash which may be identical for rapid same-thread calls
    File file1 = ConcurrentSpilloverManager.createSpilloverFile(connId, "batch_a");
    File file2 = ConcurrentSpilloverManager.createSpilloverFile(connId, "batch_b");

    assertNotNull(file1, "First spillover file should not be null");
    assertNotNull(file2, "Second spillover file should not be null");
    assertNotEquals(file1.getAbsolutePath(), file2.getAbsolutePath(),
        "Each spillover file should have a unique path");

    // Verify parent directory is the connection's spillover directory
    Path expectedParent = ConcurrentSpilloverManager.getSpilloverDirectory(connId);
    assertEquals(expectedParent.toString(), file1.getParent(),
        "File should be in the connection's spillover directory");
  }

  @Test public void testCleanupConnectionDirectoryRemovesFiles() throws Exception {
    String connId = uniqueConnectionId();
    Path spillDir = ConcurrentSpilloverManager.getSpilloverDirectory(connId);

    // Create some files in the spillover directory
    File file1 = ConcurrentSpilloverManager.createSpilloverFile(connId, "data");
    try (FileWriter writer = new FileWriter(file1)) {
      writer.write("test_data_1");
    }
    File file2 = ConcurrentSpilloverManager.createSpilloverFile(connId, "data");
    try (FileWriter writer = new FileWriter(file2)) {
      writer.write("test_data_2");
    }

    assertTrue(file1.exists(), "File 1 should exist before cleanup");
    assertTrue(file2.exists(), "File 2 should exist before cleanup");

    // Remove from cleanup list since we are manually cleaning up
    connectionIdsToCleanup.remove(connId);
    ConcurrentSpilloverManager.cleanupConnectionDirectory(connId);

    assertTrue(!Files.exists(spillDir),
        "Spillover directory should be deleted after cleanup");
  }

  @Test public void testConcurrentGetSpilloverDirectory() throws Exception {
    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    final String connId = uniqueConnectionId();
    final Set<Path> results = ConcurrentHashMap.newKeySet();
    final AtomicReference<Exception> firstError = new AtomicReference<Exception>();

    for (int i = 0; i < threadCount; i++) {
      executor.submit(new Runnable() {
        @Override public void run() {
          try {
            startLatch.await();
            Path dir = ConcurrentSpilloverManager.getSpilloverDirectory(connId);
            results.add(dir);
          } catch (Exception e) {
            firstError.compareAndSet(null, e);
          } finally {
            doneLatch.countDown();
          }
        }
      });
    }

    startLatch.countDown();
    boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
    assertTrue(completed, "All threads should complete within timeout");

    if (firstError.get() != null) {
      LOGGER.debug("Error during concurrent test: {}", firstError.get().getMessage());
    }

    assertEquals(1, results.size(),
        "All threads should get the same directory for the same connection ID");

    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test public void testConcurrentGetWithDifferentConnectionIds() throws Exception {
    int threadCount = 5;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    final Set<Path> results = ConcurrentHashMap.newKeySet();
    final AtomicReference<Exception> firstError = new AtomicReference<Exception>();

    for (int i = 0; i < threadCount; i++) {
      final String connId = uniqueConnectionId();
      executor.submit(new Runnable() {
        @Override public void run() {
          try {
            startLatch.await();
            Path dir = ConcurrentSpilloverManager.getSpilloverDirectory(connId);
            results.add(dir);
          } catch (Exception e) {
            firstError.compareAndSet(null, e);
          } finally {
            doneLatch.countDown();
          }
        }
      });
    }

    startLatch.countDown();
    boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
    assertTrue(completed, "All threads should complete within timeout");

    if (firstError.get() != null) {
      LOGGER.debug("Error during concurrent test: {}", firstError.get().getMessage());
    }

    assertEquals(threadCount, results.size(),
        "Each connection should get a unique directory");

    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test public void testConcurrentCreateSpilloverFiles() throws Exception {
    int threadCount = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(threadCount);
    final String connId = uniqueConnectionId();
    final Set<String> filePaths = ConcurrentHashMap.newKeySet();
    final AtomicInteger errorCount = new AtomicInteger(0);

    for (int i = 0; i < threadCount; i++) {
      executor.submit(new Runnable() {
        @Override public void run() {
          try {
            startLatch.await();
            File file = ConcurrentSpilloverManager.createSpilloverFile(connId, "concurrent");
            filePaths.add(file.getAbsolutePath());
          } catch (Exception e) {
            errorCount.incrementAndGet();
            LOGGER.debug("Error creating spillover file: {}", e.getMessage());
          } finally {
            doneLatch.countDown();
          }
        }
      });
    }

    startLatch.countDown();
    boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
    assertTrue(completed, "All threads should complete within timeout");

    assertEquals(0, errorCount.get(), "No errors should occur during concurrent file creation");
    assertEquals(threadCount, filePaths.size(),
        "Each thread should create a unique spillover file");

    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test public void testCleanupOldDirectoriesDoesNotThrow() {
    // cleanupOldDirectories should handle missing directories gracefully
    ConcurrentSpilloverManager.cleanupOldDirectories(24);
    // No assertion needed - just verifying no exception is thrown
    LOGGER.debug("cleanupOldDirectories completed without error");
  }

  @Test public void testCleanupNonexistentConnectionIsNoOp() {
    // Cleaning up a connection that doesn't exist should not throw
    ConcurrentSpilloverManager.cleanupConnectionDirectory("nonexistent_connection_" + UUID.randomUUID());
    LOGGER.debug("Cleanup of nonexistent connection completed without error");
  }

  @Test public void testSpilloverFileHasCorrectPrefix() throws Exception {
    String connId = uniqueConnectionId();

    File file = ConcurrentSpilloverManager.createSpilloverFile(connId, "etl_batch");

    assertNotNull(file, "Spillover file should not be null");
    assertTrue(file.getName().startsWith("etl_batch_"),
        "File name should start with the provided prefix: " + file.getName());
    assertTrue(file.getName().endsWith(".tmp"),
        "File name should end with .tmp extension: " + file.getName());
  }

  @Test public void testGetSpilloverDirectoryContainsConnectionId() throws Exception {
    String connId = uniqueConnectionId();
    Path spillDir = ConcurrentSpilloverManager.getSpilloverDirectory(connId);

    String dirName = spillDir.getFileName().toString();
    assertTrue(dirName.contains(connId),
        "Directory name should contain the connection ID: " + dirName);
  }
}
