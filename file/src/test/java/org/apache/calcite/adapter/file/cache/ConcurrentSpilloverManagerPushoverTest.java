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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Additional unit tests for {@link ConcurrentSpilloverManager}
 * to push cache package coverage past 75%.
 */
@Tag("unit")
public class ConcurrentSpilloverManagerPushoverTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConcurrentSpilloverManagerPushoverTest.class);

  @Test
  @DisplayName("getSpilloverDirectory creates directory for connection")
  void testGetSpilloverDirectory() throws IOException {
    String connId = "test_conn_" + System.nanoTime();
    try {
      Path dir = ConcurrentSpilloverManager.getSpilloverDirectory(connId);
      assertNotNull(dir);
      assertTrue(Files.exists(dir), "Spillover directory should be created");
      assertTrue(Files.isDirectory(dir));
      LOGGER.debug("Created spillover dir: {}", dir);
    } finally {
      ConcurrentSpilloverManager.cleanupConnectionDirectory(connId);
    }
  }

  @Test
  @DisplayName("getSpilloverDirectory returns same directory for same connection")
  void testGetSpilloverDirectorySameConnection() throws IOException {
    String connId = "same_conn_" + System.nanoTime();
    try {
      Path dir1 = ConcurrentSpilloverManager.getSpilloverDirectory(connId);
      Path dir2 = ConcurrentSpilloverManager.getSpilloverDirectory(connId);
      assertTrue(dir1.equals(dir2),
          "Same connection should return same directory");
    } finally {
      ConcurrentSpilloverManager.cleanupConnectionDirectory(connId);
    }
  }

  @Test
  @DisplayName("createSpilloverFile creates file in connection directory")
  void testCreateSpilloverFile() throws IOException {
    String connId = "file_conn_" + System.nanoTime();
    try {
      File file = ConcurrentSpilloverManager.createSpilloverFile(connId, "test");
      assertNotNull(file);
      assertTrue(file.getName().startsWith("test_"),
          "File name should start with prefix");
      assertTrue(file.getName().endsWith(".tmp"),
          "File name should end with .tmp");
      LOGGER.debug("Created spillover file: {}", file.getAbsolutePath());
    } finally {
      ConcurrentSpilloverManager.cleanupConnectionDirectory(connId);
    }
  }

  @Test
  @DisplayName("cleanupConnectionDirectory removes directory and contents")
  void testCleanupConnectionDirectory() throws IOException {
    String connId = "cleanup_conn_" + System.nanoTime();
    Path dir = ConcurrentSpilloverManager.getSpilloverDirectory(connId);
    assertTrue(Files.exists(dir));

    // Create a file in the directory
    File spillFile = ConcurrentSpilloverManager.createSpilloverFile(connId, "data");
    Files.write(spillFile.toPath(), "test data".getBytes());
    assertTrue(spillFile.exists());

    // Cleanup
    ConcurrentSpilloverManager.cleanupConnectionDirectory(connId);
    assertFalse(Files.exists(dir),
        "Directory should be removed after cleanup");
  }

  @Test
  @DisplayName("cleanupConnectionDirectory handles non-existent connection gracefully")
  void testCleanupNonExistentConnection() {
    // Should not throw
    ConcurrentSpilloverManager.cleanupConnectionDirectory("non_existent_" + System.nanoTime());
  }

  @Test
  @DisplayName("cleanupOldDirectories does not throw on missing base")
  void testCleanupOldDirectoriesMissingBase() {
    // Should not throw even if base directory doesn't exist
    ConcurrentSpilloverManager.cleanupOldDirectories(24);
  }

  @Test
  @DisplayName("RedisDistributedLock createIfAvailable returns null without Redis config")
  void testRedisLockReturnsNullWithoutConfig() {
    RedisDistributedLock lock = RedisDistributedLock.createIfAvailable("test_resource");
    // Without calcite.redis.url system property, should return null
    assertTrue(lock == null, "Should return null when Redis is not configured");
  }

  @Test
  @DisplayName("RedisDistributedLock tryLock returns false without Redis")
  void testRedisLockTryLockReturnsFalse() throws InterruptedException {
    RedisDistributedLock lock = new RedisDistributedLock(new Object(), "test", 5000);
    assertFalse(lock.tryLock(100), "tryLock should return false without Redis");
  }

  @Test
  @DisplayName("RedisDistributedLock unlock is no-op without Redis")
  void testRedisLockUnlockNoOp() {
    RedisDistributedLock lock = new RedisDistributedLock(new Object(), "test", 5000);
    lock.unlock(); // Should not throw
  }

  @Test
  @DisplayName("RedisDistributedLock extend returns false without Redis")
  void testRedisLockExtendReturnsFalse() {
    RedisDistributedLock lock = new RedisDistributedLock(new Object(), "test", 5000);
    assertFalse(lock.extend(1000), "extend should return false without Redis");
  }

  @Test
  @DisplayName("RedisDistributedLock close calls unlock")
  void testRedisLockClose() {
    RedisDistributedLock lock = new RedisDistributedLock(new Object(), "test", 5000);
    lock.close(); // Should not throw
  }
}
