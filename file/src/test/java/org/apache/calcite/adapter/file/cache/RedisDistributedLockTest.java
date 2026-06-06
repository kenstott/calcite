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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for {@link RedisDistributedLock}.
 */
@Tag("unit")
class RedisDistributedLockTest {

  @Test void testCreateIfAvailableReturnsNullWithoutRedis() {
    // Without calcite.redis.url system property set, should return null
    assertNull(RedisDistributedLock.createIfAvailable("test-resource"));
  }

  @Test void testCreateIfAvailableReturnsNullForInvalidRedis() {
    String oldValue = System.getProperty("calcite.redis.url");
    try {
      System.setProperty("calcite.redis.url", "redis://invalid-host:12345");
      // Should return null because Redis is not available
      assertNull(RedisDistributedLock.createIfAvailable("test-resource"));
    } finally {
      if (oldValue != null) {
        System.setProperty("calcite.redis.url", oldValue);
      } else {
        System.clearProperty("calcite.redis.url");
      }
    }
  }

  @Test void testTryLockReturnsFalseWithoutRedis() throws InterruptedException {
    RedisDistributedLock lock = new RedisDistributedLock(null, "test", 5000);
    assertFalse(lock.tryLock(100));
  }

  @Test void testExtendReturnsFalseWithoutRedis() {
    RedisDistributedLock lock = new RedisDistributedLock(null, "test", 5000);
    assertFalse(lock.extend(1000));
  }

  @Test void testUnlockDoesNotThrow() {
    RedisDistributedLock lock = new RedisDistributedLock(null, "test", 5000);
    lock.unlock(); // Should not throw
  }

  @Test void testCloseDoesNotThrow() {
    RedisDistributedLock lock = new RedisDistributedLock(null, "test", 5000);
    lock.close(); // Should not throw (calls unlock)
  }

  @Test void testAutoCloseablePattern() throws Exception {
    try (RedisDistributedLock lock = new RedisDistributedLock(null, "test", 5000)) {
      // Should not throw
      assertFalse(lock.tryLock(100));
    }
  }
}
