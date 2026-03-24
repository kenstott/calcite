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
