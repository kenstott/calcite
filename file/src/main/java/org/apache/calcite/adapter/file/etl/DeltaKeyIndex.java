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
package org.apache.calcite.adapter.file.etl;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The set of row keys a {@code computed_delta} table with {@code incremental.keyFields} already
 * holds. Each key is kept as a 128-bit digest in two primitive arrays (open addressing), so a
 * table of several million rows costs on the order of a hundred megabytes whatever the width of
 * its key columns.
 *
 * <p>Not thread-safe: callers serialize on the pipeline's write lock.
 */
final class DeltaKeyIndex {

  private static final int INITIAL_CAPACITY = 1 << 10;

  private final MessageDigest digest;
  private final ByteBuffer lengthPrefix = ByteBuffer.allocate(4);
  private long[] high = new long[INITIAL_CAPACITY];
  private long[] low = new long[INITIAL_CAPACITY];
  private int size;

  DeltaKeyIndex() {
    try {
      this.digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  /**
   * Adds a key.
   *
   * @param values the key column values, in {@code keyFields} order; none may be null or empty
   * @return true when the key was not present before
   */
  boolean add(String[] values) {
    digest.reset();
    for (String value : values) {
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      lengthPrefix.clear();
      lengthPrefix.putInt(bytes.length);
      digest.update(lengthPrefix.array());
      digest.update(bytes);
    }
    ByteBuffer hash = ByteBuffer.wrap(digest.digest());
    long h = hash.getLong();
    // A zero low word marks an empty slot, so a stored key never has one.
    long l = hash.getLong() | 1L;
    if (!insert(high, low, h, l)) {
      return false;
    }
    size++;
    if (size * 2 >= high.length) {
      grow();
    }
    return true;
  }

  int size() {
    return size;
  }

  private static boolean insert(long[] high, long[] low, long h, long l) {
    int mask = high.length - 1;
    int slot = (int) h & mask;
    while (low[slot] != 0L) {
      if (low[slot] == l && high[slot] == h) {
        return false;
      }
      slot = (slot + 1) & mask;
    }
    high[slot] = h;
    low[slot] = l;
    return true;
  }

  private void grow() {
    long[] newHigh = new long[Math.multiplyExact(high.length, 2)];
    long[] newLow = new long[newHigh.length];
    for (int i = 0; i < high.length; i++) {
      if (low[i] != 0L) {
        insert(newHigh, newLow, high[i], low[i]);
      }
    }
    high = newHigh;
    low = newLow;
  }
}
