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
package org.apache.calcite.adapter.file.iceberg;

import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

/**
 * Open-addressing multiset of 128-bit row digests, each tagged with the group (e.g. accession) it
 * was first seen under. Holds about 24 bytes per distinct row, so a year of a 20M-row table fits in
 * well under a gigabyte, where a {@code HashMap<String,Integer>} would not.
 *
 * <p>A digest is the MD5 of every column's value in schema order, so two rows share one only when
 * every column is equal. 128 bits makes an accidental collision of distinct rows negligible.
 */
final class RowDigestCounts {
  private static final double LOAD = 0.6;

  private long[] hi;
  private long[] lo;
  private int[] count;
  private int[] group;
  private int[] kept;
  private int mask;
  private int size;

  RowDigestCounts() {
    allocate(1 << 16);
  }

  private void allocate(int capacity) {
    hi = new long[capacity];
    lo = new long[capacity];
    count = new int[capacity];
    group = new int[capacity];
    mask = capacity - 1;
  }

  /** Distinct digests held. */
  int size() {
    return size;
  }

  /** Records one more occurrence of the digest under {@code groupIndex}; returns its slot. */
  int add(long h, long l, int groupIndex) {
    if (size + 1 > (long) (hi.length * LOAD)) {
      grow();
    }
    int slot = slotFor(h, l);
    while (count[slot] != 0) {
      if (hi[slot] == h && lo[slot] == l) {
        count[slot]++;
        return slot;
      }
      slot = (slot + 1) & mask;
    }
    hi[slot] = h;
    lo[slot] = l;
    count[slot] = 1;
    group[slot] = groupIndex;
    size++;
    return slot;
  }

  /** The digest's slot, or -1 when it was never added. */
  int find(long h, long l) {
    int slot = slotFor(h, l);
    while (count[slot] != 0) {
      if (hi[slot] == h && lo[slot] == l) {
        return slot;
      }
      slot = (slot + 1) & mask;
    }
    return -1;
  }

  int capacity() {
    return hi.length;
  }

  int count(int slot) {
    return count[slot];
  }

  int group(int slot) {
    return group[slot];
  }

  /** How many copies of the digest the second pass has kept so far. */
  int kept(int slot) {
    return kept == null ? 0 : kept[slot];
  }

  void keepOne(int slot) {
    if (kept == null) {
      kept = new int[hi.length];
    }
    kept[slot]++;
  }

  private int slotFor(long h, long l) {
    long mixed = h ^ (l * 0x9E3779B97F4A7C15L);
    mixed ^= mixed >>> 32;
    return (int) mixed & mask;
  }

  private void grow() {
    long[] oldHi = hi;
    long[] oldLo = lo;
    int[] oldCount = count;
    int[] oldGroup = group;
    int[] oldKept = kept;
    allocate(oldHi.length * 2);
    kept = oldKept == null ? null : new int[oldHi.length * 2];
    for (int i = 0; i < oldHi.length; i++) {
      if (oldCount[i] == 0) {
        continue;
      }
      int slot = slotFor(oldHi[i], oldLo[i]);
      while (count[slot] != 0) {
        slot = (slot + 1) & mask;
      }
      hi[slot] = oldHi[i];
      lo[slot] = oldLo[i];
      count[slot] = oldCount[i];
      group[slot] = oldGroup[i];
      if (oldKept != null) {
        kept[slot] = oldKept[i];
      }
    }
  }

  /**
   * Digest of the named columns of {@code record}, in the order given, written to {@code out[0]}
   * (high 64 bits) and {@code out[1]} (low 64 bits). A null and the string "null" differ.
   */
  static void digest(MessageDigest md, Record record, List<Types.NestedField> columns,
      long[] out) {
    md.reset();
    for (Types.NestedField column : columns) {
      Object value = record.getField(column.name());
      if (value == null) {
        md.update((byte) 0);
      } else {
        md.update((byte) 1);
        md.update(canonical(value).getBytes(StandardCharsets.UTF_8));
      }
      md.update((byte) 0x1F);
    }
    byte[] d = md.digest();
    long h = 0;
    long l = 0;
    for (int i = 0; i < 8; i++) {
      h = (h << 8) | (d[i] & 0xFF);
      l = (l << 8) | (d[8 + i] & 0xFF);
    }
    out[0] = h;
    out[1] = l;
  }

  private static String canonical(Object value) {
    if (value instanceof byte[]) {
      return hex((byte[]) value);
    }
    if (value instanceof ByteBuffer) {
      ByteBuffer copy = ((ByteBuffer) value).duplicate();
      byte[] bytes = new byte[copy.remaining()];
      copy.get(bytes);
      return hex(bytes);
    }
    return String.valueOf(value);
  }

  private static String hex(byte[] bytes) {
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
    }
    return sb.toString();
  }
}
