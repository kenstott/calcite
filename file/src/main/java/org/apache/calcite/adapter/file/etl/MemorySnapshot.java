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

/**
 * Point-in-time JVM heap snapshot for ETL memory analysis.
 *
 * <p>Used to record heap usage at key pipeline phase boundaries so that
 * post-run analysis can determine the minimum safe {@code -Xmx} allocation.
 *
 * <p>All byte values come directly from {@link Runtime#getRuntime()} without
 * forcing a GC beforehand, so they reflect the natural in-flight watermark
 * rather than post-collection residual.
 */
public class MemorySnapshot {

  private final String phase;
  private final long timestampMs;
  /** Heap bytes currently in use: totalMemory - freeMemory. */
  private final long usedBytes;
  /** Current JVM heap size (may grow up to maxBytes). */
  private final long totalBytes;
  /** Hard ceiling set by -Xmx. */
  private final long maxBytes;

  private MemorySnapshot(String phase, long timestampMs,
      long usedBytes, long totalBytes, long maxBytes) {
    this.phase = phase;
    this.timestampMs = timestampMs;
    this.usedBytes = usedBytes;
    this.totalBytes = totalBytes;
    this.maxBytes = maxBytes;
  }

  /**
   * Captures a heap snapshot for the named pipeline phase.
   * Does not trigger GC — reflects live in-flight usage.
   */
  public static MemorySnapshot capture(String phase) {
    Runtime rt = Runtime.getRuntime();
    long total = rt.totalMemory();
    long free = rt.freeMemory();
    return new MemorySnapshot(phase, System.currentTimeMillis(),
        total - free, total, rt.maxMemory());
  }

  public String getPhase() {
    return phase;
  }

  public long getTimestampMs() {
    return timestampMs;
  }

  public long getUsedBytes() {
    return usedBytes;
  }

  public long getTotalBytes() {
    return totalBytes;
  }

  public long getMaxBytes() {
    return maxBytes;
  }

  /** Returns used heap as a fraction of the -Xmx ceiling (0.0–1.0). */
  public double getUsedFraction() {
    return maxBytes > 0 ? (double) usedBytes / maxBytes : 0.0;
  }

  @Override public String toString() {
    return String.format(
        "MemorySnapshot{phase='%s', used=%dMB, total=%dMB, max=%dMB, pct=%.1f%%}",
        phase,
        usedBytes / (1024 * 1024),
        totalBytes / (1024 * 1024),
        maxBytes / (1024 * 1024),
        getUsedFraction() * 100);
  }
}
