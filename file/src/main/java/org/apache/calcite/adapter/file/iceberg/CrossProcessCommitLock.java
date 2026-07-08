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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Host-local, cross-process advisory lock that serializes Iceberg commits to a
 * single table across JVM processes on the same machine.
 *
 * <p>The per-table monitor in {@link IcebergTableWriter} serializes threads
 * within a single JVM; a single OS file lock cannot do that on its own, because
 * a second acquisition of the same file from the same JVM throws
 * {@link OverlappingFileLockException}. This class is the cross-process layer:
 * callers acquire it <em>while already holding</em> the per-table in-JVM monitor,
 * so the two layers compose into "one committer per table per host".
 *
 * <p>One lock file per table, keyed by the table location, under a host-local
 * lock directory ({@code <java.io.tmpdir>/calcite-iceberg-commit-locks}). The
 * lock file itself is always local even when the warehouse is on S3/R2 — only
 * its name derives from the (possibly remote) table location. Different tables
 * use different files, so their commits never block one another.
 *
 * <p>The acquisition is reentrant per table within a JVM: a thread that already
 * holds the lock for a table (e.g. a commit path that nests another commit
 * method) increments a hold count instead of taking a second OS lock, which
 * would otherwise self-deadlock with {@link OverlappingFileLockException}. The
 * OS releases the underlying lock automatically when the holding process exits,
 * so a crashed committer never leaves a stale lock — no recovery logic needed.
 *
 * <p>This lock is intentionally host-scoped. It is correct only under the
 * invariant that a given table is owned (written) by a single device; it
 * coordinates the parallel writer processes on that one device and nothing
 * across devices.
 */
public final class CrossProcessCommitLock {

  private CrossProcessCommitLock() {
  }

  /** Per-table reentrancy state, guarded by its own monitor. */
  private static final class Holder {
    private FileChannel channel;
    private FileLock lock;
    private Thread owner;
    private int holds;
  }

  private static final ConcurrentHashMap<String, Holder> HOLDERS =
      new ConcurrentHashMap<String, Holder>();

  /** Per-table in-JVM monitors, composed with the OS file lock by {@link #runExclusive}. */
  private static final ConcurrentHashMap<String, Object> JVM_MONITORS =
      new ConcurrentHashMap<String, Object>();

  /**
   * Runs {@code body} under both serialization layers for one table: the in-JVM per-table monitor
   * (serializes threads within this process) and the host-local OS file lock (serializes the
   * parallel writer processes on this device) — "one mutator per table per host". The OS file lock
   * alone cannot serialize threads in a single JVM, because a second {@link FileChannel#lock()} on
   * the same file from the same JVM throws {@link OverlappingFileLockException}; the monitor must be
   * held first. Use this for every operation that mutates a table's files or metadata outside a
   * single Iceberg transaction: commits, snapshot expiry, orphan-file deletion, and drop/recreate.
   *
   * @param tableLocation the Iceberg table location (lock identity)
   * @param body the exclusive operation to run
   */
  public static void runExclusive(String tableLocation, Runnable body) {
    Object monitor = JVM_MONITORS.computeIfAbsent(tableLocation,
        new java.util.function.Function<String, Object>() {
          @Override public Object apply(String k) {
            return new Object();
          }
        });
    synchronized (monitor) {
      try (Handle xlock = acquire(tableLocation)) {
        body.run();
      }
    }
  }

  /**
   * Acquires the cross-process commit lock for the given table location,
   * blocking until it is available. Reentrant within the same JVM for the same
   * table.
   *
   * @param tableLocation the Iceberg table location (lock identity)
   * @return an {@link AutoCloseable} handle; close it to release one hold
   */
  public static Handle acquire(String tableLocation) {
    Holder holder = HOLDERS.computeIfAbsent(tableLocation, new java.util.function.Function<String, Holder>() {
      @Override public Holder apply(String k) {
        return new Holder();
      }
    });
    synchronized (holder) {
      if (holder.holds > 0 && holder.owner == Thread.currentThread()) {
        holder.holds++;
        return new Handle(tableLocation);
      }
      FileChannel channel = openChannel(tableLocation);
      try {
        FileLock lock = channel.lock();
        holder.channel = channel;
        holder.lock = lock;
        holder.owner = Thread.currentThread();
        holder.holds = 1;
        return new Handle(tableLocation);
      } catch (IOException e) {
        closeQuietly(channel);
        throw new UncheckedIOException(
            "Failed to acquire commit lock for " + tableLocation, e);
      }
    }
  }

  /**
   * Attempts to acquire the lock without blocking. Returns {@code null} if the
   * lock is held by another process (or already by this JVM). Intended for
   * tests and diagnostics; production commits use {@link #acquire(String)}.
   *
   * @param tableLocation the Iceberg table location (lock identity)
   * @return a handle, or {@code null} if the lock could not be taken
   */
  public static Handle tryAcquire(String tableLocation) {
    FileChannel channel = openChannel(tableLocation);
    try {
      FileLock lock = channel.tryLock();
      if (lock == null) {
        closeQuietly(channel);
        return null;
      }
      Holder holder = HOLDERS.computeIfAbsent(tableLocation, new java.util.function.Function<String, Holder>() {
        @Override public Holder apply(String k) {
          return new Holder();
        }
      });
      synchronized (holder) {
        holder.channel = channel;
        holder.lock = lock;
        holder.owner = Thread.currentThread();
        holder.holds = 1;
      }
      return new Handle(tableLocation);
    } catch (OverlappingFileLockException e) {
      // Already held by this JVM.
      closeQuietly(channel);
      return null;
    } catch (IOException e) {
      closeQuietly(channel);
      throw new UncheckedIOException(
          "Failed to try-acquire commit lock for " + tableLocation, e);
    }
  }

  private static void release(String tableLocation) {
    Holder holder = HOLDERS.get(tableLocation);
    if (holder == null) {
      return;
    }
    synchronized (holder) {
      if (holder.holds == 0) {
        return;
      }
      holder.holds--;
      if (holder.holds == 0) {
        try {
          holder.lock.release();
        } catch (IOException ignored) {
          // Best effort; the OS releases on process exit regardless.
        }
        closeQuietly(holder.channel);
        holder.lock = null;
        holder.channel = null;
        holder.owner = null;
      }
    }
  }

  /** The host-local directory holding one lock file per table. */
  static Path lockDir() {
    return Paths.get(System.getProperty("java.io.tmpdir"), "calcite-iceberg-commit-locks");
  }

  /** The lock file path for a table location (one file per table). */
  static Path lockFileFor(String tableLocation) {
    return lockDir().resolve(sha256Hex(tableLocation) + ".lock");
  }

  private static FileChannel openChannel(String tableLocation) {
    try {
      Path file = lockFileFor(tableLocation);
      Files.createDirectories(file.getParent());
      return FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to open commit lock file for " + tableLocation, e);
    }
  }

  private static void closeQuietly(FileChannel channel) {
    try {
      channel.close();
    } catch (IOException ignored) {
      // Best effort.
    }
  }

  private static String sha256Hex(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder(bytes.length * 2);
      for (int i = 0; i < bytes.length; i++) {
        int b = bytes[i] & 0xFF;
        sb.append(Character.forDigit(b >>> 4, 16));
        sb.append(Character.forDigit(b & 0xF, 16));
      }
      return sb.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  /** A single hold on the cross-process commit lock. */
  public static final class Handle implements AutoCloseable {
    private final String tableLocation;
    private boolean closed;

    private Handle(String tableLocation) {
      this.tableLocation = tableLocation;
    }

    @Override public void close() {
      if (!closed) {
        closed = true;
        release(tableLocation);
      }
    }
  }
}
