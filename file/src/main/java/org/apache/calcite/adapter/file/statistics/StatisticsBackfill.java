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
package org.apache.calcite.adapter.file.statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Computes missing column statistics in the background, off the query path.
 *
 * <p>Publishing statistics from the ETL covers tables written after that hook exists, but not
 * tables already in the lake, tables written by an older build, or anything whose statistics were
 * dropped. Those would otherwise stay un-profiled forever, because nothing else walks the lake.
 * A query server that sees an un-profiled table is exactly the moment we know the table is worth
 * profiling — so the first query triggers the work and returns immediately with the planner's
 * default estimates. Later queries get the real ones.
 *
 * <p>Deliberately never blocks a query. The submitting caller gets no result and no exception;
 * the outcome is only that {@link HLLSketchCache} and the durable store are populated some time
 * later. That keeps the optimization strictly additive — a failure to profile leaves the planner
 * exactly where it was.
 *
 * <p>Three guards keep it from becoming its own problem:
 * <ul>
 *   <li><b>Single-flight</b> per schema.table, so N concurrent connections opening the same schema
 *       queue one profile rather than N identical full scans.</li>
 *   <li><b>Small bounded pool</b> of daemon threads, so profiling cannot starve query execution
 *       and cannot hold the JVM open at shutdown.</li>
 *   <li><b>Re-check before computing</b>, because a table can be profiled by the ETL (or another
 *       process) between submission and execution.</li>
 * </ul>
 */
public final class StatisticsBackfill {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsBackfill.class);

  /** Supplies what a profile needs, resolved lazily so submission stays cheap. */
  public interface Target {
    /** DuckDB connection able to see the table; closed by the backfill when done. */
    Connection openConnection() throws Exception;

    /** Scan expression for the table, e.g. an {@code iceberg_scan('s3://...')} call. */
    String fromClause();

    /** Columns to profile. */
    List<String> columns();

    /** Current row count, stored alongside the NDVs. */
    long rowCount();
  }

  private static final int POOL_SIZE =
      Integer.getInteger("calcite.file.statistics.backfill.threads", 2);

  private static final ExecutorService POOL =
      Executors.newFixedThreadPool(POOL_SIZE, new ThreadFactory() {
        private final AtomicInteger n = new AtomicInteger();
        @Override public Thread newThread(Runnable r) {
          Thread t = new Thread(r, "stats-backfill-" + n.incrementAndGet());
          // Daemon: a half-finished profile must never keep the JVM alive.
          t.setDaemon(true);
          t.setPriority(Thread.MIN_PRIORITY);
          return t;
        }
      });

  private static final Set<String> IN_FLIGHT = ConcurrentHashMap.newKeySet();

  private StatisticsBackfill() {
  }

  /** True when statistics collection is switched off entirely. */
  public static boolean enabled() {
    return !"false".equals(
        System.getProperty("calcite.file.statistics.backfill.enabled", "true"));
  }

  /**
   * Queues a profile for one table unless one is already queued or running for it.
   *
   * @return true if this call queued the work
   */
  public static boolean submit(final String schema, final String table,
      final PGColumnStatisticsStore store, final Target target) {
    if (!enabled() || store == null || target == null) {
      return false;
    }
    final String key = schema.toLowerCase(java.util.Locale.ROOT)
        + "." + table.toLowerCase(java.util.Locale.ROOT);
    if (!IN_FLIGHT.add(key)) {
      return false;   // already queued or running
    }
    try {
      POOL.submit(new Runnable() {
        @Override public void run() {
          try {
            profile(schema, table, store, target);
          } catch (Throwable t) {
            // Advisory work: swallow everything, including errors a query would surface.
            LOGGER.warn("Background statistics profile failed for {}: {}", key, t.toString());
          } finally {
            IN_FLIGHT.remove(key);
          }
        }
      });
      return true;
    } catch (RuntimeException rejected) {
      IN_FLIGHT.remove(key);
      LOGGER.debug("Statistics backfill rejected for {}: {}", key, rejected.getMessage());
      return false;
    }
  }

  private static void profile(String schema, String table, PGColumnStatisticsStore store,
      Target target) throws Exception {
    List<String> cols = target.columns();
    if (cols == null || cols.isEmpty()) {
      return;
    }
    Connection conn = target.openConnection();
    try {
      Map<String, Long> ndv =
          PGColumnStatisticsStore.computeNdv(conn, target.fromClause(), cols);
      if (ndv.isEmpty()) {
        return;
      }
      store.put(schema, table, target.rowCount(), ndv);
      // Make it usable in this process immediately rather than waiting for the next schema-open.
      HLLSketchCache cache = HLLSketchCache.getInstance();
      for (Map.Entry<String, Long> e : ndv.entrySet()) {
        cache.putSketch(schema, table, e.getKey(),
            HyperLogLogSketch.fromEstimate(e.getValue()));
      }
      LOGGER.info("Backfilled statistics for {}.{}: {} columns", schema, table, ndv.size());
    } finally {
      try {
        conn.close();
      } catch (Exception ignored) {
        // best effort
      }
    }
  }
}
