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
package org.apache.calcite.adapter.file.partition;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PGPipelineTracker#tryClaimAll} is atomic across connections, expires abandoned claims,
 * and {@link PGPipelineTracker#bulkGetCompletedTablesFresh} sees another connection's writes
 * despite the in-process cache. Runs in an isolated PG schema dropped afterwards; skipped without
 * {@code CALCITE_TRACKER_PG_URL}.
 */
@Tag("integration")
class PGPipelineTrackerClaimTest {
  private static String url;
  private static String user;
  private static String password;
  private static String namespace;

  @BeforeAll
  static void connect() {
    url = System.getenv("CALCITE_TRACKER_PG_URL");
    Assumptions.assumeTrue(url != null, "CALCITE_TRACKER_PG_URL not set -- skipping");
    user = System.getenv("CALCITE_TRACKER_PG_USER");
    password = System.getenv("CALCITE_TRACKER_PG_PASSWORD");
    namespace = "claim_test_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    // Create the schema and table once: connections that all create them at the same instant
    // collide in Postgres's own catalog, which says nothing about the claim under test.
    tracker().isComplete("warmup", "warmup", "warmup");
  }

  @AfterAll
  static void dropSchema() throws Exception {
    if (url == null) {
      return;
    }
    try (Connection c = user != null ? DriverManager.getConnection(url, user, password)
        : DriverManager.getConnection(url);
         Statement st = c.createStatement()) {
      st.execute("DROP SCHEMA IF EXISTS \"" + namespace + "\" CASCADE");
    }
  }

  private static PGPipelineTracker tracker() {
    return new PGPipelineTracker(url, user, password, namespace);
  }

  private static List<String> keys(int n) {
    List<String> out = new ArrayList<String>();
    for (int i = 0; i < n; i++) {
      out.add("s3://b/sec/year=2023/file_" + i + ".parquet");
    }
    return out;
  }

  @Test void concurrentCallersEachWinADisjointShareOfTheKeys() throws Exception {
    final List<String> keys = keys(1500);
    final CountDownLatch go = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(4);
    List<Future<Set<String>>> futures = new ArrayList<Future<Set<String>>>();
    for (int i = 0; i < 4; i++) {
      final PGPipelineTracker t = tracker();
      futures.add(pool.submit(new Callable<Set<String>>() {
        @Override public Set<String> call() throws Exception {
          go.await();
          return t.tryClaimAll(keys, "facts#race", "absorb_claim", 3_600_000L);
        }
      }));
    }
    go.countDown();
    Set<String> all = new HashSet<String>();
    int total = 0;
    for (Future<Set<String>> f : futures) {
      Set<String> won = f.get();
      total += won.size();
      all.addAll(won);
    }
    pool.shutdown();
    assertEquals(1500, total, "a key was claimed by more than one caller, or by none");
    assertEquals(1500, all.size());
  }

  @Test void anAbandonedClaimIsTakenOverOnlyAfterTheLease() throws Exception {
    PGPipelineTracker a = tracker();
    PGPipelineTracker b = tracker();
    List<String> key = Collections.singletonList("s3://b/sec/year=2023/lease.parquet");
    assertEquals(1, a.tryClaimAll(key, "facts#lease", "absorb_claim", 60_000L).size());
    assertEquals(0, b.tryClaimAll(key, "facts#lease", "absorb_claim", 60_000L).size());
    Thread.sleep(30);
    assertEquals(1, b.tryClaimAll(key, "facts#lease", "absorb_claim", 10L).size(),
        "a claim older than the lease must be takeable");
  }

  @Test void aReleasedClaimCanBeTakenAgain() {
    PGPipelineTracker a = tracker();
    PGPipelineTracker b = tracker();
    List<String> key = Collections.singletonList("s3://b/sec/year=2023/release.parquet");
    assertEquals(1, a.tryClaimAll(key, "facts#rel", "absorb_claim", 3_600_000L).size());
    a.markCleared(key.get(0), "facts#rel", "absorb_claim");
    assertEquals(1, b.tryClaimAll(key, "facts#rel", "absorb_claim", 3_600_000L).size());
  }

  @Test void freshReadSeesAnotherConnectionsWriteThatTheCachedReadMisses() {
    PGPipelineTracker reader = tracker();
    PGPipelineTracker writer = tracker();
    String key = "s3://b/sec/year=2023/fresh.parquet";
    List<String> one = Collections.singletonList(key);
    assertTrue(reader.bulkGetCompletedTables(one, "absorbed_file").isEmpty());   // caches "none"
    writer.markComplete(key, "facts#fresh", "absorbed_file", 1);
    assertTrue(reader.bulkGetCompletedTables(one, "absorbed_file").isEmpty(),
        "documents the stale in-process cache");
    assertEquals(Collections.singleton("facts#fresh"),
        reader.bulkGetCompletedTablesFresh(one, "absorbed_file").get(key));
  }
}
