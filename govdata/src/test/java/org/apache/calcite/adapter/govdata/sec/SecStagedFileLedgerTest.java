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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.partition.PipelineTracker;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests {@link SecStagedFileLedger}. */
@Tag("unit")
class SecStagedFileLedgerTest {
  private static final String PATTERN = "s3://b/sec/year=*/*facts*.parquet";

  /** Phase to source key to table names, which is all the ledger reads back. */
  private static final class MapTracker extends PipelineTracker.NoopPipelineTracker {
    private final Map<String, Map<String, Set<String>>> state =
        new HashMap<String, Map<String, Set<String>>>();

    @Override public synchronized void markComplete(String sourceKey, String tableName,
        String phase, long rowCount) {
      Map<String, Set<String>> byKey = state.get(phase);
      if (byKey == null) {
        byKey = new HashMap<String, Set<String>>();
        state.put(phase, byKey);
      }
      Set<String> tables = byKey.get(sourceKey);
      if (tables == null) {
        tables = new HashSet<String>();
        byKey.put(sourceKey, tables);
      }
      tables.add(tableName);
    }

    /** (phase, key, table) -> claimed-at, for the claim primitive. */
    private final Map<String, Long> claims = new HashMap<String, Long>();
    long now = 1_000L;
    Runnable beforeClaim;

    @Override public synchronized Set<String> tryClaimAll(Collection<String> sourceKeys,
        String tableName, String phase, long leaseMillis) {
      if (beforeClaim != null) {
        Runnable hook = beforeClaim;
        beforeClaim = null;
        hook.run();
      }
      Set<String> won = new LinkedHashSet<String>();
      for (String key : sourceKeys) {
        String id = phase + "|" + key + "|" + tableName;
        Long at = claims.get(id);
        if (at == null || at < now - leaseMillis) {
          claims.put(id, Long.valueOf(now));
          won.add(key);
        }
      }
      return won;
    }

    @Override public synchronized void markCleared(String sourceKey, String tableName,
        String phase) {
      claims.remove(phase + "|" + sourceKey + "|" + tableName);
    }

    @Override public synchronized Set<String> getSourceKeysForPhase(String phase) {
      Map<String, Set<String>> byKey = state.get(phase);
      return byKey == null ? Collections.<String>emptySet()
          : new LinkedHashSet<String>(byKey.keySet());
    }

    @Override public synchronized Map<String, Set<String>> bulkGetCompletedTables(
        Collection<String> sourceKeys, String phase) {
      Map<String, Set<String>> byKey = state.get(phase);
      Map<String, Set<String>> result = new HashMap<String, Set<String>>();
      if (byKey != null) {
        for (String key : sourceKeys) {
          if (byKey.containsKey(key)) {
            result.put(key, byKey.get(key));
          }
        }
      }
      return result;
    }
  }

  private static final String A_2016 = "s3://b/sec/year=2016/facts_aaaa_0001.parquet";
  private static final String B_2025 = "s3://b/sec/year=2025/facts_bbbb_0002.parquet";
  private static final String M_2025 = "s3://b/sec/year=2025/metadata_bbbb_0003.parquet";

  private static SecStagedFileLedger ledgerWithUploads() {
    SecStagedFileLedger ledger = new SecStagedFileLedger(new MapTracker());
    ledger.recordUpload(A_2016, "facts");
    ledger.recordUpload(B_2025, "facts");
    ledger.recordUpload(M_2025, "metadata");
    return ledger;
  }

  @Test void filesFromEarlierPassAreOfferedWithoutBeingUploadedByThisOne() {
    SecStagedFileLedger ledger = ledgerWithUploads();
    assertEquals(Arrays.asList(A_2016, B_2025),
        ledger.pendingFor("facts", "uuid-1", PATTERN));
  }

  @Test void patternNarrowsThePendingSetAcrossEveryYearPartition() {
    SecStagedFileLedger ledger = ledgerWithUploads();
    assertEquals(Arrays.asList(A_2016, B_2025),
        ledger.pendingFor("facts", "uuid-1", PATTERN));
    assertEquals(Collections.singletonList(M_2025),
        ledger.pendingFor("metadata", "uuid-1", "s3://b/sec/year=*/*metadata*.parquet"));
  }

  @Test void absorbedFilesAreNotOfferedAgain() {
    SecStagedFileLedger ledger = ledgerWithUploads();
    List<String> pending = ledger.pendingFor("facts", "uuid-1", PATTERN);
    ledger.markAbsorbed("facts", "uuid-1", pending);
    assertEquals(Collections.<String>emptyList(),
        ledger.pendingFor("facts", "uuid-1", PATTERN));
  }

  @Test void recreatedTableInstanceSeesEveryFileAsPendingAgain() {
    SecStagedFileLedger ledger = ledgerWithUploads();
    ledger.markAbsorbed("facts", "uuid-1",
        ledger.pendingFor("facts", "uuid-1", PATTERN));
    assertEquals(Arrays.asList(A_2016, B_2025),
        ledger.pendingFor("facts", "uuid-2", PATTERN));
  }

  @Test void absorptionIsPerTable() {
    SecStagedFileLedger ledger = ledgerWithUploads();
    ledger.markAbsorbed("other", "uuid-1", Collections.singletonList(A_2016));
    assertEquals(Arrays.asList(A_2016, B_2025),
        ledger.pendingFor("facts", "uuid-1", PATTERN));
  }

  private static final long LEASE = 3_600_000L;

  @Test void twoWorkersRacingForTheSameFilesTakeEachFileOnce() throws Exception {
    MapTracker tracker = new MapTracker();
    SecStagedFileLedger ledger = new SecStagedFileLedger(tracker);
    for (int i = 0; i < 200; i++) {
      ledger.recordUpload("s3://b/sec/year=2023/facts_" + i + ".parquet", "facts");
    }
    final java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(3);
    List<java.util.concurrent.Future<List<String>>> results =
        new java.util.ArrayList<java.util.concurrent.Future<List<String>>>();
    for (int w = 0; w < 3; w++) {
      final SecStagedFileLedger worker = new SecStagedFileLedger(tracker);
      results.add(pool.submit(new java.util.concurrent.Callable<List<String>>() {
        @Override public List<String> call() throws Exception {
          go.await();
          return worker.claimPendingFor("facts", "uuid-1", PATTERN, LEASE);
        }
      }));
    }
    go.countDown();
    Set<String> seen = new HashSet<String>();
    int total = 0;
    for (java.util.concurrent.Future<List<String>> f : results) {
      for (String path : f.get()) {
        assertEquals(true, seen.add(path), "file claimed by two workers: " + path);
        total++;
      }
    }
    pool.shutdown();
    assertEquals(200, total);
  }

  @Test void aSecondWorkerGetsNothingWhileTheFirstHoldsTheClaims() {
    SecStagedFileLedger ledger = ledgerWithUploads();
    assertEquals(Arrays.asList(A_2016, B_2025),
        ledger.claimPendingFor("facts", "uuid-1", PATTERN, LEASE));
    assertEquals(Collections.<String>emptyList(),
        ledger.claimPendingFor("facts", "uuid-1", PATTERN, LEASE));
  }

  @Test void releasedButUnabsorbedFilesAreOfferedAgain() {
    SecStagedFileLedger ledger = ledgerWithUploads();
    List<String> mine = ledger.claimPendingFor("facts", "uuid-1", PATTERN, LEASE);
    ledger.releaseClaims("facts", "uuid-1", mine);   // a failed pass
    assertEquals(Arrays.asList(A_2016, B_2025),
        ledger.claimPendingFor("facts", "uuid-1", PATTERN, LEASE));
  }

  @Test void absorbedThenReleasedFilesAreNotClaimedByALaterWorker() {
    SecStagedFileLedger ledger = ledgerWithUploads();
    List<String> mine = ledger.claimPendingFor("facts", "uuid-1", PATTERN, LEASE);
    ledger.markAbsorbed("facts", "uuid-1", mine);
    ledger.releaseClaims("facts", "uuid-1", mine);
    assertEquals(Collections.<String>emptyList(),
        ledger.claimPendingFor("facts", "uuid-1", PATTERN, LEASE));
  }

  @Test void aFileAbsorbedBetweenListingAndClaimingIsDroppedNotAbsorbedTwice() {
    final MapTracker tracker = new MapTracker();
    final SecStagedFileLedger first = new SecStagedFileLedger(tracker);
    first.recordUpload(A_2016, "facts");
    SecStagedFileLedger second = new SecStagedFileLedger(tracker);
    // After the second worker has listed A_2016 as pending but before it claims, the first worker
    // claims, absorbs and releases it.
    tracker.beforeClaim = new Runnable() {
      @Override public void run() {
        List<String> got = first.claimPendingFor("facts", "uuid-1", PATTERN, LEASE);
        first.markAbsorbed("facts", "uuid-1", got);
        first.releaseClaims("facts", "uuid-1", got);
      }
    };
    assertEquals(Collections.<String>emptyList(),
        second.claimPendingFor("facts", "uuid-1", PATTERN, LEASE));
    // and the second worker left no claim behind
    assertEquals(Collections.<String>emptyList(),
        second.claimPendingFor("facts", "uuid-1", PATTERN, LEASE));
  }

  @Test void aKilledWorkersClaimsExpireAfterTheLease() {
    MapTracker tracker = new MapTracker();
    SecStagedFileLedger ledger = new SecStagedFileLedger(tracker);
    ledger.recordUpload(A_2016, "facts");
    assertEquals(Collections.singletonList(A_2016),
        ledger.claimPendingFor("facts", "uuid-1", PATTERN, LEASE));
    tracker.now += LEASE - 1;
    assertEquals(Collections.<String>emptyList(),
        ledger.claimPendingFor("facts", "uuid-1", PATTERN, LEASE));
    tracker.now += 2;
    assertEquals(Collections.singletonList(A_2016),
        ledger.claimPendingFor("facts", "uuid-1", PATTERN, LEASE));
  }

  @Test void claimsAreIndependentPerTableInstance() {
    SecStagedFileLedger ledger = ledgerWithUploads();
    assertEquals(Arrays.asList(A_2016, B_2025),
        ledger.claimPendingFor("facts", "uuid-1", PATTERN, LEASE));
    assertEquals(Arrays.asList(A_2016, B_2025),
        ledger.claimPendingFor("facts", "uuid-2", PATTERN, LEASE));
  }

  @Test void pendingFilesIgnoreAStaleCacheOfAnotherProcessesAbsorption() {
    // A tracker whose plain bulk read returns a cached "nothing absorbed", as the PG tracker's
    // per-process cache can, while the fresh read tells the truth.
    final MapTracker truth = new MapTracker();
    PipelineTracker cached = new PipelineTracker.NoopPipelineTracker() {
      @Override public void markComplete(String k, String t, String p, long r) {
        truth.markComplete(k, t, p, r);
      }
      @Override public Set<String> getSourceKeysForPhase(String phase) {
        return truth.getSourceKeysForPhase(phase);
      }
      @Override public Map<String, Set<String>> bulkGetCompletedTables(
          Collection<String> keys, String phase) {
        return Collections.<String, Set<String>>emptyMap();   // stale: never sees absorption
      }
      @Override public Map<String, Set<String>> bulkGetCompletedTablesFresh(
          Collection<String> keys, String phase) {
        return truth.bulkGetCompletedTables(keys, phase);
      }
    };
    SecStagedFileLedger ledger = new SecStagedFileLedger(cached);
    ledger.recordUpload(A_2016, "facts");
    ledger.markAbsorbed("facts", "uuid-1", Collections.singletonList(A_2016));
    assertEquals(Collections.<String>emptyList(),
        ledger.pendingFor("facts", "uuid-1", PATTERN));
  }
}
