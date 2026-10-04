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

    @Override public void markComplete(String sourceKey, String tableName, String phase,
        long rowCount) {
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

    @Override public Set<String> getSourceKeysForPhase(String phase) {
      Map<String, Set<String>> byKey = state.get(phase);
      return byKey == null ? Collections.<String>emptySet()
          : new LinkedHashSet<String>(byKey.keySet());
    }

    @Override public Map<String, Set<String>> bulkGetCompletedTables(
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
        ledger.pendingFor("facts", "uuid-1", PATTERN, 2016, 2025));
  }

  @Test void yearRangeAndPatternNarrowThePendingSet() {
    SecStagedFileLedger ledger = ledgerWithUploads();
    assertEquals(Collections.singletonList(B_2025),
        ledger.pendingFor("facts", "uuid-1", PATTERN, 2025, 2025));
  }

  @Test void absorbedFilesAreNotOfferedAgain() {
    SecStagedFileLedger ledger = ledgerWithUploads();
    List<String> pending = ledger.pendingFor("facts", "uuid-1", PATTERN, 2016, 2025);
    ledger.markAbsorbed("facts", "uuid-1", pending);
    assertEquals(Collections.<String>emptyList(),
        ledger.pendingFor("facts", "uuid-1", PATTERN, 2016, 2025));
  }

  @Test void recreatedTableInstanceSeesEveryFileAsPendingAgain() {
    SecStagedFileLedger ledger = ledgerWithUploads();
    ledger.markAbsorbed("facts", "uuid-1",
        ledger.pendingFor("facts", "uuid-1", PATTERN, 2016, 2025));
    assertEquals(Arrays.asList(A_2016, B_2025),
        ledger.pendingFor("facts", "uuid-2", PATTERN, 2016, 2025));
  }

  @Test void absorptionIsPerTable() {
    SecStagedFileLedger ledger = ledgerWithUploads();
    ledger.markAbsorbed("other", "uuid-1", Collections.singletonList(A_2016));
    assertEquals(Arrays.asList(A_2016, B_2025),
        ledger.pendingFor("facts", "uuid-1", PATTERN, 2016, 2025));
  }
}
