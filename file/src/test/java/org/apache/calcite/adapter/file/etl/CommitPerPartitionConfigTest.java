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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@code commitPerPartition} option: publish each batch's partition as that batch finishes,
 * so an interrupted run keeps the partitions it completed instead of losing the whole run.
 */
@Tag("unit")
public class CommitPerPartitionConfigTest {

  @Test void defaultsOffSoExistingSchemasAreUnaffected() {
    assertFalse(MaterializeOptionsConfig.builder().build().isCommitPerPartition(),
        "enabling this for a schema whose partitions are coarser than its fetch unit would "
            + "replace previously committed rows, so it must be opt-in");
  }

  @Test void defaultsOffWhenTheKeyIsAbsentFromTheMap() {
    assertFalse(MaterializeOptionsConfig.fromMap(new HashMap<String, Object>())
        .isCommitPerPartition());
  }

  @Test void readsTheOptionFromSchemaYaml() {
    Map<String, Object> map = new HashMap<>();
    map.put("commitPerPartition", true);

    assertTrue(MaterializeOptionsConfig.fromMap(map).isCommitPerPartition());
  }

  @Test void survivesAlongsideTheOtherOptions() {
    Map<String, Object> map = new HashMap<>();
    map.put("batchSize", 50000);
    map.put("stagingMode", "LOCAL");
    map.put("commitPerPartition", true);

    MaterializeOptionsConfig config = MaterializeOptionsConfig.fromMap(map);

    assertTrue(config.isCommitPerPartition());
    assertTrue(config.getStagingMode() == MaterializeOptionsConfig.StagingMode.LOCAL);
  }

  /** A non-boolean value must not be coerced into enabling an unsafe commit mode. */
  @Test void ignoresANonBooleanValue() {
    Map<String, Object> map = new HashMap<>();
    map.put("commitPerPartition", "yes");

    assertFalse(MaterializeOptionsConfig.fromMap(map).isCommitPerPartition());
  }

  @Test void builderRoundTripsBothStates() {
    assertTrue(MaterializeOptionsConfig.builder().commitPerPartition(true).build()
        .isCommitPerPartition());
    assertFalse(MaterializeOptionsConfig.builder().commitPerPartition(false).build()
        .isCommitPerPartition());
  }
}
