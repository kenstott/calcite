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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests that the partitions block is read only through columnDefinitions. */
@Tag("unit")
class PartitionedTableConfigPartitionsTest {

  private static Map<String, Object> table(Map<String, Object> partitions) {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "demo_table");
    map.put("pattern", "type=demo/year=*/*.parquet");
    map.put("partitions", partitions);
    return map;
  }

  @Test void partitionsColumnsIsRejectedNamingTheTable() {
    Map<String, Object> partitions = new HashMap<String, Object>();
    partitions.put("style", "hive");
    partitions.put("columns", Collections.singletonList(Collections.singletonMap("name", "year")));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> PartitionedTableConfig.fromMap(table(partitions)));
    assertTrue(e.getMessage().contains("demo_table"), e.getMessage());
    assertTrue(e.getMessage().contains("columnDefinitions"), e.getMessage());
  }

  @Test void columnDefinitionsIsAccepted() {
    Map<String, Object> year = new HashMap<String, Object>();
    year.put("name", "year");
    year.put("type", "INTEGER");
    Map<String, Object> partitions = new HashMap<String, Object>();
    partitions.put("style", "hive");
    partitions.put("columnDefinitions", Arrays.<Object>asList(year));
    PartitionedTableConfig config = PartitionedTableConfig.fromMap(table(partitions));
    assertEquals(Collections.singletonList("year"), config.getPartitions().getColumns());
  }
}
