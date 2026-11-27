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

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A {@link Table} handle whose {@link Table#currentSnapshot()} itself throws (rather than
 * returning null) must degrade to "not measured", the same as any other unreadable statistic.
 */
@Tag("unit")
public class IcebergPrimaryKeyStatisticsTest {

  @Test void currentSnapshotThrowingIsTreatedAsNoStatistic() {
    Table table = mock(Table.class);
    when(table.currentSnapshot()).thenThrow(
        new NullPointerException("Cannot invoke \"org.apache.iceberg.TableMetadata"
            + ".currentSnapshot()\" because the return value of "
            + "\"org.apache.iceberg.TableOperations.current()\" is null"));

    assertNull(IcebergPrimaryKeyStatistics.read(table));
  }

  @Test void exactStatisticWithoutEngineMethodIsNotServed() {
    assertNull(IcebergPrimaryKeyStatistics.read(tableWith(properties("true"))));
  }

  @Test void exactStatisticMeasuredByTheEngineIsServed() {
    Map<String, String> props = properties("true");
    props.put("aperio.pk.method", "engine");

    PrimaryKeyStatistics stats = IcebergPrimaryKeyStatistics.read(tableWith(props));

    assertNotNull(stats);
    assertEquals(6940L, stats.getDistinctKeyEstimate());
  }

  @Test void estimatedStatisticIsServedWithoutMethod() {
    assertNotNull(IcebergPrimaryKeyStatistics.read(tableWith(properties("false"))));
  }

  private static Map<String, String> properties(String exact) {
    Map<String, String> props = new HashMap<>();
    props.put("aperio.pk.snapshot-id", "7");
    props.put("aperio.pk.columns", "type,date_key");
    props.put("aperio.pk.keyed-rows", "20820");
    props.put("aperio.pk.distinct-keys", "6940");
    props.put("aperio.pk.exact", exact);
    return props;
  }

  private static Table tableWith(Map<String, String> props) {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.snapshotId()).thenReturn(7L);
    Table table = mock(Table.class);
    when(table.currentSnapshot()).thenReturn(snapshot);
    when(table.properties()).thenReturn(props);
    return table;
  }
}
