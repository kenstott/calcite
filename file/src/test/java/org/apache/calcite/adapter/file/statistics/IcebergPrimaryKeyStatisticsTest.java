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

import org.apache.iceberg.Table;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
}
