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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
/**
 * Unit tests for IcebergTimeRangeResolver.
 */
@Tag("unit")public class IcebergTimeRangeResolverTest {

  @Test public void testParseTimeRange() {
    Map<String, Object> timeRangeConfig = new HashMap<>();
    timeRangeConfig.put("start", "2024-01-01T00:00:00Z");
    timeRangeConfig.put("end", "2024-02-01T00:00:00Z");

    Instant[] result = IcebergTimeRangeResolver.parseTimeRange(timeRangeConfig);

    assertNotNull(result);
    assertEquals(2, result.length);
    assertEquals(Instant.parse("2024-01-01T00:00:00Z"), result[0]);
    assertEquals(Instant.parse("2024-02-01T00:00:00Z"), result[1]);
  }

  @Test public void testParseTimeRangeInvalid() {
    // Missing start time
    Map<String, Object> config1 = new HashMap<>();
    config1.put("end", "2024-02-01T00:00:00Z");

    assertThrows(IllegalArgumentException.class, () -> {
      IcebergTimeRangeResolver.parseTimeRange(config1);
    });

    // Missing end time
    Map<String, Object> config2 = new HashMap<>();
    config2.put("start", "2024-01-01T00:00:00Z");

    assertThrows(IllegalArgumentException.class, () -> {
      IcebergTimeRangeResolver.parseTimeRange(config2);
    });

    // Start time after end time
    Map<String, Object> config3 = new HashMap<>();
    config3.put("start", "2024-02-01T00:00:00Z");
    config3.put("end", "2024-01-01T00:00:00Z");

    assertThrows(IllegalArgumentException.class, () -> {
      IcebergTimeRangeResolver.parseTimeRange(config3);
    });
  }

  @Test public void testParseTimeRangeWithInvalidFormat() {
    Map<String, Object> timeRangeConfig = new HashMap<>();
    timeRangeConfig.put("start", "invalid-date");
    timeRangeConfig.put("end", "2024-02-01T00:00:00Z");

    assertThrows(IllegalArgumentException.class, () -> {
      IcebergTimeRangeResolver.parseTimeRange(timeRangeConfig);
    });
  }

  @Test public void testIcebergDataFile() {
    String filePath = "/path/to/file.parquet";
    Instant snapshotTime = Instant.now();
    long snapshotId = 12345L;

    IcebergTimeRangeResolver.IcebergDataFile dataFile =
        new IcebergTimeRangeResolver.IcebergDataFile(filePath, snapshotTime, snapshotId);

    assertEquals(filePath, dataFile.getFilePath());
    assertEquals(snapshotTime, dataFile.getSnapshotTime());
    assertEquals(snapshotId, dataFile.getSnapshotId());
  }

  @Test public void testParseTimeRangeWithDifferentFormats() {
    // Test various ISO-8601 formats
    Map<String, Object> config = new HashMap<>();

    // With milliseconds
    config.put("start", "2024-01-01T00:00:00.000Z");
    config.put("end", "2024-02-01T23:59:59.999Z");

    Instant[] result = IcebergTimeRangeResolver.parseTimeRange(config);
    assertNotNull(result);

    // With timezone offset
    config.put("start", "2024-01-01T00:00:00-05:00");
    config.put("end", "2024-02-01T23:59:59-05:00");

    result = IcebergTimeRangeResolver.parseTimeRange(config);
    assertNotNull(result);
    assertTrue(result[0].isBefore(result[1]));
  }
}
