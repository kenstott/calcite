/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.file.iceberg;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Deep coverage tests for {@link IcebergTimeRangeResolver}.
 * Focuses on parseTimeRange, IcebergDataFile, and edge cases.
 */
@Tag("unit")
public class IcebergTimeRangeResolverDeepTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IcebergTimeRangeResolverDeepTest.class);

  // --- parseTimeRange tests ---

  @Test public void testParseTimeRangeValid() {
    Map<String, Object> config = new HashMap<>();
    config.put("start", "2024-01-01T00:00:00Z");
    config.put("end", "2024-12-31T23:59:59Z");

    Instant[] range = IcebergTimeRangeResolver.parseTimeRange(config);
    assertNotNull(range);
    assertEquals(2, range.length);
    assertEquals(Instant.parse("2024-01-01T00:00:00Z"), range[0]);
    assertEquals(Instant.parse("2024-12-31T23:59:59Z"), range[1]);
  }

  @Test public void testParseTimeRangeSameStartAndEnd() {
    Map<String, Object> config = new HashMap<>();
    config.put("start", "2024-06-15T12:00:00Z");
    config.put("end", "2024-06-15T12:00:00Z");

    Instant[] range = IcebergTimeRangeResolver.parseTimeRange(config);
    assertNotNull(range);
    assertEquals(range[0], range[1]);
  }

  @Test public void testParseTimeRangeMissingStart() {
    Map<String, Object> config = new HashMap<>();
    config.put("end", "2024-12-31T23:59:59Z");

    assertThrows(IllegalArgumentException.class,
        () -> IcebergTimeRangeResolver.parseTimeRange(config));
  }

  @Test public void testParseTimeRangeMissingEnd() {
    Map<String, Object> config = new HashMap<>();
    config.put("start", "2024-01-01T00:00:00Z");

    assertThrows(IllegalArgumentException.class,
        () -> IcebergTimeRangeResolver.parseTimeRange(config));
  }

  @Test public void testParseTimeRangeBothMissing() {
    Map<String, Object> config = new HashMap<>();

    assertThrows(IllegalArgumentException.class,
        () -> IcebergTimeRangeResolver.parseTimeRange(config));
  }

  @Test public void testParseTimeRangeStartAfterEnd() {
    Map<String, Object> config = new HashMap<>();
    config.put("start", "2024-12-31T23:59:59Z");
    config.put("end", "2024-01-01T00:00:00Z");

    assertThrows(IllegalArgumentException.class,
        () -> IcebergTimeRangeResolver.parseTimeRange(config));
  }

  @Test public void testParseTimeRangeInvalidFormat() {
    Map<String, Object> config = new HashMap<>();
    config.put("start", "not-a-date");
    config.put("end", "also-not-a-date");

    assertThrows(IllegalArgumentException.class,
        () -> IcebergTimeRangeResolver.parseTimeRange(config));
  }

  @Test public void testParseTimeRangeEpochBoundary() {
    Map<String, Object> config = new HashMap<>();
    config.put("start", "1970-01-01T00:00:00Z");
    config.put("end", "2099-12-31T23:59:59Z");

    Instant[] range = IcebergTimeRangeResolver.parseTimeRange(config);
    assertNotNull(range);
    assertEquals(Instant.EPOCH, range[0]);
  }

  // --- IcebergDataFile tests ---

  @Test public void testIcebergDataFileGetters() {
    Instant now = Instant.now();
    IcebergTimeRangeResolver.IcebergDataFile dataFile =
        new IcebergTimeRangeResolver.IcebergDataFile("/path/to/file.parquet", now, 123L);

    assertEquals("/path/to/file.parquet", dataFile.getFilePath());
    assertEquals(now, dataFile.getSnapshotTime());
    assertEquals(123L, dataFile.getSnapshotId());
  }

  @Test public void testIcebergDataFileWithDifferentPaths() {
    Instant ts = Instant.parse("2024-06-01T00:00:00Z");

    IcebergTimeRangeResolver.IcebergDataFile file1 =
        new IcebergTimeRangeResolver.IcebergDataFile("s3://bucket/data.parquet", ts, 1L);
    assertEquals("s3://bucket/data.parquet", file1.getFilePath());

    IcebergTimeRangeResolver.IcebergDataFile file2 =
        new IcebergTimeRangeResolver.IcebergDataFile("/local/path/data.parquet", ts, 2L);
    assertEquals("/local/path/data.parquet", file2.getFilePath());

    IcebergTimeRangeResolver.IcebergDataFile file3 =
        new IcebergTimeRangeResolver.IcebergDataFile("hdfs://cluster/data.parquet", ts, 3L);
    assertEquals("hdfs://cluster/data.parquet", file3.getFilePath());
  }

  // --- Constructor test ---

  @Test public void testResolverConstructor() {
    IcebergTimeRangeResolver resolver = new IcebergTimeRangeResolver();
    assertNotNull(resolver);
  }
}
