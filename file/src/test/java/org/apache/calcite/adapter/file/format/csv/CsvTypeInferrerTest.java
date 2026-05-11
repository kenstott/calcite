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
package org.apache.calcite.adapter.file.format.csv;

import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer.ColumnTypeInfo;
import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer.TypeInferenceConfig;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sources;

import com.opencsv.exceptions.CsvValidationException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CsvTypeInferrer} and {@link TypeInferenceConfig}.
 */
@Tag("unit")
public class CsvTypeInferrerTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(CsvTypeInferrerTest.class);

  @TempDir
  Path tempDir;

  // --- TypeInferenceConfig tests ---

  @Test
  @DisplayName("defaultConfig returns expected defaults")
  void testDefaultConfigReturnsExpectedDefaults() {
    TypeInferenceConfig config = TypeInferenceConfig.defaultConfig();

    assertTrue(config.isEnabled(), "should be enabled");
    assertEquals(0.1, config.getSamplingRate(), 0.001);
    assertEquals(1000, config.getMaxSampleRows());
    assertEquals(0.95, config.getConfidenceThreshold(), 0.001);
    assertTrue(config.isInferDates());
    assertTrue(config.isInferTimes());
    assertTrue(config.isInferTimestamps());
    assertTrue(config.isMakeAllNullable());
    assertEquals(0.0, config.getNullableThreshold(), 0.001);
  }

  @Test
  @DisplayName("disabled config zeroes everything")
  void testDisabledConfigZeroesEverything() {
    TypeInferenceConfig config = TypeInferenceConfig.disabled();

    assertFalse(config.isEnabled());
    assertEquals(0.0, config.getSamplingRate(), 0.001);
    // maxSampleRows is clamped to minimum of 1
    assertEquals(1, config.getMaxSampleRows());
    assertEquals(0.0, config.getConfidenceThreshold(), 0.001);
    assertFalse(config.isInferDates());
    assertFalse(config.isInferTimes());
    assertFalse(config.isInferTimestamps());
    assertFalse(config.isMakeAllNullable());
  }

  @Test
  @DisplayName("fromMap with null returns disabled config with blankStringsAsNull=true")
  void testFromMapNullReturnsDisabledWithBlankStringsAsNull() {
    TypeInferenceConfig config = TypeInferenceConfig.fromMap(null);

    assertFalse(config.isEnabled());
    assertTrue(config.isBlankStringsAsNull());
  }

  @Test
  @DisplayName("fromMap with custom sampling rate and max rows")
  void testFromMapCustomSamplingRateAndMaxRows() {
    Map<String, Object> map = new HashMap<>();
    map.put("enabled", Boolean.TRUE);
    map.put("samplingRate", 0.5);
    map.put("maxSampleRows", 500);

    TypeInferenceConfig config = TypeInferenceConfig.fromMap(map);

    assertTrue(config.isEnabled());
    assertEquals(0.5, config.getSamplingRate(), 0.001);
    assertEquals(500, config.getMaxSampleRows());
  }

  @Test
  @DisplayName("fromMap clamps samplingRate > 1.0 to 1.0")
  void testFromMapClampsSamplingRateAboveOne() {
    Map<String, Object> map = new HashMap<>();
    map.put("enabled", Boolean.TRUE);
    map.put("samplingRate", 5.0);

    TypeInferenceConfig config = TypeInferenceConfig.fromMap(map);

    assertEquals(1.0, config.getSamplingRate(), 0.001);
  }

  @Test
  @DisplayName("fromMap clamps negative confidence to 0.0")
  void testFromMapClampsNegativeConfidence() {
    Map<String, Object> map = new HashMap<>();
    map.put("enabled", Boolean.TRUE);
    map.put("confidenceThreshold", -0.5);

    TypeInferenceConfig config = TypeInferenceConfig.fromMap(map);

    assertEquals(0.0, config.getConfidenceThreshold(), 0.001);
  }

  @Test
  @DisplayName("fromMap with custom null equivalents")
  void testFromMapCustomNullEquivalents() {
    Map<String, Object> map = new HashMap<>();
    map.put("enabled", Boolean.TRUE);
    List<String> nullEquivs = new ArrayList<>(Arrays.asList("NA", "MISSING"));
    map.put("nullEquivalents", nullEquivs);

    TypeInferenceConfig config = TypeInferenceConfig.fromMap(map);

    assertTrue(config.getNullEquivalents().contains("NA"));
    assertTrue(config.getNullEquivalents().contains("MISSING"));
    // Should be stored uppercase
    assertFalse(config.getNullEquivalents().contains("na"));
  }

  @Test
  @DisplayName("blank strings as null behavior when enabled")
  void testBlankStringsAsNullWhenEnabled() {
    Map<String, Object> map = new HashMap<>();
    map.put("enabled", Boolean.TRUE);
    // When inference is enabled, default blankStringsAsNull is false
    TypeInferenceConfig config = TypeInferenceConfig.fromMap(map);
    assertFalse(config.isBlankStringsAsNull());
  }

  // --- inferTypes tests ---

  @Test
  @DisplayName("inferTypes with integer column returns INTEGER")
  void testInferTypesIntegerColumn() throws IOException, CsvValidationException {
    File csv = writeCsv("id\n1\n2\n3\n42\n100\n");
    TypeInferenceConfig config = fullSamplingConfig();

    List<ColumnTypeInfo> result = CsvTypeInferrer.inferTypes(
        Sources.of(csv), config, "TO_LOWER");

    assertEquals(1, result.size());
    assertEquals(SqlTypeName.INTEGER, result.get(0).inferredType);
    LOGGER.debug("Integer inference result: type={}, confidence={}",
        result.get(0).inferredType, result.get(0).confidence);
  }

  @Test
  @DisplayName("inferTypes with double column returns DOUBLE")
  void testInferTypesDoubleColumn() throws IOException, CsvValidationException {
    File csv = writeCsv("price\n1.5\n2.99\n3.14\n0.01\n");
    TypeInferenceConfig config = fullSamplingConfig();

    List<ColumnTypeInfo> result = CsvTypeInferrer.inferTypes(
        Sources.of(csv), config, "TO_LOWER");

    assertEquals(1, result.size());
    assertEquals(SqlTypeName.DOUBLE, result.get(0).inferredType);
  }

  @Test
  @DisplayName("inferTypes with boolean column returns BOOLEAN")
  void testInferTypesBooleanColumn() throws IOException, CsvValidationException {
    File csv = writeCsv("active\ntrue\nfalse\nTRUE\nFALSE\n");
    TypeInferenceConfig config = fullSamplingConfig();

    List<ColumnTypeInfo> result = CsvTypeInferrer.inferTypes(
        Sources.of(csv), config, "TO_LOWER");

    assertEquals(1, result.size());
    assertEquals(SqlTypeName.BOOLEAN, result.get(0).inferredType);
  }

  @Test
  @DisplayName("inferTypes with date column returns DATE")
  void testInferTypesDateColumn() throws IOException, CsvValidationException {
    File csv = writeCsv("dt\n2024-01-15\n2024-02-20\n2024-03-25\n");
    TypeInferenceConfig config = fullSamplingConfig();

    List<ColumnTypeInfo> result = CsvTypeInferrer.inferTypes(
        Sources.of(csv), config, "TO_LOWER");

    assertEquals(1, result.size());
    assertEquals(SqlTypeName.DATE, result.get(0).inferredType);
  }

  @Test
  @DisplayName("inferTypes with timestamp column returns TIMESTAMP")
  void testInferTypesTimestampColumn() throws IOException, CsvValidationException {
    File csv = writeCsv("ts\n2024-01-15 10:30:00\n2024-02-20 14:45:00\n2024-03-25 08:00:00\n");
    TypeInferenceConfig config = fullSamplingConfig();

    List<ColumnTypeInfo> result = CsvTypeInferrer.inferTypes(
        Sources.of(csv), config, "TO_LOWER");

    assertEquals(1, result.size());
    assertEquals(SqlTypeName.TIMESTAMP, result.get(0).inferredType);
  }

  @Test
  @DisplayName("inferTypes with mixed types falls back to VARCHAR")
  void testInferTypesMixedTypesFallsBackToVarchar() throws IOException, CsvValidationException {
    File csv = writeCsv("val\n42\nhello\ntrue\n2024-01-15\n");
    TypeInferenceConfig config = fullSamplingConfig();

    List<ColumnTypeInfo> result = CsvTypeInferrer.inferTypes(
        Sources.of(csv), config, "TO_LOWER");

    assertEquals(1, result.size());
    assertEquals(SqlTypeName.VARCHAR, result.get(0).inferredType);
  }

  @Test
  @DisplayName("inferTypes with all nulls defaults to nullable VARCHAR")
  void testInferTypesAllNullsDefaultsToVarchar() throws IOException, CsvValidationException {
    File csv = writeCsv("val\nNULL\nNULL\nNULL\n");
    TypeInferenceConfig config = fullSamplingConfig();

    List<ColumnTypeInfo> result = CsvTypeInferrer.inferTypes(
        Sources.of(csv), config, "TO_LOWER");

    assertEquals(1, result.size());
    assertEquals(SqlTypeName.VARCHAR, result.get(0).inferredType);
    assertTrue(result.get(0).nullable);
  }

  @Test
  @DisplayName("inferTypes with some nulls sets nullable=true")
  void testInferTypesNullabilityDetection() throws IOException, CsvValidationException {
    // makeAllNullable=false so only real nulls trigger nullable
    TypeInferenceConfig config = new TypeInferenceConfig(
        true, 1.0, 1000, 0.95, true, true, true, false, 0.0);
    File csv = writeCsv("id\n1\n2\nNULL\n4\n");

    List<ColumnTypeInfo> result = CsvTypeInferrer.inferTypes(
        Sources.of(csv), config, "TO_LOWER");

    assertEquals(1, result.size());
    assertTrue(result.get(0).nullable, "column with null values should be nullable");
    assertTrue(result.get(0).nullCount > 0);
  }

  @Test
  @DisplayName("inferTypes with empty source returns empty list")
  void testInferTypesEmptySource() throws IOException, CsvValidationException {
    File csv = writeCsv("");
    TypeInferenceConfig config = fullSamplingConfig();

    List<ColumnTypeInfo> result = CsvTypeInferrer.inferTypes(
        Sources.of(csv), config, "TO_LOWER");

    assertTrue(result.isEmpty());
  }

  @Test
  @DisplayName("inferTypes with disabled config returns immediately with empty list")
  void testInferTypesDisabledReturnsEmpty() throws IOException, CsvValidationException {
    File csv = writeCsv("id\n1\n2\n3\n");
    TypeInferenceConfig config = TypeInferenceConfig.disabled();

    List<ColumnTypeInfo> result = CsvTypeInferrer.inferTypes(
        Sources.of(csv), config, "TO_LOWER");

    assertTrue(result.isEmpty());
  }

  @Test
  @DisplayName("Integer vs BIGINT threshold - values > MAX_INT become BIGINT")
  void testIntegerVsBigintThreshold() throws IOException, CsvValidationException {
    long bigValue = (long) Integer.MAX_VALUE + 100L;
    File csv = writeCsv("val\n" + bigValue + "\n" + (bigValue + 1) + "\n");
    TypeInferenceConfig config = fullSamplingConfig();

    List<ColumnTypeInfo> result = CsvTypeInferrer.inferTypes(
        Sources.of(csv), config, "TO_LOWER");

    assertEquals(1, result.size());
    assertEquals(SqlTypeName.BIGINT, result.get(0).inferredType);
    LOGGER.debug("BIGINT inference for value {}: type={}",
        bigValue, result.get(0).inferredType);
  }

  @Test
  @DisplayName("inferTypes header-only CSV returns empty type list with no data rows")
  void testInferTypesHeaderOnlyCsv() throws IOException, CsvValidationException {
    File csv = writeCsv("id,name\n");
    TypeInferenceConfig config = fullSamplingConfig();

    List<ColumnTypeInfo> result = CsvTypeInferrer.inferTypes(
        Sources.of(csv), config, "TO_LOWER");

    // We get column entries but all values are null since no data rows
    assertEquals(2, result.size());
    // All null/empty columns default to VARCHAR nullable
    assertEquals(SqlTypeName.VARCHAR, result.get(0).inferredType);
    assertTrue(result.get(0).nullable);
  }

  // --- Helper methods ---

  private File writeCsv(String content) throws IOException {
    File csv = new File(tempDir.toFile(), "test_" + System.nanoTime() + ".csv");
    PrintWriter writer = new PrintWriter(csv);
    try {
      writer.print(content);
    } finally {
      writer.close();
    }
    return csv;
  }

  private static TypeInferenceConfig fullSamplingConfig() {
    return new TypeInferenceConfig(
        true, 1.0, 10000, 0.95, true, true, true, true, 0.0);
  }
}
