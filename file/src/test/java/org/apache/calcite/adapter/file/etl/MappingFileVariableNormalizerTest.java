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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link MappingFileVariableNormalizer}.
 */
@Tag("unit")
class MappingFileVariableNormalizerTest {

  private MappingFileVariableNormalizer normalizer;

  @BeforeEach
  void setUp() {
    normalizer =
        new MappingFileVariableNormalizer("etl-test/test-variable-mappings.json", "acs");
  }

  @Test void testNormalizeAcsAllYearsVariable() {
    Map<String, String> context = new HashMap<String, String>();
    context.put("year", "2022");
    context.put("type", "acs");

    String result = normalizer.normalize("B01001_001E", context);
    assertEquals("total_population", result);
  }

  @Test void testNormalizeDecennialYearSpecificVariable() {
    Map<String, String> context = new HashMap<String, String>();
    context.put("year", "2020");
    context.put("type", "decennial");

    String result = normalizer.normalize("P1_001N", context);
    assertEquals("total_population", result);
  }

  @Test void testNormalizeDecennial2010Variable() {
    Map<String, String> context = new HashMap<String, String>();
    context.put("year", "2010");
    context.put("type", "decennial");

    String result = normalizer.normalize("P001001", context);
    assertEquals("total_population", result);
  }

  @Test void testNormalizeUnmappedVariableReturnsOriginal() {
    Map<String, String> context = new HashMap<String, String>();
    context.put("year", "2022");
    context.put("type", "acs");

    String result = normalizer.normalize("UNKNOWN_VAR_123", context);
    assertEquals("UNKNOWN_VAR_123", result);
  }

  @Test void testNormalizeNullReturnsNull() {
    Map<String, String> context = new HashMap<String, String>();
    String result = normalizer.normalize(null, context);
    assertEquals(null, result);
  }

  @Test void testNormalizeEmptyReturnsEmpty() {
    Map<String, String> context = new HashMap<String, String>();
    String result = normalizer.normalize("", context);
    assertEquals("", result);
  }

  @Test void testNormalizeWithDefaultType() {
    // No type in context should use default "acs"
    Map<String, String> context = new HashMap<String, String>();
    context.put("year", "2022");

    String result = normalizer.normalize("B01001_001E", context);
    assertEquals("total_population", result);
  }

  @Test void testNormalizeFallsBackToOtherTypes() {
    // Request a decennial variable but with type=acs - should still find it
    Map<String, String> context = new HashMap<String, String>();
    context.put("year", "2020");
    context.put("type", "acs");

    // P1_001N is only in decennial, but fallback should find it
    String result = normalizer.normalize("P1_001N", context);
    assertEquals("total_population", result);
  }

  @Test void testNormalizeMedianIncome() {
    Map<String, String> context = new HashMap<String, String>();
    context.put("year", "2023");
    context.put("type", "acs");

    String result = normalizer.normalize("B19013_001E", context);
    assertEquals("median_income", result);
  }

  @Test void testNormalizeTotalHousing() {
    Map<String, String> context = new HashMap<String, String>();
    context.put("year", "2022");
    context.put("type", "acs");

    String result = normalizer.normalize("B25001_001E", context);
    assertEquals("total_housing", result);
  }

  @Test void testConstructorWithSingleArg() {
    MappingFileVariableNormalizer single =
        new MappingFileVariableNormalizer("etl-test/test-variable-mappings.json");
    assertNotNull(single);

    Map<String, String> context = new HashMap<String, String>();
    context.put("year", "2022");
    String result = single.normalize("B01001_001E", context);
    assertEquals("total_population", result);
  }

  @Test void testConstructorWithMapConfig() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("mappingFile", "etl-test/test-variable-mappings.json");
    config.put("defaultType", "decennial");

    MappingFileVariableNormalizer mapNormalizer =
        new MappingFileVariableNormalizer(config);
    assertNotNull(mapNormalizer);

    Map<String, String> context = new HashMap<String, String>();
    context.put("year", "2020");
    String result = mapNormalizer.normalize("P1_001N", context);
    assertEquals("total_population", result);
  }

  @Test void testConstructorWithMapConfigNoMappingFile() {
    Map<String, Object> config = new HashMap<String, Object>();
    assertThrows(IllegalArgumentException.class,
        () -> new MappingFileVariableNormalizer(config));
  }

  @Test void testConstructorWithMapConfigDefaultType() {
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("mappingFile", "etl-test/test-variable-mappings.json");
    // No defaultType specified - should use "acs"

    MappingFileVariableNormalizer mapNormalizer =
        new MappingFileVariableNormalizer(config);

    Map<String, String> context = new HashMap<String, String>();
    context.put("year", "2022");
    String result = mapNormalizer.normalize("B01001_001E", context);
    assertEquals("total_population", result);
  }

  @Test void testConstructorWithNonExistentFile() {
    assertThrows(RuntimeException.class,
        () -> new MappingFileVariableNormalizer("nonexistent-file.json"));
  }

  @Test void testNormalizeWithEmptyYear() {
    Map<String, String> context = new HashMap<String, String>();
    context.put("type", "acs");
    // No year - should still match allYears

    String result = normalizer.normalize("B01001_001E", context);
    assertEquals("total_population", result);
  }
}
