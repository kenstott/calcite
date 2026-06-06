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

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link VariableNormalizer} interface default methods.
 */
@Tag("unit")
class VariableNormalizerTest {

  /** Test implementation that returns the input unchanged. */
  private static final VariableNormalizer IDENTITY = new VariableNormalizer() {
    @Override public String normalize(String apiVariable, Map<String, String> context) {
      return apiVariable;
    }
  };

  @Test void testShouldPreserveNull() {
    assertTrue(IDENTITY.shouldPreserve(null));
  }

  @Test void testShouldPreserveName() {
    assertTrue(IDENTITY.shouldPreserve("name"));
    assertTrue(IDENTITY.shouldPreserve("NAME"));
  }

  @Test void testShouldPreserveState() {
    assertTrue(IDENTITY.shouldPreserve("state"));
    assertTrue(IDENTITY.shouldPreserve("STATE"));
  }

  @Test void testShouldPreserveCounty() {
    assertTrue(IDENTITY.shouldPreserve("county"));
    assertTrue(IDENTITY.shouldPreserve("COUNTY"));
  }

  @Test void testShouldPreserveTract() {
    assertTrue(IDENTITY.shouldPreserve("tract"));
    assertTrue(IDENTITY.shouldPreserve("TRACT"));
  }

  @Test void testShouldPreservePlace() {
    assertTrue(IDENTITY.shouldPreserve("place"));
    assertTrue(IDENTITY.shouldPreserve("PLACE"));
  }

  @Test void testShouldPreserveFips() {
    assertTrue(IDENTITY.shouldPreserve("state_fips"));
    assertTrue(IDENTITY.shouldPreserve("FIPS_CODE"));
    assertTrue(IDENTITY.shouldPreserve("county_fips"));
  }

  @Test void testShouldPreserveGeoId() {
    assertTrue(IDENTITY.shouldPreserve("geoid"));
    assertTrue(IDENTITY.shouldPreserve("GEOID"));
    assertTrue(IDENTITY.shouldPreserve("census_geoid"));
  }

  @Test void testShouldNotPreserveDataColumns() {
    assertFalse(IDENTITY.shouldPreserve("B01001_001E"));
    assertFalse(IDENTITY.shouldPreserve("total_population"));
    assertFalse(IDENTITY.shouldPreserve("value"));
    assertFalse(IDENTITY.shouldPreserve("data_value"));
  }

  @Test void testNormalizeIdentity() {
    String result =
        IDENTITY.normalize("B01001_001E", Collections.singletonMap("year", "2020"));
    // Identity normalizer returns the same value
    assertTrue("B01001_001E".equals(result));
  }
}
