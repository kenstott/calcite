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
package org.apache.calcite.adapter.govdata.lands;

import org.apache.calcite.adapter.file.etl.DimensionConfig;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link UsfsTimberLayerResolver}.
 */
@Tag("unit")
class UsfsTimberLayerResolverTest {

  private final UsfsTimberLayerResolver resolver = new UsfsTimberLayerResolver();

  @Test void layerForYearMapsModernEras() {
    assertEquals("11", UsfsTimberLayerResolver.layerForYear(2026));
    assertEquals("11", UsfsTimberLayerResolver.layerForYear(2021));
    assertEquals("0", UsfsTimberLayerResolver.layerForYear(2020));
    assertEquals("0", UsfsTimberLayerResolver.layerForYear(2011));
    assertEquals("1", UsfsTimberLayerResolver.layerForYear(2010));
    assertEquals("1", UsfsTimberLayerResolver.layerForYear(2001));
  }

  @Test void layerForYearMapsHistoricalEras() {
    assertEquals("2", UsfsTimberLayerResolver.layerForYear(2000));
    assertEquals("2", UsfsTimberLayerResolver.layerForYear(1991));
    assertEquals("3", UsfsTimberLayerResolver.layerForYear(1990));
    assertEquals("3", UsfsTimberLayerResolver.layerForYear(1981));
    assertEquals("4", UsfsTimberLayerResolver.layerForYear(1980));
    assertEquals("4", UsfsTimberLayerResolver.layerForYear(1971));
    assertEquals("5", UsfsTimberLayerResolver.layerForYear(1970));
    assertEquals("5", UsfsTimberLayerResolver.layerForYear(1956));
    assertEquals("6", UsfsTimberLayerResolver.layerForYear(1955));
    assertEquals("6", UsfsTimberLayerResolver.layerForYear(1946));
    assertEquals("7", UsfsTimberLayerResolver.layerForYear(1945));
    assertEquals("7", UsfsTimberLayerResolver.layerForYear(1820));
  }

  @Test void layerForYearRejectsPre1820() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> UsfsTimberLayerResolver.layerForYear(1819));
    assertTrue(ex.getMessage().contains("1819"));
  }

  @Test void resolveReturnsLayerForYearInContext() {
    Map<String, String> ctx = new HashMap<>();
    ctx.put("year", "2024");
    List<String> out = resolver.resolve("layer", null, ctx, null);
    assertEquals(Collections.singletonList("11"), out);
  }

  @Test void resolveReturnsEmptyForOtherDimensionNames() {
    Map<String, String> ctx = new HashMap<>();
    ctx.put("year", "2024");
    assertEquals(Collections.emptyList(),
        resolver.resolve("year", null, ctx, null));
    assertEquals(Collections.emptyList(),
        resolver.resolve("ori", null, ctx, null));
  }

  @Test void resolveThrowsWhenYearMissingFromContext() {
    Map<String, String> ctx = new HashMap<>();
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> resolver.resolve("layer", null, ctx, null));
    assertTrue(ex.getMessage().contains("year"));
  }

  @Test void resolveThrowsWhenYearNotNumeric() {
    Map<String, String> ctx = new HashMap<>();
    ctx.put("year", "not-a-year");
    IllegalStateException ex = assertThrows(IllegalStateException.class,
        () -> resolver.resolve("layer", null, ctx, null));
    assertTrue(ex.getMessage().contains("not-a-year"));
  }

  /** Smoke-check: DimensionConfig argument is unused by this resolver, so null is safe. */
  @Test void resolveIgnoresDimensionConfig() {
    Map<String, String> ctx = new HashMap<>();
    ctx.put("year", "2015");
    DimensionConfig nullConfig = null;
    assertEquals(Collections.singletonList("0"),
        resolver.resolve("layer", nullConfig, ctx, null));
  }
}
