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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DimensionConfig}.
 */
@Tag("unit")
class DimensionConfigTest {

  @Test void testBuilderRange() {
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2024)
        .step(1)
        .build();

    assertEquals("year", config.getName());
    assertEquals(DimensionType.RANGE, config.getType());
    assertEquals(Integer.valueOf(2020), config.getStart());
    assertEquals(Integer.valueOf(2024), config.getEnd());
    assertEquals(Integer.valueOf(1), config.getStep());
  }

  @Test void testBuilderList() {
    DimensionConfig config = DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("NORTH", "SOUTH", "EAST"))
        .build();

    assertEquals("region", config.getName());
    assertEquals(DimensionType.LIST, config.getType());
    assertEquals(3, config.getValues().size());
    assertEquals("NORTH", config.getValues().get(0));
  }

  @Test void testBuilderQuery() {
    DimensionConfig config = DimensionConfig.builder()
        .name("region")
        .type(DimensionType.QUERY)
        .sql("SELECT DISTINCT region FROM regions")
        .build();

    assertEquals(DimensionType.QUERY, config.getType());
    assertEquals("SELECT DISTINCT region FROM regions", config.getSql());
  }

  @Test void testBuilderYearRange() {
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2020)
        .end(null)
        .dataLag(1)
        .releaseMonth(9)
        .build();

    assertEquals(DimensionType.YEAR_RANGE, config.getType());
    assertEquals(Integer.valueOf(2020), config.getStart());
    assertNull(config.getEnd());
    assertEquals(Integer.valueOf(1), config.getDataLag());
    assertEquals(Integer.valueOf(9), config.getReleaseMonth());
  }

  @Test void testBuilderJsonCatalog() {
    DimensionConfig config = DimensionConfig.builder()
        .name("country")
        .type(DimensionType.JSON_CATALOG)
        .source("/worldbank/countries.json")
        .path("countryGroups.G20.countries")
        .build();

    assertEquals(DimensionType.JSON_CATALOG, config.getType());
    assertEquals("/worldbank/countries.json", config.getSource());
    assertEquals("countryGroups.G20.countries", config.getPath());
  }

  @Test void testBuilderCustomWithProperties() {
    DimensionConfig config = DimensionConfig.builder()
        .name("series_id")
        .type(DimensionType.CUSTOM)
        .property("contextKey", "state_abbr")
        .property("referenceDirectory", "/data/ref")
        .build();

    assertEquals(DimensionType.CUSTOM, config.getType());
    assertEquals(2, config.getProperties().size());
    assertEquals("state_abbr", config.getProperties().get("contextKey"));
  }

  @Test void testBuilderNullNameThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        DimensionConfig.builder().name(null).build());
  }

  @Test void testBuilderEmptyNameThrows() {
    assertThrows(IllegalArgumentException.class, () ->
        DimensionConfig.builder().name("").build());
  }

  @Test void testDefaultType() {
    DimensionConfig config = DimensionConfig.builder()
        .name("dim")
        .build();
    assertEquals(DimensionType.LIST, config.getType());
  }

  @Test void testDefaultStep() {
    DimensionConfig config = DimensionConfig.builder()
        .name("dim")
        .build();
    assertEquals(Integer.valueOf(1), config.getStep());
  }

  @Test void testDefaultDataLag() {
    DimensionConfig config = DimensionConfig.builder()
        .name("dim")
        .build();
    assertEquals(Integer.valueOf(0), config.getDataLag());
  }

  @Test void testExcludeYears() {
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.YEAR_RANGE)
        .start(2010)
        .end(2020)
        .excludeYears(Arrays.asList(2011, 2015))
        .build();

    assertEquals(2, config.getExcludeYears().size());
    assertTrue(config.getExcludeYears().contains(2011));
    assertTrue(config.getExcludeYears().contains(2015));
  }

  @Test void testValuesAreImmutable() {
    List<String> values = new ArrayList<String>(Arrays.asList("A", "B"));
    DimensionConfig config = DimensionConfig.builder()
        .name("dim")
        .values(values)
        .build();

    assertThrows(UnsupportedOperationException.class, () ->
        config.getValues().add("C"));
  }

  @Test void testPropertiesAreImmutable() {
    Map<String, String> props = new HashMap<String, String>();
    props.put("key", "value");
    DimensionConfig config = DimensionConfig.builder()
        .name("dim")
        .properties(props)
        .build();

    assertThrows(UnsupportedOperationException.class, () ->
        config.getProperties().put("new", "value"));
  }

  @Test void testGetPropertyWithEnvResolution() {
    DimensionConfig config = DimensionConfig.builder()
        .name("dim")
        .property("plainKey", "plainValue")
        .build();

    assertEquals("plainValue", config.getProperty("plainKey"));
  }

  @Test void testGetPropertyNull() {
    DimensionConfig config = DimensionConfig.builder()
        .name("dim")
        .build();

    assertNull(config.getProperty("missing"));
  }

  @Test void testGetPropertyWithDefault() {
    DimensionConfig config = DimensionConfig.builder()
        .name("dim")
        .build();

    assertEquals("default", config.getProperty("missing", "default"));
  }

  @Test void testGetPropertyExistingOverridesDefault() {
    DimensionConfig config = DimensionConfig.builder()
        .name("dim")
        .property("key", "value")
        .build();

    assertEquals("value", config.getProperty("key", "default"));
  }

  @Test void testFromMapRange() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "range");
    map.put("start", 2020);
    map.put("end", 2024);
    map.put("step", 2);

    DimensionConfig config = DimensionConfig.fromMap("year", map);
    assertNotNull(config);
    assertEquals("year", config.getName());
    assertEquals(DimensionType.RANGE, config.getType());
    assertEquals(Integer.valueOf(2020), config.getStart());
    assertEquals(Integer.valueOf(2024), config.getEnd());
    assertEquals(Integer.valueOf(2), config.getStep());
  }

  @Test void testFromMapList() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "list");
    map.put("values", Arrays.asList("A", "B", "C"));

    DimensionConfig config = DimensionConfig.fromMap("region", map);
    assertNotNull(config);
    assertEquals(DimensionType.LIST, config.getType());
    assertEquals(3, config.getValues().size());
  }

  @Test void testFromMapYearRange() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "yearRange");
    map.put("start", 2020);
    map.put("end", "current");
    map.put("dataLag", 1);
    map.put("releaseMonth", 9);

    DimensionConfig config = DimensionConfig.fromMap("year", map);
    assertNotNull(config);
    assertEquals(DimensionType.YEAR_RANGE, config.getType());
    assertEquals(Integer.valueOf(2020), config.getStart());
    assertNull(config.getEnd());
    assertEquals(Integer.valueOf(1), config.getDataLag());
    assertEquals(Integer.valueOf(9), config.getReleaseMonth());
  }

  @Test void testFromMapDataLag() {
    // 'dataLag' is the canonical key; the legacy 'lagYears' alias was removed.
    Map<String, Object> canonical = new HashMap<String, Object>();
    canonical.put("type", "yearRange");
    canonical.put("start", 2020);
    canonical.put("dataLag", 2);
    assertEquals(Integer.valueOf(2), DimensionConfig.fromMap("year", canonical).getDataLag());

    Map<String, Object> legacy = new HashMap<String, Object>();
    legacy.put("type", "yearRange");
    legacy.put("start", 2020);
    legacy.put("lagYears", 2);
    assertEquals(Integer.valueOf(0), DimensionConfig.fromMap("year", legacy).getDataLag());
  }

  @Test void testFromMapQuery() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "query");
    map.put("sql", "SELECT DISTINCT x FROM t");

    DimensionConfig config = DimensionConfig.fromMap("dim", map);
    assertNotNull(config);
    assertEquals(DimensionType.QUERY, config.getType());
    assertEquals("SELECT DISTINCT x FROM t", config.getSql());
  }

  @Test void testFromMapJsonCatalog() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "json_catalog");
    map.put("source", "/data/catalog.json");
    map.put("path", "items[*].code");

    DimensionConfig config = DimensionConfig.fromMap("item", map);
    assertNotNull(config);
    assertEquals(DimensionType.JSON_CATALOG, config.getType());
    assertEquals("/data/catalog.json", config.getSource());
    assertEquals("items[*].code", config.getPath());
  }

  @Test void testFromMapWithExcludeYears() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "yearRange");
    map.put("start", 2010);
    map.put("excludeYears", Arrays.asList(2011, 2015));

    DimensionConfig config = DimensionConfig.fromMap("year", map);
    assertNotNull(config);
    assertEquals(2, config.getExcludeYears().size());
  }

  @Test void testFromMapWithProperties() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "custom");
    Map<String, Object> props = new HashMap<String, Object>();
    props.put("contextKey", "state_abbr");
    props.put("pattern", "*.csv");
    map.put("properties", props);

    DimensionConfig config = DimensionConfig.fromMap("ori", map);
    assertNotNull(config);
    assertEquals(DimensionType.CUSTOM, config.getType());
    assertEquals("state_abbr", config.getProperties().get("contextKey"));
  }

  @Test void testFromMapNull() {
    assertNull(DimensionConfig.fromMap("name", null));
  }

  @Test void testFromDimensionsMapEmpty() {
    Map<String, DimensionConfig> result =
        DimensionConfig.fromDimensionsMap(null);
    assertTrue(result.isEmpty());

    result = DimensionConfig.fromDimensionsMap(Collections.<String, Object>emptyMap());
    assertTrue(result.isEmpty());
  }

  @Test void testFromDimensionsMapListShorthand() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("region", Arrays.asList("NORTH", "SOUTH"));

    Map<String, DimensionConfig> result = DimensionConfig.fromDimensionsMap(map);
    assertEquals(1, result.size());
    DimensionConfig config = result.get("region");
    assertNotNull(config);
    assertEquals(DimensionType.LIST, config.getType());
    assertEquals(2, config.getValues().size());
  }

  @Test void testFromDimensionsMapStringShorthand() {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    map.put("frequency", "annual");

    Map<String, DimensionConfig> result = DimensionConfig.fromDimensionsMap(map);
    assertEquals(1, result.size());
    DimensionConfig config = result.get("frequency");
    assertNotNull(config);
    assertEquals(DimensionType.LIST, config.getType());
    assertEquals(1, config.getValues().size());
    assertEquals("annual", config.getValues().get(0));
  }

  @Test void testToString() {
    DimensionConfig config = DimensionConfig.builder()
        .name("year")
        .type(DimensionType.RANGE)
        .start(2020)
        .end(2024)
        .build();

    String str = config.toString();
    assertTrue(str.contains("year"));
    assertTrue(str.contains("RANGE"));
    assertTrue(str.contains("2020"));
    assertTrue(str.contains("2024"));
  }

  @Test void testToStringList() {
    DimensionConfig config = DimensionConfig.builder()
        .name("region")
        .type(DimensionType.LIST)
        .values(Arrays.asList("A", "B"))
        .build();

    String str = config.toString();
    assertTrue(str.contains("LIST"));
    assertTrue(str.contains("[A, B]"));
  }

  @Test void testToStringJsonCatalog() {
    DimensionConfig config = DimensionConfig.builder()
        .name("item")
        .type(DimensionType.JSON_CATALOG)
        .source("/catalog.json")
        .path("items")
        .build();

    String str = config.toString();
    assertTrue(str.contains("JSON_CATALOG"));
    assertTrue(str.contains("/catalog.json"));
  }
}
