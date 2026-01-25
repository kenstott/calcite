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
package org.apache.calcite.adapter.govdata.geo;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TigerFieldNormalizer.
 *
 * <p>Verifies that vintage-specific field names are correctly resolved to canonical names.
 */
@Tag("unit")
class TigerFieldNormalizerTest {

  // ========== Vintage Detection Tests ==========

  @Test
  void testVintageDetection2010() {
    assertEquals(TigerFieldNormalizer.VINTAGE_2010,
        TigerFieldNormalizer.determineVintage(2010));
    assertEquals(TigerFieldNormalizer.VINTAGE_2010,
        TigerFieldNormalizer.determineVintage(2015));
    assertEquals(TigerFieldNormalizer.VINTAGE_2010,
        TigerFieldNormalizer.determineVintage(2019));
  }

  @Test
  void testVintageDetection2020() {
    assertEquals(TigerFieldNormalizer.VINTAGE_2020,
        TigerFieldNormalizer.determineVintage(2020));
    assertEquals(TigerFieldNormalizer.VINTAGE_2020,
        TigerFieldNormalizer.determineVintage(2023));
    assertEquals(TigerFieldNormalizer.VINTAGE_2020,
        TigerFieldNormalizer.determineVintage(2025));
  }

  @Test
  void testSuffixForVintage() {
    assertEquals("10", TigerFieldNormalizer.getSuffixForVintage(TigerFieldNormalizer.VINTAGE_2010));
    assertEquals("20", TigerFieldNormalizer.getSuffixForVintage(TigerFieldNormalizer.VINTAGE_2020));
  }

  // ========== Configuration Tests ==========

  @Test
  void testHasTableConfig() {
    assertTrue(TigerFieldNormalizer.hasTableConfig("zctas"));
    assertTrue(TigerFieldNormalizer.hasTableConfig("urban_areas"));
    assertTrue(TigerFieldNormalizer.hasTableConfig("pumas"));
    assertTrue(TigerFieldNormalizer.hasTableConfig("voting_districts"));
    
    // Tables without vintage-specific normalization
    assertFalse(TigerFieldNormalizer.hasTableConfig("states"));
    assertFalse(TigerFieldNormalizer.hasTableConfig("counties"));
    assertFalse(TigerFieldNormalizer.hasTableConfig("census_tracts"));
  }

  @Test
  void testGetConfiguredTables() {
    List<String> tables = TigerFieldNormalizer.getConfiguredTables();
    assertEquals(4, tables.size());
    assertTrue(tables.contains("zctas"));
    assertTrue(tables.contains("urban_areas"));
    assertTrue(tables.contains("pumas"));
    assertTrue(tables.contains("voting_districts"));
  }

  @Test
  void testGetCanonicalFields() {
    List<String> zctaFields = TigerFieldNormalizer.getCanonicalFields("zctas");
    assertTrue(zctaFields.contains("zcta"));
    assertTrue(zctaFields.contains("land_area"));
    assertTrue(zctaFields.contains("water_area"));
    
    List<String> urbanFields = TigerFieldNormalizer.getCanonicalFields("urban_areas");
    assertTrue(urbanFields.contains("uace"));
    assertTrue(urbanFields.contains("name"));
    assertTrue(urbanFields.contains("urban_type"));
    
    List<String> vtdFields = TigerFieldNormalizer.getCanonicalFields("voting_districts");
    assertTrue(vtdFields.contains("vtd_code"));
    assertTrue(vtdFields.contains("state_fips"));
    assertTrue(vtdFields.contains("county_fips"));
    assertTrue(vtdFields.contains("vtd_name"));
  }

  @Test
  void testGetSourceFields() {
    List<String> sources = TigerFieldNormalizer.getSourceFields("zctas", "zcta");
    assertTrue(sources.contains("ZCTA5CE20"));
    assertTrue(sources.contains("ZCTA5CE10"));
    assertTrue(sources.contains("GEOID20"));
    assertTrue(sources.contains("GEOID"));
    
    // Non-existent field
    List<String> empty = TigerFieldNormalizer.getSourceFields("zctas", "nonexistent");
    assertTrue(empty.isEmpty());
    
    // Non-existent table
    List<String> noTable = TigerFieldNormalizer.getSourceFields("nonexistent", "field");
    assertTrue(noTable.isEmpty());
  }

  // ========== ZCTAS Field Resolution Tests ==========

  @Test
  void testZctasNormalizer2020Vintage() {
    TigerFieldNormalizer normalizer = TigerFieldNormalizer.forTable("zctas", 2020);
    assertEquals(TigerFieldNormalizer.VINTAGE_2020, normalizer.getExpectedVintage());
    assertTrue(normalizer.hasNormalization());
    
    // 2020 vintage feature with ZCTA5CE20
    TigerShapefileParser.ShapefileFeature feature2020 = createFeature(
        "ZCTA5CE20", "90210",
        "ALAND20", 12345678.0,
        "AWATER20", 567890.0
    );
    
    assertEquals("90210", normalizer.getStringField(feature2020, "zcta"));
    assertEquals(12345678.0, normalizer.getDoubleField(feature2020, "land_area"));
    assertEquals(567890.0, normalizer.getDoubleField(feature2020, "water_area"));
  }

  @Test
  void testZctasNormalizer2010Vintage() {
    TigerFieldNormalizer normalizer = TigerFieldNormalizer.forTable("zctas", 2010);
    assertEquals(TigerFieldNormalizer.VINTAGE_2010, normalizer.getExpectedVintage());
    
    // 2010 vintage feature with ZCTA5CE10
    TigerShapefileParser.ShapefileFeature feature2010 = createFeature(
        "ZCTA5CE10", "90210",
        "ALAND", 12345678.0,
        "AWATER", 567890.0
    );
    
    assertEquals("90210", normalizer.getStringField(feature2010, "zcta"));
    assertEquals(12345678.0, normalizer.getDoubleField(feature2010, "land_area"));
    assertEquals(567890.0, normalizer.getDoubleField(feature2010, "water_area"));
  }

  @Test
  void testZctasFallbackWhenExpectedFieldMissing() {
    // Using 2020 normalizer but feature has 2010 field names
    TigerFieldNormalizer normalizer = TigerFieldNormalizer.forTable("zctas", 2020);
    
    TigerShapefileParser.ShapefileFeature feature2010 = createFeature(
        "ZCTA5CE10", "90210",
        "ALAND", 12345678.0
    );
    
    // Should still resolve via fallback chain
    assertEquals("90210", normalizer.getStringField(feature2010, "zcta"));
    assertEquals(12345678.0, normalizer.getDoubleField(feature2010, "land_area"));
  }

  // ========== Urban Areas Field Resolution Tests ==========

  @Test
  void testUrbanAreasNormalizer2020() {
    TigerFieldNormalizer normalizer = TigerFieldNormalizer.forTable("urban_areas", 2020);
    
    TigerShapefileParser.ShapefileFeature feature = createFeature(
        "UACE20", "12345",
        "NAME20", "Los Angeles--Long Beach--Anaheim, CA",
        "UATYP20", "U",
        "ALAND20", 987654321.0,
        "AWATER20", 123456.0
    );
    
    assertEquals("12345", normalizer.getStringField(feature, "uace"));
    assertEquals("Los Angeles--Long Beach--Anaheim, CA", normalizer.getStringField(feature, "name"));
    assertEquals("U", normalizer.getStringField(feature, "urban_type"));
    assertEquals(987654321.0, normalizer.getDoubleField(feature, "land_area"));
  }

  @Test
  void testUrbanAreasNormalizer2010() {
    TigerFieldNormalizer normalizer = TigerFieldNormalizer.forTable("urban_areas", 2015);
    assertEquals(TigerFieldNormalizer.VINTAGE_2010, normalizer.getExpectedVintage());
    
    TigerShapefileParser.ShapefileFeature feature = createFeature(
        "UACE10", "12345",
        "NAME10", "Los Angeles--Long Beach--Anaheim, CA",
        "UATYP10", "U",
        "ALAND", 987654321.0
    );
    
    assertEquals("12345", normalizer.getStringField(feature, "uace"));
    assertEquals("Los Angeles--Long Beach--Anaheim, CA", normalizer.getStringField(feature, "name"));
    assertEquals("U", normalizer.getStringField(feature, "urban_type"));
  }

  // ========== PUMAs Field Resolution Tests ==========

  @Test
  void testPumasNormalizer2020() {
    TigerFieldNormalizer normalizer = TigerFieldNormalizer.forTable("pumas", 2022);
    
    TigerShapefileParser.ShapefileFeature feature = createFeature(
        "GEOID20", "0600101",
        "STATEFP20", "06",
        "NAMELSAD20", "San Francisco County (North)--Marina PUMA",
        "ALAND20", 123456789.0
    );
    
    assertEquals("0600101", normalizer.getStringField(feature, "puma_code"));
    assertEquals("06", normalizer.getStringField(feature, "state_fips"));
    assertEquals("San Francisco County (North)--Marina PUMA", 
        normalizer.getStringField(feature, "puma_name"));
  }

  @Test
  void testPumasNormalizer2010() {
    TigerFieldNormalizer normalizer = TigerFieldNormalizer.forTable("pumas", 2012);
    assertEquals(TigerFieldNormalizer.VINTAGE_2010, normalizer.getExpectedVintage());
    
    TigerShapefileParser.ShapefileFeature feature = createFeature(
        "GEOID10", "0600101",
        "STATEFP10", "06",
        "NAMELSAD10", "San Francisco County PUMA"
    );
    
    assertEquals("0600101", normalizer.getStringField(feature, "puma_code"));
    assertEquals("06", normalizer.getStringField(feature, "state_fips"));
  }

  // ========== Voting Districts Field Resolution Tests ==========

  @Test
  void testVotingDistrictsNormalizer2020() {
    TigerFieldNormalizer normalizer = TigerFieldNormalizer.forTable("voting_districts", 2020);
    
    TigerShapefileParser.ShapefileFeature feature = createFeature(
        "GEOID20", "060370001",
        "STATEFP20", "06",
        "COUNTYFP20", "037",
        "NAME20", "Precinct 0001",
        "ALAND20", 5000000.0,
        "AWATER20", 100000.0
    );
    
    assertEquals("060370001", normalizer.getStringField(feature, "vtd_code"));
    assertEquals("06", normalizer.getStringField(feature, "state_fips"));
    assertEquals("037", normalizer.getStringField(feature, "county_fips"));
    assertEquals("Precinct 0001", normalizer.getStringField(feature, "vtd_name"));
    assertEquals(5000000.0, normalizer.getDoubleField(feature, "land_area"));
    assertEquals(100000.0, normalizer.getDoubleField(feature, "water_area"));
  }

  @Test
  void testVotingDistrictsNormalizer2012() {
    // 2012 data uses 2010 census vintage
    TigerFieldNormalizer normalizer = TigerFieldNormalizer.forTable("voting_districts", 2012);
    assertEquals(TigerFieldNormalizer.VINTAGE_2010, normalizer.getExpectedVintage());
    
    TigerShapefileParser.ShapefileFeature feature = createFeature(
        "GEOID10", "060370001",
        "STATEFP10", "06",
        "COUNTYFP10", "037",
        "NAME10", "Precinct 0001",
        "ALAND10", 5000000.0
    );
    
    assertEquals("060370001", normalizer.getStringField(feature, "vtd_code"));
    assertEquals("06", normalizer.getStringField(feature, "state_fips"));
    assertEquals("037", normalizer.getStringField(feature, "county_fips"));
    assertEquals("Precinct 0001", normalizer.getStringField(feature, "vtd_name"));
    assertEquals(5000000.0, normalizer.getDoubleField(feature, "land_area"));
  }

  // ========== Edge Cases ==========

  @Test
  void testNullFieldValue() {
    TigerFieldNormalizer normalizer = TigerFieldNormalizer.forTable("zctas", 2020);
    
    // Feature with missing ZCTA field
    TigerShapefileParser.ShapefileFeature feature = createFeature(
        "ALAND20", 12345678.0
    );
    
    assertNull(normalizer.getStringField(feature, "zcta"));
    assertEquals(12345678.0, normalizer.getDoubleField(feature, "land_area"));
    assertNull(normalizer.getDoubleField(feature, "water_area"));
  }

  @Test
  void testEmptyStringField() {
    TigerFieldNormalizer normalizer = TigerFieldNormalizer.forTable("zctas", 2020);
    
    TigerShapefileParser.ShapefileFeature feature = createFeature(
        "ZCTA5CE20", "",
        "ZCTA5CE10", "90210"  // Fallback should be used
    );
    
    // Empty string should fall through to next in chain
    assertEquals("90210", normalizer.getStringField(feature, "zcta"));
  }

  @Test
  void testNonNumericDoubleField() {
    TigerFieldNormalizer normalizer = TigerFieldNormalizer.forTable("zctas", 2020);
    
    TigerShapefileParser.ShapefileFeature feature = createFeature(
        "ALAND20", "not-a-number"
    );
    
    assertNull(normalizer.getDoubleField(feature, "land_area"));
  }

  @Test
  void testNormalizerForUnconfiguredTable() {
    TigerFieldNormalizer normalizer = TigerFieldNormalizer.forTable("states", 2020);
    assertFalse(normalizer.hasNormalization());
    
    // Should use direct field lookup
    TigerShapefileParser.ShapefileFeature feature = createFeature(
        "STATEFP", "06",
        "NAME", "California"
    );
    
    assertEquals("06", normalizer.getStringField(feature, "STATEFP"));
    assertEquals("California", normalizer.getStringField(feature, "NAME"));
  }

  @Test
  void testResolvedFieldsTracking() {
    TigerFieldNormalizer normalizer = TigerFieldNormalizer.forTable("zctas", 2020);
    
    TigerShapefileParser.ShapefileFeature feature = createFeature(
        "ZCTA5CE20", "90210",
        "ALAND20", 12345678.0
    );
    
    normalizer.getStringField(feature, "zcta");
    normalizer.getDoubleField(feature, "land_area");
    
    Map<String, String> resolved = normalizer.getResolvedFields();
    assertEquals("ZCTA5CE20", resolved.get("zcta"));
    assertEquals("ALAND20", resolved.get("land_area"));
  }

  @Test
  void testGenerateDocumentation() {
    String doc = TigerFieldNormalizer.generateDocumentation();
    assertNotNull(doc);
    assertTrue(doc.contains("TIGER Field Normalization"));
    assertTrue(doc.contains("zctas"));
    assertTrue(doc.contains("urban_areas"));
    assertTrue(doc.contains("pumas"));
    assertTrue(doc.contains("voting_districts"));
  }

  // ========== Helper Methods ==========

  private TigerShapefileParser.ShapefileFeature createFeature(Object... keyValuePairs) {
    Map<String, Object> attributes = new HashMap<String, Object>();
    for (int i = 0; i < keyValuePairs.length; i += 2) {
      attributes.put((String) keyValuePairs[i], keyValuePairs[i + 1]);
    }
    return new TigerShapefileParser.ShapefileFeature(attributes);
  }
}
