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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Normalizes TIGER shapefile field names across census vintages.
 *
 * <h2>Problem</h2>
 * <p>TIGER shapefiles have field name changes between census vintages:
 * <ul>
 *   <li>2010 census: ZCTA5CE10, UACE10, PUMACE10, VTD10, etc.</li>
 *   <li>2020 census: ZCTA5CE20, UACE20, PUMACE20, VTD20, etc.</li>
 * </ul>
 *
 * <h2>Solution</h2>
 * <p>This normalizer maps vintage-specific field names to canonical (unsuffixed) names,
 * using year context to determine the expected vintage. This provides:
 * <ul>
 *   <li>Explicit documentation of all field mappings</li>
 *   <li>Year-aware field resolution (prefers expected vintage)</li>
 *   <li>Graceful fallback with logging for unexpected schemas</li>
 *   <li>Centralized maintenance of field mappings</li>
 * </ul>
 *
 * <h2>Affected Tables</h2>
 * <table border="1">
 *   <tr><th>Table</th><th>2010 Fields</th><th>2020 Fields</th><th>Canonical</th></tr>
 *   <tr><td>zctas</td><td>ZCTA5CE10</td><td>ZCTA5CE20</td><td>zcta</td></tr>
 *   <tr><td>urban_areas</td><td>UACE10, NAME10, UATYP10</td><td>UACE20, NAME20, UATYP20</td>
 *       <td>uace, name, urban_type</td></tr>
 *   <tr><td>pumas</td><td>PUMACE10, STATEFP10, NAMELSAD10</td><td>PUMACE20, STATEFP20, NAMELSAD20</td>
 *       <td>puma_code, state_fips, puma_name</td></tr>
 *   <tr><td>voting_districts</td><td>GEOID10, VTD10, STATEFP10, COUNTYFP10, NAME10</td>
 *       <td>GEOID20, VTD20, STATEFP20, COUNTYFP20, NAME20</td>
 *       <td>vtd_code, state_fips, county_fips, vtd_name</td></tr>
 * </table>
 *
 * <h2>Usage</h2>
 * <pre>
 * TigerFieldNormalizer normalizer = TigerFieldNormalizer.forTable("zctas", 2020);
 * String canonicalValue = normalizer.getField(feature, "zcta");
 * </pre>
 *
 * @see TigerDataProvider
 */
public class TigerFieldNormalizer {
  private static final Logger LOGGER = LoggerFactory.getLogger(TigerFieldNormalizer.class);

  /**
   * Census vintage years that have distinct field naming conventions.
   */
  public static final int VINTAGE_2010 = 2010;
  public static final int VINTAGE_2020 = 2020;

  /**
   * Suffix patterns for each vintage.
   */
  private static final String SUFFIX_2010 = "10";
  private static final String SUFFIX_2020 = "20";

  /**
   * Field mapping configuration for each table.
   * Maps canonical name -> list of vintage-specific names (in priority order).
   */
  private static final Map<String, TableFieldConfig> TABLE_CONFIGS;

  static {
    Map<String, TableFieldConfig> configs = new HashMap<String, TableFieldConfig>();

    // ================================================================================
    // ZCTAS (ZIP Code Tabulation Areas)
    // ================================================================================
    // 2010: tl_2010_us_zcta510.zip contains ZCTA5CE10
    // 2020: tl_2020_us_zcta520.zip contains ZCTA5CE20, GEOID20
    // ================================================================================
    TableFieldConfig zctas = new TableFieldConfig("zctas");
    zctas.addField("zcta",
        Arrays.asList("ZCTA5CE20", "ZCTA5CE10", "GEOID20", "GEOID"),
        "5-digit ZIP Code Tabulation Area code");
    zctas.addField("land_area",
        Arrays.asList("ALAND20", "ALAND"),
        "Land area in square meters");
    zctas.addField("water_area",
        Arrays.asList("AWATER20", "AWATER"),
        "Water area in square meters");
    configs.put("zctas", zctas);

    // ================================================================================
    // URBAN AREAS
    // ================================================================================
    // 2010: UACE10, NAME10, UATYP10 (U=Urbanized Area, C=Urban Cluster)
    // 2020: UACE20, NAME20, UATYP20 (legacy field, may be blank for 2020+ data)
    // Note: 2020 Census eliminated distinction between UA and UC
    // ================================================================================
    TableFieldConfig urbanAreas = new TableFieldConfig("urban_areas");
    urbanAreas.addField("uace",
        Arrays.asList("UACE20", "UACE10", "GEOID"),
        "Urban area code");
    urbanAreas.addField("name",
        Arrays.asList("NAME20", "NAME10", "NAME"),
        "Urban area name");
    urbanAreas.addField("urban_type",
        Arrays.asList("UATYP20", "UATYP10"),
        "Urban type code (U=Urbanized Area, C=Urban Cluster; may be blank for 2020+)");
    urbanAreas.addField("land_area",
        Arrays.asList("ALAND20", "ALAND"),
        "Land area in square meters");
    urbanAreas.addField("water_area",
        Arrays.asList("AWATER20", "AWATER"),
        "Water area in square meters");
    configs.put("urban_areas", urbanAreas);

    // ================================================================================
    // PUMAS (Public Use Microdata Areas)
    // ================================================================================
    // 2010: PUMACE10, GEOID10, STATEFP10, NAMELSAD10
    // 2020: PUMACE20, GEOID20, STATEFP20, NAMELSAD20
    // ================================================================================
    TableFieldConfig pumas = new TableFieldConfig("pumas");
    pumas.addField("puma_code",
        Arrays.asList("GEOID20", "GEOID10", "GEOID"),
        "PUMA geographic identifier");
    pumas.addField("state_fips",
        Arrays.asList("STATEFP20", "STATEFP10", "STATEFP"),
        "2-digit state FIPS code");
    pumas.addField("puma_name",
        Arrays.asList("NAMELSAD20", "NAMELSAD10", "NAMELSAD"),
        "PUMA name with legal/statistical area description");
    pumas.addField("land_area",
        Arrays.asList("ALAND20", "ALAND"),
        "Land area in square meters");
    pumas.addField("water_area",
        Arrays.asList("AWATER20", "AWATER"),
        "Water area in square meters");
    configs.put("pumas", pumas);

    // ================================================================================
    // VOTING DISTRICTS
    // ================================================================================
    // 2010 (available in TIGER2012): GEOID10, VTD10, STATEFP10, COUNTYFP10, NAME10
    // 2020 (available in TIGER2020PL): GEOID20, VTD20, STATEFP20, COUNTYFP20, NAME20
    // Note: Different URL patterns between vintages (handled by TigerDataProvider)
    // ================================================================================
    TableFieldConfig votingDistricts = new TableFieldConfig("voting_districts");
    votingDistricts.addField("vtd_code",
        Arrays.asList("GEOID20", "GEOID10", "GEOID"),
        "Voting district code (state + county + VTD)");
    votingDistricts.addField("state_fips",
        Arrays.asList("STATEFP20", "STATEFP10", "STATEFP"),
        "2-digit state FIPS code");
    votingDistricts.addField("county_fips",
        Arrays.asList("COUNTYFP20", "COUNTYFP10", "COUNTYFP"),
        "3-digit county FIPS code (within state)");
    votingDistricts.addField("vtd_name",
        Arrays.asList("NAME20", "NAME10", "NAME"),
        "Voting district name");
    votingDistricts.addField("land_area",
        Arrays.asList("ALAND20", "ALAND10", "ALAND"),
        "Land area in square meters");
    votingDistricts.addField("water_area",
        Arrays.asList("AWATER20", "AWATER10", "AWATER"),
        "Water area in square meters");
    configs.put("voting_districts", votingDistricts);

    TABLE_CONFIGS = Collections.unmodifiableMap(configs);
  }

  private final String tableName;
  private final int year;
  private final int expectedVintage;
  private final TableFieldConfig config;
  private final Map<String, String> resolvedFields;

  /**
   * Creates a field normalizer for a specific table and year.
   *
   * @param tableName The TIGER table name (e.g., "zctas", "urban_areas")
   * @param year      The data year (used to determine expected vintage)
   */
  public TigerFieldNormalizer(String tableName, int year) {
    this.tableName = tableName;
    this.year = year;
    this.expectedVintage = determineVintage(year);
    this.config = TABLE_CONFIGS.get(tableName);
    this.resolvedFields = new HashMap<String, String>();

    if (config == null) {
      LOGGER.debug("No field normalization config for table '{}' - using pass-through", tableName);
    }
  }

  /**
   * Factory method to create a normalizer for a table.
   *
   * @param tableName The TIGER table name
   * @param year      The data year
   * @return A configured normalizer
   */
  public static TigerFieldNormalizer forTable(String tableName, int year) {
    return new TigerFieldNormalizer(tableName, year);
  }

  /**
   * Determines the census vintage based on the data year.
   *
   * <p>TIGER data years map to census vintages as follows:
   * <ul>
   *   <li>2010-2019: 2010 census vintage (suffix 10)</li>
   *   <li>2020+: 2020 census vintage (suffix 20)</li>
   * </ul>
   *
   * @param year The data year
   * @return The expected census vintage (2010 or 2020)
   */
  public static int determineVintage(int year) {
    if (year < 2020) {
      return VINTAGE_2010;
    }
    return VINTAGE_2020;
  }

  /**
   * Gets the expected field suffix for a census vintage.
   *
   * @param vintage The census vintage (2010 or 2020)
   * @return The field suffix ("10" or "20")
   */
  public static String getSuffixForVintage(int vintage) {
    if (vintage == VINTAGE_2010) {
      return SUFFIX_2010;
    }
    return SUFFIX_2020;
  }

  /**
   * Checks if this table has vintage-specific field naming.
   *
   * @return true if the table has field normalization rules
   */
  public boolean hasNormalization() {
    return config != null;
  }

  /**
   * Gets the expected vintage for this normalizer's year.
   *
   * @return The census vintage (2010 or 2020)
   */
  public int getExpectedVintage() {
    return expectedVintage;
  }

  /**
   * Resolves a canonical field name to a value from a shapefile feature.
   *
   * <p>Tries fields in priority order:
   * <ol>
   *   <li>Field matching expected vintage (based on year)</li>
   *   <li>Alternate vintage fields (with warning)</li>
   *   <li>Unversioned field name</li>
   * </ol>
   *
   * @param feature       The shapefile feature
   * @param canonicalName The canonical field name (e.g., "zcta", "uace")
   * @return The field value, or null if not found
   */
  public String getStringField(TigerShapefileParser.ShapefileFeature feature, String canonicalName) {
    if (config == null) {
      // No normalization config - use canonical name directly
      Object value = feature.getAttribute(canonicalName.toUpperCase());
      return value != null ? value.toString().trim() : null;
    }

    FieldMapping mapping = config.getMapping(canonicalName);
    if (mapping == null) {
      // Unknown canonical name - try direct lookup
      Object value = feature.getAttribute(canonicalName.toUpperCase());
      return value != null ? value.toString().trim() : null;
    }

    // Build priority order based on expected vintage
    List<String> prioritizedNames = buildPriorityOrder(mapping.sourceNames);

    // Try each source field in priority order
    for (String sourceName : prioritizedNames) {
      Object value = feature.getAttribute(sourceName);
      if (value != null) {
        String strValue = value.toString().trim();
        if (!strValue.isEmpty()) {
          // Track which field was actually resolved
          if (!resolvedFields.containsKey(canonicalName)) {
            resolvedFields.put(canonicalName, sourceName);

            // Log warning if we used a non-preferred field
            if (!isExpectedVintageField(sourceName)) {
              LOGGER.info("Field '{}' for table '{}' year {} resolved from '{}' "
                  + "(expected {} vintage field)",
                  canonicalName, tableName, year, sourceName,
                  expectedVintage);
            }
          }
          return strValue;
        }
      }
    }

    // Not found in any source field
    LOGGER.debug("Field '{}' not found in feature for table '{}' year {}. Tried: {}",
        canonicalName, tableName, year, prioritizedNames);
    return null;
  }

  /**
   * Resolves a canonical field name to a Double value.
   *
   * @param feature       The shapefile feature
   * @param canonicalName The canonical field name
   * @return The field value as Double, or null if not found or not numeric
   */
  public Double getDoubleField(TigerShapefileParser.ShapefileFeature feature, String canonicalName) {
    if (config == null) {
      // No normalization config - try direct lookup
      Object value = feature.getAttribute(canonicalName.toUpperCase());
      return parseDouble(value);
    }

    FieldMapping mapping = config.getMapping(canonicalName);
    if (mapping == null) {
      Object value = feature.getAttribute(canonicalName.toUpperCase());
      return parseDouble(value);
    }

    List<String> prioritizedNames = buildPriorityOrder(mapping.sourceNames);

    for (String sourceName : prioritizedNames) {
      Object value = feature.getAttribute(sourceName);
      if (value != null) {
        Double doubleValue = parseDouble(value);
        if (doubleValue != null) {
          if (!resolvedFields.containsKey(canonicalName)) {
            resolvedFields.put(canonicalName, sourceName);
            if (!isExpectedVintageField(sourceName)) {
              LOGGER.info("Field '{}' for table '{}' year {} resolved from '{}' "
                  + "(expected {} vintage field)",
                  canonicalName, tableName, year, sourceName,
                  expectedVintage);
            }
          }
          return doubleValue;
        }
      }
    }

    return null;
  }

  /**
   * Gets the mapping of canonical names to resolved source fields.
   *
   * <p>Useful for debugging and logging which actual shapefile fields
   * were used for each canonical name.
   *
   * @return Map of canonical name to resolved source field name
   */
  public Map<String, String> getResolvedFields() {
    return Collections.unmodifiableMap(new HashMap<String, String>(resolvedFields));
  }

  /**
   * Gets the list of canonical field names for a table.
   *
   * @param tableName The table name
   * @return List of canonical field names, or empty list if table not configured
   */
  public static List<String> getCanonicalFields(String tableName) {
    TableFieldConfig config = TABLE_CONFIGS.get(tableName);
    if (config == null) {
      return Collections.emptyList();
    }
    return new ArrayList<String>(config.mappings.keySet());
  }

  /**
   * Gets the source field names (all vintages) for a canonical field.
   *
   * @param tableName     The table name
   * @param canonicalName The canonical field name
   * @return List of source field names, or empty list if not found
   */
  public static List<String> getSourceFields(String tableName, String canonicalName) {
    TableFieldConfig config = TABLE_CONFIGS.get(tableName);
    if (config == null) {
      return Collections.emptyList();
    }
    FieldMapping mapping = config.getMapping(canonicalName);
    if (mapping == null) {
      return Collections.emptyList();
    }
    return new ArrayList<String>(mapping.sourceNames);
  }

  /**
   * Checks if a table has vintage-specific field normalization.
   *
   * @param tableName The table name
   * @return true if the table has normalization configuration
   */
  public static boolean hasTableConfig(String tableName) {
    return TABLE_CONFIGS.containsKey(tableName);
  }

  /**
   * Gets all table names that have field normalization configured.
   *
   * @return List of table names with normalization
   */
  public static List<String> getConfiguredTables() {
    return new ArrayList<String>(TABLE_CONFIGS.keySet());
  }

  // ========== Private Helper Methods ==========

  /**
   * Builds priority order of source fields based on expected vintage.
   */
  private List<String> buildPriorityOrder(List<String> sourceNames) {
    List<String> expected = new ArrayList<String>();
    List<String> fallback = new ArrayList<String>();
    List<String> unversioned = new ArrayList<String>();

    String expectedSuffix = getSuffixForVintage(expectedVintage);

    for (String name : sourceNames) {
      if (name.endsWith(expectedSuffix)) {
        expected.add(name);
      } else if (name.endsWith(SUFFIX_2010) || name.endsWith(SUFFIX_2020)) {
        fallback.add(name);
      } else {
        unversioned.add(name);
      }
    }

    // Build final order: expected vintage first, then fallbacks, then unversioned
    List<String> result = new ArrayList<String>();
    result.addAll(expected);
    result.addAll(fallback);
    result.addAll(unversioned);
    return result;
  }

  /**
   * Checks if a field name matches the expected vintage.
   */
  private boolean isExpectedVintageField(String fieldName) {
    String expectedSuffix = getSuffixForVintage(expectedVintage);
    return fieldName.endsWith(expectedSuffix)
        || (!fieldName.endsWith(SUFFIX_2010) && !fieldName.endsWith(SUFFIX_2020));
  }

  /**
   * Parses an object value as a Double.
   */
  private static Double parseDouble(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    }
    try {
      String strValue = value.toString().trim();
      if (strValue.isEmpty()) {
        return null;
      }
      return Double.parseDouble(strValue);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  // ========== Configuration Classes ==========

  /**
   * Configuration for a single table's field mappings.
   */
  private static class TableFieldConfig {
    final String tableName;
    final Map<String, FieldMapping> mappings;

    TableFieldConfig(String tableName) {
      this.tableName = tableName;
      this.mappings = new LinkedHashMap<String, FieldMapping>();
    }

    void addField(String canonicalName, List<String> sourceNames, String description) {
      mappings.put(canonicalName, new FieldMapping(canonicalName, sourceNames, description));
    }

    FieldMapping getMapping(String canonicalName) {
      return mappings.get(canonicalName);
    }
  }

  /**
   * Mapping from a canonical field name to source field names.
   */
  private static class FieldMapping {
    final String canonicalName;
    final List<String> sourceNames;
    final String description;

    FieldMapping(String canonicalName, List<String> sourceNames, String description) {
      this.canonicalName = canonicalName;
      this.sourceNames = Collections.unmodifiableList(new ArrayList<String>(sourceNames));
      this.description = description;
    }
  }

  /**
   * Generates documentation of all field mappings.
   *
   * @return Formatted documentation string
   */
  public static String generateDocumentation() {
    StringBuilder sb = new StringBuilder();
    sb.append("TIGER Field Normalization Configuration\n");
    sb.append("========================================\n\n");

    for (Map.Entry<String, TableFieldConfig> entry : TABLE_CONFIGS.entrySet()) {
      TableFieldConfig config = entry.getValue();
      sb.append("Table: ").append(config.tableName).append("\n");
      sb.append("----------------------------------------").append("\n");

      for (FieldMapping mapping : config.mappings.values()) {
        sb.append(String.format("  %-15s <- %s%n",
            mapping.canonicalName,
            mapping.sourceNames));
        sb.append(String.format("  %-15s    %s%n", "", mapping.description));
      }
      sb.append("\n");
    }

    return sb.toString();
  }
}
