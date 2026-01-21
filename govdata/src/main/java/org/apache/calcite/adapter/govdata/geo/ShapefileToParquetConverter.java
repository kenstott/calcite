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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts TIGER/Line shapefiles to Parquet format using StorageProvider pattern.
 *
 * Since shapefiles consist of multiple files (.shp, .dbf, .shx, etc.),
 * this converter focuses on extracting attribute data from the .dbf file
 * and converting it to Parquet. Geometry is stored as WKT strings for now.
 */
public class ShapefileToParquetConverter extends AbstractGeoDataDownloader {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShapefileToParquetConverter.class);

  /**
   * Constructor that requires a StorageProvider for writing parquet files.
   */
  public ShapefileToParquetConverter(org.apache.calcite.adapter.file.storage.StorageProvider storageProvider) {
    // Note: ShapefileToParquetConverter doesn't use cache or HTTP features, so we pass nulls/empty strings
    // Only storageProvider is needed for writing parquet files
    // Use placeholder years (2020, 2024) since this is just for conversion and doesn't use year ranges
    super("", storageProvider, storageProvider, 2020, 2024);
  }

  @Override protected long getMinRequestIntervalMs() {
    return 0; // No rate limit for shapefile conversion (local processing)
  }

  @Override protected int getMaxRetries() {
    return 0; // No retries needed for shapefile conversion (no network calls)
  }

  @Override protected long getRetryDelayMs() {
    return 0; // No retry delay needed
  }

  /**
   * Convert shapefiles in a directory to Parquet format using StorageProvider.
   */
  public void convertShapefilesToParquet(File sourceDir, String targetRelativePath) throws IOException {
    if (!sourceDir.exists() || !sourceDir.isDirectory()) {
      LOGGER.warn("Source directory does not exist: {}", sourceDir);
      return;
    }

    // Process each year directory
    File[] yearDirs = sourceDir.listFiles(f -> f.isDirectory() && f.getName().startsWith("year="));
    if (yearDirs == null || yearDirs.length == 0) {
      LOGGER.info("No year directories found in {}", sourceDir);
      return;
    }

    for (File yearDir : yearDirs) {
      String year = yearDir.getName().substring(5); // Remove "year=" prefix
      String yearRelativePath = targetRelativePath + "/" + yearDir.getName();

      // Convert states shapefile
      convertStatesShapefile(yearDir, yearRelativePath, year);

      // Convert counties shapefile
      convertCountiesShapefile(yearDir, yearRelativePath, year);

      // Convert places shapefile
      convertPlacesShapefile(yearDir, yearRelativePath, year);

      // Convert ZCTAs shapefile
      convertZctasShapefile(yearDir, yearRelativePath, year);

      // Convert census tracts shapefile
      convertCensusTractsShapefile(yearDir, yearRelativePath, year);

      // Convert block groups shapefile
      convertBlockGroupsShapefile(yearDir, yearRelativePath, year);

      // Convert CBSA shapefile
      convertCbsaShapefile(yearDir, yearRelativePath, year);
    }
  }

  @SuppressWarnings("deprecation")
  private void convertStatesShapefile(File yearDir, String targetRelativePath, String year) {
    try {
      File statesDir = new File(yearDir, "states");
      if (!statesDir.exists()) {
        LOGGER.debug("No states directory found in {}", yearDir);
        return;
      }

      // Create schema with TIGER state attributes
      Schema schema = SchemaBuilder.record("State")
          .fields()
          .name("state_fips").doc("2-digit FIPS code identifying the state (e.g., '06' for California)").type().stringType().noDefault()
          .name("state_code").doc("Geographic identifier (GEOID) for the state, matches state_fips").type().stringType().noDefault()
          .name("state_name").doc("Full state name (e.g., 'California')").type().stringType().noDefault()
          .name("state_abbr").doc("2-letter postal abbreviation (e.g., 'CA')").type().nullable().stringType().noDefault()
          .name("land_area").doc("Land area in square meters").type().nullable().doubleType().noDefault()
          .name("water_area").doc("Water area in square meters").type().nullable().doubleType().noDefault()
          .name("geometry").doc("WKT representation of state boundary polygon").type().nullable().stringType().noDefault()
          .endRecord();

      String outputRelativePath = targetRelativePath + "/states.parquet";

      // Use TigerShapefileParser to parse real shapefile data
      String expectedPrefix = "tl_" + year + "_us_state";
      List<Object[]> statesData = TigerShapefileParser.parseShapefile(statesDir, expectedPrefix, feature -> {
        String stateFips = TigerShapefileParser.getStringAttribute(feature, "STATEFP");
        String stateCode = TigerShapefileParser.getStringAttribute(feature, "GEOID");
        String stateName = TigerShapefileParser.getStringAttribute(feature, "NAME");
        String stateAbbr = TigerShapefileParser.getStringAttribute(feature, "STUSPS");
        Double landArea = TigerShapefileParser.getDoubleAttribute(feature, "ALAND");
        Double waterArea = TigerShapefileParser.getDoubleAttribute(feature, "AWATER");

        return new Object[]{
            stateFips,   // state_fips
            stateCode,   // state_code
            stateName,   // state_name
            stateAbbr,   // state_abbr
            landArea,    // land_area
            waterArea,   // water_area
            TigerShapefileParser.getGeometryAttribute(feature)  // geometry from .shp file
        };
      });

      // Convert data to GenericRecord list
      List<GenericRecord> records = new ArrayList<>();
      for (Object[] stateData : statesData) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("state_fips", stateData[0]);
        record.put("state_code", stateData[1]);
        record.put("state_name", stateData[2]);
        record.put("state_abbr", stateData[3]);
        record.put("land_area", stateData[4]);
        record.put("water_area", stateData[5]);
        record.put("geometry", stateData[6]);
        records.add(record);
      }

      // Write using StorageProvider pattern
      storageProvider.writeAvroParquet(outputRelativePath, schema, records, schema.getName());

      LOGGER.info("Created states parquet file: {} with {} records from real TIGER data",
          outputRelativePath, statesData.size());

    } catch (Exception e) {
      LOGGER.error("Error converting states shapefile", e);
    }
  }

  @SuppressWarnings("deprecation")
  private void convertCountiesShapefile(File yearDir, String targetRelativePath, String year) {
    try {
      File countiesDir = new File(yearDir, "counties");
      if (!countiesDir.exists()) {
        LOGGER.debug("No counties directory found in {}", yearDir);
        return;
      }

      // Create schema for counties
      Schema schema = SchemaBuilder.record("County")
          .fields()
          .name("county_fips").doc("5-digit FIPS code for county (state + county, e.g., '06037' for Los Angeles County)").type().stringType().noDefault()
          .name("state_fips").doc("2-digit FIPS code of the parent state").type().stringType().noDefault()
          .name("county_name").doc("Full county name (e.g., 'Los Angeles County')").type().stringType().noDefault()
          .name("county_code").doc("Geographic identifier (GEOID) for the county").type().nullable().stringType().noDefault()
          .name("land_area").doc("Land area in square meters").type().nullable().doubleType().noDefault()
          .name("water_area").doc("Water area in square meters").type().nullable().doubleType().noDefault()
          .name("geometry").doc("WKT representation of county boundary polygon").type().nullable().stringType().noDefault()
          .endRecord();

      String outputRelativePath = targetRelativePath + "/counties.parquet";

      // Use TigerShapefileParser to parse real shapefile data
      String expectedPrefix = "tl_" + year + "_us_county";
      List<Object[]> countiesData = TigerShapefileParser.parseShapefile(countiesDir, expectedPrefix, feature -> {
        String countyFips = TigerShapefileParser.getStringAttribute(feature, "GEOID");
        String stateFips = TigerShapefileParser.getStringAttribute(feature, "STATEFP");
        String countyName = TigerShapefileParser.getStringAttribute(feature, "NAME");
        String countyCode = TigerShapefileParser.getStringAttribute(feature, "COUNTYFP");
        Double landArea = TigerShapefileParser.getDoubleAttribute(feature, "ALAND");
        Double waterArea = TigerShapefileParser.getDoubleAttribute(feature, "AWATER");

        return new Object[]{
            countyFips,  // county_fips
            stateFips,   // state_fips
            countyName,  // county_name
            countyCode,  // county_code
            landArea,    // land_area
            waterArea,   // water_area
            TigerShapefileParser.getGeometryAttribute(feature)  // geometry from .shp file
        };
      });

      // Convert data to GenericRecord list
      List<GenericRecord> records = new ArrayList<>();
      for (Object[] countyData : countiesData) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("county_fips", countyData[0]);
        record.put("state_fips", countyData[1]);
        record.put("county_name", countyData[2]);
        record.put("county_code", countyData[3]);
        record.put("land_area", countyData[4]);
        record.put("water_area", countyData[5]);
        record.put("geometry", countyData[6]);
        records.add(record);
      }

      // Write using StorageProvider pattern
      storageProvider.writeAvroParquet(outputRelativePath, schema, records, schema.getName());

      LOGGER.info("Created counties parquet file: {} with {} records from real TIGER data",
          outputRelativePath, countiesData.size());

    } catch (Exception e) {
      LOGGER.error("Error converting counties shapefile", e);
    }
  }

  @SuppressWarnings("deprecation")
  private void convertPlacesShapefile(File yearDir, String targetRelativePath, String year) {
    try {
      File placesDir = new File(yearDir, "places");
      if (!placesDir.exists()) {
        LOGGER.debug("No places directory found in {}", yearDir);
        return;
      }

      // Create schema for places
      Schema schema = SchemaBuilder.record("Place")
          .fields()
          .name("place_fips").doc("7-digit FIPS code for place (state + place code, e.g., '0644000' for Los Angeles city)").type().stringType().noDefault()
          .name("state_fips").doc("2-digit FIPS code of the parent state").type().stringType().noDefault()
          .name("place_name").doc("Name of incorporated place or census-designated place").type().stringType().noDefault()
          .name("place_type").doc("Classification of place (e.g., 'city', 'town', 'CDP')").type().nullable().stringType().noDefault()
          .name("geometry").doc("WKT representation of place boundary polygon").type().nullable().stringType().noDefault()
          .endRecord();

      String outputRelativePath = targetRelativePath + "/places.parquet";

      // Process places for each state
      List<Object[]> allPlacesData = new ArrayList<>();

      // Look for state directories within places
      File[] stateDirs = placesDir.listFiles(File::isDirectory);
      if (stateDirs != null) {
        for (File stateDir : stateDirs) {
          String stateFips = stateDir.getName();
          String expectedPrefix = "tl_" + year + "_" + stateFips + "_place";

          List<Object[]> statePlaces = TigerShapefileParser.parseShapefile(stateDir, expectedPrefix, feature -> {
            String placeFips = TigerShapefileParser.getStringAttribute(feature, "GEOID");
            String stateFipsAttr = TigerShapefileParser.getStringAttribute(feature, "STATEFP");
            String placeName = TigerShapefileParser.getStringAttribute(feature, "NAME");
            String placeType = TigerShapefileParser.getStringAttribute(feature, "CLASSFP");

            return new Object[]{
                placeFips,      // place_fips
                stateFipsAttr,  // state_fips
                placeName,      // place_name
                placeType,      // place_type
                TigerShapefileParser.getGeometryAttribute(feature)  // geometry from .shp file
            };
          });

          allPlacesData.addAll(statePlaces);
        }
      }

      // Convert data to GenericRecord list
      List<GenericRecord> records = new ArrayList<>();
      for (Object[] placeData : allPlacesData) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("place_fips", placeData[0]);
        record.put("state_fips", placeData[1]);
        record.put("place_name", placeData[2]);
        record.put("place_type", placeData[3]);
        record.put("geometry", placeData[4]);
        records.add(record);
      }

      // Write using StorageProvider pattern
      storageProvider.writeAvroParquet(outputRelativePath, schema, records, schema.getName());

      LOGGER.info("Created places parquet file: {} with {} records from real TIGER data",
          outputRelativePath, allPlacesData.size());

    } catch (Exception e) {
      LOGGER.error("Error converting places shapefile", e);
    }
  }

  @SuppressWarnings("deprecation")
  private void convertZctasShapefile(File yearDir, String targetRelativePath, String year) {
    try {
      File zctasDir = new File(yearDir, "zctas");
      if (!zctasDir.exists()) {
        LOGGER.debug("No zctas directory found in {}", yearDir);
        return;
      }

      // Create schema for ZCTAs
      Schema schema = SchemaBuilder.record("ZCTA")
          .fields()
          .name("zcta").doc("5-digit ZIP Code Tabulation Area code approximating USPS ZIP Code delivery areas").type().stringType().noDefault()
          .name("land_area").doc("Land area in square meters").type().nullable().doubleType().noDefault()
          .name("water_area").doc("Water area in square meters").type().nullable().doubleType().noDefault()
          .name("geometry").doc("WKT representation of ZCTA boundary polygon").type().nullable().stringType().noDefault()
          .endRecord();

      String outputRelativePath = targetRelativePath + "/zctas.parquet";

      // Use TigerShapefileParser to parse real ZCTA shapefile data
      String expectedPrefix = "tl_" + year + "_us_zcta520";
      List<Object[]> zctasData = TigerShapefileParser.parseShapefile(zctasDir, expectedPrefix, feature -> {
        // Try different attribute names used across ZCTA years
        String zcta5 = TigerShapefileParser.getStringAttribute(feature, "ZCTA5CE20");
        if (zcta5.isEmpty()) {
          zcta5 = TigerShapefileParser.getStringAttribute(feature, "ZCTA5CE10");
        }
        if (zcta5.isEmpty()) {
          zcta5 = TigerShapefileParser.getStringAttribute(feature, "ZCTA5CE");
        }

        // Try different land/water area attribute names
        Double landArea = TigerShapefileParser.getDoubleAttribute(feature, "ALAND20");
        if (landArea == 0.0) {
          landArea = TigerShapefileParser.getDoubleAttribute(feature, "ALAND10");
        }
        if (landArea == 0.0) {
          landArea = TigerShapefileParser.getDoubleAttribute(feature, "ALAND");
        }

        Double waterArea = TigerShapefileParser.getDoubleAttribute(feature, "AWATER20");
        if (waterArea == 0.0) {
          waterArea = TigerShapefileParser.getDoubleAttribute(feature, "AWATER10");
        }
        if (waterArea == 0.0) {
          waterArea = TigerShapefileParser.getDoubleAttribute(feature, "AWATER");
        }

        return new Object[]{
            zcta5,       // zcta
            landArea,    // land_area
            waterArea,   // water_area
            TigerShapefileParser.getGeometryAttribute(feature)  // geometry from .shp file
        };
      });

      // Convert data to GenericRecord list
      List<GenericRecord> records = new ArrayList<>();
      for (Object[] zctaData : zctasData) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("zcta", zctaData[0]);
        record.put("land_area", zctaData[1]);
        record.put("water_area", zctaData[2]);
        record.put("geometry", zctaData[3]);
        records.add(record);
      }

      // Write using StorageProvider pattern
      storageProvider.writeAvroParquet(outputRelativePath, schema, records, schema.getName());

      LOGGER.info("Created zctas parquet file: {} with {} records from real TIGER data",
          outputRelativePath, zctasData.size());

    } catch (Exception e) {
      LOGGER.error("Error converting zctas shapefile", e);
    }
  }

  @SuppressWarnings("deprecation")
  private void convertStatesCsvToParquet(File csvFile, String outputRelativePath) throws IOException {
    // If a CSV export is available, convert it to Parquet
    LOGGER.info("Converting CSV file {} to Parquet", csvFile);

    Schema schema = SchemaBuilder.record("State")
        .fields()
        .name("state_fips").doc("2-digit FIPS code identifying the state (e.g., '06' for California)").type().stringType().noDefault()
        .name("state_code").doc("Geographic identifier (GEOID) for the state, matches state_fips").type().stringType().noDefault()
        .name("state_name").doc("Full state name (e.g., 'California')").type().stringType().noDefault()
        .name("state_abbr").doc("2-letter postal abbreviation (e.g., 'CA')").type().nullable().stringType().noDefault()
        .name("land_area").doc("Land area in square meters").type().nullable().doubleType().noDefault()
        .name("water_area").doc("Water area in square meters").type().nullable().doubleType().noDefault()
        .name("geometry").doc("WKT representation of state boundary polygon").type().nullable().stringType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();

    try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
      String line = reader.readLine(); // Skip header
      int count = 0;

      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(",");
        if (parts.length >= 3) {
          GenericRecord record = new GenericData.Record(schema);
          record.put("state_fips", parts[0].trim());
          record.put("state_code", parts.length > 1 ? parts[1].trim() : "");
          record.put("state_name", parts.length > 2 ? parts[2].trim() : "");
          record.put("state_abbr", parts.length > 3 ? parts[3].trim() : null);
          record.put("land_area", null);
          record.put("water_area", null);
          record.put("geometry", null);
          records.add(record);
          count++;
        }
      }

      // Write using StorageProvider pattern
      storageProvider.writeAvroParquet(outputRelativePath, schema, records, schema.getName());
      LOGGER.info("Converted {} records from CSV to Parquet", count);
    }
  }

  @SuppressWarnings("deprecation")
  private void convertCensusTractsShapefile(File yearDir, String targetRelativePath, String year) {
    try {
      File tractsDir = new File(yearDir, "census_tracts");
      if (!tractsDir.exists()) {
        LOGGER.debug("No census_tracts directory found in {}", yearDir);
        return;
      }

      // Create schema for census tracts
      Schema schema = SchemaBuilder.record("CensusTract")
          .fields()
          .name("tract_fips").doc("11-digit FIPS code for census tract (state + county + tract)").type().stringType().noDefault()
          .name("state_fips").doc("2-digit FIPS code of the parent state").type().stringType().noDefault()
          .name("county_fips").doc("5-digit FIPS code of the parent county").type().stringType().noDefault()
          .name("tract_name").doc("Census tract number (e.g., '4201.02')").type().nullable().stringType().noDefault()
          .name("land_area").doc("Land area in square meters").type().nullable().doubleType().noDefault()
          .name("water_area").doc("Water area in square meters").type().nullable().doubleType().noDefault()
          .name("geometry").doc("WKT representation of census tract boundary polygon").type().nullable().stringType().noDefault()
          .endRecord();

      String outputRelativePath = targetRelativePath + "/census_tracts.parquet";

      // Process census tracts for each state
      List<Object[]> allTractsData = new ArrayList<>();

      File[] stateDirs = tractsDir.listFiles(File::isDirectory);
      if (stateDirs != null) {
        for (File stateDir : stateDirs) {
          String stateFips = stateDir.getName();
          String expectedPrefix = "tl_" + year + "_" + stateFips + "_tract";

          List<Object[]> stateTracts = TigerShapefileParser.parseShapefile(stateDir, expectedPrefix, feature -> {
            String tractFips = TigerShapefileParser.getStringAttribute(feature, "GEOID");
            String stateFipsAttr = TigerShapefileParser.getStringAttribute(feature, "STATEFP");
            String countyFips = TigerShapefileParser.getStringAttribute(feature, "COUNTYFP");
            String tractName = TigerShapefileParser.getStringAttribute(feature, "NAME");
            Double landArea = TigerShapefileParser.getDoubleAttribute(feature, "ALAND");
            Double waterArea = TigerShapefileParser.getDoubleAttribute(feature, "AWATER");

            return new Object[]{
                tractFips,      // tract_fips
                stateFipsAttr,  // state_fips
                countyFips,     // county_fips
                tractName,      // tract_name
                landArea,       // land_area
                waterArea,      // water_area
                TigerShapefileParser.getGeometryAttribute(feature) // geometry
            };
          });

          allTractsData.addAll(stateTracts);
        }
      }

      if (!allTractsData.isEmpty()) {
        List<GenericRecord> records = convertToGenericRecords(schema, allTractsData);
        storageProvider.writeAvroParquet(outputRelativePath, schema, records, schema.getName());
        LOGGER.info("Created census_tracts parquet file: {} with {} records from real TIGER data",
            outputRelativePath, records.size());
      }
    } catch (Exception e) {
      LOGGER.error("Error converting census_tracts shapefile", e);
    }
  }

  @SuppressWarnings("deprecation")
  private void convertBlockGroupsShapefile(File yearDir, String targetRelativePath, String year) {
    try {
      File blockGroupsDir = new File(yearDir, "block_groups");
      if (!blockGroupsDir.exists()) {
        LOGGER.debug("No block_groups directory found in {}", yearDir);
        return;
      }

      // Create schema for block groups
      Schema schema = SchemaBuilder.record("BlockGroup")
          .fields()
          .name("block_group_fips").doc("12-digit FIPS code for block group (state + county + tract + block group)").type().stringType().noDefault()
          .name("state_fips").doc("2-digit FIPS code of the parent state").type().stringType().noDefault()
          .name("county_fips").doc("5-digit FIPS code of the parent county").type().stringType().noDefault()
          .name("tract_fips").doc("11-digit FIPS code of the parent census tract").type().stringType().noDefault()
          .name("land_area").doc("Land area in square meters").type().nullable().doubleType().noDefault()
          .name("water_area").doc("Water area in square meters").type().nullable().doubleType().noDefault()
          .name("geometry").doc("WKT representation of block group boundary polygon").type().nullable().stringType().noDefault()
          .endRecord();

      String outputRelativePath = targetRelativePath + "/block_groups.parquet";

      // Process block groups for each state
      List<Object[]> allBlockGroupsData = new ArrayList<>();

      File[] stateDirs = blockGroupsDir.listFiles(File::isDirectory);
      if (stateDirs != null) {
        for (File stateDir : stateDirs) {
          String stateFips = stateDir.getName();
          String expectedPrefix = "tl_" + year + "_" + stateFips + "_bg";

          List<Object[]> stateBlockGroups = TigerShapefileParser.parseShapefile(stateDir, expectedPrefix, feature -> {
            String blockGroupFips = TigerShapefileParser.getStringAttribute(feature, "GEOID");
            String stateFipsAttr = TigerShapefileParser.getStringAttribute(feature, "STATEFP");
            String countyFp = TigerShapefileParser.getStringAttribute(feature, "COUNTYFP");
            String tractFips = TigerShapefileParser.getStringAttribute(feature, "TRACTCE");
            Double landArea = TigerShapefileParser.getDoubleAttribute(feature, "ALAND");
            Double waterArea = TigerShapefileParser.getDoubleAttribute(feature, "AWATER");

            // Construct 5-digit county FIPS code (state + county) to match other TIGER tables
            String countyFips = stateFipsAttr + countyFp;

            return new Object[]{
                blockGroupFips, // block_group_fips
                stateFipsAttr,  // state_fips
                countyFips,     // county_fips (5-digit: state + county)
                tractFips,      // tract_fips
                landArea,       // land_area
                waterArea,      // water_area
                TigerShapefileParser.getGeometryAttribute(feature) // geometry
            };
          });

          allBlockGroupsData.addAll(stateBlockGroups);
        }
      }

      if (!allBlockGroupsData.isEmpty()) {
        List<GenericRecord> records = convertToGenericRecords(schema, allBlockGroupsData);
        storageProvider.writeAvroParquet(outputRelativePath, schema, records, schema.getName());
        LOGGER.info("Created block_groups parquet file: {} with {} records from real TIGER data",
            outputRelativePath, records.size());
      }
    } catch (Exception e) {
      LOGGER.error("Error converting block_groups shapefile", e);
    }
  }

  @SuppressWarnings("deprecation")
  private void convertCbsaShapefile(File yearDir, String targetRelativePath, String year) {
    try {
      File cbsaDir = new File(yearDir, "cbsa");
      if (!cbsaDir.exists()) {
        LOGGER.debug("No cbsa directory found in {}", yearDir);
        return;
      }

      // Create schema for CBSA (Core Based Statistical Areas)
      Schema schema = SchemaBuilder.record("CBSA")
          .fields()
          .name("cbsa_fips").doc("5-digit CBSA code for Core Based Statistical Area").type().stringType().noDefault()
          .name("cbsa_name").doc("Name of metropolitan or micropolitan statistical area").type().nullable().stringType().noDefault()
          .name("metro_micro").doc("LSAD code indicating Metropolitan or Micropolitan designation").type().nullable().stringType().noDefault()
          .name("land_area").doc("Land area in square meters").type().nullable().doubleType().noDefault()
          .name("water_area").doc("Water area in square meters").type().nullable().doubleType().noDefault()
          .name("geometry").doc("WKT representation of CBSA boundary polygon").type().nullable().stringType().noDefault()
          .endRecord();

      String outputRelativePath = targetRelativePath + "/cbsa.parquet";
      String expectedPrefix = "tl_" + year + "_us_cbsa";

      List<Object[]> cbsaData = TigerShapefileParser.parseShapefile(cbsaDir, expectedPrefix, feature -> {
        String cbsaFips = TigerShapefileParser.getStringAttribute(feature, "GEOID");
        String cbsaName = TigerShapefileParser.getStringAttribute(feature, "NAME");
        String metroMicro = TigerShapefileParser.getStringAttribute(feature, "LSAD");
        Double landArea = TigerShapefileParser.getDoubleAttribute(feature, "ALAND");
        Double waterArea = TigerShapefileParser.getDoubleAttribute(feature, "AWATER");

        return new Object[]{
            cbsaFips,       // cbsa_fips
            cbsaName,       // cbsa_name
            metroMicro,     // metro_micro
            landArea,       // land_area
            waterArea,      // water_area
            TigerShapefileParser.getGeometryAttribute(feature) // geometry
        };
      });

      if (!cbsaData.isEmpty()) {
        List<GenericRecord> records = convertToGenericRecords(schema, cbsaData);
        storageProvider.writeAvroParquet(outputRelativePath, schema, records, schema.getName());
        LOGGER.info("Created cbsa parquet file: {} with {} records from real TIGER data",
            outputRelativePath, records.size());
      }
    } catch (Exception e) {
      LOGGER.error("Error converting cbsa shapefile", e);
    }
  }

  /**
   * Utility method to efficiently convert Object[] data to GenericRecord list.
   * This avoids repetitive manual record creation and is more maintainable.
   */
  private List<GenericRecord> convertToGenericRecords(Schema schema, List<Object[]> dataList) {
    List<GenericRecord> records = new ArrayList<>(dataList.size());

    for (Object[] data : dataList) {
      GenericRecord record = new GenericData.Record(schema);

      // Automatically map array elements to schema fields in order
      List<Schema.Field> fields = schema.getFields();
      for (int i = 0; i < fields.size() && i < data.length; i++) {
        record.put(fields.get(i).name(), data[i]);
      }

      records.add(record);
    }

    return records;
  }

  /**
   * Convert a single shapefile type to Parquet format.
   * This method is called by TigerDataDownloader for specific table types.
   */
  public void convertSingleShapefileType(File sourceDir, String targetFilePath, String tableType) throws IOException {
    LOGGER.info("Converting {} shapefile from {} to {}", tableType, sourceDir, targetFilePath);

    // Extract year from the source directory name if it contains "year="
    String year = null;
    if (sourceDir.getName().startsWith("year=")) {
      year = sourceDir.getName().substring(5);
    } else if (sourceDir.getParentFile() != null && sourceDir.getParentFile().getName().startsWith("year=")) {
      year = sourceDir.getParentFile().getName().substring(5);
    }

    if (year == null) {
      LOGGER.warn("Could not determine year from source directory: {}", sourceDir);
      return;
    }

    // Call the direct conversion method with the full target path
    // sourceDir is already the year directory (e.g., /path/year=2024)
    convertDirectToParquet(sourceDir, targetFilePath, tableType, year);
  }

  /**
   * Direct conversion method that writes to the exact target file path.
   */
  @SuppressWarnings("deprecation")
  private void convertDirectToParquet(File yearDir, String targetFilePath, String tableType, String year) {
    try {
      LOGGER.info("Direct conversion of {} to {}", tableType, targetFilePath);

      switch (tableType) {
        case "states":
          convertStatesDirectToParquet(yearDir, targetFilePath, year);
          break;
        case "counties":
          convertCountiesDirectToParquet(yearDir, targetFilePath, year);
          break;
        case "places":
          convertPlacesDirectToParquet(yearDir, targetFilePath, year);
          break;
        case "zctas":
          convertZctasDirectToParquet(yearDir, targetFilePath, year);
          break;
        case "census_tracts":
          convertCensusTractsDirectToParquet(yearDir, targetFilePath, year);
          break;
        case "block_groups":
          convertBlockGroupsDirectToParquet(yearDir, targetFilePath, year);
          break;
        case "cbsa":
          convertCbsaDirectToParquet(yearDir, targetFilePath, year);
          break;
        case "congressional_districts":
          convertCongressionalDistrictsDirectToParquet(yearDir, targetFilePath, year);
          break;
        case "school_districts":
          convertSchoolDistrictsDirectToParquet(yearDir, targetFilePath, year);
          break;
        default:
          LOGGER.warn("Unknown table type for shapefile conversion: {}", tableType);
      }
    } catch (Exception e) {
      LOGGER.error("Error in direct conversion of {} to {}", tableType, targetFilePath, e);
    }
  }

  @SuppressWarnings("deprecation")
  private void convertStatesDirectToParquet(File yearDir, String targetFilePath, String year) throws Exception {
    File statesDir = new File(yearDir, "states");
    if (!statesDir.exists()) {
      LOGGER.debug("No states directory found in {}", yearDir);
      return;
    }

    // Load schema metadata from geo-schema.json
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("states");

    String expectedPrefix = "tl_" + year + "_us_state";
    List<Object[]> statesDataArrays = TigerShapefileParser.parseShapefile(statesDir, expectedPrefix, feature -> {
      return new Object[]{
          TigerShapefileParser.getStringAttribute(feature, "STATEFP"),
          TigerShapefileParser.getStringAttribute(feature, "GEOID"),
          TigerShapefileParser.getStringAttribute(feature, "NAME"),
          TigerShapefileParser.getStringAttribute(feature, "STUSPS"),
          TigerShapefileParser.getDoubleAttribute(feature, "ALAND"),
          TigerShapefileParser.getDoubleAttribute(feature, "AWATER"),
          TigerShapefileParser.getGeometryAttribute(feature)
      };
    });

    if (!statesDataArrays.isEmpty()) {
      // Convert Object[] to Map<String, Object>
      java.util.List<java.util.Map<String, Object>> dataList = new java.util.ArrayList<>();
      for (Object[] arr : statesDataArrays) {
        java.util.Map<String, Object> record = new java.util.HashMap<>();
        record.put("state_fips", arr[0]);
        record.put("state_code", arr[1]);
        record.put("state_name", arr[2]);
        record.put("state_abbr", arr[3]);
        record.put("land_area", arr[4]);
        record.put("water_area", arr[5]);
        record.put("geometry", arr[6]);
        dataList.add(record);
      }
      storageProvider.writeAvroParquet(targetFilePath, columns, dataList, "State", "State");
      LOGGER.info("Created states parquet file: {} with {} records", targetFilePath, dataList.size());
    }
  }

  @SuppressWarnings("deprecation")
  private void convertCountiesDirectToParquet(File yearDir, String targetFilePath, String year) throws Exception {
    File countiesDir = new File(yearDir, "counties");
    if (!countiesDir.exists()) {
      LOGGER.debug("No counties directory found in {}", yearDir);
      return;
    }

    // Load schema metadata from geo-schema.json
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("counties");

    String expectedPrefix = "tl_" + year + "_us_county";
    List<Object[]> countiesDataArrays = TigerShapefileParser.parseShapefile(countiesDir, expectedPrefix, feature -> {
      return new Object[]{
          TigerShapefileParser.getStringAttribute(feature, "GEOID"),
          TigerShapefileParser.getStringAttribute(feature, "STATEFP"),
          TigerShapefileParser.getStringAttribute(feature, "NAME"),
          TigerShapefileParser.getStringAttribute(feature, "COUNTYFP"),
          TigerShapefileParser.getDoubleAttribute(feature, "ALAND"),
          TigerShapefileParser.getDoubleAttribute(feature, "AWATER"),
          TigerShapefileParser.getGeometryAttribute(feature)
      };
    });

    if (!countiesDataArrays.isEmpty()) {
      // Convert Object[] to Map<String, Object>
      java.util.List<java.util.Map<String, Object>> dataList = new java.util.ArrayList<>();
      for (Object[] arr : countiesDataArrays) {
        java.util.Map<String, Object> record = new java.util.HashMap<>();
        record.put("county_fips", arr[0]);
        record.put("state_fips", arr[1]);
        record.put("county_name", arr[2]);
        record.put("county_code", arr[3]);
        record.put("land_area", arr[4]);
        record.put("water_area", arr[5]);
        record.put("geometry", arr[6]);
        dataList.add(record);
      }
      storageProvider.writeAvroParquet(targetFilePath, columns, dataList, "County", "County");
      LOGGER.info("Created counties parquet file: {} with {} records", targetFilePath, dataList.size());
    }
  }

  @SuppressWarnings("deprecation")
  private void convertPlacesDirectToParquet(File yearDir, String targetFilePath, String year) throws Exception {
    File placesDir = new File(yearDir, "places");
    if (!placesDir.exists()) {
      LOGGER.debug("No places directory found in {}", yearDir);
      return;
    }

    // Load schema metadata from geo-schema.json
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("places");

    java.util.List<java.util.Map<String, Object>> allData = new java.util.ArrayList<>();

    // Process places from each state subdirectory
    File[] stateDirs = placesDir.listFiles(File::isDirectory);
    if (stateDirs != null) {
      for (File stateDir : stateDirs) {
        String stateFips = stateDir.getName();
        String expectedPrefix = "tl_" + year + "_" + stateFips + "_place";

        List<Object[]> placesDataArrays = TigerShapefileParser.parseShapefile(stateDir, expectedPrefix, feature -> {
          return new Object[]{
              TigerShapefileParser.getStringAttribute(feature, "GEOID"),
              TigerShapefileParser.getStringAttribute(feature, "STATEFP"),
              TigerShapefileParser.getStringAttribute(feature, "NAME"),
              TigerShapefileParser.getStringAttribute(feature, "LSAD"),
              TigerShapefileParser.getGeometryAttribute(feature)
          };
        });

        // Convert Object[] to Map<String, Object>
        for (Object[] arr : placesDataArrays) {
          java.util.Map<String, Object> record = new java.util.HashMap<>();
          record.put("place_fips", arr[0]);
          record.put("state_fips", arr[1]);
          record.put("place_name", arr[2]);
          record.put("place_type", arr[3]);
          record.put("geometry", arr[4]);
          allData.add(record);
        }
      }
    }

    if (!allData.isEmpty()) {
      storageProvider.writeAvroParquet(targetFilePath, columns, allData, "Place", "Place");
      LOGGER.info("Created places parquet file: {} with {} records", targetFilePath, allData.size());
    }
  }

  @SuppressWarnings("deprecation")
  private void convertZctasDirectToParquet(File yearDir, String targetFilePath, String year) throws Exception {
    File zctasDir = new File(yearDir, "zctas");
    if (!zctasDir.exists()) {
      LOGGER.debug("No zctas directory found in {}", yearDir);
      return;
    }

    // Load schema metadata from geo-schema.json
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("zctas");

    // 2010 uses different naming: zcta510 instead of zcta520, and field suffix "10" instead of "20"
    boolean is2010 = "2010".equals(year);
    String expectedPrefix = is2010 ? "tl_2010_us_zcta510" : "tl_" + year + "_us_zcta520";
    String zctaField = is2010 ? "ZCTA5CE10" : "ZCTA5CE20";
    String alandField = is2010 ? "ALAND10" : "ALAND20";
    String awaterField = is2010 ? "AWATER10" : "AWATER20";

    List<Object[]> zctasDataArrays = TigerShapefileParser.parseShapefile(zctasDir, expectedPrefix, feature -> {
      return new Object[]{
          TigerShapefileParser.getStringAttribute(feature, zctaField),
          TigerShapefileParser.getDoubleAttribute(feature, alandField),
          TigerShapefileParser.getDoubleAttribute(feature, awaterField),
          TigerShapefileParser.getGeometryAttribute(feature)
      };
    });

    if (!zctasDataArrays.isEmpty()) {
      // Convert Object[] to Map<String, Object>
      java.util.List<java.util.Map<String, Object>> dataList = new java.util.ArrayList<>();
      for (Object[] arr : zctasDataArrays) {
        java.util.Map<String, Object> record = new java.util.HashMap<>();
        record.put("zcta", arr[0]);
        record.put("land_area", arr[1]);
        record.put("water_area", arr[2]);
        record.put("geometry", arr[3]);
        dataList.add(record);
      }
      storageProvider.writeAvroParquet(targetFilePath, columns, dataList, "ZCTA", "ZCTA");
      LOGGER.info("Created zctas parquet file: {} with {} records", targetFilePath, dataList.size());
    }
  }

  @SuppressWarnings("deprecation")
  private void convertCensusTractsDirectToParquet(File yearDir, String targetFilePath, String year) throws Exception {
    File tractsDir = new File(yearDir, "census_tracts");
    if (!tractsDir.exists()) {
      LOGGER.debug("No census_tracts directory found in {}", yearDir);
      return;
    }

    // Load schema metadata from geo-schema.json
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("census_tracts");

    java.util.List<java.util.Map<String, Object>> allData = new java.util.ArrayList<>();

    // Process tracts from each state subdirectory
    File[] stateDirs = tractsDir.listFiles(File::isDirectory);
    if (stateDirs != null) {
      for (File stateDir : stateDirs) {
        String stateFips = stateDir.getName();
        String expectedPrefix = "tl_" + year + "_" + stateFips + "_tract";

        List<Object[]> tractsDataArrays = TigerShapefileParser.parseShapefile(stateDir, expectedPrefix, feature -> {
          String countyfp = TigerShapefileParser.getStringAttribute(feature, "COUNTYFP");
          return new Object[]{
              TigerShapefileParser.getStringAttribute(feature, "GEOID"),
              TigerShapefileParser.getStringAttribute(feature, "STATEFP"),
              countyfp != null ? stateFips + countyfp : null,
              TigerShapefileParser.getStringAttribute(feature, "NAME"),
              TigerShapefileParser.getDoubleAttribute(feature, "ALAND"),
              TigerShapefileParser.getDoubleAttribute(feature, "AWATER"),
              TigerShapefileParser.getGeometryAttribute(feature)
          };
        });

        // Convert Object[] to Map<String, Object>
        for (Object[] arr : tractsDataArrays) {
          java.util.Map<String, Object> record = new java.util.HashMap<>();
          record.put("tract_fips", arr[0]);
          record.put("state_fips", arr[1]);
          record.put("county_fips", arr[2]);
          record.put("tract_name", arr[3]);
          record.put("land_area", arr[4]);
          record.put("water_area", arr[5]);
          record.put("geometry", arr[6]);
          allData.add(record);
        }
      }
    }

    if (!allData.isEmpty()) {
      storageProvider.writeAvroParquet(targetFilePath, columns, allData, "CensusTract", "CensusTract");
      LOGGER.info("Created census_tracts parquet file: {} with {} records", targetFilePath, allData.size());
    }
  }

  @SuppressWarnings("deprecation")
  private void convertBlockGroupsDirectToParquet(File yearDir, String targetFilePath, String year) throws Exception {
    File blockGroupsDir = new File(yearDir, "block_groups");
    if (!blockGroupsDir.exists()) {
      LOGGER.debug("No block_groups directory found in {}", yearDir);
      return;
    }

    // Load schema metadata from geo-schema.json
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("block_groups");

    java.util.List<java.util.Map<String, Object>> allData = new java.util.ArrayList<>();

    // Process block groups from each state subdirectory
    File[] stateDirs = blockGroupsDir.listFiles(File::isDirectory);
    if (stateDirs != null) {
      for (File stateDir : stateDirs) {
        String stateFips = stateDir.getName();
        String expectedPrefix = "tl_" + year + "_" + stateFips + "_bg";

        List<Object[]> bgDataArrays = TigerShapefileParser.parseShapefile(stateDir, expectedPrefix, feature -> {
          String geoid = TigerShapefileParser.getStringAttribute(feature, "GEOID");
          String statefp = TigerShapefileParser.getStringAttribute(feature, "STATEFP");
          String countyfp = TigerShapefileParser.getStringAttribute(feature, "COUNTYFP");
          String tractce = TigerShapefileParser.getStringAttribute(feature, "TRACTCE");
          return new Object[]{
              geoid,
              statefp,
              countyfp != null ? statefp + countyfp : null,
              tractce,
              TigerShapefileParser.getDoubleAttribute(feature, "ALAND"),
              TigerShapefileParser.getDoubleAttribute(feature, "AWATER"),
              TigerShapefileParser.getGeometryAttribute(feature)
          };
        });

        // Convert Object[] to Map<String, Object>
        for (Object[] arr : bgDataArrays) {
          java.util.Map<String, Object> record = new java.util.HashMap<>();
          record.put("block_group_fips", arr[0]);
          record.put("state_fips", arr[1]);
          record.put("county_fips", arr[2]);
          record.put("tract_fips", arr[3]);
          record.put("land_area", arr[4]);
          record.put("water_area", arr[5]);
          record.put("geometry", arr[6]);
          allData.add(record);
        }
      }
    }

    if (!allData.isEmpty()) {
      storageProvider.writeAvroParquet(targetFilePath, columns, allData, "BlockGroup", "BlockGroup");
      LOGGER.info("Created block_groups parquet file: {} with {} records", targetFilePath, allData.size());
    }
  }

  @SuppressWarnings("deprecation")
  private void convertCbsaDirectToParquet(File yearDir, String targetFilePath, String year) throws Exception {
    File cbsaDir = new File(yearDir, "cbsa");
    LOGGER.info("Looking for CBSA directory at: {}", cbsaDir);
    if (!cbsaDir.exists()) {
      LOGGER.warn("No cbsa directory found in {}", yearDir);
      return;
    }
    LOGGER.info("CBSA directory exists, proceeding with conversion");

    // Load schema metadata from geo-schema.json
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("cbsa");

    String expectedPrefix = "tl_" + year + "_us_cbsa";
    List<Object[]> cbsaDataArrays = TigerShapefileParser.parseShapefile(cbsaDir, expectedPrefix, feature -> {
      return new Object[]{
          TigerShapefileParser.getStringAttribute(feature, "GEOID"),
          TigerShapefileParser.getStringAttribute(feature, "NAME"),
          TigerShapefileParser.getStringAttribute(feature, "LSAD"),
          TigerShapefileParser.getDoubleAttribute(feature, "ALAND"),
          TigerShapefileParser.getDoubleAttribute(feature, "AWATER"),
          TigerShapefileParser.getGeometryAttribute(feature)
      };
    });

    if (!cbsaDataArrays.isEmpty()) {
      // Convert Object[] to Map<String, Object>
      java.util.List<java.util.Map<String, Object>> dataList = new java.util.ArrayList<>();
      for (Object[] arr : cbsaDataArrays) {
        java.util.Map<String, Object> record = new java.util.HashMap<>();
        record.put("cbsa_fips", arr[0]);
        record.put("cbsa_name", arr[1]);
        record.put("metro_micro", arr[2]);
        record.put("land_area", arr[3]);
        record.put("water_area", arr[4]);
        record.put("geometry", arr[5]);
        dataList.add(record);
      }
      storageProvider.writeAvroParquet(targetFilePath, columns, dataList, "CBSA", "CBSA");
      LOGGER.info("Created cbsa parquet file: {} with {} records", targetFilePath, dataList.size());
    }
  }

  @SuppressWarnings("deprecation")
  private void convertCongressionalDistrictsDirectToParquet(File yearDir, String targetFilePath, String year) throws Exception {
    File cdDir = new File(yearDir, "congressional_districts");
    if (!cdDir.exists()) {
      LOGGER.debug("No congressional_districts directory found in {}", yearDir);
      return;
    }

    // Load schema metadata from geo-schema.json
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("congressional_districts");

    // Calculate correct Congress number: ((year - 1789) / 2) + 1
    int yearNum = Integer.parseInt(year);
    int congressNum = ((yearNum - 1789) / 2) + 1;

    // 2010 has different field naming (suffix "10" instead of no suffix)
    boolean is2010 = yearNum == 2010;
    String geoidField = is2010 ? "GEOID10" : "GEOID";
    String statefpField = is2010 ? "STATEFP10" : "STATEFP";
    String namelsadField = is2010 ? "NAMELSAD10" : "NAMELSAD";
    String alandField = is2010 ? "ALAND10" : "ALAND";
    String awaterField = is2010 ? "AWATER10" : "AWATER";

    java.util.List<java.util.Map<String, Object>> allData = new java.util.ArrayList<>();
    File[] shpFiles = cdDir.listFiles((dir, name) -> name.endsWith(".shp") && name.contains("_cd" + congressNum));

    if (shpFiles != null) {
      for (File shpFile : shpFiles) {
        LOGGER.debug("Processing congressional district file: {}", shpFile.getName());
        String expectedPrefix = shpFile.getName().replace(".shp", "");
        List<Object[]> cdDataArrays = TigerShapefileParser.parseShapefile(cdDir, expectedPrefix, feature -> {
          return new Object[]{
              TigerShapefileParser.getStringAttribute(feature, geoidField),
              TigerShapefileParser.getStringAttribute(feature, statefpField),
              TigerShapefileParser.getStringAttribute(feature, namelsadField),
              TigerShapefileParser.getDoubleAttribute(feature, alandField),
              TigerShapefileParser.getDoubleAttribute(feature, awaterField),
              TigerShapefileParser.getGeometryAttribute(feature)
          };
        });

        // Convert Object[] to Map<String, Object>
        for (Object[] arr : cdDataArrays) {
          java.util.Map<String, Object> record = new java.util.HashMap<>();
          record.put("cd_fips", arr[0]);
          record.put("state_fips", arr[1]);
          record.put("cd_name", arr[2]);
          record.put("land_area", arr[3]);
          record.put("water_area", arr[4]);
          record.put("geometry", arr[5]);
          allData.add(record);
        }
      }
    }

    if (!allData.isEmpty()) {
      storageProvider.writeAvroParquet(targetFilePath, columns, allData, "CongressionalDistrict", "CongressionalDistrict");
      LOGGER.info("Created congressional_districts parquet file: {} with {} records", targetFilePath, allData.size());
    } else {
      LOGGER.warn("No congressional district data found in {}", cdDir);
    }
  }

  @SuppressWarnings("deprecation")
  private void convertSchoolDistrictsDirectToParquet(File yearDir, String targetFilePath, String year) throws Exception {
    File sdDir = new File(yearDir, "school_districts");
    if (!sdDir.exists()) {
      LOGGER.debug("No school_districts directory found in {}", yearDir);
      return;
    }

    // Load schema metadata from geo-schema.json
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        loadTableColumns("school_districts");

    java.util.List<java.util.Map<String, Object>> allData = new java.util.ArrayList<>();

    // Process school districts from each state subdirectory
    File[] stateDirs = sdDir.listFiles(File::isDirectory);
    if (stateDirs != null) {
      for (File stateDir : stateDirs) {
        String stateFips = stateDir.getName();

        // Try different school district types
        String[] districtTypes = {"unsd", "elsd", "scsd"};
        for (String type : districtTypes) {
          // 2010 has different naming with type suffix "10" (e.g., unsd10)
          String typeSuffix = "2010".equals(year) ? type + "10" : type;
          String expectedPrefix = "tl_" + year + "_" + stateFips + "_" + typeSuffix;

          // 2010 also has different field naming (suffix "10" instead of no suffix)
          boolean is2010 = "2010".equals(year);
          String geoidField = is2010 ? "GEOID10" : "GEOID";
          String statefpField = is2010 ? "STATEFP10" : "STATEFP";
          String nameField = is2010 ? "NAME10" : "NAME";
          String alandField = is2010 ? "ALAND10" : "ALAND";
          String awaterField = is2010 ? "AWATER10" : "AWATER";

          List<Object[]> sdDataArrays = TigerShapefileParser.parseShapefile(stateDir, expectedPrefix, feature -> {
            return new Object[]{
                TigerShapefileParser.getStringAttribute(feature, geoidField),
                TigerShapefileParser.getStringAttribute(feature, statefpField),
                TigerShapefileParser.getStringAttribute(feature, nameField),
                type.toUpperCase(),
                TigerShapefileParser.getDoubleAttribute(feature, alandField),
                TigerShapefileParser.getDoubleAttribute(feature, awaterField),
                TigerShapefileParser.getGeometryAttribute(feature)
            };
          });

          // Convert Object[] to Map<String, Object>
          for (Object[] arr : sdDataArrays) {
            java.util.Map<String, Object> record = new java.util.HashMap<>();
            record.put("sd_lea", arr[0]);
            record.put("state_fips", arr[1]);
            record.put("sd_name", arr[2]);
            record.put("sd_type", arr[3]);
            record.put("land_area", arr[4]);
            record.put("water_area", arr[5]);
            record.put("geometry", arr[6]);
            allData.add(record);
          }
        }
      }
    }

    if (!allData.isEmpty()) {
      storageProvider.writeAvroParquet(targetFilePath, columns, allData, "SchoolDistrict", "SchoolDistrict");
      LOGGER.info("Created school_districts parquet file: {} with {} records", targetFilePath, allData.size());
    }
  }

}
