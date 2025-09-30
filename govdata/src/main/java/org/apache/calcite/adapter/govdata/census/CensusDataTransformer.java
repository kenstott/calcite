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
package org.apache.calcite.adapter.govdata.census;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Transforms Census Bureau JSON data to Parquet format with friendly column names.
 *
 * <p>This class handles the conversion from raw Census API JSON responses to
 * structured Parquet files with human-readable column names and proper data types.
 */
public class CensusDataTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(CensusDataTransformer.class);

  private final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * Transform Census JSON data to Parquet format with friendly column names.
   *
   * @param jsonFiles Array of JSON cache files to process
   * @param targetPath Target parquet file path
   * @param tableName Name of the table being created
   * @param year Data year
   * @param variableMap Mapping from Census variable codes to friendly names
   * @param storageProvider Storage provider for file operations
   */
  public void transformToParquet(File[] jsonFiles, String targetPath, String tableName,
      int year, Map<String, String> variableMap, StorageProvider storageProvider)
      throws IOException {
    transformToParquet(jsonFiles, targetPath, tableName, year, variableMap, storageProvider, null);
  }

  /**
   * Transform Census JSON data to Parquet format with conceptual variable mappings.
   *
   * @param jsonFiles Array of JSON cache files to process
   * @param targetPath Target parquet file path
   * @param tableName Name of the table being created
   * @param year Data year
   * @param variableMap Mapping from Census variable codes to friendly names (legacy)
   * @param storageProvider Storage provider for file operations
   * @param censusType Type of census (for conceptual mapping)
   */
  public void transformToParquet(File[] jsonFiles, String targetPath, String tableName,
      int year, Map<String, String> variableMap, StorageProvider storageProvider, String censusType)
      throws IOException {

    LOGGER.info("Transforming Census data to Parquet: {} files -> {}", jsonFiles.length, targetPath);

    // Collect all data from JSON files
    List<Map<String, Object>> allData = new ArrayList<>();
    for (File jsonFile : jsonFiles) {
      List<Map<String, Object>> fileData = parseJsonFile(jsonFile);
      if (fileData != null && !fileData.isEmpty()) {
        allData.addAll(fileData);
      }
    }

    if (allData.isEmpty()) {
      LOGGER.info("No data available for table: {} year {} - creating zero-row file", tableName, year);
      createZeroRowParquetFile(targetPath, tableName, year, variableMap, storageProvider);
      return;
    }

    LOGGER.info("Collected {} records from {} JSON files", allData.size(), jsonFiles.length);

    // Get variable mappings - use conceptual if available, fallback to legacy
    Map<String, String> effectiveVariableMap = variableMap;
    if (censusType != null) {
      Map<String, String> conceptualMap = ConceptualVariableMapper.getVariablesForTable(tableName, year, censusType);
      if (!conceptualMap.isEmpty()) {
        effectiveVariableMap = conceptualMap;
        LOGGER.info("Using conceptual variable mappings for {} ({} year {}): {} variables",
            tableName, censusType, year, conceptualMap.size());
      } else {
        LOGGER.info("No conceptual mappings available for {} ({} year {}), using legacy mappings",
            tableName, censusType, year);
      }
    }

    // Transform data with friendly column names
    List<Map<String, Object>> transformedData = transformDataToMaps(allData, effectiveVariableMap, tableName, year);

    if (transformedData.isEmpty()) {
      LOGGER.warn("No transformed data for table: {}", tableName);
      return;
    }

    // Build Avro schema from sample transformed data
    Schema avroSchema = buildAvroSchema(transformedData.get(0), tableName);

    // Convert to GenericRecords using consistent schema
    List<GenericRecord> transformedRecords = convertToGenericRecords(transformedData, avroSchema);

    // Write to Parquet
    writeParquetFile(transformedRecords, avroSchema, targetPath, storageProvider);

    LOGGER.info("Successfully transformed {} records to Parquet: {}", transformedRecords.size(), targetPath);
  }

  /**
   * Parse a Census API JSON file.
   */
  private List<Map<String, Object>> parseJsonFile(File jsonFile) {
    try (FileInputStream fis = new FileInputStream(jsonFile)) {
      LOGGER.debug("Parsing JSON file: {}", jsonFile.getName());

      // Census API returns arrays of arrays, where first array is headers
      List<List<String>> rawData = objectMapper.readValue(fis, new TypeReference<List<List<String>>>() {});

      if (rawData.size() < 2) {
        LOGGER.warn("JSON file has insufficient data: {}", jsonFile.getName());
        return new ArrayList<>();
      }

      List<String> headers = rawData.get(0);
      List<Map<String, Object>> records = new ArrayList<>();

      // Convert each data row to a map
      for (int i = 1; i < rawData.size(); i++) {
        List<String> row = rawData.get(i);
        Map<String, Object> record = new HashMap<>();

        for (int j = 0; j < Math.min(headers.size(), row.size()); j++) {
          String header = headers.get(j);
          String value = row.get(j);

          // Convert numeric values
          Object convertedValue = convertValue(value);
          record.put(header, convertedValue);
        }

        if (!record.isEmpty()) {
          records.add(record);
        }
      }

      LOGGER.debug("Parsed {} records from {}", records.size(), jsonFile.getName());
      return records;

    } catch (Exception e) {
      LOGGER.error("Error parsing JSON file {}: {}", jsonFile.getName(), e.getMessage());
      return new ArrayList<>();
    }
  }

  /**
   * Convert string values to appropriate data types.
   */
  private Object convertValue(String value) {
    if (value == null || value.trim().isEmpty() || "null".equalsIgnoreCase(value)) {
      return null;
    }

    // Try to convert to number
    try {
      if (value.contains(".")) {
        return Double.parseDouble(value);
      } else {
        return Long.parseLong(value);
      }
    } catch (NumberFormatException e) {
      // Return as string if not a number
      return value;
    }
  }

  /**
   * Transform raw Census data with friendly column names and derived fields.
   *
   * NOTE: This does NOT include partition columns (source, type, year) in the data.
   * Those are encoded in the directory structure for proper Hive-style partitioning.
   */
  private List<Map<String, Object>> transformDataToMaps(List<Map<String, Object>> rawData,
      Map<String, String> variableMap, String tableName, int year) {

    List<Map<String, Object>> transformedData = new ArrayList<>();

    for (Map<String, Object> rawRecord : rawData) {
      Map<String, Object> transformedRecord = new HashMap<>();

      // NOTE: Do NOT add year, source, or type to the data - they are partition columns
      // and should only exist in the directory structure (e.g., source=census/type=acs/year=2020/)

      // Add geoid column
      String geoid = createGeoid(rawRecord);
      transformedRecord.put("geoid", geoid);

      // Transform variable codes to friendly names
      for (Map.Entry<String, String> mapping : variableMap.entrySet()) {
        String variableCode = mapping.getKey();
        String friendlyName = mapping.getValue();

        Object value = rawRecord.get(variableCode);
        if (value != null) {
          transformedRecord.put(friendlyName, value);
        }
      }

      // Add any geographic columns directly
      if (rawRecord.containsKey("state")) {
        transformedRecord.put("state_fips", rawRecord.get("state"));
      }
      if (rawRecord.containsKey("county")) {
        transformedRecord.put("county_fips", rawRecord.get("county"));
      }

      transformedData.add(transformedRecord);
    }

    LOGGER.info("Transformed {} records for table {}", transformedData.size(), tableName);
    return transformedData;
  }

  /**
   * Create a geographic identifier (GEOID) from the raw record.
   */
  private String createGeoid(Map<String, Object> rawRecord) {
    // Census API returns FIPS codes as various types (String, Long, Integer)
    // Convert to String to handle all cases
    String state = convertToString(rawRecord.get("state"));
    String county = convertToString(rawRecord.get("county"));

    if (state != null && !state.trim().isEmpty()) {
      if (county != null && !county.trim().isEmpty()) {
        // County-level GEOID: state + county (ensure proper padding)
        return String.format("%02d%03d", Integer.parseInt(state), Integer.parseInt(county));
      } else {
        // State-level GEOID: just state (ensure 2-digit padding)
        return String.format("%02d", Integer.parseInt(state));
      }
    }

    return "unknown";
  }

  /**
   * Convert various data types to String, handling nulls and different numeric types.
   */
  private String convertToString(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return (String) value;
    }
    if (value instanceof Number) {
      return String.valueOf(((Number) value).intValue());
    }
    return String.valueOf(value);
  }

  /**
   * Convert transformed data maps to GenericRecords using a consistent schema.
   */
  private List<GenericRecord> convertToGenericRecords(List<Map<String, Object>> transformedData, Schema schema) {
    List<GenericRecord> records = new ArrayList<>();

    for (Map<String, Object> data : transformedData) {
      try {
        GenericRecord record = new GenericData.Record(schema);

        // Set fields in the same order as defined in schema
        for (Schema.Field field : schema.getFields()) {
          String fieldName = field.name();
          Object value = data.get(fieldName);

          // Set field value with proper type conversion
          if (value != null) {
            record.put(fieldName, value);
          }
        }

        records.add(record);
      } catch (Exception e) {
        LOGGER.error("Error creating GenericRecord for data {}: {}", data, e.getMessage(), e);
      }
    }

    return records;
  }

  /**
   * Build Avro schema from sample data.
   *
   * NOTE: Does NOT include partition columns (source, type, year) in schema.
   * Those are in the directory structure only for proper Hive-style partitioning.
   */
  private Schema buildAvroSchema(Map<String, Object> sampleData, String tableName) {
    SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder
        .record(tableName)
        .namespace("org.apache.calcite.adapter.govdata.census")
        .doc("Census " + tableName + " data");

    SchemaBuilder.FieldAssembler<Schema> fields = recordBuilder.fields();

    // Build fields in consistent order: geoid first, then data fields in sorted order
    // NOTE: year, source, type are NOT included - they are partition columns
    fields = fields.name("geoid").type().stringType().noDefault();

    // Get all non-geoid field names, sort them for consistency
    Set<String> dataFieldNames = new TreeSet<>();
    for (String fieldName : sampleData.keySet()) {
      if (!"geoid".equals(fieldName)) {
        dataFieldNames.add(fieldName);
      }
    }

    // Add data fields in sorted order
    for (String fieldName : dataFieldNames) {
      Object value = sampleData.get(fieldName);

      if (value instanceof Long) {
        fields = fields.name(fieldName).type().nullable().longType().noDefault();
      } else if (value instanceof Double) {
        fields = fields.name(fieldName).type().nullable().doubleType().noDefault();
      } else {
        fields = fields.name(fieldName).type().nullable().stringType().noDefault();
      }
    }

    return fields.endRecord();
  }

  /**
   * Write transformed records to Parquet file using StorageProvider.
   */
  private void writeParquetFile(List<GenericRecord> records, Schema schema, String targetPath,
      StorageProvider storageProvider) throws IOException {

    LOGGER.info("Writing {} records to Parquet file: {}", records.size(), targetPath);

    try {
      LOGGER.info("About to call StorageProvider.writeAvroParquet with {} records", records.size());
      LOGGER.info("Schema: {}", schema.toString());
      LOGGER.info("Target path: {}", targetPath);

      storageProvider.writeAvroParquet(targetPath, schema, records, "GenericRecord");
      LOGGER.info("Successfully wrote {} records to Parquet: {}", records.size(), targetPath);

      // Clean up any macOS metadata files created during writing
      String parentDir = targetPath.substring(0, targetPath.lastIndexOf('/'));
      storageProvider.cleanupMacosMetadata(parentDir);

      // Verify file was created
      try {
        if (storageProvider.exists(targetPath)) {
          StorageProvider.FileMetadata metadata = storageProvider.getMetadata(targetPath);
          LOGGER.info("Verified file exists with size: {} bytes", metadata.getSize());
        } else {
          LOGGER.error("File does not exist after writing: {}", targetPath);
        }
      } catch (Exception verifyEx) {
        LOGGER.error("Could not verify file after writing: {}", verifyEx.getMessage());
      }

    } catch (Exception e) {
      LOGGER.error("Error writing Parquet file {}: {}", targetPath, e.getMessage(), e);
      throw new IOException("Failed to write Parquet file", e);
    }
  }

  /**
   * Create a zero-row parquet file with proper schema for tables with no data.
   *
   * NOTE: Does NOT include partition columns (source, type, year) in schema.
   * Those are in the directory structure only for proper Hive-style partitioning.
   */
  public void createZeroRowParquetFile(String targetPath, String tableName, int year,
      Map<String, String> variableMap, StorageProvider storageProvider) throws IOException {

    LOGGER.info("Creating zero-row parquet file: {}", targetPath);

    // Create minimal schema with required columns
    SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder
        .record(tableName)
        .namespace("org.apache.calcite.adapter.govdata.census")
        .doc("Census " + tableName + " data (zero rows)");

    SchemaBuilder.FieldAssembler<Schema> fields = recordBuilder.fields();

    // Add required fields (geoid only - year/type/source are partition columns)
    fields = fields.name("geoid").type().stringType().noDefault();

    // Add a few sample variable columns with nullable types
    fields = fields.name("state_fips").type().nullable().longType().noDefault();

    // Add one representative data column from variable map if available
    if (!variableMap.isEmpty()) {
      String firstVar = variableMap.values().iterator().next();
      fields = fields.name(firstVar).type().nullable().longType().noDefault();
    }

    Schema avroSchema = fields.endRecord();

    // Create empty record list
    List<GenericRecord> emptyRecords = new ArrayList<>();

    // Write empty parquet file
    writeParquetFile(emptyRecords, avroSchema, targetPath, storageProvider);

    LOGGER.info("Successfully created zero-row parquet file: {}", targetPath);
  }
}