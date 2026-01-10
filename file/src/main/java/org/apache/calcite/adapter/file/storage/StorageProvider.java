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
package org.apache.calcite.adapter.file.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.List;

/**
 * Storage provider interface for abstracting file access across different storage systems.
 * Implementations can provide access to local files, S3, HTTP, SharePoint, etc.
 */
public interface StorageProvider {

  /**
   * Lists files in a directory or container.
   *
   * @param path The directory or container path
   * @param recursive Whether to include subdirectories
   * @return List of file entries
   * @throws IOException If an I/O error occurs
   */
  List<FileEntry> listFiles(String path, boolean recursive) throws IOException;

  /**
   * Gets metadata for a single file.
   *
   * @param path The file path
   * @return File metadata
   * @throws IOException If an I/O error occurs
   */
  FileMetadata getMetadata(String path) throws IOException;

  /**
   * Opens an input stream for reading file content.
   *
   * @param path The file path
   * @return Input stream for the file
   * @throws IOException If an I/O error occurs
   */
  InputStream openInputStream(String path) throws IOException;

  /**
   * Opens a reader for reading text file content.
   *
   * @param path The file path
   * @return Reader for the file
   * @throws IOException If an I/O error occurs
   */
  Reader openReader(String path) throws IOException;

  /**
   * Checks if a path exists.
   *
   * @param path The path to check
   * @return true if the path exists
   * @throws IOException If an I/O error occurs
   */
  boolean exists(String path) throws IOException;

  /**
   * Checks if a path is a directory.
   *
   * @param path The path to check
   * @return true if the path is a directory
   * @throws IOException If an I/O error occurs
   */
  boolean isDirectory(String path) throws IOException;

  /**
   * Gets the storage type identifier.
   *
   * @return Storage type (e.g., "local", "s3", "http")
   */
  String getStorageType();

  /**
   * Resolves a relative path against a base path.
   *
   * @param basePath The base path
   * @param relativePath The relative path
   * @return The resolved absolute path
   */
  String resolvePath(String basePath, String relativePath);

  /**
   * Writes content to a file.
   * Creates the file if it doesn't exist, overwrites if it does.
   *
   * @param path The file path
   * @param content The content to write
   * @throws IOException If an I/O error occurs
   * @throws UnsupportedOperationException If this storage provider is read-only
   */
  default void writeFile(String path, byte[] content) throws IOException {
    throw new UnsupportedOperationException(
        "Write operations are not supported by " + getStorageType() + " storage provider");
  }

  /**
   * Writes content from an input stream to a file.
   * Creates the file if it doesn't exist, overwrites if it does.
   *
   * @param path The file path
   * @param content The input stream to write from
   * @throws IOException If an I/O error occurs
   * @throws UnsupportedOperationException If this storage provider is read-only
   */
  default void writeFile(String path, InputStream content) throws IOException {
    throw new UnsupportedOperationException(
        "Write operations are not supported by " + getStorageType() + " storage provider");
  }

  /**
   * Creates directories for the given path.
   * Creates parent directories as needed.
   *
   * @param path The directory path to create
   * @throws IOException If an I/O error occurs
   * @throws UnsupportedOperationException If this storage provider is read-only
   */
  default void createDirectories(String path) throws IOException {
    throw new UnsupportedOperationException(
        "Directory creation is not supported by " + getStorageType() + " storage provider");
  }

  /**
   * Deletes a file or directory.
   *
   * @param path The path to delete
   * @return true if the file was deleted, false if it didn't exist
   * @throws IOException If an I/O error occurs
   * @throws UnsupportedOperationException If this storage provider is read-only
   */
  default boolean delete(String path) throws IOException {
    throw new UnsupportedOperationException(
        "Delete operations are not supported by " + getStorageType() + " storage provider");
  }

  /**
   * Deletes multiple files in a single batch operation.
   * For S3-compatible storage, this uses the DeleteObjects API (up to 1000 per request).
   * Default implementation falls back to individual deletes.
   *
   * @param paths The paths to delete
   * @return number of files successfully deleted
   * @throws IOException If an I/O error occurs
   */
  default int deleteBatch(java.util.List<String> paths) throws IOException {
    int deleted = 0;
    for (String path : paths) {
      if (delete(path)) {
        deleted++;
      }
    }
    return deleted;
  }

  /**
   * Ensures a lifecycle rule exists for auto-expiring objects with a given prefix.
   * For S3-compatible storage, this creates a lifecycle rule that auto-deletes objects.
   * Default implementation does nothing (local filesystem doesn't support lifecycle).
   *
   * @param prefix The key prefix for the lifecycle rule
   * @param expirationDays Number of days after which objects expire
   * @throws IOException If an I/O error occurs
   */
  default void ensureLifecycleRule(String prefix, int expirationDays) throws IOException {
    // Default: no-op for non-S3 storage providers
  }

  /**
   * Gets a staging directory for temporary files with automatic cleanup guarantee.
   *
   * <p>This method returns a directory path suitable for temporary/staging files
   * that will be automatically cleaned up:
   * <ul>
   *   <li>Local storage: Uses system temp directory (OS handles cleanup)</li>
   *   <li>S3 storage: Uses a .staging/ prefix with lifecycle rule for auto-expiration</li>
   * </ul>
   *
   * <p>Callers should still attempt to clean up staging files when done, but
   * this provides a safety net for failed cleanups.
   *
   * @param purpose Subdirectory name to isolate different staging uses (e.g., "iceberg", "etl")
   * @return Path to staging directory that will be auto-cleaned
   * @throws IOException If an I/O error occurs
   */
  default String getStagingDirectory(String purpose) throws IOException {
    // Default implementation uses system temp directory
    String tempDir = System.getProperty("java.io.tmpdir");
    String stagingPath = tempDir + "/.staging/" + purpose;
    createDirectories(stagingPath);
    return stagingPath;
  }

  /**
   * Copies a file from source to destination.
   *
   * @param source The source file path
   * @param destination The destination file path
   * @throws IOException If an I/O error occurs
   * @throws UnsupportedOperationException If this storage provider is read-only
   */
  default void copyFile(String source, String destination) throws IOException {
    throw new UnsupportedOperationException(
        "Copy operations are not supported by " + getStorageType() + " storage provider");
  }

  /**
   * Checks if a file has changed compared to cached metadata.
   * This method helps avoid unnecessary updates by comparing current file state
   * with previously cached metadata.
   *
   * @param path The path to the file
   * @param cachedMetadata Previously cached metadata to compare against
   * @return true if the file has changed, false if it's the same
   * @throws IOException if an I/O error occurs
   */
  default boolean hasChanged(String path, FileMetadata cachedMetadata) throws IOException {
    if (cachedMetadata == null) {
      return true; // No cached data, assume changed
    }

    try {
      FileMetadata currentMetadata = getMetadata(path);

      // Compare size first (quick check)
      if (currentMetadata.getSize() != cachedMetadata.getSize()) {
        return true;
      }

      // Compare ETags if available (most reliable for S3, HTTP)
      if (currentMetadata.getEtag() != null && cachedMetadata.getEtag() != null) {
        return !currentMetadata.getEtag().equals(cachedMetadata.getEtag());
      }

      // Compare last modified times
      // Allow small time differences (< 1 second) to handle filesystem precision issues
      long timeDiff = Math.abs(currentMetadata.getLastModified()
          - cachedMetadata.getLastModified());
      return timeDiff > 1000;

    } catch (IOException e) {
      // If we can't get current metadata, assume changed
      return true;
    }
  }


  /**
   * Writes data to a Parquet file using column metadata to generate schema.
   * This overload accepts raw data as Maps and handles all Avro/Parquet details internally.
   *
   * @param path The file path where the Parquet file should be written
   * @param columns The table column definitions with types, nullability, and comments
   * @param dataRecords The list of data records as Maps (field name → value)
   * @param recordType Descriptive name for logging (e.g., "facts", "metadata")
   * @param recordName The record name for the Avro schema
   * @throws IOException If an I/O error occurs
   * @throws UnsupportedOperationException If this storage provider is read-only
   */
  default void writeAvroParquet(String path,
                               java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns,
                               java.util.List<java.util.Map<String, Object>> dataRecords,
                               String recordType,
                               String recordName) throws IOException {
    // Build Avro schema from column metadata
    org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fields =
        org.apache.avro.SchemaBuilder.record(recordName).fields();

    for (org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn column : columns) {
      // Map column type to Avro type
      String colType = column.getType().toLowerCase();

      // Build field based on type and nullability
      switch (colType) {
        case "string":
          if (column.isNullable()) {
            fields = fields.name(column.getName()).doc(column.getComment())
                .type().nullable().stringType().noDefault();
          } else {
            fields = fields.name(column.getName()).doc(column.getComment())
                .type().stringType().noDefault();
          }
          break;
        case "int":
        case "integer":
          if (column.isNullable()) {
            fields = fields.name(column.getName()).doc(column.getComment())
                .type().nullable().intType().noDefault();
          } else {
            fields = fields.name(column.getName()).doc(column.getComment())
                .type().intType().noDefault();
          }
          break;
        case "long":
          if (column.isNullable()) {
            fields = fields.name(column.getName()).doc(column.getComment())
                .type().nullable().longType().noDefault();
          } else {
            fields = fields.name(column.getName()).doc(column.getComment())
                .type().longType().noDefault();
          }
          break;
        case "double":
          if (column.isNullable()) {
            fields = fields.name(column.getName()).doc(column.getComment())
                .type().nullable().doubleType().noDefault();
          } else {
            fields = fields.name(column.getName()).doc(column.getComment())
                .type().doubleType().noDefault();
          }
          break;
        case "boolean":
          if (column.isNullable()) {
            fields = fields.name(column.getName()).doc(column.getComment())
                .type().nullable().booleanType().noDefault();
          } else {
            fields = fields.name(column.getName()).doc(column.getComment())
                .type().booleanType().noDefault();
          }
          break;
        default:
          throw new IllegalArgumentException("Unsupported column type: " + column.getType()
              + " for column: " + column.getName());
      }
    }

    org.apache.avro.Schema schema = fields.endRecord();

    // Convert data maps to GenericRecords
    java.util.List<org.apache.avro.generic.GenericRecord> records = new java.util.ArrayList<>();

    // Track schema mismatches for debug logging (sample first record only for performance)
    java.util.Set<String> extraFieldsInData = null;
    java.util.Set<String> missingFieldsInData = null;
    boolean firstRecord = true;

    for (java.util.Map<String, Object> dataRecord : dataRecords) {
      org.apache.avro.generic.GenericRecord record =
          new org.apache.avro.generic.GenericData.Record(schema);

      // Sample first record to detect schema mismatches
      if (firstRecord && !dataRecords.isEmpty()) {
        firstRecord = false;

        // Build set of schema column names
        java.util.Set<String> schemaFields = new java.util.HashSet<>();
        for (org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn column : columns) {
          schemaFields.add(column.getName());
        }

        // Find JSON fields NOT in schema (would be dropped)
        extraFieldsInData = new java.util.HashSet<>(dataRecord.keySet());
        extraFieldsInData.removeAll(schemaFields);

        // Find schema columns NOT in JSON (would be null/defaulted)
        missingFieldsInData = new java.util.HashSet<>();
        for (org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn column : columns) {
          if (!dataRecord.containsKey(column.getName())) {
            missingFieldsInData.add(column.getName());
          }
        }
      }

      for (org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn column : columns) {
        record.put(column.getName(), dataRecord.get(column.getName()));
      }
      records.add(record);
    }

    // Log schema mismatches if detected
    if ((extraFieldsInData != null && !extraFieldsInData.isEmpty())
        || (missingFieldsInData != null && !missingFieldsInData.isEmpty())) {
      org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StorageProvider.class);
      logger.debug("Schema mismatch detected for '{}' ({} records):", path, dataRecords.size());
      if (extraFieldsInData != null && !extraFieldsInData.isEmpty()) {
        logger.debug("  - JSON fields NOT in schema (will be DROPPED): {}", extraFieldsInData);
      }
      if (missingFieldsInData != null && !missingFieldsInData.isEmpty()) {
        logger.debug("  - Schema columns NOT in JSON (will be NULL/defaulted): {}",
            missingFieldsInData);
      }
    }

    // Delegate to existing implementation
    writeAvroParquet(path, schema, records, recordType);
  }

  /**
   * Writes Avro GenericRecords to a Parquet file using modern ParquetWriter API.
   * This is a consolidated method that all adapters can use for Parquet writing.
   *
   * @param path The file path where the Parquet file should be written
   * @param schema The Avro schema for the records
   * @param records The list of GenericRecords to write
   * @param recordType Descriptive name for logging (e.g., "facts", "metadata")
   * @throws IOException If an I/O error occurs
   * @throws UnsupportedOperationException If this storage provider is read-only
   */
  @SuppressWarnings("deprecation")
  default void writeAvroParquet(String path, org.apache.avro.Schema schema,
                               java.util.List<org.apache.avro.generic.GenericRecord> records,
                               String recordType) throws IOException {
    org.slf4j.LoggerFactory.getLogger(StorageProvider.class)
        .info("DEBUG: writeAvroParquet() START - path: " + path + ", recordType: " + recordType + ", recordCount: " + records.size());

    if (records.isEmpty()) {
      org.slf4j.LoggerFactory.getLogger(StorageProvider.class)
          .info("DEBUG: writeAvroParquet() - No records to write, creating empty parquet file");
      // Don't return - we still want to create an empty file with the schema
    }

    // Create temporary file for writing with more unique name - use a UUID for maximum uniqueness
    String uniqueId = java.util.UUID.randomUUID().toString();
    java.io.File tempFile = java.io.File.createTempFile(recordType.toLowerCase() + "_" + uniqueId + "_", ".parquet");
    // Delete the temp file immediately so ParquetWriter can create it fresh
    tempFile.delete();
    try {
      // Use modern ParquetWriter API with try-with-resources
      try (org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericRecord> writer =
           org.apache.parquet.avro.AvroParquetWriter
               .<org.apache.avro.generic.GenericRecord>builder(
                   new org.apache.hadoop.fs.Path(tempFile.toURI()))
               .withSchema(schema)
               .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY)
               .build()) {

        for (org.apache.avro.generic.GenericRecord record : records) {
          writer.write(record);
        }
      }

      // Use StorageProvider to move temp file to final location via streaming to avoid large in-memory buffers
      try (java.io.InputStream in = java.nio.file.Files.newInputStream(tempFile.toPath())) {
        org.slf4j.LoggerFactory.getLogger(StorageProvider.class)
            .info("DEBUG: writeAvroParquet() - Streaming temp parquet to final location: " + path);
        writeFile(path, in);
      }
      org.slf4j.LoggerFactory.getLogger(StorageProvider.class)
          .info("DEBUG: writeAvroParquet() COMPLETED successfully - path: " + path);

    } finally {
      // Clean up temp file
      if (tempFile.exists() && !tempFile.delete()) {
        // Log warning but don't fail the operation
        System.err.println("Warning: Could not delete temporary file: " + tempFile.getAbsolutePath());
      }
    }

    // Clean up macOS metadata files after writing
    String parentDir = java.nio.file.Paths.get(path).getParent().toString();
    cleanupMacosMetadata(parentDir);
  }

  /**
   * Reads data from a Parquet file and returns as List of Maps.
   * This method reads the parquet file using Parquet reader API and converts
   * GenericRecords to Maps for easy processing.
   *
   * @param path The file path to read from
   * @return List of records as Maps (field name → value)
   * @throws IOException If an I/O error occurs
   */
  default java.util.List<java.util.Map<String, Object>> readParquet(String path) throws IOException {
    java.util.List<java.util.Map<String, Object>> records = new java.util.ArrayList<>();

    // Download to temp file first if remote storage
    java.io.File tempFile = java.io.File.createTempFile("read_parquet_", ".parquet");
    try {
      // Copy file content to temp location
      try (java.io.InputStream in = openInputStream(path);
           java.io.OutputStream out = java.nio.file.Files.newOutputStream(tempFile.toPath())) {
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = in.read(buffer)) != -1) {
          out.write(buffer, 0, bytesRead);
        }
      }

      // Read parquet file
      org.apache.parquet.io.InputFile inputFile =
          org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
              new org.apache.hadoop.fs.Path(tempFile.toURI()),
              new org.apache.hadoop.conf.Configuration());

      try (org.apache.parquet.hadoop.ParquetReader<org.apache.avro.generic.GenericRecord> reader =
           org.apache.parquet.avro.AvroParquetReader
               .<org.apache.avro.generic.GenericRecord>builder(inputFile)
               .build()) {

        org.apache.avro.generic.GenericRecord record;
        while ((record = reader.read()) != null) {
          java.util.Map<String, Object> map = new java.util.LinkedHashMap<>();
          for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
            Object value = record.get(field.name());
            // Convert Avro arrays to Java arrays if needed
            if (value instanceof org.apache.avro.generic.GenericData.Array) {
              org.apache.avro.generic.GenericData.Array<?> array =
                  (org.apache.avro.generic.GenericData.Array<?>) value;
              // Check if it's a double array
              if (!array.isEmpty() && array.get(0) instanceof Double) {
                double[] doubleArray = new double[array.size()];
                for (int i = 0; i < array.size(); i++) {
                  doubleArray[i] = ((Number) array.get(i)).doubleValue();
                }
                value = doubleArray;
              }
            }
            map.put(field.name(), value);
          }
          records.add(map);
        }
      }

      return records;

    } finally {
      if (tempFile.exists() && !tempFile.delete()) {
        System.err.println("Warning: Could not delete temporary file: " + tempFile.getAbsolutePath());
      }
    }
  }

  /**
   * Appends data to an existing Parquet file or creates a new one if it doesn't exist.
   * This method reads existing data, merges with new records, and rewrites the file.
   *
   * @param path The file path where the Parquet file should be written
   * @param newRecords The list of new data records to append as Maps (field name → value)
   * @param schema Schema definition as list of column maps
   * @throws IOException If an I/O error occurs
   */
  default void appendParquet(String path, java.util.List<java.util.Map<String, Object>> newRecords,
                            java.util.List<java.util.Map<String, Object>> schema) throws IOException {
    java.util.List<java.util.Map<String, Object>> allRecords = new java.util.ArrayList<>();

    // Read existing records if file exists
    if (exists(path)) {
      try {
        allRecords.addAll(readParquet(path));
      } catch (Exception e) {
        org.slf4j.LoggerFactory.getLogger(StorageProvider.class)
            .warn("Failed to read existing parquet file {}: {}, will overwrite", path, e.getMessage());
      }
    }

    // Add new records
    allRecords.addAll(newRecords);

    // Build Avro schema from column schema
    org.apache.avro.SchemaBuilder.FieldAssembler<org.apache.avro.Schema> fields =
        org.apache.avro.SchemaBuilder.record("CacheRecord").fields();

    for (java.util.Map<String, Object> columnDef : schema) {
      String colName = (String) columnDef.get("name");
      String colType = (String) columnDef.get("type");

      if (colType.startsWith("array<")) {
        // Array type
        String elementType = colType.substring(6, colType.length() - 1);
        if (elementType.equals("double")) {
          fields = fields.name(colName).type().nullable()
              .array().items().doubleType().noDefault();
        } else {
          throw new IllegalArgumentException("Unsupported array element type: " + elementType);
        }
      } else {
        // Scalar types
        switch (colType.toLowerCase()) {
          case "string":
            fields = fields.name(colName).type().nullable().stringType().noDefault();
            break;
          case "long":
            fields = fields.name(colName).type().nullable().longType().noDefault();
            break;
          case "int":
          case "integer":
            fields = fields.name(colName).type().nullable().intType().noDefault();
            break;
          case "double":
            fields = fields.name(colName).type().nullable().doubleType().noDefault();
            break;
          default:
            throw new IllegalArgumentException("Unsupported column type: " + colType);
        }
      }
    }

    org.apache.avro.Schema avroSchema = fields.endRecord();

    // Convert maps to GenericRecords
    java.util.List<org.apache.avro.generic.GenericRecord> genericRecords = new java.util.ArrayList<>();
    for (java.util.Map<String, Object> record : allRecords) {
      org.apache.avro.generic.GenericRecord genericRecord =
          new org.apache.avro.generic.GenericData.Record(avroSchema);

      for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {
        Object value = record.get(field.name());

        // Convert double[] to Avro array
        if (value instanceof double[]) {
          double[] array = (double[]) value;
          java.util.List<Double> avroArray = new java.util.ArrayList<>(array.length);
          for (double d : array) {
            avroArray.add(d);
          }
          value = avroArray;
        }

        genericRecord.put(field.name(), value);
      }
      genericRecords.add(genericRecord);
    }

    // Write to parquet
    writeAvroParquet(path, avroSchema, genericRecords, "cache");
  }

  /**
   * Clean up macOS metadata files in the specified directory.
   * Removes files like ._*, .DS_Store, *.crc, *~, *.tmp that can interfere
   * with data processing tools like DuckDB.
   * Only effective for local storage providers.
   *
   * @param directoryPath The directory path to clean up
   * @throws IOException If an I/O error occurs
   */
  default void cleanupMacosMetadata(String directoryPath) throws IOException {
    // Default implementation is no-op
    // Only local storage providers need to implement this
  }

  /**
   * Returns S3 configuration for DuckDB access, if this is an S3-based storage provider.
   *
   * <p>The returned map may contain:
   * <ul>
   *   <li>accessKeyId - S3 access key</li>
   *   <li>secretAccessKey - S3 secret key</li>
   *   <li>region - AWS region</li>
   *   <li>endpoint - Custom S3 endpoint (for R2, MinIO, etc.)</li>
   * </ul>
   *
   * @return S3 configuration map, or null if not an S3 provider or no credentials available
   */
  default java.util.Map<String, String> getS3Config() {
    return null;
  }

  /**
   * Normalizes a path to ensure proper S3/S3A URI format.
   *
   * <p>Hadoop's Path.toString() can return malformed URIs like "s3a:/bucket"
   * (single slash) instead of "s3a://bucket" (double slashes). This method
   * fixes such malformed paths.
   *
   * @param path Path to normalize
   * @return Normalized path with proper URI format
   */
  static String normalizePath(String path) {
    if (path == null) {
      return null;
    }
    // Fix s3a:/ (single slash) to s3a:// (double slashes)
    if (path.startsWith("s3a:/") && !path.startsWith("s3a://")) {
      return "s3a://" + path.substring(5);
    }
    // Fix s3:/ (single slash) to s3:// (double slashes)
    if (path.startsWith("s3:/") && !path.startsWith("s3://")) {
      return "s3://" + path.substring(4);
    }
    // Fix hdfs:/ (single slash) to hdfs:// (double slashes)
    if (path.startsWith("hdfs:/") && !path.startsWith("hdfs://")) {
      return "hdfs://" + path.substring(6);
    }
    return path;
  }

  /**
   * File entry representing a file in a directory listing.
   */
  class FileEntry {
    private final String path;
    private final String name;
    private final boolean isDirectory;
    private final long size;
    private final long lastModified;

    public FileEntry(String path, String name, boolean isDirectory,
                     long size, long lastModified) {
      this.path = path;
      this.name = name;
      this.isDirectory = isDirectory;
      this.size = size;
      this.lastModified = lastModified;
    }

    public String getPath() {
      return path;
    }

    public String getName() {
      return name;
    }

    public boolean isDirectory() {
      return isDirectory;
    }

    public long getSize() {
      return size;
    }

    public long getLastModified() {
      return lastModified;
    }
  }

  /**
   * File metadata containing detailed information about a file.
   */
  class FileMetadata {
    private final String path;
    private final long size;
    private final long lastModified;
    private final String contentType;
    private final String etag;

    public FileMetadata(String path, long size, long lastModified,
                        String contentType, String etag) {
      this.path = path;
      this.size = size;
      this.lastModified = lastModified;
      this.contentType = contentType;
      this.etag = etag;
    }

    public String getPath() {
      return path;
    }

    public long getSize() {
      return size;
    }

    public long getLastModified() {
      return lastModified;
    }

    public String getContentType() {
      return contentType;
    }

    public String getEtag() {
      return etag;
    }
  }
}
