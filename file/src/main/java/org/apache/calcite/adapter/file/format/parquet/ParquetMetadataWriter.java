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
package org.apache.calcite.adapter.file.format.parquet;

import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility for adding metadata to existing Parquet files.
 *
 * <p>This class provides methods to add table and column comments to Parquet files
 * that were written without metadata (e.g., by DuckDB COPY).
 *
 * <p>The process involves reading the existing file and rewriting it with
 * additional key-value metadata in the footer.
 */
public class ParquetMetadataWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetMetadataWriter.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ParquetMetadataWriter() {
    // Utility class
  }

  /**
   * Adds metadata to all Parquet files in a directory (recursive).
   *
   * @param storageProvider Storage provider for file operations
   * @param directory Directory containing Parquet files
   * @param tableComment Table-level comment
   * @param columnComments Column comments map
   * @return Number of files processed
   * @throws IOException If processing fails
   */
  public static int addMetadataToDirectory(StorageProvider storageProvider,
      String directory, String tableComment, Map<String, String> columnComments)
      throws IOException {

    if ((tableComment == null || tableComment.isEmpty())
        && (columnComments == null || columnComments.isEmpty())) {
      LOGGER.debug("No metadata to add, skipping post-processing");
      return 0;
    }

    // Build metadata map
    Map<String, String> metadata = buildMetadataMap(tableComment, columnComments);

    // Find all .parquet files recursively
    List<StorageProvider.FileEntry> allFiles = storageProvider.listFiles(directory, true);
    List<String> parquetFiles = new java.util.ArrayList<>();
    for (StorageProvider.FileEntry entry : allFiles) {
      if (entry.getPath().endsWith(".parquet")) {
        parquetFiles.add(entry.getPath());
      }
    }

    if (parquetFiles.isEmpty()) {
      LOGGER.debug("No Parquet files found in {}", directory);
      return 0;
    }

    LOGGER.info("Adding metadata to {} Parquet files in {}", parquetFiles.size(), directory);
    int processed = 0;

    for (String filePath : parquetFiles) {
      try {
        addMetadataToFile(filePath, metadata);
        processed++;
      } catch (Exception e) {
        LOGGER.warn("Failed to add metadata to {}: {}", filePath, e.getMessage());
      }
    }

    LOGGER.info("Successfully added metadata to {} of {} files", processed, parquetFiles.size());
    return processed;
  }

  /**
   * Adds metadata to a single Parquet file by rewriting it.
   *
   * @param filePath Path to the Parquet file
   * @param metadata Key-value metadata to add
   * @throws IOException If processing fails
   */
  public static void addMetadataToFile(String filePath, Map<String, String> metadata)
      throws IOException {

    Path inputPath = new Path(filePath);
    Configuration conf = new Configuration();

    // Create temp file path for output (create then delete to get unique name)
    java.nio.file.Path tempFile = Files.createTempFile("parquet_meta_", ".parquet");
    Files.delete(tempFile);  // Delete so ParquetWriter can create it
    Path outputPath = new Path(tempFile.toString());

    try {
      // Read existing metadata
      LOGGER.debug("Reading existing Parquet file: {}", filePath);
      InputFile inputFile = HadoopInputFile.fromPath(inputPath, conf);
      ParquetMetadata footer;
      MessageType schema;

      try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
        footer = reader.getFooter();
        schema = footer.getFileMetaData().getSchema();
        LOGGER.debug("Parquet schema: {}", schema);
      }

      // Merge existing metadata with new metadata
      Map<String, String> existingMeta = footer.getFileMetaData().getKeyValueMetaData();
      Map<String, String> mergedMeta = new HashMap<>();
      if (existingMeta != null) {
        mergedMeta.putAll(existingMeta);
      }
      mergedMeta.putAll(metadata);

      // Get compression from original file
      CompressionCodecName compression = CompressionCodecName.SNAPPY;
      if (!footer.getBlocks().isEmpty() && !footer.getBlocks().get(0).getColumns().isEmpty()) {
        compression = footer.getBlocks().get(0).getColumns().get(0).getCodec();
      }
      LOGGER.debug("Using compression: {}", compression);

      // Set up reader/writer configuration
      GroupWriteSupport.setSchema(schema, conf);

      // Create writer with metadata
      LOGGER.debug("Opening reader and writer...");
      try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), inputPath)
              .withConf(conf)
              .build();
           ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputPath)
              .withConf(conf)
              .withType(schema)
              .withCompressionCodec(compression)
              .withExtraMetaData(mergedMeta)
              .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
              .build()) {

        // Copy all records
        LOGGER.debug("Copying records...");
        Group record;
        int count = 0;
        while ((record = reader.read()) != null) {
          writer.write(record);
          count++;
        }
        LOGGER.debug("Copied {} records", count);
      }

      // Replace original file with temp file
      LOGGER.debug("Replacing original file with temp file...");
      Files.delete(java.nio.file.Paths.get(filePath));
      Files.move(tempFile, java.nio.file.Paths.get(filePath));

      LOGGER.debug("Added metadata to {}", filePath);

    } catch (Exception e) {
      // Clean up temp file on failure
      Files.deleteIfExists(tempFile);
      LOGGER.warn("Exception adding metadata to {}: {}", filePath, e.toString(), e);
      throw new IOException("Failed to add metadata to " + filePath, e);
    }
  }

  /**
   * Builds the metadata map from table and column comments.
   */
  private static Map<String, String> buildMetadataMap(String tableComment,
      Map<String, String> columnComments) {

    Map<String, String> metadata = new HashMap<>();

    if (tableComment != null && !tableComment.isEmpty()) {
      metadata.put("table_comment", tableComment);
    }

    if (columnComments != null && !columnComments.isEmpty()) {
      try {
        String json = MAPPER.writeValueAsString(columnComments);
        metadata.put("column_comments", json);
      } catch (Exception e) {
        LOGGER.warn("Failed to serialize column comments: {}", e.getMessage());
        // Fallback to individual keys
        for (Map.Entry<String, String> entry : columnComments.entrySet()) {
          if (entry.getValue() != null && !entry.getValue().isEmpty()) {
            metadata.put("column_comment:" + entry.getKey(), entry.getValue());
          }
        }
      }
    }

    return metadata;
  }
}
