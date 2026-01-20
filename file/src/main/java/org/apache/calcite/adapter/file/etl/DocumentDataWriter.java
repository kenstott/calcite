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

import org.apache.calcite.adapter.file.converters.FileConverter;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * DataWriter implementation for document-based ETL sources.
 *
 * <p>This writer uses {@link DocumentETLProcessor} to download and convert
 * documents (XBRL, HTML) into multiple Parquet files. Unlike standard
 * DataWriters that write a single table, this writer produces multiple
 * tables from each document:
 * <ul>
 *   <li>filing_metadata - Filing header information</li>
 *   <li>financial_line_items - XBRL facts/values</li>
 *   <li>filing_contexts - XBRL context definitions</li>
 *   <li>mda_sections - MD&amp;A text sections</li>
 *   <li>xbrl_relationships - XBRL relationships</li>
 * </ul>
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * // Create document writer with XBRL converter
 * FileConverter converter = new XbrlToParquetConverter(storageProvider);
 * DocumentDataWriter writer = new DocumentDataWriter(
 *     storageProvider, cacheDir, outputDir, converter);
 *
 * // Use with EtlPipeline
 * EtlPipeline pipeline = new EtlPipeline(config, storageProvider, baseDir,
 *     null, tracker, null, writer);
 * pipeline.execute();
 * }</pre>
 *
 * @see DocumentETLProcessor
 * @see DocumentSource
 */
public class DocumentDataWriter implements DataWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDataWriter.class);

  private final StorageProvider storageProvider;
  private final String cacheDirectory;
  private final String outputDirectory;
  private final FileConverter documentConverter;

  // Track last processing result
  private DocumentETLProcessor.DocumentETLResult lastResult;

  /**
   * Creates a document data writer.
   *
   * @param storageProvider Storage provider for output files
   * @param cacheDirectory Cache directory for downloaded documents (can be S3 or local path)
   * @param outputDirectory Output directory for converted Parquet files (can be S3 or local path)
   * @param documentConverter Converter for document processing (e.g., XbrlToParquetConverter)
   */
  public DocumentDataWriter(StorageProvider storageProvider,
      String cacheDirectory, String outputDirectory, FileConverter documentConverter) {
    this.storageProvider = storageProvider;
    this.cacheDirectory = cacheDirectory;
    this.outputDirectory = outputDirectory;
    this.documentConverter = documentConverter;
  }

  /**
   * Writes data by processing documents for the given entity.
   *
   * <p>For document sources:
   * <ol>
   *   <li>Creates DocumentETLProcessor with the entity variables</li>
   *   <li>Fetches metadata to enumerate documents</li>
   *   <li>Downloads and converts each document</li>
   *   <li>Converter writes multiple Parquet files directly</li>
   *   <li>Returns count of documents processed (not rows)</li>
   * </ol>
   *
   * @param config Pipeline configuration
   * @param data Ignored for document sources (converter handles writing)
   * @param variables Entity variables (e.g., cik for SEC)
   * @return Number of documents processed, or -1 if this writer cannot handle the source
   * @throws IOException If processing fails
   */
  @Override
  public long write(EtlPipelineConfig config, Iterator<Map<String, Object>> data,
      Map<String, String> variables) throws IOException {

    // Verify this is a document source
    if (!EtlPipelineConfig.SOURCE_TYPE_DOCUMENT.equals(config.getSourceType())) {
      LOGGER.debug("DocumentDataWriter does not handle source type: {}", config.getSourceType());
      return -1;
    }

    HttpSourceConfig sourceConfig = config.getSource();
    if (sourceConfig == null || sourceConfig.getDocumentSource() == null) {
      LOGGER.warn("No document source configuration for table: {}", config.getName());
      return -1;
    }

    LOGGER.info("Starting document processing for entity: {}", variables);

    // Create processor for this entity
    DocumentETLProcessor processor = new DocumentETLProcessor(
        sourceConfig,
        storageProvider,
        outputDirectory,
        cacheDirectory,
        documentConverter);

    // Build entity list (single entity for this batch)
    List<Map<String, String>> entities = new ArrayList<Map<String, String>>();
    entities.add(variables);

    // Process the entity
    lastResult = processor.processEntities(entities);

    if (!lastResult.isSuccess()) {
      LOGGER.warn("Document processing completed with {} errors: {}",
          lastResult.getDocumentsFailed(), lastResult.getErrors());
    }

    LOGGER.info("Document processing completed: {} processed, {} skipped, {} failed",
        lastResult.getDocumentsProcessed(),
        lastResult.getDocumentsSkipped(),
        lastResult.getDocumentsFailed());

    // Return number of documents processed
    // Note: actual row counts are within each Parquet file written by converter
    return lastResult.getDocumentsProcessed();
  }

  /**
   * Returns the last processing result.
   *
   * @return Last DocumentETLResult, or null if no processing has occurred
   */
  public DocumentETLProcessor.DocumentETLResult getLastResult() {
    return lastResult;
  }

  /**
   * Returns the document converter being used.
   *
   * @return FileConverter instance
   */
  public FileConverter getDocumentConverter() {
    return documentConverter;
  }

  /**
   * Returns the cache directory path.
   *
   * @return Cache directory path for downloaded documents (can be S3 or local)
   */
  public String getCacheDirectory() {
    return cacheDirectory;
  }

  /**
   * Returns the output directory.
   *
   * @return Output directory for converted files
   */
  public String getOutputDirectory() {
    return outputDirectory;
  }
}
