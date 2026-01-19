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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Table lifecycle listener for document-based ETL sources.
 *
 * <p>This listener handles tables where data comes from documents (XBRL, HTML)
 * that produce multiple output tables when converted. Unlike standard ETL where
 * one API call produces one table's data, document-based ETL downloads documents
 * and extracts multiple tables from each document.
 *
 * <h3>Document ETL Flow</h3>
 * <pre>
 * 1. beforeTable() - Initialize document converter
 * 2. fetchData() - Returns iterator that:
 *    a. Fetches metadata to enumerate documents
 *    b. Downloads each document
 *    c. Invokes converter (e.g., XbrlToParquetConverter)
 *    d. Converter writes multiple Parquet files directly
 *    e. Returns empty iterator (data written by converter)
 * 3. afterTable() - Report conversion statistics
 * </pre>
 *
 * <h3>Multi-Table Output</h3>
 * <p>Document converters produce multiple tables from each document:
 * <ul>
 *   <li>filing_metadata - Filing header information</li>
 *   <li>financial_line_items - XBRL facts/values</li>
 *   <li>filing_contexts - XBRL context definitions</li>
 *   <li>mda_sections - MD&amp;A text sections</li>
 *   <li>xbrl_relationships - Presentation/calculation links</li>
 * </ul>
 *
 * @see DocumentSource
 * @see DocumentETLProcessor
 * @see HttpSourceConfig.DocumentSourceConfig
 */
public class DocumentTableLifecycleListener implements TableLifecycleListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(DocumentTableLifecycleListener.class);

  private final StorageProvider storageProvider;
  private final String cacheDirectory;
  private final String outputDirectory;
  private FileConverter documentConverter;
  private DocumentETLProcessor.DocumentETLResult lastResult;

  /**
   * Creates a document table lifecycle listener.
   *
   * @param storageProvider Storage provider for output files
   * @param cacheDirectory Local cache directory for downloaded documents
   * @param outputDirectory Output directory for converted files
   */
  public DocumentTableLifecycleListener(StorageProvider storageProvider,
      String cacheDirectory, String outputDirectory) {
    this.storageProvider = storageProvider;
    this.cacheDirectory = cacheDirectory;
    this.outputDirectory = outputDirectory;
  }

  /**
   * Sets the document converter to use.
   *
   * @param converter File converter for document processing
   */
  public void setDocumentConverter(FileConverter converter) {
    this.documentConverter = converter;
  }

  @Override
  public void beforeTable(TableContext context) throws Exception {
    LOGGER.info("Starting document-based ETL for table: {}", context.getTableName());

    // Initialize converter from config if not set
    if (documentConverter == null) {
      HttpSourceConfig sourceConfig = context.getTableConfig().getSource();
      if (sourceConfig != null && sourceConfig.getDocumentSource() != null) {
        String converterClass = sourceConfig.getDocumentSource().getDocumentConverter();
        if (converterClass != null) {
          documentConverter = instantiateConverter(converterClass);
        }
      }
    }

    if (documentConverter == null) {
      throw new IllegalStateException(
          "No document converter configured for table: " + context.getTableName());
    }
  }

  @Override
  public void afterTable(TableContext context, EtlResult result) {
    if (lastResult != null) {
      LOGGER.info("Document ETL completed for {}: {} processed, {} skipped, {} failed",
          context.getTableName(),
          lastResult.getDocumentsProcessed(),
          lastResult.getDocumentsSkipped(),
          lastResult.getDocumentsFailed());
    }
  }

  @Override
  public boolean onTableError(TableContext context, Exception error) {
    LOGGER.error("Document ETL failed for table {}: {}",
        context.getTableName(), error.getMessage());
    // Continue with other tables
    return true;
  }

  /**
   * Fetches data by processing documents.
   *
   * <p>For document-based sources, this method:
   * <ol>
   *   <li>Creates a DocumentETLProcessor</li>
   *   <li>Processes entities (CIKs for SEC) from dimensions</li>
   *   <li>Returns an empty iterator (converter writes files directly)</li>
   * </ol>
   *
   * @param context Table processing context
   * @param variables Current dimension variable values
   * @return Empty iterator (data written by converter)
   */
  @Override
  public Iterator<Map<String, Object>> fetchData(TableContext context,
      Map<String, String> variables) {

    HttpSourceConfig sourceConfig = context.getTableConfig().getSource();

    // Create processor
    DocumentETLProcessor processor = new DocumentETLProcessor(
        sourceConfig,
        storageProvider,
        outputDirectory,
        new File(cacheDirectory),
        documentConverter);

    // Build entity list from dimensions
    List<Map<String, String>> entities = buildEntityList(context, variables);

    if (entities.isEmpty()) {
      LOGGER.warn("No entities to process for table: {}", context.getTableName());
      return new ArrayList<Map<String, Object>>().iterator();
    }

    // Process all entities
    try {
      lastResult = processor.processEntities(entities);
    } catch (IOException e) {
      LOGGER.error("Document processing failed: {}", e.getMessage());
      lastResult = null;
      // Return empty iterator - error will be handled by caller
    }

    // Document converter writes files directly, so return empty iterator
    // The EtlPipeline should skip materialization for document sources
    return new ArrayList<Map<String, Object>>().iterator();
  }

  /**
   * Builds the list of entities to process from dimension values.
   *
   * @param context Table context
   * @param currentVariables Current variable values
   * @return List of entity variable maps
   */
  protected List<Map<String, String>> buildEntityList(TableContext context,
      Map<String, String> currentVariables) {
    List<Map<String, String>> entities = new ArrayList<Map<String, String>>();

    // If we have a single CIK, process just that entity
    if (currentVariables.containsKey("cik")) {
      Map<String, String> entity = new HashMap<String, String>(currentVariables);
      entities.add(entity);
      return entities;
    }

    // Otherwise, look for cik dimension in config
    EtlPipelineConfig tableConfig = context.getTableConfig();
    DimensionConfig cikDimension = tableConfig.getDimensions().get("cik");

    if (cikDimension != null) {
      List<String> ciks = resolveDimensionValues(cikDimension);
      for (String cik : ciks) {
        Map<String, String> entity = new HashMap<String, String>(currentVariables);
        entity.put("cik", cik);
        entities.add(entity);
      }
    }

    return entities;
  }

  /**
   * Resolves dimension values to a list of strings.
   */
  private List<String> resolveDimensionValues(DimensionConfig dimension) {
    List<String> values = new ArrayList<String>();

    if (dimension.getValues() != null) {
      for (Object v : dimension.getValues()) {
        values.add(String.valueOf(v));
      }
    }

    return values;
  }

  /**
   * Instantiates a converter from class name.
   */
  private FileConverter instantiateConverter(String className) {
    try {
      Class<?> clazz = Class.forName(className);

      // Try constructor with StorageProvider
      try {
        return (FileConverter) clazz
            .getConstructor(StorageProvider.class)
            .newInstance(storageProvider);
      } catch (NoSuchMethodException e) {
        // Try default constructor
        return (FileConverter) clazz.getDeclaredConstructor().newInstance();
      }
    } catch (Exception e) {
      LOGGER.error("Failed to instantiate converter: {}", className, e);
      return null;
    }
  }

  /**
   * Returns the last processing result.
   *
   * @return Last DocumentETLResult, or null if not yet processed
   */
  public DocumentETLProcessor.DocumentETLResult getLastResult() {
    return lastResult;
  }
}
