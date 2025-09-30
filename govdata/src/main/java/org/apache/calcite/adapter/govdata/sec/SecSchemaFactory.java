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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.time.Year;

/**
 * Factory for SEC schemas that extends FileSchema with SEC-specific capabilities.
 * Uses file adapter's infrastructure for HTTP operations, HTML processing, and
 * partitioned Parquet storage.
 *
 * <p>This factory leverages the file adapter's FileConversionManager for XBRL
 * processing and HtmlToJsonConverter for HTML table extraction.
 */
public class SecSchemaFactory implements GovDataSubSchemaFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecSchemaFactory.class);
  private StorageProvider storageProvider;

  // Standard SEC data cache directory - XBRL files are immutable, cache forever
  // These are now accessed at runtime via interface methods

  // Parallel processing configuration
  private static final int DOWNLOAD_THREADS = 3; // Reduced to 3 concurrent downloads for better rate limiting
  private static final int CONVERSION_THREADS = 8; // 8 concurrent conversions
  private static final int SEC_RATE_LIMIT_PER_SECOND = 8; // Conservative: 8 requests/sec (SEC allows 10)
  private static final long INITIAL_RATE_LIMIT_DELAY_MS = 125; // Initial: 125ms between requests (8/sec)
  private static final long MAX_RATE_LIMIT_DELAY_MS = 500; // Max: 500ms between requests (2/sec)

  // Dynamic rate limiting
  private final AtomicLong currentRateLimitDelayMs = new AtomicLong(INITIAL_RATE_LIMIT_DELAY_MS);

  // Thread pools and rate limiting
  private ExecutorService downloadExecutor;
  private ExecutorService conversionExecutor;
  private final Semaphore rateLimiter = new Semaphore(1); // One permit, released every 125ms
  private final ConcurrentLinkedQueue<CompletableFuture<Void>> filingProcessingFutures = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<CompletableFuture<Void>> conversionFutures = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<FilingToDownload> retryQueue = new ConcurrentLinkedQueue<>();
  private final List<File> scheduledForReconversion = new ArrayList<>();
  private final List<File> scheduledForInlineXbrlProcessing = new ArrayList<>();
  private final Set<String> downloadedInThisCycle = java.util.concurrent.ConcurrentHashMap.newKeySet();
  private final AtomicInteger totalFilingsToProcess = new AtomicInteger(0);
  private final AtomicInteger completedFilingProcessing = new AtomicInteger(0);
  private final AtomicInteger totalConversions = new AtomicInteger(0);
  private final AtomicInteger completedConversions = new AtomicInteger(0);
  private final AtomicInteger rateLimitHits = new AtomicInteger(0);
  private Map<String, Object> currentOperand; // Store operand for table auto-discovery
  private RSSRefreshMonitor rssMonitor; // RSS monitor for automatic refresh

  public static final SecSchemaFactory INSTANCE = new SecSchemaFactory();

  // Constraint metadata support
  private Map<String, Map<String, Object>> tableConstraints = new HashMap<>();

  @Override
  public String getSchemaResourceName() {
    return "/sec-schema.json";
  }

  private synchronized void initializeExecutors() {
    if (downloadExecutor == null || downloadExecutor.isShutdown()) {
      downloadExecutor = Executors.newFixedThreadPool(DOWNLOAD_THREADS);
    }
    if (conversionExecutor == null || conversionExecutor.isShutdown()) {
      conversionExecutor = Executors.newFixedThreadPool(CONVERSION_THREADS);
    }
  }

  private void shutdownExecutors() {
    try {
      if (downloadExecutor != null) {
        downloadExecutor.shutdown();
        if (!downloadExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
          downloadExecutor.shutdownNow();
        }
      }
      if (conversionExecutor != null) {
        conversionExecutor.shutdown();
        if (!conversionExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
          conversionExecutor.shutdownNow();
        }
      }
    } catch (InterruptedException e) {
      if (downloadExecutor != null) {
        downloadExecutor.shutdownNow();
      }
      if (conversionExecutor != null) {
        conversionExecutor.shutdownNow();
      }
      Thread.currentThread().interrupt();
    }
  }

  public SecSchemaFactory() {
    LOGGER.debug("SecSchemaFactory constructor called");
    LOGGER.debug("SecSchemaFactory constructor called");
  }

  /**
   * Loads default configuration from sec-schema-factory.defaults.json resource.
   * Returns null if the resource cannot be loaded.
   */
  private Map<String, Object> loadDefaults() {
    try (InputStream is = getClass().getResourceAsStream("/sec-schema-factory.defaults.json")) {
      if (is == null) {
        LOGGER.debug("No defaults file found at /sec-schema-factory.defaults.json");
        return null;
      }

      ObjectMapper mapper = new ObjectMapper();
      JsonNode root = mapper.readTree(is);
      JsonNode operandNode = root.get("operand");
      if (operandNode == null) {
        LOGGER.debug("No 'operand' section in defaults file");
        return null;
      }

      // Convert JsonNode to Map
      Map<String, Object> defaults = mapper.convertValue(operandNode, Map.class);

      // Process environment variable substitutions
      processEnvironmentVariables(defaults);

      return defaults;
    } catch (Exception e) {
      LOGGER.warn("Failed to load defaults from sec-schema-factory.defaults.json: " + e.getMessage());
      return null;
    }
  }

  /**
   * Recursively processes a map to substitute environment variables.
   * Supports ${VAR_NAME} syntax for environment variable substitution.
   */
  private void processEnvironmentVariables(Map<String, Object> map) {
    Pattern envPattern = Pattern.compile("\\$\\{([A-Z_][A-Z0-9_]*)\\}");

    for (Map.Entry<String, Object> entry : map.entrySet()) {
      Object value = entry.getValue();

      if (value instanceof String) {
        String strValue = (String) value;
        Matcher matcher = envPattern.matcher(strValue);

        if (matcher.matches()) {
          // Full match - replace entire value
          String envVar = matcher.group(1);
          String envValue = System.getenv(envVar);

          if (envValue == null && "CURRENT_YEAR".equals(envVar)) {
            // Special handling for CURRENT_YEAR
            envValue = String.valueOf(Year.now().getValue());
          }

          if (envValue != null) {
            // Try to parse as integer if it looks like a number
            if (envValue.matches("\\d+")) {
              try {
                entry.setValue(Integer.parseInt(envValue));
              } catch (NumberFormatException e) {
                entry.setValue(envValue);
              }
            } else {
              entry.setValue(envValue);
            }
          } else {
            LOGGER.debug("Environment variable {} not set, keeping placeholder", envVar);
          }
        } else if (matcher.find()) {
          // Partial match - substitute within string
          StringBuffer sb = new StringBuffer();
          matcher.reset();
          while (matcher.find()) {
            String envVar = matcher.group(1);
            String envValue = System.getenv(envVar);

            if (envValue == null && "CURRENT_YEAR".equals(envVar)) {
              envValue = String.valueOf(Year.now().getValue());
            }

            if (envValue != null) {
              matcher.appendReplacement(sb, envValue);
            } else {
              matcher.appendReplacement(sb, matcher.group(0)); // Keep original
            }
          }
          matcher.appendTail(sb);
          entry.setValue(sb.toString());
        }
      } else if (value instanceof Map) {
        // Recursive processing for nested maps
        processEnvironmentVariables((Map<String, Object>) value);
      } else if (value instanceof List) {
        // Process lists (though we don't expect env vars in lists for SEC adapter)
        List<?> list = (List<?>) value;
        for (int i = 0; i < list.size(); i++) {
          if (list.get(i) instanceof String) {
            String strValue = (String) list.get(i);
            Matcher matcher = envPattern.matcher(strValue);
            if (matcher.matches()) {
              String envVar = matcher.group(1);
              String envValue = System.getenv(envVar);
              if (envValue == null && "CURRENT_YEAR".equals(envVar)) {
                envValue = String.valueOf(Year.now().getValue());
              }
              if (envValue != null) {
                ((List<Object>) list).set(i, envValue);
              }
            }
          }
        }
      }
    }
  }

  /**
   * Merges default values into the operand map, without overwriting existing values.
   * This ensures that explicit configuration takes precedence over defaults.
   */
  private void applyDefaults(Map<String, Object> operand, Map<String, Object> defaults) {
    if (defaults == null) {
      return;
    }

    for (Map.Entry<String, Object> entry : defaults.entrySet()) {
      String key = entry.getKey();
      Object defaultValue = entry.getValue();

      if (!operand.containsKey(key)) {
        // Key not present, add default
        operand.put(key, defaultValue);
      } else if (defaultValue instanceof Map && operand.get(key) instanceof Map) {
        // Both are maps, merge recursively
        Map<String, Object> existingMap = (Map<String, Object>) operand.get(key);
        Map<String, Object> defaultMap = (Map<String, Object>) defaultValue;
        applyDefaults(existingMap, defaultMap);
      }
      // If operand already has the key with a non-map value, keep the existing value
    }
  }

  /**
   * Defines default constraint metadata for SEC tables.
   * These constraints serve as documentation hints for query optimization.
   */
  private Map<String, Map<String, Object>> defineSecTableConstraints() {
    Map<String, Map<String, Object>> constraints = new HashMap<>();

    // financial_line_items table
    Map<String, Object> financialLineItems = new HashMap<>();
    financialLineItems.put("primaryKey", Arrays.asList("cik", "filing_type", "year", "accession_number", "concept", "period"));

    Map<String, Object> filingMetadataFK = new HashMap<>();
    filingMetadataFK.put("columns", Arrays.asList("cik", "filing_type", "year", "accession_number"));
    filingMetadataFK.put("targetTable", "filing_metadata");
    filingMetadataFK.put("targetColumns", Arrays.asList("cik", "filing_type", "year", "accession_number"));

    financialLineItems.put("foreignKeys", Arrays.asList(filingMetadataFK));
    constraints.put("financial_line_items", financialLineItems);

    // filing_metadata table
    Map<String, Object> filingMetadata = new HashMap<>();
    filingMetadata.put("primaryKey", Arrays.asList("cik", "filing_type", "year", "accession_number"));
    filingMetadata.put("unique", Arrays.asList(Arrays.asList("accession_number")));

    // Cross-domain FK to GEO schema is now handled in GovDataSchemaFactory.defineCrossDomainConstraintsForSec()
    // This ensures it's only added when both SEC and GEO schemas are present

    constraints.put("filing_metadata", filingMetadata);

    // footnotes table
    Map<String, Object> footnotes = new HashMap<>();
    footnotes.put("primaryKey", Arrays.asList("cik", "filing_type", "year", "accession_number", "footnote_id"));

    Map<String, Object> footnotesFilingFK = new HashMap<>();
    footnotesFilingFK.put("columns", Arrays.asList("cik", "filing_type", "year", "accession_number"));
    footnotesFilingFK.put("targetTable", "filing_metadata");
    footnotesFilingFK.put("targetColumns", Arrays.asList("cik", "filing_type", "year", "accession_number"));

    // Note: Relationships to financial_line_items and xbrl_relationships are conceptual
    // since footnotes reference concepts rather than specific line items
    // These are better handled as JOIN queries using the referenced_concept field

    footnotes.put("foreignKeys", Arrays.asList(footnotesFilingFK));
    constraints.put("footnotes", footnotes);

    // insider_transactions table
    Map<String, Object> insiderTransactions = new HashMap<>();
    insiderTransactions.put("primaryKey", Arrays.asList("cik", "filing_type", "year", "accession_number", "transaction_id"));

    Map<String, Object> insiderFilingFK = new HashMap<>();
    insiderFilingFK.put("columns", Arrays.asList("cik", "filing_type", "year", "accession_number"));
    insiderFilingFK.put("targetTable", "filing_metadata");
    insiderFilingFK.put("targetColumns", Arrays.asList("cik", "filing_type", "year", "accession_number"));

    insiderTransactions.put("foreignKeys", Arrays.asList(insiderFilingFK));
    constraints.put("insider_transactions", insiderTransactions);

    // stock_prices table
    Map<String, Object> stockPrices = new HashMap<>();
    stockPrices.put("primaryKey", Arrays.asList("ticker", "date"));
    stockPrices.put("unique", Arrays.asList(Arrays.asList("cik", "date")));
    constraints.put("stock_prices", stockPrices);

    // earnings_transcripts table (if present)
    Map<String, Object> earningsTranscripts = new HashMap<>();
    earningsTranscripts.put("primaryKey", Arrays.asList("cik", "filing_type", "year", "accession_number"));

    Map<String, Object> earningsFilingFK = new HashMap<>();
    earningsFilingFK.put("columns", Arrays.asList("cik", "filing_type", "year", "accession_number"));
    earningsFilingFK.put("targetTable", "filing_metadata");
    earningsFilingFK.put("targetColumns", Arrays.asList("cik", "filing_type", "year", "accession_number"));

    earningsTranscripts.put("foreignKeys", Arrays.asList(earningsFilingFK));
    constraints.put("earnings_transcripts", earningsTranscripts);

    // vectorized_blobs table
    Map<String, Object> vectorizedBlobs = new HashMap<>();
    vectorizedBlobs.put("primaryKey", Arrays.asList("cik", "filing_type", "year", "accession_number", "blob_id"));

    Map<String, Object> vectorizedFilingFK = new HashMap<>();
    vectorizedFilingFK.put("columns", Arrays.asList("cik", "filing_type", "year", "accession_number"));
    vectorizedFilingFK.put("targetTable", "filing_metadata");
    vectorizedFilingFK.put("targetColumns", Arrays.asList("cik", "filing_type", "year", "accession_number"));

    // Note: Relationships to mda_sections, footnotes, and earnings_transcripts are handled
    // via the source_table and source_id fields which contain the table name and row identifier

    vectorizedBlobs.put("foreignKeys", Arrays.asList(vectorizedFilingFK));
    constraints.put("vectorized_blobs", vectorizedBlobs);

    // mda_sections table
    Map<String, Object> mdaSections = new HashMap<>();
    mdaSections.put("primaryKey", Arrays.asList("cik", "filing_type", "year", "accession_number", "section_id"));

    Map<String, Object> mdaFilingFK = new HashMap<>();
    mdaFilingFK.put("columns", Arrays.asList("cik", "filing_type", "year", "accession_number"));
    mdaFilingFK.put("targetTable", "filing_metadata");
    mdaFilingFK.put("targetColumns", Arrays.asList("cik", "filing_type", "year", "accession_number"));

    mdaSections.put("foreignKeys", Arrays.asList(mdaFilingFK));
    constraints.put("mda_sections", mdaSections);

    // xbrl_relationships table
    Map<String, Object> xbrlRelationships = new HashMap<>();
    xbrlRelationships.put("primaryKey", Arrays.asList("cik", "filing_type", "year", "accession_number", "relationship_id"));

    Map<String, Object> xbrlFilingFK = new HashMap<>();
    xbrlFilingFK.put("columns", Arrays.asList("cik", "filing_type", "year", "accession_number"));
    xbrlFilingFK.put("targetTable", "filing_metadata");
    xbrlFilingFK.put("targetColumns", Arrays.asList("cik", "filing_type", "year", "accession_number"));

    xbrlRelationships.put("foreignKeys", Arrays.asList(xbrlFilingFK));
    constraints.put("xbrl_relationships", xbrlRelationships);

    // filing_contexts table
    Map<String, Object> filingContexts = new HashMap<>();
    filingContexts.put("primaryKey", Arrays.asList("cik", "filing_type", "year", "accession_number", "context_id"));

    Map<String, Object> contextsFilingFK = new HashMap<>();
    contextsFilingFK.put("columns", Arrays.asList("cik", "filing_type", "year", "accession_number"));
    contextsFilingFK.put("targetTable", "filing_metadata");
    contextsFilingFK.put("targetColumns", Arrays.asList("cik", "filing_type", "year", "accession_number"));

    filingContexts.put("foreignKeys", Arrays.asList(contextsFilingFK));
    constraints.put("filing_contexts", filingContexts);

    return constraints;
  }


  /**
   * Builds the operand configuration for SEC schema without creating a FileSchema instance.
   * This method is called by GovDataSchemaFactory to get SEC-specific configuration
   * that will be merged into a unified FileSchema.
   *
   * @param operand The base operand from the model file
   * @return Modified operand with SEC-specific configuration
   */
  public Map<String, Object> buildOperand(Map<String, Object> operand, StorageProvider storageProvider) {
    LOGGER.debug("SecSchemaFactory.buildOperand() called with operand: {}", operand);

    // Store the storage provider for later use
    this.storageProvider = storageProvider;
    LOGGER.debug("SEC buildOperand: storageProvider set to {}", storageProvider);

    // Create mutable copy of operand to allow modifications
    Map<String, Object> mutableOperand = new HashMap<>(operand);

    // Check auto-download setting (default true like ECON)
    Boolean autoDownload = (Boolean) operand.get("autoDownload");
    if (autoDownload == null) {
      autoDownload = true;  // Default to true like ECON
      LOGGER.debug("autoDownload not specified, defaulting to true");
    }
    mutableOperand.put("autoDownload", autoDownload);

    // Check stockPriceTtlHours setting (default 24 hours)
    Integer stockPriceTtlHours = (Integer) operand.get("stockPriceTtlHours");
    if (stockPriceTtlHours == null) {
      stockPriceTtlHours = 24;  // Default to 24 hours
      LOGGER.debug("stockPriceTtlHours not specified, defaulting to 24 hours");
    }
    mutableOperand.put("stockPriceTtlHours", stockPriceTtlHours);

    // Load and apply defaults before processing
    Map<String, Object> defaults = loadDefaults();
    if (defaults != null) {
      LOGGER.debug("Applying defaults from sec-schema-factory.defaults.json");
      applyDefaults(mutableOperand, defaults);
      LOGGER.debug("Operand after applying defaults: {}", mutableOperand);
    }

    this.currentOperand = mutableOperand; // Store for table auto-discovery

    // Get cache directories from interface methods
    String govdataCacheDir = getGovDataCacheDir();
    String govdataParquetDir = getGovDataParquetDir();

    // Check required environment variables
    if (govdataCacheDir == null || govdataCacheDir.isEmpty()) {
      throw new IllegalStateException("GOVDATA_CACHE_DIR environment variable must be set");
    }
    if (govdataParquetDir == null || govdataParquetDir.isEmpty()) {
      throw new IllegalStateException("GOVDATA_PARQUET_DIR environment variable must be set");
    }

    // SEC data directories
    String secRawDir = govdataCacheDir + "/sec";
    String secParquetDir = govdataParquetDir + "/source=sec";

    // Determine cache directory
    String configuredDir = (String) mutableOperand.get("directory");
    if (configuredDir == null) {
      configuredDir = (String) mutableOperand.get("cacheDirectory");
    }

    // Use GOVDATA_PARQUET_DIR if available, otherwise fall back to configured or default
    String cacheHome;
    if (govdataCacheDir != null && govdataParquetDir != null) {
      // Raw XBRL data goes to GOVDATA_CACHE_DIR/sec
      // Parquet data goes to GOVDATA_PARQUET_DIR/source=sec
      cacheHome = secRawDir; // For raw XBRL data
      LOGGER.info("Using unified govdata directories - cache: {}, parquet: {}/source=sec", govdataCacheDir, govdataParquetDir);
    } else {
      cacheHome = configuredDir != null ? configuredDir : secRawDir;
    }

    // Handle SEC data download if configured
    LOGGER.debug("SEC buildOperand: checking download with autoDownload={}", mutableOperand.get("autoDownload"));
    LOGGER.debug("About to check shouldDownloadData");
    LOGGER.debug("Checking shouldDownloadData...");
    if (shouldDownloadData(mutableOperand)) {
      LOGGER.debug("SEC: shouldDownloadData = true, calling downloadSecData");
      // Get base directory from operand
      if (configuredDir != null) {
        File baseDir = new File(configuredDir);
        // Rebuild manifest from existing files first
        rebuildManifestFromExistingFiles(baseDir);
      }
      downloadSecData(mutableOperand);

      // CRITICAL: Wait for all downloads and conversions to complete
      // The downloadSecData method starts async tasks but returns immediately
      // We need to wait for them to complete before creating the FileSchema
      waitForAllConversions();
    } else {
      LOGGER.debug("SEC: shouldDownloadData = false, skipping download (autoDownload={}, useMockData={})",
                  mutableOperand.get("autoDownload"), mutableOperand.get("useMockData"));
    }

    // Pre-define partitioned tables using the partitionedTables format
    // These will be resolved by FileSchemaFactory after downloads complete
    List<Map<String, Object>> partitionedTables = new ArrayList<>();

    // Define partition configuration (shared by both tables)
    Map<String, Object> partitionConfig = new HashMap<>();
    partitionConfig.put("style", "hive");  // Use Hive-style partitioning (key=value)

    // Define partition columns
    List<Map<String, Object>> columnDefs = new ArrayList<>();

    Map<String, Object> cikCol = new HashMap<>();
    cikCol.put("name", "cik");
    cikCol.put("type", "VARCHAR");
    columnDefs.add(cikCol);

    Map<String, Object> filingTypeCol = new HashMap<>();
    filingTypeCol.put("name", "filing_type");
    filingTypeCol.put("type", "VARCHAR");
    columnDefs.add(filingTypeCol);

    Map<String, Object> yearCol = new HashMap<>();
    yearCol.put("name", "year");
    yearCol.put("type", "INTEGER");
    columnDefs.add(yearCol);

    partitionConfig.put("columnDefinitions", columnDefs);

    // Define financial_line_items as a partitioned table with constraints
    Map<String, Object> financialLineItems = new HashMap<>();
    financialLineItems.put("name", "financial_line_items");
    financialLineItems.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_facts.parquet");
    financialLineItems.put("partitions", partitionConfig);

    // Add constraint definitions if enabled
    Boolean enableConstraints = (Boolean) mutableOperand.get("enableConstraints");
    if (enableConstraints == null || enableConstraints) {
      Map<String, Object> constraints = new HashMap<>();
      // Primary key: (cik, filing_type, year, filing_date, concept, context_ref)
      constraints.put("primaryKey", Arrays.asList("cik", "filing_type", "year",
          "filing_date", "concept", "context_ref"));

      // Foreign key to filing_contexts table
      Map<String, Object> fk = new HashMap<>();
      fk.put("sourceTable", Arrays.asList("SEC", "financial_line_items"));
      fk.put("columns", Arrays.asList("cik", "filing_type", "year", "filing_date", "context_ref"));
      fk.put("targetTable", Arrays.asList("SEC", "filing_contexts"));
      fk.put("targetColumns", Arrays.asList("cik", "filing_type", "year", "filing_date", "context_id"));
      constraints.put("foreignKeys", Arrays.asList(fk));

      financialLineItems.put("constraints", constraints);
    }

    partitionedTables.add(financialLineItems);

    // Define filing_metadata as a partitioned table with filing-level information
    Map<String, Object> filingMetadata = new HashMap<>();
    filingMetadata.put("name", "filing_metadata");
    filingMetadata.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_metadata.parquet");
    filingMetadata.put("partitions", partitionConfig);

    if (enableConstraints == null || enableConstraints) {
      Map<String, Object> constraints = new HashMap<>();
      // Primary key: (cik, filing_type, year, accession_number)
      constraints.put("primaryKey", Arrays.asList("cik", "filing_type", "year", "accession_number"));
      constraints.put("unique", Arrays.asList(Arrays.asList("accession_number")));

      // Cross-domain FK to GEO schema is now handled in GovDataSchemaFactory.defineCrossDomainConstraintsForSec()
      // This ensures it's only added when both SEC and GEO schemas are present

      filingMetadata.put("constraints", constraints);
    }

    partitionedTables.add(filingMetadata);

    // Define filing_contexts as a partitioned table with constraints
    Map<String, Object> filingContexts = new HashMap<>();
    filingContexts.put("name", "filing_contexts");
    filingContexts.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_contexts.parquet");
    filingContexts.put("partitions", partitionConfig);

    if (enableConstraints == null || enableConstraints) {
      Map<String, Object> constraints = new HashMap<>();
      // Primary key: (cik, filing_type, year, filing_date, context_id)
      constraints.put("primaryKey", Arrays.asList("cik", "filing_type", "year",
          "filing_date", "context_id"));
      filingContexts.put("constraints", constraints);
    }

    partitionedTables.add(filingContexts);

    // Define mda_sections as a partitioned table for MD&A paragraphs
    Map<String, Object> mdaSections = new HashMap<>();
    mdaSections.put("name", "mda_sections");
    mdaSections.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_mda.parquet");
    mdaSections.put("partitions", partitionConfig);
    if (enableConstraints == null || enableConstraints) {
      Map<String, Object> constraints = new HashMap<>();
      constraints.put("primaryKey", Arrays.asList("cik", "filing_type", "year",
          "filing_date", "section_id"));
      mdaSections.put("constraints", constraints);
    }
    partitionedTables.add(mdaSections);

    // Define xbrl_relationships as a partitioned table
    Map<String, Object> xbrlRelationships = new HashMap<>();
    xbrlRelationships.put("name", "xbrl_relationships");
    xbrlRelationships.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_relationships.parquet");
    xbrlRelationships.put("partitions", partitionConfig);
    if (enableConstraints == null || enableConstraints) {
      Map<String, Object> constraints = new HashMap<>();
      constraints.put("primaryKey", Arrays.asList("cik", "filing_type", "year",
          "filing_date", "relationship_id"));
      xbrlRelationships.put("constraints", constraints);
    }
    partitionedTables.add(xbrlRelationships);

    // Define insider_transactions table for Forms 3, 4, 5
    Map<String, Object> insiderTransactions = new HashMap<>();
    insiderTransactions.put("name", "insider_transactions");
    insiderTransactions.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_insider.parquet");
    insiderTransactions.put("partitions", partitionConfig);
    if (enableConstraints == null || enableConstraints) {
      Map<String, Object> constraints = new HashMap<>();
      constraints.put("primaryKey", Arrays.asList("cik", "filing_type", "year",
          "filing_date", "transaction_id"));
      insiderTransactions.put("constraints", constraints);
    }
    partitionedTables.add(insiderTransactions);

    // Define earnings_transcripts table for 8-K exhibits
    Map<String, Object> earningsTranscripts = new HashMap<>();
    earningsTranscripts.put("name", "earnings_transcripts");
    earningsTranscripts.put("pattern", "cik=*/filing_type=*/year=*/[!.]*_earnings.parquet");
    earningsTranscripts.put("partitions", partitionConfig);
    if (enableConstraints == null || enableConstraints) {
      Map<String, Object> constraints = new HashMap<>();
      constraints.put("primaryKey", Arrays.asList("cik", "filing_type", "year",
          "filing_date", "transcript_id"));
      earningsTranscripts.put("constraints", constraints);
    }
    partitionedTables.add(earningsTranscripts);

    // Define stock_prices table for daily EOD prices
    Map<String, Object> stockPricesPartitionConfig = new HashMap<>();
    stockPricesPartitionConfig.put("style", "hive");  // Use Hive-style partitioning (key=value)

    // Define partition columns for stock_prices (ticker and year only)
    // CIK will be a regular column in the Parquet files for joins
    List<Map<String, Object>> stockPricesColumnDefs = new ArrayList<>();

    Map<String, Object> tickerCol = new HashMap<>();
    tickerCol.put("name", "ticker");
    tickerCol.put("type", "VARCHAR");
    stockPricesColumnDefs.add(tickerCol);

    Map<String, Object> stockYearCol = new HashMap<>();
    stockYearCol.put("name", "year");
    stockYearCol.put("type", "INTEGER");
    stockPricesColumnDefs.add(stockYearCol);

    stockPricesPartitionConfig.put("columnDefinitions", stockPricesColumnDefs);

    Map<String, Object> stockPrices = new HashMap<>();
    stockPrices.put("name", "stock_prices");
    stockPrices.put("pattern", "stock_prices/ticker=*/year=*/[!.]*.parquet");
    stockPrices.put("partitions", stockPricesPartitionConfig);
    partitionedTables.add(stockPrices);

    // Define company_info as a single file (non-partitioned but in partitionedTables for consistency)
    Map<String, Object> companyInfo = new HashMap<>();
    companyInfo.put("name", "company_info");
    companyInfo.put("pattern", "[!.]company_info.parquet");
    // No partitions config needed - this is a single file
    partitionedTables.add(companyInfo);

    // Add vectorized_blobs table when text similarity is enabled
    Map<String, Object> textSimilarityConfig = (Map<String, Object>) operand.get("textSimilarity");
    if (textSimilarityConfig != null && Boolean.TRUE.equals(textSimilarityConfig.get("enabled"))) {
      Map<String, Object> vectorizedBlobsTable = new HashMap<>();
      vectorizedBlobsTable.put("name", "vectorized_blobs");
      vectorizedBlobsTable.put("pattern", "cik=*/filing_type=*/year=*/*_vectorized.parquet");
      vectorizedBlobsTable.put("partitions", partitionConfig);
      partitionedTables.add(vectorizedBlobsTable);
      LOGGER.debug("Added vectorized_blobs table configuration for text similarity");
    }

    // Add partitioned tables to operand
    mutableOperand.put("partitionedTables", partitionedTables);

    // Set the directory for FileSchemaFactory to search
    // Use the unified secParquetDir directly
    mutableOperand.put("directory", secParquetDir);

    LOGGER.debug("Pre-defined {} partitioned table patterns", partitionedTables.size());

    // Ensure the parquet directory exists
    File secParquetDirFile = new File(secParquetDir);

    if (secParquetDirFile.exists() && secParquetDirFile.isDirectory()) {
      LOGGER.info("Using SEC parquet cache directory: {}", secParquetDirFile.getAbsolutePath());

      // Debug: List all .parquet files
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("DEBUG: Listing all .parquet files in directory:");
        File[] parquetFiles = secParquetDirFile.listFiles((dir, fileName) -> fileName.endsWith(".parquet"));
        if (parquetFiles != null && parquetFiles.length > 0) {
          for (File f : parquetFiles) {
            LOGGER.debug("DEBUG: Found table file: {} (size={})", f.getName(), f.length());
          }
        } else {
          LOGGER.warn("DEBUG: No .parquet files found in {}", secParquetDirFile.getAbsolutePath());
        }
      }
    } else {
      // Parquet dir doesn't exist yet, but still use secParquetDir path
      // FileSchemaFactory will handle the non-existent directory
      LOGGER.info("SEC parquet directory doesn't exist yet: {}", secParquetDir);
    }

    // Don't set execution engine - let GovDataSchemaFactory control it
    // The parent factory will set it based on global configuration

    // Preserve text similarity configuration if present, or enable it by default
    // This ensures COSINE_SIMILARITY and other vector functions are registered
    Map<String, Object> textSimConfig = (Map<String, Object>) mutableOperand.get("textSimilarity");
    if (textSimConfig == null) {
      textSimConfig = new HashMap<>();
      textSimConfig.put("enabled", true);
      mutableOperand.put("textSimilarity", textSimConfig);
    } else if (!Boolean.TRUE.equals(textSimConfig.get("enabled"))) {
      // Ensure it's enabled even if config exists but disabled
      textSimConfig.put("enabled", true);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Text similarity functions configuration: {}", textSimConfig);
    }

    // Start RSS monitor if configured
    Map<String, Object> refreshConfig = (Map<String, Object>) mutableOperand.get("refreshMonitoring");
    if (refreshConfig != null && Boolean.TRUE.equals(refreshConfig.get("enabled"))) {
      if (rssMonitor == null) {
        LOGGER.info("Starting RSS refresh monitor");
        rssMonitor = new RSSRefreshMonitor(mutableOperand);
        rssMonitor.start();
      }
    }

    // Merge default constraints with provided constraints
    Map<String, Map<String, Object>> allConstraints = defineSecTableConstraints();

    // Add any model-provided constraints, overriding defaults where specified
    if (!tableConstraints.isEmpty()) {
      LOGGER.debug("Merging {} model-provided constraints with defaults", tableConstraints.size());
      for (Map.Entry<String, Map<String, Object>> entry : tableConstraints.entrySet()) {
        String tableName = entry.getKey();
        Map<String, Object> modelConstraints = entry.getValue();

        if (allConstraints.containsKey(tableName)) {
          // Merge with existing defaults
          Map<String, Object> defaultConstraints = allConstraints.get(tableName);
          Map<String, Object> merged = new HashMap<>(defaultConstraints);
          merged.putAll(modelConstraints);  // Model constraints override defaults
          allConstraints.put(tableName, merged);
        } else {
          // New table not in defaults
          allConstraints.put(tableName, modelConstraints);
        }
      }
    }

    // Add constraint metadata to operand if we have any
    if (!allConstraints.isEmpty()) {
      LOGGER.debug("Adding constraint metadata for {} tables to operand", allConstraints.size());
      mutableOperand.put("tableConstraints", allConstraints);
    }

    // Return the configured operand for GovDataSchemaFactory to use
    LOGGER.info("SEC schema operand configuration complete");
    return mutableOperand;
  }


  private boolean shouldDownloadData(Map<String, Object> operand) {
    Boolean autoDownload = (Boolean) operand.get("autoDownload");
    Boolean useMockData = (Boolean) operand.get("useMockData");
    boolean result = (autoDownload != null && autoDownload) || (useMockData != null && useMockData);
    LOGGER.debug("shouldDownloadData: autoDownload={}, useMockData={}, result={}", autoDownload, useMockData, result);
    return result;
  }

  /**
   * Wait for all downloads and conversions to complete.
   * This is critical to ensure the FileSchema sees the converted Parquet files.
   */
  private void waitForAllConversions() {
    try {
      // Wait for download futures if any are still running
      if (!filingProcessingFutures.isEmpty()) {
        LOGGER.info("Waiting for {} filing tasks to complete...", filingProcessingFutures.size());
        CompletableFuture<Void> allDownloads =
            CompletableFuture.allOf(filingProcessingFutures.toArray(new CompletableFuture[0]));
        allDownloads.get(Integer.MAX_VALUE, TimeUnit.MINUTES);
        LOGGER.info("All downloads completed");
      }

      // Wait for conversion futures if any are still running
      if (!conversionFutures.isEmpty()) {
        LOGGER.info("Waiting for {} conversions to complete...", conversionFutures.size());
        CompletableFuture<Void> allConversions =
            CompletableFuture.allOf(conversionFutures.toArray(new CompletableFuture[0]));
        allConversions.get(30, TimeUnit.MINUTES);
        LOGGER.info("All conversions completed: {} files", completedConversions.get());
      }
    } catch (Exception e) {
      LOGGER.warn("Error waiting for downloads/conversions to complete: " + e.getMessage());
    }
  }

  private void addToManifest(File baseDirectory, File xbrlFile, File secParquetDir) {
    try {
      // Extract CIK and accession from file path
      // Path structure: baseDir/sec-cache/raw/{cik}/{accession}/{file}
      File accessionDir = xbrlFile.getParentFile();
      File cikDir = accessionDir.getParentFile();

      String cik = cikDir.getName();
      String accession = accessionDir.getName();
      // Format accession with dashes
      if (accession.length() == 18) {
        accession = accession.substring(0, 10) + "-" +
                   accession.substring(10, 12) + "-" +
                   accession.substring(12);
      }

      // Check which files were actually created for this accession
      boolean hasVectorized = false;
      Map<String, Object> textSimilarityConfig = (Map<String, Object>) currentOperand.get("textSimilarity");
      if (textSimilarityConfig != null && Boolean.TRUE.equals(textSimilarityConfig.get("enabled"))) {
        // Check if vectorized file exists for this accession in the parquet directory
        File parquetDir = secParquetDir;
        if (parquetDir.exists()) {
          // Look for vectorized files with this accession number
          String accessionNoHyphens = accession.replace("-", "");
          File[] cikDirs = parquetDir.listFiles((dir, name) -> name.startsWith("cik="));
          for (File ckDir : cikDirs != null ? cikDirs : new File[0]) {
            File[] filingTypeDirs = ckDir.listFiles((dir, name) -> name.startsWith("filing_type="));
            for (File ftDir : filingTypeDirs != null ? filingTypeDirs : new File[0]) {
              File[] yearDirs = ftDir.listFiles((dir, name) -> name.startsWith("year="));
              for (File yDir : yearDirs != null ? yearDirs : new File[0]) {
                File[] vectorizedFiles = yDir.listFiles((dir, name) ->
                    name.equals(cik + "_" + accessionNoHyphens + "_vectorized.parquet"));
                if (vectorizedFiles != null && vectorizedFiles.length > 0) {
                  hasVectorized = true;
                  break;
                }
              }
              if (hasVectorized) break;
            }
            if (hasVectorized) break;
          }
        }
      }

      // Include vectorized status in manifest key
      String fileTypes = hasVectorized ? "PROCESSED_WITH_VECTORS" : "PROCESSED";
      String manifestKey = cik + "|" + accession + "|" + fileTypes + "|" + System.currentTimeMillis();

      File manifestFile = new File(baseDirectory, "processed_filings.manifest");
      synchronized (SecSchemaFactory.class) {
        try (PrintWriter pw = new PrintWriter(new FileOutputStream(manifestFile, true))) {
          pw.println(manifestKey);
        }
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Added to manifest after Parquet conversion: {} (vectorized={})", manifestKey, hasVectorized);
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Could not update manifest: " + e.getMessage());
    }
  }


  private void rebuildManifestFromExistingFiles(File baseDirectory) {
    // Rebuild manifest from existing files on startup
    File manifestFile = new File(baseDirectory, "processed_filings.manifest");
    if (manifestFile.exists()) {
      try {
        long count = Files.lines(manifestFile.toPath()).count();
        LOGGER.debug("Manifest already exists with {} entries", count);
        return;
      } catch (Exception e) {
        LOGGER.warn("Could not read manifest: " + e.getMessage());
      }
    }

    LOGGER.debug("Building manifest from existing files...");
    Set<String> processedFilings = new HashSet<>();

    File secCacheDir = new File(baseDirectory, "sec-cache");
    if (secCacheDir.exists() && secCacheDir.isDirectory()) {
      File rawDir = new File(secCacheDir, "raw");
      if (rawDir.exists()) {
        // Scan all CIK directories
        File[] cikDirs = rawDir.listFiles(File::isDirectory);
        if (cikDirs != null) {
          for (File cikDir : cikDirs) {
            String cik = cikDir.getName();
            // Scan all accession directories
            File[] accessionDirs = cikDir.listFiles(File::isDirectory);
            if (accessionDirs != null) {
              for (File accessionDir : accessionDirs) {
                String accession = accessionDir.getName();
                // Format accession with dashes
                if (accession.length() == 18) {
                  accession = accession.substring(0, 10) + "-" +
                             accession.substring(10, 12) + "-" +
                             accession.substring(12);
                }

                // Check for HTML files (indicates a filing was downloaded)
                // Exclude macOS metadata files
                File[] htmlFiles = accessionDir.listFiles((dir, name) ->
                    !name.startsWith("._") && (name.endsWith(".htm") || name.endsWith(".html")));
                if (htmlFiles != null && htmlFiles.length > 0) {
                  // For now, add with generic form/date - actual values would need parsing
                  String manifestKey = cik + "|" + accession + "|UNKNOWN|UNKNOWN";
                  processedFilings.add(manifestKey);
                }
              }
            }
          }
        }
      }
    }

    // Write manifest
    if (!processedFilings.isEmpty()) {
      try (PrintWriter pw = new PrintWriter(manifestFile)) {
        for (String filing : processedFilings) {
          pw.println(filing);
        }
        LOGGER.debug("Created manifest with {} existing filings", processedFilings.size());
      } catch (Exception e) {
        LOGGER.warn("Could not create manifest: " + e.getMessage());
      }
    }
  }

  /**
   * Public static method to trigger SEC data download from external components like RSS monitor.
   */
  public static void triggerDownload(Map<String, Object> operand) {
    SecSchemaFactory factory = new SecSchemaFactory();
    factory.downloadSecData(operand);
  }

  private void downloadSecData(Map<String, Object> operand) {
    LOGGER.info("Starting SEC data download");
    LOGGER.debug("downloadSecData() called - STretun ARTING SEC DATA DOWNLOAD");

    // Clear download tracking for new cycle
    downloadedInThisCycle.clear();

    try {
      // Check if directory is specified in config, otherwise use default
      String configuredDir = (String) operand.get("directory");
      if (configuredDir == null) {
        configuredDir = (String) operand.get("cacheDirectory");
      }
      // Get cache directories from interface methods
      String govdataCacheDir = getGovDataCacheDir();
      String secRawDir = govdataCacheDir != null ? govdataCacheDir + "/sec" : null;
      String cacheHome = configuredDir != null ? configuredDir : secRawDir;

      // XBRL files are immutable - once downloaded, they never change
      // Use configured cache directory
      File baseDir = new File(cacheHome, "sec-data");
      baseDir.mkdirs();

      LOGGER.info("Using SEC cache directory: {}", baseDir.getAbsolutePath());

      // Update operand to use our cache directory
      operand.put("directory", baseDir.getAbsolutePath());

      // Check if we should use mock data instead of downloading
      Boolean useMockData = (Boolean) operand.get("useMockData");
      if (useMockData != null && useMockData) {
        LOGGER.info("Using mock data for testing");
        List<String> ciks = getCiksFromConfig(operand);
        int startYear = (Integer) operand.getOrDefault("startYear", 2020);
        int endYear = (Integer) operand.getOrDefault("endYear", 2023);
        LOGGER.debug("DEBUG: Calling createSecTablesFromXbrl for mock data");
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("DEBUG: baseDir={}", baseDir.getAbsolutePath());
        }
        // Create minimal mock Parquet files so tables are discovered
        downloadStockPrices(baseDir, Arrays.asList("320187", "51143", "789019"), 2021, 2024);
        LOGGER.debug("DEBUG: Mock data mode - created mock Parquet files");

        // Also create mock stock prices if enabled
        boolean fetchStockPrices = (Boolean) operand.getOrDefault("fetchStockPrices", true);
        if (fetchStockPrices) {
          LOGGER.debug("Creating mock stock prices for testing");
          createMockStockPrices(baseDir, ciks, startYear, endYear);
        }

        return;
      }

      // Get CIKs to download
      List<String> ciks = getCiksFromConfig(operand);
      LOGGER.debug("CIKs: {}", ciks);
      List<String> filingTypes = getFilingTypes(operand);
      LOGGER.debug("Filing types: {}", filingTypes);
      int startYear = (Integer) operand.getOrDefault("startYear", 2020);
      int endYear = (Integer) operand.getOrDefault("endYear", 2023);
      LOGGER.debug("Year range: {} - {}", startYear, endYear);

      // Use SEC storage provider to download filings
      SecHttpStorageProvider provider = SecHttpStorageProvider.forEdgar();

      for (String cik : ciks) {
        downloadCikFilings(provider, cik, filingTypes, startYear, endYear, baseDir);
      }

      // Wait for all downloads AND immediate processing to complete
      if (!filingProcessingFutures.isEmpty()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Waiting for {} filing download and processing tasks to complete...", filingProcessingFutures.size());
        }
        CompletableFuture<Void> allTasks =
            CompletableFuture.allOf(filingProcessingFutures.toArray(new CompletableFuture[0]));
        try {
          allTasks.get(Integer.MAX_VALUE, TimeUnit.MINUTES); // Effectively no timeout
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("All filing processing completed: {} files processed", completedFilingProcessing.get());
          }
        } catch (Exception e) {
          LOGGER.warn("Some processing tasks may have failed: " + e.getMessage());
        }
      }

      // Process any remaining files that weren't converted inline (backward compatibility)
      LOGGER.info("Processing any remaining XBRL files");
      createSecTablesFromXbrl(baseDir, ciks, startYear, endYear);

      // Download or create stock prices if enabled
      boolean fetchStockPrices = (Boolean) operand.getOrDefault("fetchStockPrices", true);
      Boolean testModeObj = (Boolean) operand.get("testMode");
      boolean isTestMode = testModeObj != null && testModeObj;
      if (fetchStockPrices) {
        if (isTestMode) {
          LOGGER.debug("Creating mock stock prices for testing (testMode=true)");
          createMockStockPrices(baseDir, ciks, startYear, endYear);
        } else {
          LOGGER.info("Downloading stock prices for configured CIKs");
          downloadStockPrices(baseDir, ciks, startYear, endYear);
        }
      }

    } catch (Exception e) {
      LOGGER.error("Error in downloadSecData", e);
      LOGGER.warn("Failed to download SEC data: " + e.getMessage());
    }
  }

  private void downloadCikFilings(SecHttpStorageProvider provider, String cik,
      List<String> filingTypes, int startYear, int endYear, File baseDir) {
    // Ensure executors are initialized
    initializeExecutors();

    try {
      // Normalize CIK
      String normalizedCik = String.format("%010d", Long.parseLong(cik.replaceAll("[^0-9]", "")));

      // Create CIK directory in the configured cache location
      // Use the baseDir passed in from downloadSecData
      File cikDir = new File(baseDir, normalizedCik);
      cikDir.mkdirs();

      // Download submissions metadata first (with rate limiting)
      String submissionsUrl =
          String.format("https://data.sec.gov/submissions/CIK%s.json", normalizedCik);
      File submissionsFile = new File(cikDir, "submissions.json");

      // Apply rate limiting
      try {
        rateLimiter.acquire();
        Thread.sleep(currentRateLimitDelayMs.get()); // Use dynamic rate limit delay
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      try (InputStream is = provider.openInputStream(submissionsUrl)) {
        try (FileOutputStream fos = new FileOutputStream(submissionsFile)) {
          byte[] buffer = new byte[8192];
          int bytesRead;
          while ((bytesRead = is.read(buffer)) != -1) {
            fos.write(buffer, 0, bytesRead);
          }
        }
        // Clean up macOS metadata files after writing
        storageProvider.cleanupMacosMetadata(cikDir.getAbsolutePath());
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Downloaded submissions metadata for CIK {}", normalizedCik);
        }
      } finally {
        rateLimiter.release();
      }

      // Parse submissions.json to get filing details
      ObjectMapper mapper = new ObjectMapper();
      JsonNode submissionsJson = mapper.readTree(submissionsFile);
      JsonNode filings = submissionsJson.get("filings");

      if (filings == null || !filings.has("recent")) {
        LOGGER.warn("No recent filings found in submissions for CIK " + normalizedCik);
        return;
      }

      JsonNode recent = filings.get("recent");
      JsonNode accessionNumbers = recent.get("accessionNumber");
      JsonNode filingDates = recent.get("filingDate");
      JsonNode forms = recent.get("form");
      JsonNode primaryDocuments = recent.get("primaryDocument");

      if (accessionNumbers == null || !accessionNumbers.isArray()) {
        LOGGER.warn("No accession numbers found for CIK " + normalizedCik);
        return;
      }

      // Collect all filings to download
      List<FilingToDownload> filingsToDownload = new ArrayList<>();
      for (int i = 0; i < accessionNumbers.size(); i++) {
        String accession = accessionNumbers.get(i).asText();
        String filingDate = filingDates.get(i).asText();
        String form = forms.get(i).asText();
        String primaryDoc = primaryDocuments.get(i).asText();

        // Check if filing type matches and is in date range
        // Normalize form comparison (SEC uses "10K", config uses "10-K")
        String normalizedForm = form.replace("-", "");

        // Use explicit whitelist for allowed forms (excludes 424B forms)
        List<String> allowedForms = Arrays.asList(
            "3", "4", "5",           // Insider forms
            "10-K", "10K",           // Annual reports
            "10-Q", "10Q",           // Quarterly reports
            "8-K", "8K",             // Current reports
            "8-K/A", "8KA",          // Amended current reports
            "DEF 14A", "DEF14A",     // Proxy statements
            "S-3", "S3",             // Registration statements
            "S-4", "S4",             // Business combination registration
            "S-8", "S8"              // Employee benefit plan registration
        );
        boolean matchesType = allowedForms.stream()
            .anyMatch(type -> type.replace("-", "").equalsIgnoreCase(normalizedForm));
        if (!matchesType) {
          continue;
        }

        int filingYear = Integer.parseInt(filingDate.substring(0, 4));
        if (filingYear < startYear || filingYear > endYear) {
          continue;
        }

        filingsToDownload.add(
            new FilingToDownload(normalizedCik, accession,
            primaryDoc, form, filingDate, cikDir));
      }

      // Download filings in parallel with rate limiting
      totalFilingsToProcess.addAndGet(filingsToDownload.size());
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Scheduling {} filings for processing (cache check/download) for CIK {}", filingsToDownload.size(), normalizedCik);
      }

      for (FilingToDownload filing : filingsToDownload) {
        // Create unique key for deduplication
        String filingKey = filing.cik + "|" + filing.accession + "|" + filing.form + "|" + filing.filingDate;

        // Skip if already scheduled for download in this cycle
        if (!downloadedInThisCycle.add(filingKey)) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Skipping duplicate download task for: {} {}", filing.form, filing.filingDate);
          }
          continue;
        }

        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
          downloadFilingDocumentWithRateLimit(provider, filing);
          int completed = completedFilingProcessing.incrementAndGet();
          if (completed % 10 == 0) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Processing progress: {}/{} filings (checking cache and downloading if needed)", completed, totalFilingsToProcess.get());
            }
          }
        }, downloadExecutor);
        filingProcessingFutures.add(future);
      }

    } catch (Exception e) {
      LOGGER.warn("Failed to download filings for CIK " + cik + ": " + e.getMessage());
      e.printStackTrace();
    }
  }

  // Helper class to hold filing download information
  private static class FilingToDownload {
    final String cik;
    final String accession;
    final String primaryDoc;
    final String form;
    final String filingDate;
    final File cikDir;

    FilingToDownload(String cik, String accession, String primaryDoc,
        String form, String filingDate, File cikDir) {
      this.cik = cik;
      this.accession = accession;
      this.primaryDoc = primaryDoc;
      this.form = form;
      this.filingDate = filingDate;
      this.cikDir = cikDir;
    }
  }

  // Enum for filing status
  private enum FilingStatus {
    FULLY_PROCESSED,      // In manifest with all required files
    HAS_ALL_PARQUET,      // Has all Parquet files but not in manifest
    NEEDS_PROCESSING,     // Missing Parquet files or source files
    EXCLUDED_424B         // 424B forms are excluded from processing
  }

  // Check filing status - manifest, Parquet files, source files
  private FilingStatus checkFilingStatus(String cik, String accession, String form,
                                          String filingDate, File manifestFile) {
    try {
      // Skip checking entirely for 424B forms - they are excluded from processing
      if (form.startsWith("424B")) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("424B filing excluded from processing: {} {}", form, filingDate);
        }
        return FilingStatus.EXCLUDED_424B;
      }

      // Check manifest first
      if (manifestFile.exists()) {
        Set<String> processedFilings = new HashSet<>(Files.readAllLines(manifestFile.toPath()));

        boolean isInManifest = false;
        boolean hasVectorized = false;
        for (String entry : processedFilings) {
          if (entry.startsWith(cik + "|" + accession + "|")) {
            isInManifest = true;
            hasVectorized = entry.contains("PROCESSED_WITH_VECTORS");
            break;
          }
        }

        // Check if text similarity is enabled
        Map<String, Object> textSimilarityConfig = (Map<String, Object>) currentOperand.get("textSimilarity");
        boolean needsVectorized = textSimilarityConfig != null &&
            Boolean.TRUE.equals(textSimilarityConfig.get("enabled"));

        if (isInManifest && (!needsVectorized || hasVectorized)) {
          return FilingStatus.FULLY_PROCESSED;
        }
      }

      // DISABLED: Cannot reliably check Parquet files before parsing XBRL because fiscal year
      // (used for partitioning) is only known after parsing XBRL data. The heuristic-based
      // getPartitionYear() method produces incorrect year folders, causing false cache misses.
      // Solution: Rely on manifest file only. After processing, files are added to manifest.
      // if (hasAllParquetFiles(cik, accession, form, filingDate)) {
      //   return FilingStatus.HAS_ALL_PARQUET;
      // }

      return FilingStatus.NEEDS_PROCESSING;
    } catch (Exception e) {
      LOGGER.debug("Error checking filing status: " + e.getMessage());
      return FilingStatus.NEEDS_PROCESSING;
    }
  }

  // Check if all required Parquet files exist for a filing
  private boolean hasAllParquetFiles(String cik, String accession, String form, String filingDate) {
    try {
      String year = getPartitionYear(form, filingDate);
      String govdataParquetDir = getGovDataParquetDir();
      File parquetDir = new File(govdataParquetDir, "source=sec");
      File cikParquetDir = new File(parquetDir, "cik=" + cik);
      File filingTypeDir = new File(cikParquetDir, "filing_type=" + form.replace("-", ""));
      File yearDir = new File(filingTypeDir, "year=" + year);

      String accessionClean = accession.replace("-", "");
      boolean isInsiderForm = form.matches("[345]");

      // Check primary file
      String filenameSuffix = isInsiderForm ? "insider" : "facts";
      String primaryParquetPath = storageProvider.resolvePath(yearDir.getPath(),
          String.format("%s_%s_%s.parquet", cik, accessionClean, filenameSuffix));

      try {
        StorageProvider.FileMetadata primaryMetadata = storageProvider.getMetadata(primaryParquetPath);
        if (primaryMetadata.getSize() == 0) {
          return false;
        }
      } catch (IOException e) {
        return false;
      }

      // Check relationships file for non-insider forms
      if (!isInsiderForm) {
        String relationshipsParquetPath = storageProvider.resolvePath(yearDir.getPath(),
            String.format("%s_%s_relationships.parquet", cik, accessionClean));
        try {
          storageProvider.getMetadata(relationshipsParquetPath);
          // File exists - relationships file can be empty
        } catch (IOException e) {
          return false;
        }
      }

      // Check vectorized file if enabled
      Map<String, Object> textSimilarityConfig = (Map<String, Object>) currentOperand.get("textSimilarity");
      if (textSimilarityConfig != null && Boolean.TRUE.equals(textSimilarityConfig.get("enabled"))) {
        if (supportsVectorization(form)) {
          String vectorizedPath = storageProvider.resolvePath(yearDir.getPath(),
              String.format("%s_%s_vectorized.parquet", cik, accessionClean));
          try {
            StorageProvider.FileMetadata vectorizedMetadata = storageProvider.getMetadata(vectorizedPath);
            if (vectorizedMetadata.getSize() == 0) {
              return false;
            }
          } catch (IOException e) {
            return false;
          }
        }
      }

      return true;
    } catch (Exception e) {
      LOGGER.debug("Error checking Parquet files: " + e.getMessage());
      return false;
    }
  }

  // Process a single filing immediately after download
  private void processSingleFiling(File sourceFile, File baseDir) {
    try {
      // Get parquet directory from interface method
      String govdataParquetDir = getGovDataParquetDir();
      File secParquetDir = new File(govdataParquetDir + "/source=sec");
      secParquetDir.mkdirs();

      // Check if text similarity is enabled from operand
      Map<String, Object> textSimilarityConfig = (Map<String, Object>) currentOperand.get("textSimilarity");
      boolean enableVectorization = textSimilarityConfig != null &&
          Boolean.TRUE.equals(textSimilarityConfig.get("enabled"));

      XbrlToParquetConverter converter = new XbrlToParquetConverter(this.storageProvider, enableVectorization);

      // Extract accession from file path
      String accession = sourceFile.getParentFile().getName();

      // Create metadata
      ConversionMetadata metadata = new ConversionMetadata(secParquetDir);
      ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
      record.originalFile = sourceFile.getAbsolutePath();
      record.sourceFile = accession;
      metadata.recordConversion(sourceFile, record);

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Processing filing immediately: {}", sourceFile.getName());
      }

      List<File> outputFiles = converter.convert(sourceFile, secParquetDir, metadata);

      if (outputFiles.isEmpty()) {
        LOGGER.warn("No parquet files created for " + sourceFile.getName());
      } else {
        // Add to manifest after successful conversion
        addToManifest(baseDir, sourceFile, secParquetDir);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Successfully processed filing: {} - created {} parquet files",
              sourceFile.getName(), outputFiles.size());
        }
      }
    } catch (java.nio.channels.OverlappingFileLockException e) {
      // Non-fatal: Another thread is processing this file
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Skipping {} - already being processed by another thread", sourceFile.getName());
      }
    } catch (Exception e) {
      LOGGER.error("Failed to process filing immediately: " + sourceFile.getName(), e);
    }
  }

  private void downloadFilingDocumentWithRateLimit(SecHttpStorageProvider provider, FilingToDownload filing) {
    int maxAttempts = 3;
    int attempt = 0;

    while (attempt < maxAttempts) {
      try {
        // Apply rate limiting with current delay
        rateLimiter.acquire();
        Thread.sleep(currentRateLimitDelayMs.get());
        try {
          downloadFilingDocument(provider, filing.cik, filing.accession,
              filing.primaryDoc, filing.form, filing.filingDate, filing.cikDir);
          break; // Success - exit retry loop
        } finally {
          rateLimiter.release();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Download interrupted for " + filing.accession);
        break;
      } catch (Exception e) {
        if (e.getMessage() != null && e.getMessage().contains("429")) {
          // Rate limit hit - dynamically expand rate limit delay
          rateLimitHits.incrementAndGet();
          long currentDelay = currentRateLimitDelayMs.get();
          long newDelay = Math.min(currentDelay + 50, MAX_RATE_LIMIT_DELAY_MS); // Increase by 50ms up to max
          currentRateLimitDelayMs.set(newDelay);
          LOGGER.warn("Rate limit hit for " + filing.accession +
              ". Expanding rate limit delay from " + currentDelay + "ms to " + newDelay + "ms. Waiting 10 seconds...");
          try {
            Thread.sleep(10000); // Wait 10 seconds for rate limit
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
          attempt++;
        } else {
          LOGGER.warn("Download failed for " + filing.accession + ": " + e.getMessage());
          break;
        }
      }
    }

    if (attempt >= maxAttempts) {
      LOGGER.error("Failed to download " + filing.accession + " after " + maxAttempts + " attempts");
    }
  }

  private void downloadFilingDocument(SecHttpStorageProvider provider, String cik,
      String accession, String primaryDoc, String form, String filingDate, File cikDir) {
    try {
      // Check manifest first to see if this filing was already fully processed
      File manifestFile = new File(cikDir.getParentFile(), "processed_filings.manifest");
      String manifestKey = cik + "|" + accession + "|" + form + "|" + filingDate;

      // Check if already in manifest or has all required Parquet files
      FilingStatus status = checkFilingStatus(cik, accession, form, filingDate, manifestFile);

      if (status == FilingStatus.FULLY_PROCESSED) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Filing fully processed, skipping: {} {}", form, filingDate);
        }
        return;
      } else if (status == FilingStatus.EXCLUDED_424B) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("424B filing excluded from processing, skipping: {} {}", form, filingDate);
        }
        return;
      } else if (status == FilingStatus.HAS_ALL_PARQUET) {
        // Has all Parquet files but not in manifest - add to manifest
        addToManifest(cikDir.getParentFile(), null, null);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Filing has all Parquet files, added to manifest: {} {}", form, filingDate);
        }
        return;
      }

      // Need to check source files and possibly download/process

      // Create accession directory
      String accessionClean = accession.replace("-", "");
      File accessionDir = new File(cikDir, accessionClean);
      accessionDir.mkdirs();

      // Download both HTML (for preview) and XBRL (for data extraction)
      boolean needHtml = false;
      boolean needXbrl = false;

      // For Forms 3/4/5, we need to download the .txt file and extract the raw XML
      boolean isInsiderForm = form.equals("3") || form.equals("4") || form.equals("5");

      // Check if HTML file exists (for human-readable preview)
      File htmlFile = new File(accessionDir, primaryDoc);
      // Create parent directory if primaryDoc contains path (e.g., xslF345X05/wk-form4_*.xml for Form 4)
      if (primaryDoc.contains("/")) {
        htmlFile.getParentFile().mkdirs();
      }
      if (!isInsiderForm && (!htmlFile.exists() || htmlFile.length() == 0)) {
        needHtml = true;
      }

      // Check if XBRL file exists (for structured data)
      String xbrlDoc;
      File xbrlFile;
      if (isInsiderForm) {
        // For Forms 3/4/5, we'll save the extracted XML as ownership.xml
        xbrlDoc = "ownership.xml";
        xbrlFile = new File(accessionDir, xbrlDoc);
      } else {
        // For other forms, look for separate XBRL file
        xbrlDoc = primaryDoc.replace(".htm", "_htm.xml");
        xbrlFile = new File(accessionDir, xbrlDoc);
      }

      // Create parent directory if xbrlDoc contains path
      if (xbrlDoc.contains("/")) {
        xbrlFile.getParentFile().mkdirs();
      }

      File xbrlNotFoundMarker = new File(accessionDir, xbrlDoc + ".notfound");
      // Only need XBRL if: file doesn't exist AND we haven't already marked it as not found
      if ((!xbrlFile.exists() || xbrlFile.length() == 0) && !xbrlNotFoundMarker.exists()) {
        needXbrl = true;
      }

      // DISABLED: Parquet file validation before XBRL parsing
      // Cannot reliably check Parquet files before parsing XBRL because fiscal year (used for
      // partitioning) is only known after parsing XBRL data. The heuristic-based
      // getPartitionYear() method produces incorrect year folders for filings where filing date
      // year != fiscal year, causing false cache misses and unnecessary reprocessing.
      // Solution: Rely on manifest file and source file (HTML/XBRL) existence checks only.
      // After XBRL is parsed and converted to Parquet, files are added to manifest.
      boolean needParquetReprocessing = false;

      // Critical fix: Detect inline XBRL in cached HTML files that need Parquet processing
      // This addresses the core cache effectiveness issue where HTML files contain inline XBRL
      // but Parquet files were never generated due to cache logic gaps
      if (htmlFile.exists()) {
        try {
          byte[] headerBytes = new byte[10240];
          try (FileInputStream fis = new FileInputStream(htmlFile)) {
            int bytesRead = fis.read(headerBytes);
            if (bytesRead > 0) {
              String header = new String(headerBytes, 0, bytesRead);
              boolean htmlHasInlineXbrl = header.contains("xmlns:ix=") ||
                                         header.contains("http://www.xbrl.org/2013/inlineXBRL") ||
                                         header.contains("<ix:") ||
                                         header.contains("iXBRL");

              if (htmlHasInlineXbrl && needParquetReprocessing) {
                // This is the critical case: HTML file exists with inline XBRL but Parquet files are missing
                // Schedule the HTML file for inline XBRL processing
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("Cached HTML file contains inline XBRL, scheduling for processing: {} {}", form, filingDate);
                }

                // Ensure the HTML file gets scheduled for inline XBRL processing
                if (!scheduledForInlineXbrlProcessing.contains(htmlFile)) {
                  scheduledForInlineXbrlProcessing.add(htmlFile);
                  if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Scheduled HTML file for inline XBRL processing: {}", htmlFile.getName());
                  }
                }

                // Since we have inline XBRL in HTML, we don't need separate XBRL download
                needXbrl = false;
              }
            }
          }
        } catch (Exception e) {
          LOGGER.debug("Could not check HTML for inline XBRL detection: " + e.getMessage());
        }
      }

      if (!needHtml && !needXbrl && !needParquetReprocessing) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Filing already fully cached: {} {}", form, filingDate);
        }
        return; // Both files already downloaded and converted
      }

      // DISABLED: Parquet reprocessing logic
      // needParquetReprocessing is now always false because parquet file validation
      // before XBRL parsing has been disabled due to fiscal year mismatch issues.
      // if (needParquetReprocessing) {
      //   ... reprocessing logic ...
      // }

      // Download HTML file first if needed (for preview and iXBRL check)
      if (needHtml) {
        String htmlUrl =
            String.format("https://www.sec.gov/Archives/edgar/data/%s/%s/%s",
            cik, accessionClean, primaryDoc);

        try (InputStream is = provider.openInputStream(htmlUrl)) {
          try (FileOutputStream fos = new FileOutputStream(htmlFile)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
              fos.write(buffer, 0, bytesRead);
            }
          }
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Downloaded HTML filing: {} {} ({})", form, filingDate, primaryDoc);
          }
        } catch (Exception e) {
          LOGGER.debug("Could not download HTML: " + e.getMessage());
          return; // Can't proceed without HTML
        }
      }

      // Check if HTML contains inline XBRL (iXBRL) - if it does, we don't need separate XBRL
      boolean hasInlineXbrl = false;
      if (htmlFile.exists()) {
        try {
          // Quick check for iXBRL markers in the first 10KB of the HTML
          byte[] headerBytes = new byte[10240];
          try (FileInputStream fis = new FileInputStream(htmlFile)) {
            int bytesRead = fis.read(headerBytes);
            if (bytesRead > 0) {
              String header = new String(headerBytes, 0, bytesRead);
              hasInlineXbrl = header.contains("xmlns:ix=") ||
                             header.contains("http://www.xbrl.org/2013/inlineXBRL") ||
                             header.contains("<ix:") ||
                             header.contains("iXBRL");
              if (hasInlineXbrl) {
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("HTML file contains inline XBRL (iXBRL), will process HTML file directly: {}", primaryDoc);
                }
                // Create marker to avoid checking XBRL in future
                if (!xbrlNotFoundMarker.exists()) {
                  xbrlNotFoundMarker.createNewFile();
                }
                // Continue processing - HTML file will be included in conversion process
              }
            }
          }
        } catch (Exception e) {
          LOGGER.debug("Could not check HTML for iXBRL: " + e.getMessage());
        }
      }

      // Download XBRL file if needed (only if HTML doesn't have iXBRL)
      if (needXbrl && !hasInlineXbrl) {
        // Check if we already know this XBRL doesn't exist
        if (xbrlNotFoundMarker.exists()) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Skipping XBRL download - already marked as not found: {}", xbrlDoc);
          }
        } else {
          if (isInsiderForm) {
            // For Forms 3/4/5, download the .txt file and extract the XML
            String txtUrl = String.format("https://www.sec.gov/Archives/edgar/data/%s/%s.txt",
                cik, accession); // Use hyphenated accession number for .txt files

            try (InputStream is = provider.openInputStream(txtUrl)) {
              // Read the entire .txt file to extract the XML
              ByteArrayOutputStream baos = new ByteArrayOutputStream();
              byte[] buffer = new byte[8192];
              int bytesRead;
              while ((bytesRead = is.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
              }
              String txtContent = baos.toString("UTF-8");

              // Extract the ownershipDocument XML from the .txt file
              int xmlStart = txtContent.indexOf("<ownershipDocument>");
              int xmlEnd = txtContent.indexOf("</ownershipDocument>");

              if (xmlStart != -1 && xmlEnd != -1) {
                String xmlContent = "<?xml version=\"1.0\"?>\n" +
                    txtContent.substring(xmlStart, xmlEnd + "</ownershipDocument>".length());

                // Save the extracted XML
                try (FileWriter writer = new FileWriter(xbrlFile)) {
                  writer.write(xmlContent);
                }
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("Extracted ownership XML for Form {} {}", form, filingDate);
                }
              } else {
                LOGGER.warn("Could not find ownershipDocument in Form " + form + " .txt file");
                xbrlNotFoundMarker.createNewFile();
              }
            } catch (Exception e) {
              LOGGER.warn("Failed to download/extract Form " + form + " ownership XML: " + e.getMessage());
              try {
                xbrlNotFoundMarker.createNewFile();
              } catch (IOException ioe) {
                LOGGER.debug("Could not create .notfound marker: " + ioe.getMessage());
              }
            }
          } else {
            // For other forms, download the XBRL file directly
            String xbrlUrl =
                String.format("https://www.sec.gov/Archives/edgar/data/%s/%s/%s",
                cik, accessionClean, xbrlDoc);

            try (InputStream is = provider.openInputStream(xbrlUrl)) {
              try (FileOutputStream fos = new FileOutputStream(xbrlFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) {
                  fos.write(buffer, 0, bytesRead);
                }
              }
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Downloaded XBRL filing: {} {} ({})", form, filingDate, xbrlDoc);
              }
            } catch (Exception e) {
              // XBRL doesn't exist for this filing - create marker to avoid retrying
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("XBRL not available for {} {}, will use HTML with inline XBRL", form, filingDate);
              }
              try {
                xbrlNotFoundMarker.createNewFile();
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("Created .notfound marker for: {}", xbrlDoc);
                }
              } catch (IOException ioe) {
                LOGGER.debug("Could not create .notfound marker: " + ioe.getMessage());
              }
            }
          }
        }
      }

      // STREAMING PROCESSING: Process filing immediately after download
      // Check if we have all required source files and process them
      boolean shouldProcess = false;
      File fileToProcess = null;

      if (hasInlineXbrl && htmlFile.exists()) {
        shouldProcess = true;
        fileToProcess = htmlFile;
      } else if (xbrlFile.exists() && xbrlFile.length() > 0) {
        shouldProcess = true;
        fileToProcess = xbrlFile;
      } else if (isInsiderForm && xbrlFile.exists() && xbrlFile.length() > 0) {
        shouldProcess = true;
        fileToProcess = xbrlFile;
      }

      if (shouldProcess && fileToProcess != null) {
        // Process this filing immediately in a separate thread
        final File finalFileToProcess = fileToProcess;
        final File baseDirectory = cikDir.getParentFile();
        CompletableFuture<Void> processingFuture = CompletableFuture.runAsync(() -> {
          processSingleFiling(finalFileToProcess, baseDirectory);
        }, conversionExecutor);

        // Track the processing future (but don't wait for it)
        filingProcessingFutures.add(processingFuture);

        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Scheduled immediate processing for filing: {} {}", form, filingDate);
        }
      } else {
        LOGGER.debug("Filing downloaded, but missing required files for processing: {} {}", form, filingDate);
      }

    } catch (Exception e) {
      LOGGER.warn("Failed to download filing " + accession + ": " + e.getMessage());
    }
  }

  private void downloadInlineXbrl(SecHttpStorageProvider provider, String cik,
      String accession, String primaryDoc, String form, String filingDate, File cikDir) throws Exception {
    // Some filings use inline XBRL embedded in HTML
    // Try to download the HTML and extract XBRL data
    String accessionClean = accession.replace("-", "");
    File accessionDir = new File(cikDir, accessionClean);

    File outputFile = new File(accessionDir, primaryDoc);

    // Check if file already exists - XBRL files are immutable
    if (outputFile.exists() && outputFile.length() > 0) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Inline XBRL filing already cached: {} {} ({})", form, filingDate, primaryDoc);
      }
      return;
    }

    String htmlUrl =
        String.format("https://www.sec.gov/Archives/edgar/data/%s/%s/%s",
        cik, accessionClean, primaryDoc);
    try (InputStream is = provider.openInputStream(htmlUrl)) {
      try (FileOutputStream fos = new FileOutputStream(outputFile)) {
        byte[] buffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = is.read(buffer)) != -1) {
          fos.write(buffer, 0, bytesRead);
        }
      }
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Downloaded inline XBRL (HTML): {} {} ({})", form, filingDate, primaryDoc);
    }
  }

  private void createSecTablesFromXbrl(File baseDir, List<String> ciks, int startYear, int endYear) {
    // Ensure executors are initialized
    initializeExecutors();

    LOGGER.debug("Creating SEC tables from XBRL data");
    LOGGER.debug("DEBUG: createSecTablesFromXbrl START");
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("DEBUG: baseDir={}", baseDir.getAbsolutePath());
      LOGGER.debug("DEBUG: ciks.size()={}", ciks.size());
      LOGGER.debug("DEBUG: startYear={}, endYear={}", startYear, endYear);
    }

    try {
      // baseDir is already the sec-data directory, don't nest another level
      File secRawDir = baseDir;
      // Get parquet directory from interface method
      String govdataParquetDir = getGovDataParquetDir();
      String secParquetDirPath = govdataParquetDir != null ? govdataParquetDir + "/source=sec" : null;
      // Parquet directory uses unified GOVDATA_PARQUET_DIR structure
      File secParquetDir = new File(secParquetDirPath);
      secParquetDir.mkdirs();

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("DEBUG: secRawDir={} exists={}", secRawDir.getAbsolutePath(), secRawDir.exists());
        LOGGER.debug("DEBUG: secParquetDir={} exists={}", secParquetDir.getAbsolutePath(), secParquetDir.exists());
      }
      LOGGER.info("Processing XBRL files from " + secRawDir + " to create Parquet tables");

      // Collect all XBRL files to convert
      List<File> xbrlFilesToConvert = new ArrayList<>();

      // Process all downloaded XBRL files
      for (String cik : ciks) {
        String normalizedCik = String.format("%010d", Long.parseLong(cik.replaceAll("[^0-9]", "")));
        File cikDir = new File(secRawDir, normalizedCik);

        if (!cikDir.exists()) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("No data directory found for CIK {}", normalizedCik);
          }
          continue;
        }

        // Process each filing in the CIK directory
        File[] accessionDirs = cikDir.listFiles(File::isDirectory);
        if (accessionDirs != null) {
          for (File accessionDir : accessionDirs) {
            // Find XBRL and HTML files in this accession directory
            // Exclude macOS metadata files that start with ._
            File[] xbrlFiles = accessionDir.listFiles((dir, name) ->
                !name.startsWith("._") && (
                name.endsWith("_htm.xml") || name.endsWith(".xml") ||
                name.endsWith(".htm") || name.endsWith(".html")));

            if (xbrlFiles != null) {
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Found {} XBRL/HTML files in {}", xbrlFiles.length, accessionDir.getName());
                for (File xbrlFile : xbrlFiles) {
                  LOGGER.debug("  Adding for conversion: {}", xbrlFile.getName());
                }
              }
              for (File xbrlFile : xbrlFiles) {
                xbrlFilesToConvert.add(xbrlFile);
              }
            } else {
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("No XBRL/HTML files found in {}", accessionDir.getName());
              }
            }
          }
        }
      }

      // Add any files scheduled for reconversion (missing Parquet files)
      if (!scheduledForReconversion.isEmpty()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Adding {} existing XBRL files for reconversion", scheduledForReconversion.size());
        }
        xbrlFilesToConvert.addAll(scheduledForReconversion);
        scheduledForReconversion.clear();
      }

      // Critical fix: Add HTML files with inline XBRL that need Parquet processing
      // This addresses the cache effectiveness issue where HTML files contain inline XBRL
      // but were never processed because cache logic didn't recognize them as processable files
      if (!scheduledForInlineXbrlProcessing.isEmpty()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Adding {} cached HTML files with inline XBRL for processing", scheduledForInlineXbrlProcessing.size());
        }
        xbrlFilesToConvert.addAll(scheduledForInlineXbrlProcessing);
        scheduledForInlineXbrlProcessing.clear();
      }

      // Convert XBRL files in parallel
      totalConversions.set(xbrlFilesToConvert.size());
      LOGGER.info("Scheduling " + xbrlFilesToConvert.size() + " XBRL files for conversion");

      for (File xbrlFile : xbrlFilesToConvert) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
          try {
            // Use the storageProvider that was passed from GovDataSchemaFactory
            if (this.storageProvider == null) {
              LOGGER.error("StorageProvider is null - cannot convert XBRL to Parquet");
              throw new IllegalStateException("StorageProvider not initialized - must be provided by GovDataSchemaFactory");
            }
            // Check if text similarity is enabled from operand
            Map<String, Object> textSimilarityConfig = (Map<String, Object>) currentOperand.get("textSimilarity");
            boolean enableVectorization = textSimilarityConfig != null &&
                Boolean.TRUE.equals(textSimilarityConfig.get("enabled"));

            XbrlToParquetConverter converter = new XbrlToParquetConverter(this.storageProvider, enableVectorization);

            // Extract accession number from file path (parent directory name)
            // Path is like: /sec-data/0000789019/000078901922000007/ownership.xml
            String accession = xbrlFile.getParentFile().getName();

            // Create a simple ConversionMetadata to pass accession to converter
            ConversionMetadata metadata = new ConversionMetadata(secParquetDir);
            ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
            record.originalFile = xbrlFile.getAbsolutePath();
            // Store accession in the sourceFile field for now - converter can extract it
            record.sourceFile = accession;
            metadata.recordConversion(xbrlFile, record);

            // The converter now properly extracts metadata from the XML content itself
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("DEBUG: Starting conversion of {} to parquet in {}", xbrlFile.getAbsolutePath(), secParquetDir.getAbsolutePath());
            }
            List<File> outputFiles = converter.convert(xbrlFile, secParquetDir, metadata);
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("DEBUG: Conversion completed for {} - created {} parquet files", xbrlFile.getName(), outputFiles.size());
            }
            if (outputFiles.isEmpty()) {
              LOGGER.warn("DEBUG: No parquet files created for " + xbrlFile.getName());
            } else {
              for (File outputFile : outputFiles) {
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("DEBUG: Created parquet file: {}", outputFile.getAbsolutePath());
                }
              }
            }

            // Add to manifest after successful conversion
            addToManifest(xbrlFile.getParentFile().getParentFile().getParentFile(), xbrlFile, secParquetDir);
          } catch (java.nio.channels.OverlappingFileLockException e) {
            // Non-fatal: Another thread is processing this file, skip it
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Skipping {} - already being processed by another thread", xbrlFile.getName());
            }
          } catch (Exception e) {
            // Check if the cause is an OverlappingFileLockException
            Throwable cause = e.getCause();
            if (cause instanceof java.nio.channels.OverlappingFileLockException) {
              // Non-fatal: Another thread is processing this file, skip it
              if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Skipping {} - already being processed by another thread", xbrlFile.getName());
            }
            } else {
              LOGGER.error("DEBUG: Failed to convert {} - Exception: {}", xbrlFile.getName(), e.getMessage(), e);
            }
          }
          int completed = completedConversions.incrementAndGet();
          if (completed % 10 == 0) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Conversion progress: {}/{} files", completed, totalConversions.get());
            }
          }
        }, conversionExecutor);
        conversionFutures.add(future);
      }

      // Wait for all conversions to complete
      CompletableFuture<Void> allConversions =
          CompletableFuture.allOf(conversionFutures.toArray(new CompletableFuture[0]));
      allConversions.get(30, TimeUnit.MINUTES); // Wait up to 30 minutes

      LOGGER.info("Processed " + completedConversions.get() + " XBRL files into Parquet tables in: " + secParquetDir);

      // Bulk cleanup of macOS metadata files at the end of processing
      cleanupAllMacOSMetadataFiles(secParquetDir);

      LOGGER.debug("DEBUG: Checking what was created in secParquetDir after conversion");
      File[] afterConversion = secParquetDir.listFiles();
      if (afterConversion != null) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("DEBUG: Found {} items in secParquetDir", afterConversion.length);
          for (File f : afterConversion) {
            LOGGER.debug("DEBUG: - {} (isDir={}, size={})", f.getName(), f.isDirectory(), f.length());
          }
        }
      }

    } catch (Exception e) {
      LOGGER.error("Failed to create SEC tables from XBRL", e);
      throw new RuntimeException("Failed to create SEC tables", e);
    }
  }


  // REMOVED: consolidateFactsIntoFinancialLineItems method
  // The FileSchema will automatically discover individual *_facts.parquet files
  // No consolidation needed - each filing remains in its own partition











  private void createSecFilingsTable(File baseDir, Map<String, Object> operand) {
    try {
      File secRawDir = new File(baseDir, "sec-raw");
      // Get parquet directory from interface method
      String govdataParquetDir = getGovDataParquetDir();
      String secParquetDirPath = govdataParquetDir != null ? govdataParquetDir + "/source=sec" : null;
      File secParquetDir = new File(secParquetDirPath);
      secParquetDir.mkdirs();

      if (!secRawDir.exists() || !secRawDir.isDirectory()) {
        LOGGER.warn("No sec-raw directory found: " + secRawDir);
        return;
      }

      // Create Avro schema for SEC filings metadata
      org.apache.avro.Schema schema = SchemaBuilder.record("SecFiling")
          .fields()
          .requiredString("cik")
          .requiredString("accession_number")
          .requiredString("filing_type")
          .requiredString("filing_date")
          .optionalString("primary_document")
          .optionalString("company_name")
          .optionalString("period_of_report")
          .optionalString("acceptance_datetime")
          .optionalLong("file_size")
          .requiredInt("fiscal_year")
          .endRecord();

      List<GenericRecord> allRecords = new ArrayList<>();
      ObjectMapper mapper = new ObjectMapper();

      // Process each CIK directory
      File[] cikDirs = secRawDir.listFiles(File::isDirectory);
      if (cikDirs != null) {
        for (File cikDir : cikDirs) {
          File submissionsFile = new File(cikDir, "submissions.json");
          if (!submissionsFile.exists()) {
            continue;
          }

          try {
            JsonNode submissionsJson = mapper.readTree(submissionsFile);
            String cik = cikDir.getName();
            String companyName = submissionsJson.path("name").asText("");

            JsonNode filings = submissionsJson.get("filings");
            if (filings == null || !filings.has("recent")) {
              continue;
            }

            JsonNode recent = filings.get("recent");
            JsonNode accessionNumbers = recent.get("accessionNumber");
            JsonNode filingDates = recent.get("filingDate");
            JsonNode forms = recent.get("form");
            JsonNode primaryDocuments = recent.get("primaryDocument");
            JsonNode periodsOfReport = recent.get("reportDate");
            JsonNode acceptanceDatetimes = recent.get("acceptanceDateTime");
            JsonNode fileSizes = recent.get("size");

            if (accessionNumbers == null || !accessionNumbers.isArray()) {
              continue;
            }

            for (int i = 0; i < accessionNumbers.size(); i++) {
              GenericRecord record = new GenericData.Record(schema);
              record.put("cik", cik);
              record.put("accession_number", accessionNumbers.get(i).asText());
              record.put("filing_type", forms.get(i).asText());
              record.put("filing_date", filingDates.get(i).asText());
              record.put("primary_document", primaryDocuments.get(i).asText(""));
              record.put("company_name", companyName);
              record.put("period_of_report", periodsOfReport != null && i < periodsOfReport.size()
                  ? periodsOfReport.get(i).asText("") : "");
              record.put("acceptance_datetime", acceptanceDatetimes != null && i < acceptanceDatetimes.size()
                  ? acceptanceDatetimes.get(i).asText("") : "");
              record.put("file_size", fileSizes != null && i < fileSizes.size()
                  ? fileSizes.get(i).asLong(0L) : 0L);

              String filingDate = filingDates.get(i).asText();
              int fiscalYear = filingDate.length() >= 4 ?
                  Integer.parseInt(filingDate.substring(0, 4)) : 0;
              record.put("fiscal_year", fiscalYear);

              allRecords.add(record);
            }
          } catch (Exception e) {
            LOGGER.warn("Failed to process submissions for CIK " + cikDir.getName() + ": " + e.getMessage());
          }
        }
      }

      // Write consolidated SEC filings table
      String outputFilePath = storageProvider.resolvePath(secParquetDir.getPath(), "sec_filings.parquet");
      // Use StorageProvider for parquet writing
      storageProvider.writeAvroParquet(outputFilePath, schema, allRecords, "sec_filings");
      LOGGER.info("Created SEC filings table with " + allRecords.size() + " records: " + outputFilePath);

    } catch (Exception e) {
      LOGGER.warn("Failed to create SEC filings table: " + e.getMessage());
      e.printStackTrace();
    }
  }


  private List<File> findXbrlFiles(File directory) {
    List<File> xbrlFiles = new ArrayList<>();
    File[] files = directory.listFiles();

    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          xbrlFiles.addAll(findXbrlFiles(file));
        } else if (file.getName().endsWith(".xml") || file.getName().endsWith(".xbrl")) {
          xbrlFiles.add(file);
        } else if (file.getName().endsWith(".htm") || file.getName().endsWith(".html")) {
          // Include HTML files - XbrlToParquetConverter can handle inline XBRL
          xbrlFiles.add(file);
        }
      }
    }

    return xbrlFiles;
  }

  /**
   * Creates mock stock prices for testing.
   */
  private void createMockStockPrices(File baseDir, List<String> ciks, int startYear, int endYear) {
    try {
      // baseDir is the cache directory (sec-cache)
      // We need to create files in sec-parquet/stock_prices
      File parquetDir = new File(baseDir.getParentFile(), "sec-parquet");
      File stockPricesDir = new File(parquetDir, "stock_prices");
      LOGGER.debug("Creating mock stock prices in: {}", stockPricesDir.getAbsolutePath());

      // For each CIK, create mock price data
      for (String cik : ciks) {
        String normalizedCik = cik.replaceAll("[^0-9]", "");
        while (normalizedCik.length() < 10) {
          normalizedCik = "0" + normalizedCik;
        }

        // Get ticker for this CIK (or use CIK as ticker if not found)
        List<String> tickers = CikRegistry.getTickersForCik(normalizedCik);
        String ticker = tickers.isEmpty() ? cik.toUpperCase() : tickers.get(0);

        // Create mock data for each year
        for (int year = startYear; year <= endYear; year++) {
          // Create directory structure: ticker=*/year=*/
          File tickerDir = new File(stockPricesDir, "ticker=" + ticker);
          File yearDir = new File(tickerDir, "year=" + year);
          yearDir.mkdirs();

          String parquetFilePath = storageProvider.resolvePath(yearDir.getPath(), ticker + "_" + year + "_prices.parquet");
          LOGGER.debug("About to create/check file: {}", parquetFilePath);
          LOGGER.debug("Parent directory exists: {}, isDirectory: {}", yearDir.exists(), yearDir.isDirectory());
          try {
            StorageProvider.FileMetadata parquetMetadata = storageProvider.getMetadata(parquetFilePath);
            LOGGER.debug("Mock stock price file already exists: {}", parquetFilePath);
          } catch (IOException e) {
            createMockPriceParquetFileViaStorageProvider(parquetFilePath, ticker, normalizedCik, year);
            LOGGER.debug("Created mock stock price file: {}", parquetFilePath);
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to create mock stock prices: " + e.getMessage());
    }
  }

  /**
   * Creates a mock Parquet file with sample stock price data.
   */
  @SuppressWarnings("deprecation")
  private void createMockPriceParquetFile(File parquetFile, String ticker, String cik, int year)
      throws IOException {
    // Note: ticker and year are partition columns from directory structure,
    // so they are NOT in the Parquet file. CIK is included as a regular column for joins.
    String schemaString = "{"
        + "\"type\": \"record\","
        + "\"name\": \"StockPrice\","
        + "\"fields\": ["
        + "{\"name\": \"cik\", \"type\": \"string\"},"
        + "{\"name\": \"date\", \"type\": \"string\"},"
        + "{\"name\": \"open\", \"type\": [\"null\", \"double\"], \"default\": null},"
        + "{\"name\": \"high\", \"type\": [\"null\", \"double\"], \"default\": null},"
        + "{\"name\": \"low\", \"type\": [\"null\", \"double\"], \"default\": null},"
        + "{\"name\": \"close\", \"type\": [\"null\", \"double\"], \"default\": null},"
        + "{\"name\": \"adj_close\", \"type\": [\"null\", \"double\"], \"default\": null},"
        + "{\"name\": \"volume\", \"type\": [\"null\", \"long\"], \"default\": null}"
        + "]"
        + "}";

    org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(schemaString);

    try (org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericRecord> writer =
        org.apache.parquet.avro.AvroParquetWriter
            .<org.apache.avro.generic.GenericRecord>builder(
                new org.apache.hadoop.fs.Path(parquetFile.getAbsolutePath()))
            .withSchema(schema)
            .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY)
            .build()) {

      // Create a few sample records
      double basePrice = 100.0 + (ticker.hashCode() % 100);
      for (int month = 1; month <= 3; month++) { // Just 3 months of data for testing
        String date = String.format("%04d-%02d-%02d", year, month, 15);

        org.apache.avro.generic.GenericRecord record =
            new org.apache.avro.generic.GenericData.Record(schema);
        // Include CIK as regular column, ticker and year come from directory structure
        record.put("cik", cik);
        record.put("date", date);
        record.put("open", basePrice + month);
        record.put("high", basePrice + month + 2);
        record.put("low", basePrice + month - 1);
        record.put("close", basePrice + month + 1);
        record.put("adj_close", basePrice + month + 0.5);
        record.put("volume", 1000000L * month);

        writer.write(record);
      }
    }
  }

  private void createMockPriceParquetFileViaStorageProvider(String parquetPath, String ticker, String cik, int year)
      throws IOException {
    // Note: ticker and year are partition columns from directory structure,
    // so they are NOT in the Parquet file. CIK is included as a regular column for joins.
    String schemaString = "{"
        + "\"type\": \"record\","
        + "\"name\": \"StockPrice\","
        + "\"fields\": ["
        + "{\"name\": \"cik\", \"type\": \"string\"},"
        + "{\"name\": \"date\", \"type\": \"string\"},"
        + "{\"name\": \"open\", \"type\": [\"null\", \"double\"], \"default\": null},"
        + "{\"name\": \"high\", \"type\": [\"null\", \"double\"], \"default\": null},"
        + "{\"name\": \"low\", \"type\": [\"null\", \"double\"], \"default\": null},"
        + "{\"name\": \"close\", \"type\": [\"null\", \"double\"], \"default\": null},"
        + "{\"name\": \"adj_close\", \"type\": [\"null\", \"double\"], \"default\": null},"
        + "{\"name\": \"volume\", \"type\": [\"null\", \"long\"], \"default\": null}"
        + "]"
        + "}";

    org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(schemaString);

    // Create records using StorageProvider approach
    java.util.List<org.apache.avro.generic.GenericRecord> records = new java.util.ArrayList<>();
    double basePrice = 100.0 + (ticker.hashCode() % 100);

    for (int month = 1; month <= 3; month++) { // Just 3 months of data for testing
      String date = String.format("%04d-%02d-%02d", year, month, 15);

      org.apache.avro.generic.GenericRecord record =
          new org.apache.avro.generic.GenericData.Record(schema);
      // Include CIK as regular column, ticker and year come from directory structure
      record.put("cik", cik);
      record.put("date", date);
      record.put("open", basePrice + month);
      record.put("high", basePrice + month + 2);
      record.put("low", basePrice + month - 1);
      record.put("close", basePrice + month + 1);
      record.put("adj_close", basePrice + month + 0.5);
      record.put("volume", 1000000L * month);

      records.add(record);
    }

    // Use StorageProvider to write parquet file
    storageProvider.writeAvroParquet(parquetPath, schema, records, "stock_prices");
  }

  /**
   * Downloads stock prices for all configured CIKs.
   * Implements daily caching - stock prices are only downloaded once per day.
   */
  private void downloadStockPrices(File baseDir, List<String> ciks, int startYear, int endYear) {
    try {
      // Use the parquet directory for stock prices - same as other SEC data
      String govdataParquetDir = getGovDataParquetDir();
      if (govdataParquetDir == null || govdataParquetDir.isEmpty()) {
        LOGGER.warn("GOVDATA_PARQUET_DIR not set, skipping stock price download");
        return;
      }
      String basePath = govdataParquetDir + "/source=sec";

      // Check if stock prices are already cached and up-to-date (daily refresh)
      if (areStockPricesCached(basePath, ciks, startYear, endYear)) {
        LOGGER.info("Stock prices already cached and up-to-date, skipping download");
        return;
      }

      // Build list of ticker-CIK pairs
      List<AlphaVantageDownloader.TickerCikPair> tickerCikPairs = new ArrayList<>();
      Set<String> processedTickers = new HashSet<>();

      for (String cik : ciks) {
        // Normalize CIK to 10 digits with leading zeros
        String normalizedCik = cik.replaceAll("[^0-9]", "");
        while (normalizedCik.length() < 10) {
          normalizedCik = "0" + normalizedCik;
        }

        // Get tickers for this CIK
        List<String> tickers = CikRegistry.getTickersForCik(normalizedCik);

        if (tickers.isEmpty()) {
          // Try to resolve the CIK as a ticker first
          List<String> resolvedCiks = CikRegistry.resolveCiks(cik);
          if (!resolvedCiks.isEmpty() && !cik.equals(resolvedCiks.get(0))) {
            // This was actually a ticker, use it
            String ticker = cik.toUpperCase();
            if (!processedTickers.contains(ticker)) {
              tickerCikPairs.add(new AlphaVantageDownloader.TickerCikPair(ticker, normalizedCik));
              processedTickers.add(ticker);
            }
          } else {
            LOGGER.debug("No ticker found for CIK {}, skipping stock prices", normalizedCik);
          }
        } else {
          // Add all tickers for this CIK
          for (String ticker : tickers) {
            if (!processedTickers.contains(ticker)) {
              tickerCikPairs.add(new AlphaVantageDownloader.TickerCikPair(ticker, normalizedCik));
              processedTickers.add(ticker);
            }
          }
        }
      }

      LOGGER.debug("DEBUG downloadStockPrices: govdataParquetDir={}, basePath={}", govdataParquetDir, basePath);
      LOGGER.info("STOCK DOWNLOAD STARTING NOW WITH basePath={}", basePath);

      if (!tickerCikPairs.isEmpty()) {
        // Get Alpha Vantage API key from environment
        String apiKey = System.getenv("ALPHA_VANTAGE_KEY");

        // If not found in system env, try TestEnvironmentLoader (for tests)
        if (apiKey == null || apiKey.isEmpty()) {
          try {
            Class<?> testEnvClass = Class.forName("org.apache.calcite.adapter.govdata.TestEnvironmentLoader");
            java.lang.reflect.Method getEnvMethod = testEnvClass.getMethod("getEnv", String.class);
            apiKey = (String) getEnvMethod.invoke(null, "ALPHA_VANTAGE_KEY");
          } catch (Exception e) {
            // TestEnvironmentLoader not available or error accessing it
          }
        }

        if (apiKey == null || apiKey.isEmpty()) {
          LOGGER.warn("ALPHA_VANTAGE_KEY not found in environment, skipping stock price download");
          return;
        }

        LOGGER.info("Downloading stock prices for {} tickers using Alpha Vantage", tickerCikPairs.size());
        AlphaVantageDownloader downloader = new AlphaVantageDownloader(apiKey, storageProvider);
        downloader.downloadStockPrices(basePath, tickerCikPairs, startYear, endYear);
      } else {
        LOGGER.info("No tickers found for stock price download");
      }

    } catch (Exception e) {
      LOGGER.warn("Failed to download stock prices: " + e.getMessage());
      // Don't fail the schema creation if stock prices fail
    }
  }

  /**
   * Checks if stock prices are already cached and up-to-date.
   * Uses smart TTL logic: current year data uses configurable TTL, historical years are immutable.
   */
  private boolean areStockPricesCached(String basePath, List<String> ciks, int startYear, int endYear) {
    try {
      // Get TTL configuration from operand (default 24 hours)
      Integer stockPriceTtlHours = (Integer) currentOperand.get("stockPriceTtlHours");
      if (stockPriceTtlHours == null) {
        stockPriceTtlHours = 24;
      }
      long ttlMillis = stockPriceTtlHours * 60 * 60 * 1000L;
      long currentTime = System.currentTimeMillis();

      // Determine current year for TTL logic
      int currentYear = java.time.Year.now().getValue();
      LOGGER.debug("Current year: {}, TTL hours: {}", currentYear, stockPriceTtlHours);

      // Check each CIK for cached stock prices
      for (String cik : ciks) {
        // Normalize CIK to 10 digits with leading zeros
        String normalizedCik = cik.trim();
        while (normalizedCik.length() < 10) {
          normalizedCik = "0" + normalizedCik;
        }

        // Get ticker symbols for this CIK
        List<String> tickers = CikRegistry.getTickersForCik(normalizedCik);
        if (tickers.isEmpty()) {
          // Try to resolve the CIK as a ticker first (same logic as downloadStockPrices)
          List<String> resolvedCiks = CikRegistry.resolveCiks(cik);
          if (!resolvedCiks.isEmpty() && !cik.equals(resolvedCiks.get(0))) {
            // This was actually a ticker, use it
            tickers = Arrays.asList(cik.toUpperCase());
          } else {
            // Default fallback - use the CIK as ticker
            tickers = Arrays.asList(cik.toUpperCase());
          }
        }

        // Check each ticker for each year
        for (String ticker : tickers) {
          for (int year = startYear; year <= endYear; year++) {
            String stockPriceDir = basePath + "/stock_prices";
            String parquetPath = storageProvider.resolvePath(stockPriceDir,
                String.format("ticker=%s/year=%d/%s_%d.parquet", ticker, year, ticker, year));

            try {
              StorageProvider.FileMetadata metadata = storageProvider.getMetadata(parquetPath);

              // Smart TTL logic: current year uses TTL, historical years are immutable
              if (year == currentYear) {
                // Current year data: check TTL
                long fileAge = currentTime - metadata.getLastModified();
                if (fileAge > ttlMillis) {
                  LOGGER.debug("Current year ({}) stock price file is older than {} hours: {}",
                      year, stockPriceTtlHours, parquetPath);
                  return false;
                }
                LOGGER.debug("Current year ({}) stock price file is within TTL: {}", year, parquetPath);
              } else {
                // Historical year data: considered immutable, just check existence and non-empty
                LOGGER.debug("Historical year ({}) stock price file exists and is immutable: {}", year, parquetPath);
              }

              // Check if file has content (applies to both current and historical years)
              if (metadata.getSize() == 0) {
                LOGGER.debug("Stock price file is empty: {}", parquetPath);
                return false;
              }

            } catch (Exception e) {
              // File doesn't exist or can't be accessed
              LOGGER.debug("Stock price file not cached or accessible: {}", parquetPath);
              return false;
            }
          }
        }
      }

      return true; // All stock price files are cached and fresh
    } catch (Exception e) {
      LOGGER.warn("Error checking stock price cache: " + e.getMessage());
      return false; // Assume not cached if we can't check
    }
  }

  /**
   * Extract and resolve company identifiers from schema operand.
   *
   * <p>Automatically resolves identifiers using CikRegistry:
   * <ul>
   *   <li>Ticker symbols: "AAPL" → "0000320193" (Apple's CIK)</li>
   *   <li>Company groups: "FAANG" → ["0001326801", "0000320193", "0001018724", "0001065280", "0001652044"]</li>
   *   <li>Raw CIKs: "0000320193" → "0000320193" (normalized to 10 digits)</li>
   *   <li>Mixed inputs: ["AAPL", "FAANG", "0001018724"] → all resolved to CIKs</li>
   * </ul>
   *
   * @param operand Schema operand map containing 'ciks' parameter
   * @return List of resolved 10-digit CIK strings ready for SEC data fetching
   */
  private List<String> getCiksFromConfig(Map<String, Object> operand) {
    Object ciks = operand.get("ciks");
    if (ciks instanceof List) {
      // Handle array of identifiers: ["AAPL", "MSFT", "FAANG"]
      List<String> cikList = new ArrayList<>();
      for (Object cik : (List<?>) ciks) {
        // Each identifier resolved via CikRegistry: ticker→CIK, group→multiple CIKs, raw CIK→normalized
        cikList.addAll(CikRegistry.resolveCiks(cik.toString()));
      }
      return cikList;
    } else if (ciks instanceof String) {
      // Handle single identifier: "AAPL" or "FAANG" or "0000320193"
      return CikRegistry.resolveCiks((String) ciks);
    }
    return new ArrayList<>();
  }

  private List<String> getFilingTypes(Map<String, Object> operand) {
    Object types = operand.get("filingTypes");
    if (types instanceof List) {
      return (List<String>) types;
    }

    // Return explicit list of allowed filing types (excludes 424B forms)
    return Arrays.asList(
        "3", "4", "5",           // Insider forms
        "10-K", "10K",           // Annual reports
        "10-Q", "10Q",           // Quarterly reports
        "8-K", "8K",             // Current reports
        "8-K/A", "8KA",          // Amended current reports
        "DEF 14A", "DEF14A",     // Proxy statements
        "S-3", "S3",             // Registration statements
        "S-4", "S4",             // Business combination registration
        "S-8", "S8"              // Employee benefit plan registration
    );
  }

  /**
   * Bulk cleanup of all macOS metadata files in the parquet directory.
   * This is run at the end of processing to clean up any ._* files that were created.
   */
  private void cleanupAllMacOSMetadataFiles(File directory) {
    if (directory == null || !directory.exists()) {
      return;
    }

    try {
      storageProvider.cleanupMacosMetadata(directory.getAbsolutePath());
      LOGGER.info("Bulk cleanup: Removed macOS metadata files from " + directory.getAbsolutePath());
    } catch (Exception e) {
      LOGGER.debug("Error during bulk macOS metadata cleanup: " + e.getMessage());
    }
  }

  /**
   * Clean up partial parquet files for a filing to ensure clean conversion.
   * Deletes all existing parquet files with the given cik and accession prefix.
   */
  /**
   * Check if a form type supports vectorization when text similarity is enabled.
   *
   * @param form The SEC form type (e.g., "3", "4", "5", "10-K", "10-Q", "8-K", "DEF 14A")
   * @return true if the form generates vectorized files, false otherwise
   */
  private boolean supportsVectorization(String form) {
    if (form == null) return false;

    // Insider forms (3, 4, 5) generate vectorized files
    if ("3".equals(form) || "4".equals(form) || "5".equals(form)) {
      return true;
    }

    // Financial forms (10-K, 10-Q, 8-K, 8-K/A) generate vectorized files
    if ("10K".equals(form) || "10-K".equals(form) ||
        "10Q".equals(form) || "10-Q".equals(form) ||
        "8K".equals(form) || "8-K".equals(form) ||
        "8KA".equals(form) || "8-K/A".equals(form)) {
      return true;
    }

    // Other forms (DEF 14A, S-1, etc.) do not generate vectorized files
    return false;
  }

  /**
   * Determines if a filing type commonly contains inline XBRL that should be proactively checked.
   * Based on analysis of the 40 files requiring reprocessing, these types frequently have inline XBRL.
   */
  private boolean isInlineXbrlFilingType(String form) {
    if (form == null) return false;

    // Forms that commonly contain inline XBRL (from analysis of reprocessing patterns)
    // 424B2 excluded - prospectus supplements have minimal analytical value
    // if ("424B2".equals(form)) return true;

    // 10-Q (10 files) - Quarterly reports with mandatory inline XBRL
    if ("10Q".equals(form) || "10-Q".equals(form)) return true;

    // DEF 14A (9 files) - Proxy statements with financial tables
    if ("DEF 14A".equals(form) || "DEF14A".equals(form)) return true;

    // S-8 (3 files) - Registration statements often with financial data
    if ("S-8".equals(form) || "S8".equals(form)) return true;

    // S-4 (2 files) - Registration statements for business combinations
    if ("S-4".equals(form) || "S4".equals(form)) return true;

    // 8-K and variants (3 files) - Current reports with financial data
    if ("8K".equals(form) || "8-K".equals(form) || "8KA".equals(form) || "8-K/A".equals(form)) return true;

    // 10-K forms also commonly have inline XBRL
    if ("10K".equals(form) || "10-K".equals(form)) return true;

    return false;
  }

  private void cleanupPartialParquetFiles(File yearDir, String cik, String accessionClean) {
    if (!yearDir.exists()) {
      return;
    }

    String prefix = cik + "_" + accessionClean + "_";
    File[] parquetFiles = yearDir.listFiles((dir, name) ->
        name.startsWith(prefix) && name.endsWith(".parquet"));

    if (parquetFiles != null && parquetFiles.length > 0) {
      LOGGER.debug("Cleaning up {} partial parquet files for filing {}", parquetFiles.length, accessionClean);
      for (File file : parquetFiles) {
        try {
          if (file.delete()) {
            LOGGER.debug("Deleted partial parquet file: {}", file.getPath());
          } else {
            LOGGER.warn("Failed to delete partial parquet file: {}", file.getPath());
          }
        } catch (Exception e) {
          LOGGER.warn("Error deleting partial parquet file {}: {}", file.getPath(), e.getMessage());
        }
      }
    }
  }

  private String getPartitionYear(String filingType, String filingDate) {
    String normalizedType = filingType.replace("-", "").replace("/", "");

    // For 10-Q and 10-K filings, use fiscal year approach
    // Filed 10-Qs typically cover quarters ending in March, June, Sep, Dec
    // and are filed within 40 days. So filings in Jan-Apr likely cover the previous year's Q4
    if ("10Q".equals(normalizedType) || "10K".equals(normalizedType)) {
      java.time.LocalDate filing = java.time.LocalDate.parse(filingDate);
      // For Microsoft and most companies, fiscal year ends in June
      // If filed Jan-Apr, likely covers previous calendar year
      // This is a heuristic since we don't have access to the document content here
      if (filing.getMonthValue() <= 4) {
        String partitionYear = String.valueOf(filing.getYear() - 1);
        LOGGER.debug("Partition year calculation: {} {} → fiscal year {} (filed month {})", filingType, filingDate, partitionYear, filing.getMonthValue());
        return partitionYear;
      } else {
        String partitionYear = String.valueOf(filing.getYear());
        LOGGER.debug("Partition year calculation: {} {} → filing year {} (filed month {})", filingType, filingDate, partitionYear, filing.getMonthValue());
        return partitionYear;
      }
    }

    // For all other filing types, use filing year
    String partitionYear = String.valueOf(java.time.LocalDate.parse(filingDate).getYear());
    LOGGER.debug("Partition year calculation: {} {} → filing year {} (non-10Q/10K)", filingType, filingDate, partitionYear);
    return partitionYear;
  }

}
