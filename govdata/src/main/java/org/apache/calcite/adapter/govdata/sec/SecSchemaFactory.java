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

import org.apache.calcite.adapter.file.FileSchemaBuilder;
import org.apache.calcite.adapter.file.converters.FileConverter;
import org.apache.calcite.adapter.file.etl.DocumentETLProcessor;
import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.GovDataSubSchemaFactory;
import org.apache.calcite.adapter.govdata.GovDataUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Year;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
  private SecCacheManifest cacheManifest; // ETag-based caching for submissions.json

  // Cache filing status decisions to avoid repeated manifest searches
  private final Map<String, FilingStatus> filingStatusCache = new java.util.concurrent.ConcurrentHashMap<>();

  // Directory paths following standardized naming
  private String secCacheDirectory;      // Raw XBRL cache: $GOVDATA_CACHE_DIR/sec/
  private String secOperatingDirectory;  // Metadata directory: $GOVDATA_PARQUET_DIR/.aperio/sec/

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

  /**
   * Helper to get cache directory using stored operand.
   */
  public String getGovDataCacheDir() {
    return GovDataUtils.getCacheDir(this.currentOperand);
  }

  /**
   * Helper to get parquet directory using stored operand.
   */
  public String getGovDataParquetDir() {
    return GovDataUtils.getParquetDir(this.currentOperand);
  }

  public static final SecSchemaFactory INSTANCE = new SecSchemaFactory();

  // Constraint metadata support
  private Map<String, Map<String, Object>> tableConstraints = new HashMap<>();

  @Override public String getSchemaResourceName() {
    return "/sec/sec-schema.yaml";
  }

  @Override public void configureHooks(FileSchemaBuilder builder, Map<String, Object> operand) {
    LOGGER.debug("Configuring hooks for SEC schema");

    // Initialize storage provider from operand if available
    StorageProvider sp = (StorageProvider) operand.get("_storageProvider");
    if (sp != null && this.storageProvider == null) {
      this.storageProvider = sp;
    }

    // Trigger document download IMMEDIATELY during hook configuration
    // This must happen BEFORE the ETL loop starts (not in beforeSource hook)
    // because isEnabled returns false for all tables, skipping beforeSource
    if (!downloadTriggered) {
      downloadTriggered = true;
      Boolean autoDownload = (Boolean) operand.get("autoDownload");
      if (autoDownload != null && autoDownload) {
        LOGGER.info("Triggering SEC document download (autoDownload=true)");

        // Set up cache directory
        String cacheDir = (String) operand.get("cacheDirectory");
        if (cacheDir != null && storageProvider != null) {
          this.secCacheDirectory = storageProvider.resolvePath(cacheDir, "sec");
        } else if (cacheDir != null) {
          this.secCacheDirectory = cacheDir + "/sec";
        }

        // Download SEC data using DocumentETLProcessor
        downloadSecDataUsingDocumentEtl(operand);
      }
    }

    // SEC tables are populated by DocumentETLProcessor, not standard ETL
    // Use isEnabled hook to skip standard ETL processing for all tables
    // The hook returns false during ETL (to skip source fetching), but tables
    // will still be created from existing Parquet files
    builder.isEnabled("*", context -> {
      // Skip ETL processing - tables are populated by DocumentETLProcessor
      LOGGER.debug("Skipping standard ETL for table '{}' - populated by DocumentETLProcessor",
          context.getTableName());
      return false;
    });
  }

  // Flag to ensure download only runs once
  private volatile boolean downloadTriggered = false;

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
    try (InputStream is = getClass().getResourceAsStream("/sec/sec-schema-factory.defaults.json")) {
      if (is == null) {
        LOGGER.debug("No defaults file found at /sec/sec-schema-factory.defaults.json");
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
  public Map<String, Object> buildOperand(Map<String, Object> operand, org.apache.calcite.adapter.govdata.GovDataSchemaFactory parent) {
    LOGGER.info("=== SecSchemaFactory.buildOperand() START ===");
    LOGGER.info("Operand keys: {}", operand != null ? operand.keySet() : "NULL");

    try {
      // Access shared services from parent
      LOGGER.info("Getting storage provider from parent...");
      this.storageProvider = parent.getStorageProvider();
      LOGGER.info("Storage provider obtained: {}", storageProvider);

      // Create a mutable copy of operand to allow modifications
      LOGGER.info("Creating mutable operand copy...");
      Map<String, Object> mutableOperand = new HashMap<>(operand);
      LOGGER.info("Mutable operand created");

    // Load SecCacheManifest for ETag-based caching of submissions.json files
    // Note: Cache manifest loading is deferred until after getGovDataCacheDir() is called
    // to avoid duplicate variable declarations

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
      LOGGER.info("Loading defaults...");
      Map<String, Object> defaults = loadDefaults();
      if (defaults != null) {
        LOGGER.info("Applying defaults from sec-schema-factory.defaults.json");
        applyDefaults(mutableOperand, defaults);
        LOGGER.info("Defaults applied");
      }

      LOGGER.info("Storing currentOperand...");
      this.currentOperand = mutableOperand; // Store for table auto-discovery
      LOGGER.info("currentOperand stored");

      // Get cache directories from operand or environment variables
      LOGGER.info("Getting cache directories...");
      String govdataCacheDir = GovDataUtils.getCacheDir(mutableOperand);
      String govdataParquetDir = GovDataUtils.getParquetDir(mutableOperand);
      LOGGER.info("Cache directories obtained: cache={}, parquet={}", govdataCacheDir, govdataParquetDir);

    // Check required directories are configured
    if (govdataCacheDir == null || govdataCacheDir.isEmpty()) {
      throw new IllegalStateException("cacheDirectory must be set in model operand or GOVDATA_CACHE_DIR environment variable must be set");
    }
    if (govdataParquetDir == null || govdataParquetDir.isEmpty()) {
      throw new IllegalStateException("parquetDirectory must be set in model operand or GOVDATA_PARQUET_DIR environment variable must be set");
    }

    // SEC cache directory for raw content (XBRL/HTML files)
    String secCacheDir = storageProvider.resolvePath(govdataCacheDir, "sec");
    this.secCacheDirectory = secCacheDir;

    // Operating directory for metadata (.aperio/sec/)
    // This is passed from GovDataSchemaFactory which establishes it centrally
    // The .aperio directory is ALWAYS on local filesystem (working directory), even if parquet data is on S3
    this.secOperatingDirectory = (String) operand.get("operatingDirectory");
    if (this.secOperatingDirectory == null) {
      throw new IllegalStateException("Operating directory must be established by GovDataSchemaFactory");
    }
    LOGGER.debug("Received operating directory from parent: {}", this.secOperatingDirectory);

    // Load SecCacheManifest from operating directory
    this.cacheManifest = SecCacheManifest.load(this.secOperatingDirectory);
    LOGGER.debug("Loaded SEC cache manifest from {}", this.secOperatingDirectory);

    // Migrate legacy .notfound markers to manifest
    migrateNotFoundMarkers(secCacheDir);

    // SEC data directories - NEVER concatenate paths, always use storageProvider.resolvePath
    String secRawDir = storageProvider.resolvePath(govdataCacheDir, "sec");
    String secParquetDir = storageProvider.resolvePath(govdataParquetDir, "source=sec");
    LOGGER.info("SecSchemaFactory: govdataParquetDir='{}', secParquetDir='{}', operatingDir='{}'",
                govdataParquetDir, secParquetDir, this.secOperatingDirectory);

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
      LOGGER.info("Checking if data download is needed (autoDownload={})...", mutableOperand.get("autoDownload"));
      if (shouldDownloadData(mutableOperand)) {
        LOGGER.info("Data download needed - starting download...");
      // Get base directory from operand
      if (configuredDir != null) {
        File baseDir = new File(configuredDir);
        // Rebuild manifest from existing files first
        rebuildManifestFromExistingFiles(baseDir);
      }
        LOGGER.info("Calling downloadSecData...");
        downloadSecData(mutableOperand);
        LOGGER.info("downloadSecData completed");

        // CRITICAL: Wait for all downloads and conversions to complete
        // The downloadSecData method starts async tasks but returns immediately
        // We need to wait for them to complete before creating the FileSchema
        LOGGER.info("Waiting for all conversions to complete...");
        waitForAllConversions();
        LOGGER.info("All conversions completed");
      } else {
        LOGGER.info("Data download not needed (autoDownload={}, useMockData={})",
                    mutableOperand.get("autoDownload"), mutableOperand.get("useMockData"));
      }

      // Load table definitions from sec-schema.yaml (no fallback to hardcoded)
      LOGGER.info("Loading partitioned tables from sec-schema.yaml...");
      List<Map<String, Object>> partitionedTables = loadPartitionedTablesFromYaml();

      if (partitionedTables.isEmpty()) {
        throw new IllegalStateException(
            "Failed to load table definitions from sec-schema.yaml - file missing or invalid");
      }

      LOGGER.info("Using {} table definitions from sec-schema.yaml", partitionedTables.size());
      mutableOperand.put("partitionedTables", partitionedTables);
      skipHardcodedTableDefinitions(mutableOperand, partitionedTables, secParquetDir);
      return mutableOperand;
    } catch (Exception e) {
      LOGGER.error("ERROR in SecSchemaFactory.buildOperand(): {}", e.getMessage(), e);
      throw new RuntimeException("Failed to build SEC schema operand", e);
    }
  }


  /**
   * Completes operand setup when using YAML table definitions.
   *
   * <p>This method handles the remainder of buildOperand() when tables are loaded
   * from sec-schema.yaml instead of being hardcoded.
   */
  @SuppressWarnings("unchecked")
  private void skipHardcodedTableDefinitions(Map<String, Object> operand,
      List<Map<String, Object>> partitionedTables, String secParquetDir) {
    LOGGER.info("Completing operand setup with {} YAML table definitions", partitionedTables.size());

    // Set the directory for FileSchemaFactory to search
    operand.put("directory", secParquetDir);

    // Set materializeDirectory if not already set (required by SchemaLifecycleProcessor)
    if (!operand.containsKey("materializeDirectory")) {
      operand.put("materializeDirectory", secParquetDir);
    }

    // Preserve text similarity configuration if present, or enable it by default
    Map<String, Object> textSimConfig = (Map<String, Object>) operand.get("textSimilarity");
    if (textSimConfig == null) {
      textSimConfig = new HashMap<String, Object>();
      textSimConfig.put("enabled", true);
      operand.put("textSimilarity", textSimConfig);
    } else if (!Boolean.TRUE.equals(textSimConfig.get("enabled"))) {
      textSimConfig.put("enabled", true);
    }

    // Create conversion metadata with viewScanPatterns
    Map<String, org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord>
        conversionRecords = new HashMap<String,
            org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord>();

    for (Map<String, Object> table : partitionedTables) {
      String tableName = (String) table.get("name");
      String pattern = (String) table.get("pattern");

      if (tableName != null && pattern != null) {
        String viewScanPattern =
            storageProvider.resolvePath(storageProvider.resolvePath(secParquetDir, tableName),
                pattern);

        org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord record =
            new org.apache.calcite.adapter.file.metadata.ConversionMetadata.ConversionRecord();
        record.viewScanPattern = viewScanPattern;
        record.originalFile = tableName;
        record.conversionType = "SEC_XBRL_TO_PARQUET";
        record.tableName = tableName;
        record.tableConfig = table;

        conversionRecords.put(tableName, record);
        LOGGER.debug("Registered viewScanPattern for table '{}': {}", tableName, viewScanPattern);
      }
    }

    if (!conversionRecords.isEmpty()) {
      operand.put("conversionRecords", conversionRecords);
      LOGGER.info("Registered viewScanPatterns for {} SEC tables", conversionRecords.size());
    }

    // Disable cache priming for SEC adapter
    operand.put("primeCache", false);

    // Add schema-level comment
    String schemaComment = GovDataUtils.loadSchemaComment(getClass(), getSchemaResourceName());
    if (schemaComment != null) {
      operand.put("comment", schemaComment);
    }

    // Pass storage provider instance through operand
    if (this.storageProvider != null) {
      operand.put("_storageProvider", this.storageProvider);
    }

    LOGGER.info("SEC schema operand configuration complete (using YAML definitions)");
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

  private void addToManifest(String xbrlFilePath, String secParquetDirPath, List<File> outputFiles) {
    try {
      // Extract CIK and accession from file path
      // Path structure: cacheDir/{cik}/{accession}/{file}
      int lastSlash = xbrlFilePath.lastIndexOf('/');
      String accessionPath = xbrlFilePath.substring(0, lastSlash);
      int secondLastSlash = accessionPath.lastIndexOf('/');
      String accession = accessionPath.substring(secondLastSlash + 1);
      String cikPath = accessionPath.substring(0, secondLastSlash);
      int thirdLastSlash = cikPath.lastIndexOf('/');
      String cik = cikPath.substring(thirdLastSlash + 1);

      // Format accession with dashes if needed
      if (accession.length() == 18 && !accession.contains("-")) {
        accession = accession.substring(0, 10) + "-" +
                   accession.substring(10, 12) + "-" +
                   accession.substring(12);
      }

      // Check if vectorized files were created by looking at outputFiles list
      boolean hasVectorized = false;
      if (outputFiles != null) {
        for (File f : outputFiles) {
          if (f.getName().endsWith("_vectorized.parquet")) {
            hasVectorized = true;
            break;
          }
        }
      }

      // If vectorization is enabled in config, mark as PROCESSED_WITH_VECTORS even if no
      // vectorized files were created (e.g., no text content to vectorize).
      // This prevents re-checking on every run.
      boolean vectorizationEnabled = false;
      if (currentOperand != null) {
        Map<String, Object> textSimilarityConfig = (Map<String, Object>) currentOperand.get("textSimilarity");
        vectorizationEnabled = textSimilarityConfig != null &&
            Boolean.TRUE.equals(textSimilarityConfig.get("enabled"));
      }

      String fileTypes = (hasVectorized || vectorizationEnabled) ? "PROCESSED_WITH_VECTORS" : "PROCESSED";
      String entryPrefix = cik + "|" + accession + "|";
      String manifestKey = entryPrefix + fileTypes + "|" + System.currentTimeMillis();

      // Use per-CIK manifest for better organization and no lock contention
      File cikManifestDir = new File(this.secOperatingDirectory, "cik=" + cik);
      cikManifestDir.mkdirs();
      File manifestFile = new File(cikManifestDir, "processed_filings.manifest");

      synchronized (("manifest_" + cik).intern()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("addToManifest: ACQUIRED LOCK for cik={} accession={} hasVectorized={} thread={}",
              cik, accession, hasVectorized, Thread.currentThread().getName());
        }

        // Check if this entry already exists in the manifest (prevent duplicates)
        boolean alreadyExists = false;
        String existingEntry = null;
        if (manifestFile.exists()) {
          try (BufferedReader reader = new BufferedReader(new FileReader(manifestFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
              if (line.startsWith(entryPrefix)) {
                existingEntry = line;
                // Entry already exists - check if we need to upgrade it
                // Upgrade if: vectorization is enabled AND entry doesn't have PROCESSED_WITH_VECTORS
                if ((hasVectorized || vectorizationEnabled) && !line.contains("PROCESSED_WITH_VECTORS")) {
                  // Need to upgrade from PROCESSED to PROCESSED_WITH_VECTORS
                  // This is handled by rewriting below
                  if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("addToManifest: UPGRADING entry from '{}' to '{}' thread={}",
                        line, manifestKey, Thread.currentThread().getName());
                  }
                } else {
                  // Already has the correct status
                  alreadyExists = true;
                  if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("addToManifest: SKIPPING (already correct): {} thread={}",
                        line, Thread.currentThread().getName());
                  }
                  break;
                }
              }
            }
          }
        }

        if (LOGGER.isDebugEnabled() && existingEntry == null) {
          LOGGER.debug("addToManifest: NEW ENTRY '{}' thread={}",
              manifestKey, Thread.currentThread().getName());
        }

        // Only write if new or needs upgrade
        if (!alreadyExists) {
          // If upgrading, we need to rewrite the entire manifest to remove old entry
          if (manifestFile.exists()) {
            List<String> updatedLines = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new FileReader(manifestFile))) {
              String line;
              while ((line = reader.readLine()) != null) {
                // Skip old entries for this CIK|ACCESSION
                if (!line.startsWith(entryPrefix)) {
                  updatedLines.add(line);
                }
              }
            }
            // Add new entry
            updatedLines.add(manifestKey);

            // Write all lines back
            try (PrintWriter pw = new PrintWriter(new FileOutputStream(manifestFile, false))) {
              for (String line : updatedLines) {
                pw.println(line);
              }
            }

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("addToManifest: WROTE {} lines to manifest (updated entry for {}) thread={}",
                  updatedLines.size(), entryPrefix, Thread.currentThread().getName());
            }
          } else {
            // New manifest file
            try (PrintWriter pw = new PrintWriter(new FileOutputStream(manifestFile, true))) {
              pw.println(manifestKey);
            }

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("addToManifest: CREATED new manifest with entry '{}' thread={}",
                  manifestKey, Thread.currentThread().getName());
            }
          }
        } else {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("addToManifest: SKIPPED write (alreadyExists=true) thread={}",
                Thread.currentThread().getName());
          }
        }

        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("addToManifest: RELEASING LOCK for cik={} thread={}",
              cik, Thread.currentThread().getName());
        }
      }
    } catch (Exception e) {
      LOGGER.error("CRITICAL: Could not update manifest - filing will be reprocessed on next run: {}", e.getMessage(), e);
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
    LOGGER.debug("downloadSecData() called - STARTING SEC DATA DOWNLOAD");

    // Store operand for use by helper methods (e.g. createSecTablesFromXbrl)
    this.currentOperand = operand;

    // Clear download tracking for the new cycle
    downloadedInThisCycle.clear();

    try {
      // Metadata (submissions.json and processed_filings.manifest) goes to operating directory under cik= subdirectories
      LOGGER.info("Using SEC operating directory: {}", this.secOperatingDirectory);

      // Raw content (XBRL/HTML files) goes to cache directory
      // StorageProvider automatically creates parent directories when writing files
      LOGGER.info("Using SEC cache directory: {}", this.secCacheDirectory);

      // Check if we should use mock data instead of downloading
      Boolean useMockData = (Boolean) operand.get("useMockData");
      if (useMockData != null && useMockData) {
        LOGGER.info("Using mock data for testing");
        List<String> ciks = getCiksFromConfig(operand);
        int startYear = (Integer) operand.getOrDefault("startYear", 2020);
        int endYear = (Integer) operand.getOrDefault("endYear", 2023);
        LOGGER.debug("DEBUG: Calling createSecTablesFromXbrl for mock data");
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("DEBUG: secOperatingDirectory={}", this.secOperatingDirectory);
        }
        // Create minimal mock Parquet files so tables are discovered
        downloadStockPrices(this.secOperatingDirectory, Arrays.asList("320187", "51143", "789019"), 2021, 2024);
        LOGGER.debug("DEBUG: Mock data mode - created mock Parquet files");

        // Also create mock stock prices if enabled
        boolean fetchStockPrices = (Boolean) operand.getOrDefault("fetchStockPrices", true);
        if (fetchStockPrices) {
          LOGGER.debug("Creating mock stock prices for testing");
          createMockStockPrices(this.secOperatingDirectory, ciks, startYear, endYear);
        }

        return;
      }

      // Use document-based ETL model (default, no fallback to legacy)
      LOGGER.info("Using document-based ETL model");
      DocumentETLProcessor.DocumentETLResult result = downloadSecDataUsingDocumentEtl(operand);

      // Log results
      if (result.isSuccess()) {
        LOGGER.info("Document ETL completed successfully: {} documents processed",
            result.getDocumentsProcessed());
      } else {
        LOGGER.warn("Document ETL completed with {} failures", result.getDocumentsFailed());
        for (String error : result.getErrors()) {
          LOGGER.warn("  Error: {}", error);
        }
      }

      // Download stock prices (still needed as separate step)
      boolean fetchStockPrices = (Boolean) operand.getOrDefault("fetchStockPrices", true);
      if (fetchStockPrices) {
        List<String> ciks = getCiksFromConfig(operand);
        int startYear = (Integer) operand.getOrDefault("startYear", 2020);
        int endYear = (Integer) operand.getOrDefault("endYear", Year.now().getValue());
        downloadStockPrices(this.secCacheDirectory, ciks, startYear, endYear);
      }

      // Save SecCacheManifest after all downloads complete
      if (cacheManifest != null) {
        cacheManifest.save(this.secOperatingDirectory);
        LOGGER.debug("Saved SEC cache manifest to {}", this.secOperatingDirectory);
      }

    } catch (Exception e) {
      LOGGER.error("Error in downloadSecData", e);
      LOGGER.warn("Failed to download SEC data: " + e.getMessage());
    } finally {
      // Save manifest in finally block to ensure it's saved even if errors occur
      if (cacheManifest != null) {
        String cacheDir = getGovDataCacheDir();
        if (cacheDir != null) {
          String secCacheDir = storageProvider.resolvePath(cacheDir, "sec");
          cacheManifest.save(this.secOperatingDirectory);
          LOGGER.debug("Saved SEC cache manifest in finally block to {}", secCacheDir);
        }
      }
    }
  }

  /**
   * Downloads SEC data using the document-based ETL model.
   *
   * <p>This method uses {@link DocumentETLProcessor} to process SEC filings,
   * delegating to the XbrlToParquetConverter for document conversion.
   *
   * @param operand Schema operand with configuration
   * @return Processing result with statistics
   */
  private DocumentETLProcessor.DocumentETLResult downloadSecDataUsingDocumentEtl(
      Map<String, Object> operand) {
    LOGGER.info("Starting SEC data download using DocumentETLProcessor");

    try {
      // Get CIKs from configuration
      List<String> ciks = getCiksFromConfig(operand);
      if (ciks.isEmpty()) {
        LOGGER.warn("No CIKs configured for document-based ETL");
        return new DocumentETLProcessor.DocumentETLResult(
            0, 0, 0, new ArrayList<File>(), new ArrayList<String>(), 0);
      }

      // Get filing types and year range
      List<String> filingTypes = getFilingTypes(operand);
      int startYear = getIntValue(operand, "startYear", 2020);
      int endYear = getIntValue(operand, "endYear", Year.now().getValue());
      LOGGER.info("Year range: {} to {}", startYear, endYear);

      // Get parquet directory for output
      String govdataParquetDir = GovDataUtils.getParquetDir(operand);
      String secParquetDir = storageProvider.resolvePath(govdataParquetDir, "source=sec");

      // Create HttpSourceConfig from YAML-style configuration
      HttpSourceConfig httpSourceConfig = createHttpSourceConfig(operand, filingTypes);

      // Get or create document converter
      FileConverter documentConverter = getOrCreateXbrlConverter();

      // Create processor
      DocumentETLProcessor processor = new DocumentETLProcessor(
          httpSourceConfig,
          storageProvider,
          secParquetDir,
          new File(this.secCacheDirectory),
          documentConverter);

      // Build entity list with CIKs and year range
      List<Map<String, String>> entities = new ArrayList<Map<String, String>>();
      for (String cik : ciks) {
        String normalizedCik = normalizeCik(cik);
        for (int year = startYear; year <= endYear; year++) {
          Map<String, String> entity = new HashMap<String, String>();
          entity.put("cik", normalizedCik);
          entity.put("year", String.valueOf(year));
          entities.add(entity);
        }
      }

      LOGGER.info("Processing {} CIKs x {} years = {} entity-year combinations",
          ciks.size(), (endYear - startYear + 1), entities.size());

      // Process all entities
      DocumentETLProcessor.DocumentETLResult result = processor.processEntities(entities);

      LOGGER.info("Document ETL completed: {} processed, {} skipped, {} failed in {}ms",
          result.getDocumentsProcessed(),
          result.getDocumentsSkipped(),
          result.getDocumentsFailed(),
          result.getDurationMs());

      return result;

    } catch (Exception e) {
      LOGGER.error("Document ETL failed: {}", e.getMessage(), e);
      List<String> errors = new ArrayList<String>();
      errors.add(e.getMessage());
      return new DocumentETLProcessor.DocumentETLResult(
          0, 0, 1, new ArrayList<File>(), errors, 0);
    }
  }

  /**
   * Extracts an integer value from operand, handling both Integer and Number types.
   */
  private int getIntValue(Map<String, Object> operand, String key, int defaultValue) {
    Object value = operand.get(key);
    if (value == null) {
      return defaultValue;
    }
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    if (value instanceof String) {
      try {
        return Integer.parseInt((String) value);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid integer value for {}: {}", key, value);
        return defaultValue;
      }
    }
    return defaultValue;
  }

  /**
   * Creates HttpSourceConfig from operand for DocumentETLProcessor.
   */
  private HttpSourceConfig createHttpSourceConfig(Map<String, Object> operand,
      List<String> filingTypes) {
    // Create DocumentSourceConfig using fromMap
    Map<String, Object> docSourceMap = new HashMap<String, Object>();
    docSourceMap.put("metadataUrl", "https://data.sec.gov/submissions/CIK{cik}.json");
    // SEC URLs require CIK without leading zeros and accession without dashes
    docSourceMap.put("documentUrl",
        "https://www.sec.gov/Archives/edgar/data/{cik_url}/{accession_url}/{document}");
    docSourceMap.put("documentTypes", filingTypes);
    docSourceMap.put("extractionType", "xbrl");
    docSourceMap.put("documentConverter",
        "org.apache.calcite.adapter.govdata.sec.XbrlToParquetConverter");
    docSourceMap.put("responseTransformer",
        "org.apache.calcite.adapter.govdata.sec.EdgarResponseTransformer");

    HttpSourceConfig.DocumentSourceConfig docConfig =
        HttpSourceConfig.DocumentSourceConfig.fromMap(docSourceMap);

    // Create rate limit config using fromMap
    Map<String, Object> rateLimitMap = new HashMap<String, Object>();
    rateLimitMap.put("requestsPerSecond", SEC_RATE_LIMIT_PER_SECOND);
    rateLimitMap.put("retryOn", Arrays.asList(429, 503));
    rateLimitMap.put("maxRetries", 5);
    rateLimitMap.put("retryBackoffMs", 5000L);
    HttpSourceConfig.RateLimitConfig rateLimitConfig =
        HttpSourceConfig.RateLimitConfig.fromMap(rateLimitMap);

    // Create headers
    Map<String, String> headers = new HashMap<String, String>();
    headers.put("User-Agent", "Apache Calcite SEC Adapter (apache-calcite@apache.org)");
    headers.put("Accept-Encoding", "gzip, deflate");

    // Build HttpSourceConfig
    return new HttpSourceConfig.Builder()
        .documentSource(docConfig)
        .rateLimit(rateLimitConfig)
        .headers(headers)
        .build();
  }

  /**
   * Loads partitioned table definitions from sec-schema.yaml.
   *
   * <p>When using the document-based ETL model, table definitions come from
   * the YAML schema file instead of being hardcoded.
   *
   * @return List of table configurations
   */
  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> loadPartitionedTablesFromYaml() {
    List<Map<String, Object>> partitionedTables = new ArrayList<Map<String, Object>>();

    try (InputStream is = getClass().getResourceAsStream("/sec/sec-schema.yaml")) {
      if (is == null) {
        LOGGER.warn("sec-schema.yaml not found, using hardcoded table definitions");
        return partitionedTables;
      }

      // Load YAML with high alias limit for complex schemas
      org.yaml.snakeyaml.LoaderOptions loaderOptions = new org.yaml.snakeyaml.LoaderOptions();
      loaderOptions.setMaxAliasesForCollections(500);
      org.yaml.snakeyaml.Yaml yaml = new org.yaml.snakeyaml.Yaml(loaderOptions);
      Map<String, Object> config = yaml.load(is);

      // Extract tables array from YAML
      Object tablesObj = config.get("tables");
      if (!(tablesObj instanceof List)) {
        LOGGER.warn("No 'tables' array found in sec-schema.yaml");
        return partitionedTables;
      }

      List<?> tables = (List<?>) tablesObj;
      for (Object tableObj : tables) {
        if (!(tableObj instanceof Map)) {
          continue;
        }

        Map<String, Object> tableConfig = (Map<String, Object>) tableObj;
        String tableName = (String) tableConfig.get("name");
        String pattern = (String) tableConfig.get("pattern");
        Object partitionsObj = tableConfig.get("partitions");

        if (tableName == null || pattern == null) {
          continue;
        }

        // Create table definition compatible with FileSchemaFactory
        Map<String, Object> tableDefinition = new HashMap<String, Object>();
        tableDefinition.put("name", tableName);
        tableDefinition.put("pattern", pattern);

        // Process partitions
        if (partitionsObj instanceof Map) {
          Map<String, Object> partitions = (Map<String, Object>) partitionsObj;
          Map<String, Object> partitionConfig = new HashMap<String, Object>();

          // Copy partition style
          String style = (String) partitions.get("style");
          if (style != null) {
            partitionConfig.put("style", style);
          }

          // Process column definitions
          Object columnDefsObj = partitions.get("columnDefinitions");
          if (columnDefsObj instanceof List) {
            List<?> columnDefs = (List<?>) columnDefsObj;
            List<Map<String, Object>> processedCols = new ArrayList<Map<String, Object>>();

            for (Object colObj : columnDefs) {
              if (colObj instanceof Map) {
                Map<String, Object> col = (Map<String, Object>) colObj;
                Map<String, Object> colDef = new HashMap<String, Object>();
                colDef.put("name", col.get("name"));
                colDef.put("type", col.get("type"));
                processedCols.add(colDef);
              }
            }

            partitionConfig.put("columnDefinitions", processedCols);
          }

          tableDefinition.put("partitions", partitionConfig);
        }

        // Process constraints if present
        Object constraintsObj = tableConfig.get("constraints");
        if (constraintsObj instanceof Map) {
          tableDefinition.put("constraints", constraintsObj);
        }

        partitionedTables.add(tableDefinition);
        LOGGER.debug("Loaded table definition from YAML: {}", tableName);
      }

      LOGGER.info("Loaded {} table definitions from sec-schema.yaml", partitionedTables.size());

    } catch (Exception e) {
      LOGGER.warn("Failed to load table definitions from sec-schema.yaml: {}", e.getMessage());
    }

    return partitionedTables;
  }

  /**
   * Gets or creates the XBRL to Parquet converter.
   */
  private FileConverter getOrCreateXbrlConverter() {
    try {
      Class<?> clazz = Class.forName(
          "org.apache.calcite.adapter.govdata.sec.XbrlToParquetConverter");

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
      LOGGER.error("Failed to instantiate XbrlToParquetConverter: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Normalizes a CIK to 10-digit zero-padded format.
   */
  private String normalizeCik(String cik) {
    if (cik == null || cik.isEmpty()) {
      return cik;
    }
    // Remove non-numeric characters and pad to 10 digits
    String numericCik = cik.replaceAll("[^0-9]", "");
    return String.format("%010d", Long.parseLong(numericCik));
  }

  private void downloadCikFilings(SecHttpStorageProvider provider, String cik,
      List<String> filingTypes, int startYear, int endYear) {
    // Ensure executors are initialized
    initializeExecutors();

    LOGGER.info("downloadCikFilings() START for CIK: {}", cik);

    try {
      // Normalize CIK
      String normalizedCik = String.format("%010d", Long.parseLong(cik.replaceAll("[^0-9]", "")));
      LOGGER.info("  Normalized CIK: {}", normalizedCik);

      // Metadata (submissions.json and processed_filings.manifest) goes to operating directory
      File cikMetadataDir = new File(this.secOperatingDirectory, "cik=" + normalizedCik);
      cikMetadataDir.mkdirs();
      LOGGER.info("  CIK metadata directory: {}", cikMetadataDir.getAbsolutePath());

      // Raw content (XBRL/HTML files) goes to cache directory
      String cikCachePath = storageProvider.resolvePath(this.secCacheDirectory, normalizedCik);
      LOGGER.info("  CIK cache directory: {}", cikCachePath);

      // Download submissions metadata with ETag-based caching
      File submissionsFile = new File(cikMetadataDir, "submissions.json");
      LOGGER.info("  Checking submissions file: {} (exists={})", submissionsFile.getAbsolutePath(), submissionsFile.exists());

      // OPTIMIZATION: Check if this CIK is fully processed before doing any individual filing checks
      if (cacheManifest != null && cacheManifest.isCikFullyProcessed(normalizedCik)) {
        LOGGER.info("CIK {} is fully processed and submissions.json unchanged - skipping entire CIK (FAST PATH)", normalizedCik);
        return; // Skip this entire CIK - no need to check individual filings
      }

      // Check if submissions are cached using manifest
      boolean needsDownload = true;
      if (cacheManifest != null && cacheManifest.isCached(normalizedCik)) {
        LOGGER.info("  Cache manifest indicates CIK {} is cached", normalizedCik);
        String cachedFilePath = cacheManifest.getFilePath(normalizedCik);
        if (cachedFilePath != null && submissionsFile.exists()) {
          // Check if we should use conditional GET with ETag
          String cachedETag = cacheManifest.getETag(normalizedCik);
          if (cachedETag != null && !cachedETag.isEmpty()) {
            // We have an ETag - try conditional GET
            needsDownload = true; // Will check via HTTP 304
            LOGGER.debug("Have cached submissions with ETag for CIK {}, will check via conditional GET", normalizedCik);
          } else {
            // Cached without ETag - manifest says it's fresh
            needsDownload = false;
            LOGGER.debug("Using cached submissions for CIK {} (valid per manifest)", normalizedCik);
          }
        }
      }

      if (needsDownload) {
        // Apply rate limiting
        try {
          rateLimiter.acquire();
          Thread.sleep(currentRateLimitDelayMs.get()); // Use dynamic rate limit delay
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        try {
          String submissionsUrl =
              String.format("https://data.sec.gov/submissions/CIK%s.json", normalizedCik);

          // Build request with conditional GET support
          java.net.HttpURLConnection conn =
              (java.net.HttpURLConnection) java.net.URI.create(submissionsUrl).toURL().openConnection();
          conn.setRequestMethod("GET");
          conn.setRequestProperty("User-Agent", "Apache Calcite SEC Adapter (apache-calcite@apache.org)");
          conn.setRequestProperty("Accept", "application/json");

          // Add conditional GET header if we have cached metadata
          String cachedETag = (cacheManifest != null) ? cacheManifest.getETag(normalizedCik) : null;
          if (cachedETag != null && !cachedETag.isEmpty()) {
            // Check if it's a Last-Modified value (prefixed with "lm:")
            if (cachedETag.startsWith("lm:")) {
              String lastModified = cachedETag.substring(3);
              conn.setRequestProperty("If-Modified-Since", lastModified);
              LOGGER.debug("Sending conditional GET with Last-Modified: {}", lastModified);
            } else {
              conn.setRequestProperty("If-None-Match", cachedETag);
              LOGGER.debug("Sending conditional GET with ETag: {}", cachedETag);
            }
          }

          int responseCode = conn.getResponseCode();

          if (responseCode == 304) {
            // Not Modified - use cached file
            LOGGER.info("Submissions unchanged for CIK {} (304 Not Modified)", normalizedCik);
            // Manifest already knows about this file, no need to update
          } else if (responseCode == 200) {
            // New or updated content - download and cache
            String newETag = conn.getHeaderField("ETag");
            String lastModified = conn.getHeaderField("Last-Modified");

            try (InputStream is = conn.getInputStream();
                 FileOutputStream fos = new FileOutputStream(submissionsFile)) {
              byte[] buffer = new byte[8192];
              int bytesRead;
              while ((bytesRead = is.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
              }
            }

            // Clean up macOS metadata files after writing
            storageProvider.cleanupMacosMetadata(cikCachePath);

            // Update manifest with ETag or Last-Modified
            if (cacheManifest != null) {
              long fileSize = submissionsFile.length();
              // Prefer ETag, fallback to Last-Modified, final fallback to 24-hour TTL
              String refreshReason;
              long refreshAfter;
              if (newETag != null) {
                refreshAfter = Long.MAX_VALUE;
                refreshReason = "etag_based";
              } else if (lastModified != null) {
                refreshAfter = Long.MAX_VALUE;
                refreshReason = "last_modified_based";
                // Store Last-Modified as "ETag" for conditional request
                newETag = "lm:" + lastModified;
              } else {
                refreshAfter = System.currentTimeMillis() + java.util.concurrent.TimeUnit.HOURS.toMillis(24);
                refreshReason = "daily_fallback_no_metadata";
              }
              // Invalidate fully_processed flag since submissions.json changed
              cacheManifest.invalidateCikFullyProcessed(normalizedCik);

              cacheManifest.markCached(normalizedCik, submissionsFile.getAbsolutePath(),
                                      newETag, fileSize, refreshAfter, refreshReason);

              // Save manifest immediately to avoid losing cache metadata on interruption
              String cacheDir = getGovDataCacheDir();
              if (cacheDir != null) {
                String secCacheDir = storageProvider.resolvePath(cacheDir, "sec");
                cacheManifest.save(this.secOperatingDirectory);
              }

              if (newETag != null) {
                LOGGER.info("Downloaded and cached submissions for CIK {} (ETag: {})", normalizedCik, newETag);
              } else {
                LOGGER.info("Downloaded and cached submissions for CIK {} (no ETag)", normalizedCik);
              }
            } else {
              LOGGER.debug("Downloaded submissions metadata for CIK {} (no manifest available)", normalizedCik);
            }
          } else {
            LOGGER.warn("Unexpected response code {} for submissions CIK {}", responseCode, normalizedCik);
          }
        } finally {
          rateLimiter.release();
        }
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
      JsonNode reportDates = recent.get("reportDate");  // Period end date for XBRL filename
      JsonNode forms = recent.get("form");
      JsonNode primaryDocuments = recent.get("primaryDocument");
      JsonNode isXBRLArray = recent.get("isXBRL");
      JsonNode isInlineXBRLArray = recent.get("isInlineXBRL");

      if (accessionNumbers == null || !accessionNumbers.isArray()) {
        LOGGER.warn("No accession numbers found for CIK " + normalizedCik);
        return;
      }

      // Collect all filings to download
      List<FilingToDownload> filingsToDownload = new ArrayList<>();
      int skippedNonXBRL = 0;
      int skipped424B = 0;
      int skippedFormType = 0;
      int skippedYearRange = 0;
      for (int i = 0; i < accessionNumbers.size(); i++) {
        String accession = accessionNumbers.get(i).asText();
        String filingDate = filingDates.get(i).asText();
        String reportDate = (reportDates != null && i < reportDates.size()) ? reportDates.get(i).asText() : null;
        String form = forms.get(i).asText();
        String primaryDoc = primaryDocuments.get(i).asText();

        // Skip 424B forms first - they're prospectuses and we never want to process them
        // even if they have XBRL data
        if (form.startsWith("424B")) {
          skipped424B++;
          continue;
        }

        // Check if this is an insider form (3/4/5) - these are XML but NOT XBRL
        boolean isInsiderForm = form.equals("3") || form.equals("4") || form.equals("5")
            || form.equals("3/A") || form.equals("4/A") || form.equals("5/A");

        // Skip non-XBRL filings entirely - UNLESS they're insider forms
        // Insider forms (3/4/5) are XML ownership documents, not XBRL, but we still want to process them
        // This is a major optimization: prevents downloading, parsing, and manifest checking
        // for filings that would produce no Parquet output anyway
        boolean hasXBRL = false;
        boolean hasInlineXBRL = false;
        if (isXBRLArray != null && i < isXBRLArray.size()) {
          int isXBRL = isXBRLArray.get(i).asInt(0);
          hasXBRL = (isXBRL == 1);
        }
        // Check inline XBRL independently - some filings have BOTH isXBRL=1 and isInlineXBRL=1
        if (isInlineXBRLArray != null && i < isInlineXBRLArray.size()) {
          int isInlineXBRL = isInlineXBRLArray.get(i).asInt(0);
          hasInlineXBRL = (isInlineXBRL == 1);
          if (hasInlineXBRL) {
            hasXBRL = true;  // Inline XBRL counts as XBRL
          }
        }
        if (!hasXBRL && !isInsiderForm) {
          skippedNonXBRL++;
          continue;
        }

        // Check if filing type matches and is in date range
        // Normalize form comparison (SEC uses "10K", config uses "10-K")
        String normalizedForm = form.replace("-", "");

        // Use explicit whitelist for allowed forms
        // NOTE: S-* forms (S-3, S-4, S-8, etc.) are registration statements that rarely
        // contain useful financial XBRL data and are explicitly excluded
        List<String> allowedForms =
            Arrays.asList("3", "4", "5",           // Insider forms
            "10-K", "10K",           // Annual reports
            "10-Q", "10Q",           // Quarterly reports
            "8-K", "8K",             // Current reports
            "8-K/A", "8KA",          // Amended current reports
            "DEF 14A", "DEF14A");      // Proxy statements
        boolean matchesType = allowedForms.stream()
            .anyMatch(type -> type.replace("-", "").equalsIgnoreCase(normalizedForm));
        if (!matchesType) {
          skippedFormType++;
          continue;
        }

        int filingYear = Integer.parseInt(filingDate.substring(0, 4));
        if (filingYear < startYear || filingYear > endYear) {
          skippedYearRange++;
          continue;
        }

        filingsToDownload.add(
            new FilingToDownload(normalizedCik, accession,
            primaryDoc, form, filingDate, reportDate, cikCachePath, hasInlineXBRL));
      }

      if (skipped424B > 0) {
        LOGGER.info("Skipped {} 424B filings for CIK {} (prospectuses excluded)", skipped424B, normalizedCik);
      }
      if (skippedNonXBRL > 0) {
        LOGGER.info("Skipped {} non-XBRL filings for CIK {} (isXBRL=0, isInlineXBRL=0)", skippedNonXBRL, normalizedCik);
      }
      if (skippedFormType > 0) {
        LOGGER.info("Skipped {} filings for CIK {} (form type not in whitelist)", skippedFormType, normalizedCik);
      }
      if (skippedYearRange > 0) {
        LOGGER.info("Skipped {} filings for CIK {} (outside year range {}-{})", skippedYearRange, normalizedCik, startYear, endYear);
      }

      // Load per-CIK manifest (huge performance improvement over global manifest)
      File cikManifestDir = new File(this.secOperatingDirectory, "cik=" + normalizedCik);
      File manifestFile = new File(cikManifestDir, "processed_filings.manifest");
      Set<String> processedFilingsManifest = new HashSet<>();
      if (manifestFile.exists()) {
        try {
          Set<String> rawEntries = new HashSet<>(Files.readAllLines(manifestFile.toPath()));
          // Migrate old manifest entries to current format if needed
          processedFilingsManifest = migrateManifestIfNeeded(manifestFile, rawEntries);

          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Loaded {} entries from manifest for CIK {}", processedFilingsManifest.size(), normalizedCik);
          }
        } catch (IOException e) {
          LOGGER.warn("Failed to load manifest file for CIK {}: {}", normalizedCik, e.getMessage());
        }
      }

      // Quick check: if ALL filings are already processed, skip the entire CIK
      if (!processedFilingsManifest.isEmpty() && filingsToDownload.size() > 0) {
        boolean allFilingsProcessed = true;
        for (FilingToDownload filing : filingsToDownload) {
          String filingKey = filing.cik + "|" + filing.accession + "|" + filing.form + "|" + filing.filingDate;
          if (downloadedInThisCycle.contains(filingKey)) {
            continue; // Skip duplicates
          }
          FilingStatus status = checkFilingStatusInMemory(filing.cik, filing.accession, filing.form, filing.filingDate, processedFilingsManifest);
          if (status != FilingStatus.FULLY_PROCESSED && status != FilingStatus.EXCLUDED_424B && status != FilingStatus.HAS_ALL_PARQUET) {
            allFilingsProcessed = false;
            break;
          }
        }

        if (allFilingsProcessed) {
          LOGGER.info("All {} filings for CIK {} are already fully processed - skipping entire CIK", filingsToDownload.size(), normalizedCik);

          // Mark this CIK as fully processed in the cache manifest for future runs
          if (cacheManifest != null) {
            cacheManifest.markCikFullyProcessed(normalizedCik, filingsToDownload.size());
            String cacheDir = getGovDataCacheDir();
            if (cacheDir != null) {
              cacheManifest.save(this.secOperatingDirectory);
            }
          }

          return; // Skip this entire CIK
        }
      }

      // Filter filings - only create futures for those that need processing
      List<FilingToDownload> filingsNeedingProcessing = new ArrayList<>();
      int skippedCached = 0;

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

        // Check manifest to see if this filing is already fully processed
        FilingStatus status = checkFilingStatusInMemory(filing.cik, filing.accession, filing.form, filing.filingDate, processedFilingsManifest);

        if (status == FilingStatus.FULLY_PROCESSED || status == FilingStatus.EXCLUDED_424B || status == FilingStatus.HAS_ALL_PARQUET) {
          skippedCached++;
          continue;
        }

        // Filing needs processing - add to list
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("DEBUG: Filing needs processing - CIK: {}, Accession: {}, Form: {}, Status: {}",
              filing.cik, filing.accession, filing.form, status);
        }
        filingsNeedingProcessing.add(filing);
      }

      if (skippedCached > 0) {
        LOGGER.info("Skipped {} already-processed filings for CIK {} (checked manifest before scheduling)", skippedCached, normalizedCik);
      }

      // Download filings in parallel with rate limiting
      totalFilingsToProcess.addAndGet(filingsNeedingProcessing.size());
      if (filingsNeedingProcessing.size() > 0) {
        LOGGER.info("Scheduling {} filings for download/processing for CIK {}", filingsNeedingProcessing.size(), normalizedCik);
      } else {
        // If we scheduled 0 filings, it means all filings are already processed
        // Mark this CIK as fully processed for future runs
        if (cacheManifest != null && filingsToDownload.size() > 0) {
          LOGGER.info("All {} filings for CIK {} are already processed - marking as fully processed", filingsToDownload.size(), normalizedCik);
          cacheManifest.markCikFullyProcessed(normalizedCik, filingsToDownload.size());
          String cacheDir = getGovDataCacheDir();
          if (cacheDir != null) {
            cacheManifest.save(this.secOperatingDirectory);
          }
        }
      }

      for (FilingToDownload filing : filingsNeedingProcessing) {
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
    final String reportDate;  // Period end date (for XBRL filename construction)
    final String cikCachePath;
    final boolean hasInlineXBRL;

    FilingToDownload(String cik, String accession, String primaryDoc,
        String form, String filingDate, String reportDate, String cikCachePath, boolean hasInlineXBRL) {
      this.cik = cik;
      this.accession = accession;
      this.primaryDoc = primaryDoc;
      this.form = form;
      this.filingDate = filingDate;
      this.reportDate = reportDate;
      this.cikCachePath = cikCachePath;
      this.hasInlineXBRL = hasInlineXBRL;
    }
  }

  // Helper class to hold aggregated manifest entry status
  private static class FilingManifestEntry {
    boolean hasVectorized = false;
    boolean isNoXbrl = false;
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
    // Create cache key
    String cacheKey = cik + "|" + accession + "|" + form + "|" + filingDate;

    // Check cache first
    FilingStatus cachedStatus = filingStatusCache.get(cacheKey);
    if (cachedStatus != null) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Filing status CACHE HIT: {} -> {}", cacheKey, cachedStatus);
      }
      return cachedStatus;
    }

    // Cache miss - compute status
    FilingStatus status;
    try {
      // Skip checking entirely for 424B forms - they are excluded from processing
      if (form.startsWith("424B")) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("424B filing excluded from processing: {} {}", form, filingDate);
        }
        status = FilingStatus.EXCLUDED_424B;
        filingStatusCache.put(cacheKey, status);
        return status;
      }

      // Check manifest first
      if (manifestFile.exists()) {
        Set<String> rawEntries = new HashSet<>(Files.readAllLines(manifestFile.toPath()));
        // Migrate old manifest entries to current format if needed
        Set<String> processedFilings = migrateManifestIfNeeded(manifestFile, rawEntries);

        String searchPrefix = cik + "|" + accession + "|";
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("checkFilingStatus: searching manifest for prefix='{}' in {} entries from {}",
              searchPrefix, processedFilings.size(), manifestFile.getAbsolutePath());
        }

        boolean isInManifest = false;
        boolean hasVectorized = false;
        boolean isNoXbrl = false;
        int matchAttempts = 0;
        for (String entry : processedFilings) {
          if (entry.startsWith(searchPrefix)) {
            isInManifest = true;
            hasVectorized = entry.contains("PROCESSED_WITH_VECTORS");
            isNoXbrl = entry.contains("NO_XBRL");
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("checkFilingStatus: FOUND entry='{}' for prefix='{}'", entry, searchPrefix);
            }
            break;
          }
          // Debug: show first few entries that don't match
          if (LOGGER.isDebugEnabled() && matchAttempts < 3 && entry.contains(accession)) {
            LOGGER.debug("checkFilingStatus: NEAR MISS - entry contains accession but doesn't match prefix. entry='{}' searchPrefix='{}'",
                entry, searchPrefix);
            matchAttempts++;
          }
        }

        // Check if text similarity is enabled
        Map<String, Object> textSimilarityConfig = (Map<String, Object>) currentOperand.get("textSimilarity");
        boolean needsVectorized = textSimilarityConfig != null &&
            Boolean.TRUE.equals(textSimilarityConfig.get("enabled"));

        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("checkFilingStatus: cik={} accession={} isInManifest={} needsVectorized={} hasVectorized={} isNoXbrl={}",
              cik, accession, isInManifest, needsVectorized, hasVectorized, isNoXbrl);
        }

        if (isInManifest && isNoXbrl) {
          // Filing has no XBRL data - fully processed (nothing to convert)
          status = FilingStatus.FULLY_PROCESSED;
          filingStatusCache.put(cacheKey, status);
          return status;
        } else if (isInManifest && (!needsVectorized || hasVectorized)) {
          // Filing is in manifest and either:
          // 1. Vectorization not needed, OR
          // 2. Has vectorization flag (was processed with vectorization enabled)
          status = FilingStatus.FULLY_PROCESSED;
          filingStatusCache.put(cacheKey, status);
          return status;
        } else if (isInManifest && needsVectorized && !hasVectorized) {
          // Filing in manifest but missing vectorization flag - needs reprocessing
          // Note: With the fix above, new filings will be marked PROCESSED_WITH_VECTORS
          // even if no vectorized files created, so this should be rare.
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Filing in manifest but needs vectorization reprocessing: {} {}", form, filingDate);
          }
          // Fall through to return NEEDS_PROCESSING
        }

      }

      // DISABLED: Cannot reliably check Parquet files before parsing XBRL because fiscal year
      // (used for partitioning) is only known after parsing XBRL data. The heuristic-based
      // getPartitionYear() method produces incorrect year folders, causing false cache misses.
      // Solution: Rely on manifest file only. After processing, files are added to manifest.
      // if (hasAllParquetFiles(cik, accession, form, filingDate)) {
      //   return FilingStatus.HAS_ALL_PARQUET;
      // }

      status = FilingStatus.NEEDS_PROCESSING;
      filingStatusCache.put(cacheKey, status);
      return status;
    } catch (Exception e) {
      LOGGER.debug("Error checking filing status: " + e.getMessage());
      status = FilingStatus.NEEDS_PROCESSING;
      filingStatusCache.put(cacheKey, status);
      return status;
    }
  }

  /**
   * Check filing status using an in-memory manifest (performance-optimized version).
   * This avoids repeated file I/O and stale reads by working with a pre-loaded manifest.
   *
   * IMPORTANT: Manifest may contain duplicate entries for the same filing (historical issue).
   * We must scan ALL entries and use the LATEST status (PROCESSED_WITH_VECTORS takes precedence).
   */
  /**
   * Check if the given form type is allowed for XBRL conversion.
   * Excludes 424B prospectuses and S-* registration statements.
   */
  private boolean isFormTypeAllowed(String formType) {
    // Normalize form type (remove dashes and slashes)
    String normalizedForm = formType.replace("-", "").replace("/", "");

    // Whitelist of allowed forms
    List<String> allowedForms =
        Arrays.asList("3", "4", "5",           // Insider forms
        "10K",                   // Annual reports
        "10Q",                   // Quarterly reports
        "8K",                    // Current reports
        "8KA",                   // Amended current reports
        "DEF14A");                 // Proxy statements

    return allowedForms.stream()
        .anyMatch(type -> type.equalsIgnoreCase(normalizedForm));
  }

  /**
   * Migrates old manifest entries to current format if needed.
   *
   * <p>Manifest format evolution:
   * - v0 (very old): CIK|ACCESSION (2 fields)
   * - v1 (old): CIK|ACCESSION|STATUS (3 fields)
   * - v2 (current): CIK|ACCESSION|STATUS|TIMESTAMP (4 fields)
   *
   * <p>This method detects old format entries and upgrades them transparently,
   * preventing unnecessary reprocessing of filings that were already converted
   * in previous code versions.
   *
   * @param manifestFile The manifest file (used for saving migrated entries)
   * @param entries The raw manifest entries loaded from disk
   * @return Migrated entries in current format
   */
  private Set<String> migrateManifestIfNeeded(File manifestFile, Set<String> entries) {
    Set<String> migrated = new HashSet<>();
    int v0Count = 0; // CIK|ACCESSION
    int v1Count = 0; // CIK|ACCESSION|STATUS
    int v2Count = 0; // CIK|ACCESSION|STATUS|TIMESTAMP

    for (String entry : entries) {
      if (entry.trim().isEmpty()) {
        continue; // Skip empty lines
      }

      String[] parts = entry.split("\\|");

      // Current format: CIK|ACCESSION|STATUS|TIMESTAMP (4 parts)
      if (parts.length == 4) {
        migrated.add(entry); // Already correct
        v2Count++;
      } else if (parts.length == 3) {
        // Old format: CIK|ACCESSION|STATUS (missing timestamp)
        String upgraded = entry + "|" + System.currentTimeMillis();
        migrated.add(upgraded);
        v1Count++;
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Migrated v1 entry: {} -> {}", entry, upgraded);
        }
      } else if (parts.length == 2) {
        // Very old format: CIK|ACCESSION (missing status and timestamp)
        String upgraded = parts[0] + "|" + parts[1] + "|PROCESSED|" + System.currentTimeMillis();
        migrated.add(upgraded);
        v0Count++;
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Migrated v0 entry: {} -> {}", entry, upgraded);
        }
      } else {
        // Unknown format - keep as-is to avoid data loss
        migrated.add(entry);
        LOGGER.warn("Unknown manifest format ({}): {}", parts.length, entry);
      }
    }

    // Save migrated manifest if any entries were upgraded
    if (v0Count > 0 || v1Count > 0) {
      LOGGER.info("Migrating manifest {} - v0: {}, v1: {}, v2: {}, total: {}",
          manifestFile.getName(), v0Count, v1Count, v2Count, migrated.size());
      try {
        // Write sorted entries for better readability
        List<String> sortedEntries = new ArrayList<>(migrated);
        Collections.sort(sortedEntries);
        Files.write(manifestFile.toPath(), sortedEntries);
        LOGGER.info("Successfully migrated manifest to current format");
      } catch (IOException e) {
        LOGGER.error("Failed to save migrated manifest: {}", e.getMessage());
        // Continue with migrated in-memory entries even if save fails
      }
    }

    return migrated;
  }

  /**
   * Index manifest entries into a HashMap for O(1) lookups.
   * Aggregates duplicate entries by keeping the most permissive flags.
   *
   * @param manifest The set of manifest entries to index
   * @return Map from "cik|accession" to aggregated FilingManifestEntry
   */
  private Map<String, FilingManifestEntry> indexManifestEntries(Set<String> manifest) {
    Map<String, FilingManifestEntry> index = new HashMap<>();

    for (String entry : manifest) {
      // Entry format: "cik|accession|...|STATUS|timestamp"
      // Example: "0000732712|0001062993-25-011201|PROCESSED_WITH_VECTORS|1234567890"
      String[] parts = entry.split("\\|");
      if (parts.length >= 2) {
        String key = parts[0] + "|" + parts[1]; // "cik|accession"

        FilingManifestEntry existing = index.get(key);
        if (existing == null) {
          existing = new FilingManifestEntry();
          index.put(key, existing);
        }

        // Aggregate flags from all entries (handles duplicates)
        if (entry.contains("PROCESSED_WITH_VECTORS")) {
          existing.hasVectorized = true;
        }
        if (entry.contains("NO_XBRL")) {
          existing.isNoXbrl = true;
        }
      }
    }

    return index;
  }

  private FilingStatus checkFilingStatusInMemory(String cik, String accession, String form,
                                                  String filingDate, Set<String> processedFilingsManifest) {
    try {
      // Build indexed map for O(1) lookups
      Map<String, FilingManifestEntry> manifestIndex = indexManifestEntries(processedFilingsManifest);

      String lookupKey = cik + "|" + accession;
      FilingManifestEntry entry = manifestIndex.get(lookupKey);

      boolean isInManifest = (entry != null);
      boolean hasVectorized = isInManifest && entry.hasVectorized;
      boolean isNoXbrl = isInManifest && entry.isNoXbrl;

      // Check if text similarity is enabled
      Map<String, Object> textSimilarityConfig = (Map<String, Object>) currentOperand.get("textSimilarity");
      boolean needsVectorized = textSimilarityConfig != null &&
          Boolean.TRUE.equals(textSimilarityConfig.get("enabled"));

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[MANIFEST-CHECK] In-memory manifest lookup (O(1)): CIK={}, Accession={}, isInManifest={}, hasVectorized={}, needsVectorized={}, isNoXbrl={}",
            cik, accession, isInManifest, hasVectorized, needsVectorized, isNoXbrl);
      }

      if (isInManifest && (isNoXbrl || !needsVectorized || hasVectorized)) {
        return FilingStatus.FULLY_PROCESSED;
      }

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

      // Build path using StorageProvider
      String yearDirPath =
          storageProvider.resolvePath(govdataParquetDir, "source=sec/cik=" + cik + "/filing_type=" + form.replace("-", "") + "/year=" + year);

      // Use same uniqueId logic as converter: accession if available, otherwise filingDate
      // This matches XbrlToParquetConverter line 2662
      String uniqueId = (accession != null && !accession.isEmpty()) ? accession.replace("-", "") : filingDate;
      boolean isInsiderForm = form.matches("[345]");

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("[FILE-EXISTENCE-CHECK] Checking Parquet files (I/O): cik={}, accession={}, form={}, filingDate={}, uniqueId={}, isInsiderForm={}",
            cik, accession, form, filingDate, uniqueId, isInsiderForm);
      }

      // Check primary file
      String filenameSuffix = isInsiderForm ? "insider" : "facts";
      String primaryParquetPath =
          storageProvider.resolvePath(yearDirPath, String.format("%s_%s_%s.parquet", cik, uniqueId, filenameSuffix));

      try {
        StorageProvider.FileMetadata primaryMetadata = storageProvider.getMetadata(primaryParquetPath);
        if (primaryMetadata.getSize() == 0) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("hasAllParquetFiles: primary file has zero size: {}", primaryParquetPath);
          }
          return false;
        }
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("hasAllParquetFiles: primary file exists: {}", primaryParquetPath);
        }
      } catch (IOException e) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("hasAllParquetFiles: primary file not found: {}", primaryParquetPath);
        }
        return false;
      }

      // Check relationships file for non-insider forms
      if (!isInsiderForm) {
        String relationshipsParquetPath =
            storageProvider.resolvePath(yearDirPath, String.format("%s_%s_relationships.parquet", cik, uniqueId));
        try {
          storageProvider.getMetadata(relationshipsParquetPath);
          // File exists - relationships file can be empty
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("hasAllParquetFiles: relationships file exists: {}", relationshipsParquetPath);
          }
        } catch (IOException e) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("hasAllParquetFiles: relationships file not found: {}", relationshipsParquetPath);
          }
          return false;
        }
      }

      // Check vectorized file if enabled
      Map<String, Object> textSimilarityConfig = (Map<String, Object>) currentOperand.get("textSimilarity");
      if (textSimilarityConfig != null && Boolean.TRUE.equals(textSimilarityConfig.get("enabled"))) {
        if (supportsVectorization(form)) {
          String vectorizedPath =
              storageProvider.resolvePath(yearDirPath, String.format("%s_%s_vectorized.parquet", cik, uniqueId));
          try {
            StorageProvider.FileMetadata vectorizedMetadata = storageProvider.getMetadata(vectorizedPath);
            if (vectorizedMetadata.getSize() == 0) {
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("hasAllParquetFiles: vectorized file has zero size: {}", vectorizedPath);
              }
              return false;
            }
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("hasAllParquetFiles: vectorized file exists: {}", vectorizedPath);
            }
          } catch (IOException e) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("hasAllParquetFiles: vectorized file not found: {}", vectorizedPath);
            }
            return false;
          }
        }
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("hasAllParquetFiles: ALL files found for cik={} uniqueId={}", cik, uniqueId);
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
      String secParquetDirPath = storageProvider.resolvePath(govdataParquetDir, "source=sec");
      // Don't create File or call mkdirs() for parquet directory - it may be S3
      // StorageProvider will create directories as needed when writing files

      // Check if text similarity is enabled from operand
      Map<String, Object> textSimilarityConfig = (Map<String, Object>) currentOperand.get("textSimilarity");
      boolean enableVectorization = textSimilarityConfig != null &&
          Boolean.TRUE.equals(textSimilarityConfig.get("enabled"));

      XbrlToParquetConverter converter = new XbrlToParquetConverter(this.storageProvider, enableVectorization);

      // Source files are now in operating directory: .aperio/sec/raw/CIK/ACCESSION/file.xml
      // Metadata (.conversions.json) goes in same directory as source file
      File sourceParentDir = sourceFile.getParentFile();
      if (!sourceParentDir.exists()) {
        sourceParentDir.mkdirs();
      }
      ConversionMetadata metadata = new ConversionMetadata(sourceParentDir);
      ConversionMetadata.ConversionRecord record = new ConversionMetadata.ConversionRecord();
      record.originalFile = sourceFile.getAbsolutePath();
      record.sourceFile = sourceFile.getParentFile().getName();  // accession number
      metadata.recordConversion(sourceFile, record);

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Processing filing immediately: {}", sourceFile.getName());
      }

      // Use S3-compatible convertInternal method
      List<File> outputFiles = converter.convertInternal(sourceFile.getAbsolutePath(), secParquetDirPath, metadata);

      if (outputFiles.isEmpty()) {
        LOGGER.warn("No parquet files created for " + sourceFile.getName());
      } else {
        // Add to manifest after successful conversion
        addToManifest(sourceFile.getAbsolutePath(), secParquetDirPath, outputFiles);
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
              filing.primaryDoc, filing.form, filing.filingDate, filing.reportDate, filing.cikCachePath, filing.hasInlineXBRL);
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
      String accession, String primaryDoc, String form, String filingDate, String reportDate, String cikCachePath, boolean hasInlineXBRL) {
    try {
      // Check per-CIK manifest first to see if this filing was already fully processed
      File cikManifestDir = new File(this.secOperatingDirectory, "cik=" + cik);
      File manifestFile = new File(cikManifestDir, "processed_filings.manifest");
      String manifestKey = cik + "|" + accession + "|" + form + "|" + filingDate;

      // Check if already in manifest or has all required Parquet files
      FilingStatus status = checkFilingStatus(cik, accession, form, filingDate, manifestFile);

      // Track if we need to reprocess for vectorization
      boolean needsVectorizationReprocessing = false;

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
      } else if (status == FilingStatus.NEEDS_PROCESSING) {
        // Check if this is specifically for vectorization
        Map<String, Object> textSimilarityConfig = (Map<String, Object>) currentOperand.get("textSimilarity");
        boolean needsVectorized = textSimilarityConfig != null &&
            Boolean.TRUE.equals(textSimilarityConfig.get("enabled"));
        if (needsVectorized) {
          needsVectorizationReprocessing = true;
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Filing needs vectorization reprocessing: {} {}", form, filingDate);
          }
        }
      } else if (status == FilingStatus.HAS_ALL_PARQUET) {
        // Has all Parquet files but not in manifest - add to manifest
        try {
          cikManifestDir.mkdirs();
          synchronized (("manifest_" + cik).intern()) {
            String entryPrefix = cik + "|" + accession + "|";
            String manifestEntry = entryPrefix + "PROCESSED|" + System.currentTimeMillis();

            // Write to manifest
            try (PrintWriter pw = new PrintWriter(new FileOutputStream(manifestFile, true))) {
              pw.println(manifestEntry);
            }

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Added filing to manifest (has all Parquet files): {} {}", form, filingDate);
            }
          }
        } catch (IOException e) {
          LOGGER.warn("Failed to add filing to manifest: {}", e.getMessage());
        }
        return;
      }

      // Need to check source files and possibly download/process

      // StorageProvider automatically creates parent directories when writing files
      String accessionClean = accession.replace("-", "");
      String accessionPath = storageProvider.resolvePath(cikCachePath, accessionClean);

      // Download both HTML (for preview) and XBRL (for data extraction)
      boolean needHtml = false;
      boolean needXbrl = false;

      // For Forms 3/4/5, we need to download the .txt file and extract the raw XML
      boolean isInsiderForm = form.equals("3") || form.equals("4") || form.equals("5");

      // Check if HTML file exists (for human-readable preview)
      String htmlPath = storageProvider.resolvePath(accessionPath, primaryDoc);
      // StorageProvider automatically creates parent directories when writing files
      // Download primary document for ALL filing types (including insider forms)
      boolean htmlExists = false;
      // Check manifest first to avoid S3 API call
      if (cacheManifest.isFileDownloaded(cik, accession, primaryDoc)) {
        htmlExists = true;
      } else {
        // Only make S3 call if file status is unknown
        try {
          htmlExists = storageProvider.exists(htmlPath) && storageProvider.getMetadata(htmlPath).getSize() > 0;
          if (htmlExists) {
            // File exists but wasn't tracked in manifest - add it now
            cacheManifest.markFileDownloaded(cik, accession, primaryDoc);
          }
        } catch (IOException e) {
          // File doesn't exist
        }
      }
      if (!htmlExists) {
        needHtml = true;
      }

      // Check if XBRL file exists (for structured data)
      String xbrlDoc;
      String xbrlPath;
      boolean confirmedInlineXbrl = false;  // Track if we've confirmed inline XBRL by HTML inspection

      if (isInsiderForm) {
        // For Forms 3/4/5, we'll save the extracted XML as ownership.xml
        xbrlDoc = "ownership.xml";
        xbrlPath = storageProvider.resolvePath(accessionPath, xbrlDoc);
      } else {
        // For other forms, detect XBRL type and filename
        // Strategy: Check HTML file FIRST for inline XBRL (most reliable), then try FilingSummary.xml

        // Step 1: If HTML exists, check for inline XBRL markers (authoritative test)
        if (htmlExists) {
          confirmedInlineXbrl = checkHtmlForInlineXbrl(htmlPath);
          if (confirmedInlineXbrl) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Confirmed inline XBRL by HTML inspection: {}", primaryDoc);
            }
          }
        }

        if (confirmedInlineXbrl || hasInlineXBRL) {
          // Inline XBRL confirmed - no separate XBRL file, use placeholder
          xbrlDoc = primaryDoc.replace(".htm", "_inline.xml");
          xbrlPath = storageProvider.resolvePath(accessionPath, xbrlDoc);
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Filing has inline XBRL, skipping separate XBRL file detection: {}", primaryDoc);
          }
        } else {
          // For separate XBRL files, use pattern-based construction for initial cache check
          // We'll try FilingSummary.xml later if we actually need to download
          xbrlDoc = constructXbrlFilename(cik, reportDate);
          if (xbrlDoc == null) {
            // Fallback to old pattern
            xbrlDoc = primaryDoc.replace(".htm", "_htm.xml");
          }
          xbrlPath = storageProvider.resolvePath(accessionPath, xbrlDoc);
        }
      }

      // StorageProvider automatically creates parent directories when writing files

      // DEFER XBRL download decision until AFTER HTML is downloaded
      // We can only make the decision after checking the HTML file for inline XBRL markers
      // For now, just check if we already have the XBRL file or know it doesn't exist
      // BUT: skip this check entirely if we already know it's inline XBRL (no separate .xml file exists)
      boolean xbrlAlreadyExists = false;
      boolean xbrlKnownNotFound = false;

      // Skip existence check if inline XBRL already confirmed
      if (hasInlineXBRL || confirmedInlineXbrl) {
        // For inline XBRL, there is no separate .xml file, so no need to check
        xbrlKnownNotFound = true; // Mark as not found since separate XBRL doesn't exist
      } else {
        // Only check for separate XBRL file if NOT inline XBRL
        // Check manifest first to avoid S3 API call
        if (cacheManifest.isFileDownloaded(cik, accession, xbrlDoc)) {
          xbrlAlreadyExists = true;
        } else if (!cacheManifest.isFileNotFound(cik, accession, xbrlDoc)) {
          // Only make S3 call if file status is unknown
          try {
            xbrlAlreadyExists = storageProvider.exists(xbrlPath) && storageProvider.getMetadata(xbrlPath).getSize() > 0;
            if (xbrlAlreadyExists) {
              // File exists but wasn't tracked in manifest - add it now
              cacheManifest.markFileDownloaded(cik, accession, xbrlDoc);
            }
          } catch (IOException e) {
            // File doesn't exist
          }
        }
        xbrlKnownNotFound = cacheManifest.isFileNotFound(cik, accession, xbrlDoc);
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Initial XBRL check: form={} date={} hasInlineXBRL={} confirmedInlineXbrl={} xbrlExists={} inManifest={}",
            form, filingDate, hasInlineXBRL, confirmedInlineXbrl, xbrlAlreadyExists, xbrlKnownNotFound);
      }

      // Only skip XBRL if we're certain it's inline XBRL (from HTML inspection or submissions.json)
      // OR if we already have it cached
      if (hasInlineXBRL || confirmedInlineXbrl) {
        if (LOGGER.isDebugEnabled()) {
          String source = confirmedInlineXbrl ? "HTML inspection" : "submissions.json";
          LOGGER.debug("Confirmed inline XBRL per {}, will skip separate XBRL file: {} {}", source, form, filingDate);
        }
      }

      // DISABLED: Parquet file validation before XBRL parsing
      // Cannot reliably check Parquet files before parsing XBRL because fiscal year (used for
      // partitioning) is only known after parsing XBRL data. The heuristic-based
      // getPartitionYear() method produces incorrect year folders for filings where filing date
      // year != fiscal year, causing false cache misses and unnecessary reprocessing.
      // Solution: Rely on manifest file and source file (HTML/XBRL) existence checks only.
      // After XBRL is parsed and converted to Parquet, files are added to manifest.

      // Use the vectorization flag set earlier based on checkFilingStatus()
      boolean needParquetReprocessing = needsVectorizationReprocessing;

      // Critical fix: Detect inline XBRL in cached HTML files that need Parquet processing
      // This addresses the core cache effectiveness issue where HTML files contain inline XBRL
      // but Parquet files were never generated due to cache logic gaps
      // NOTE: needParquetReprocessing is now true when vectorization is needed

      if (!needHtml && !needXbrl && !needParquetReprocessing) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Filing already fully cached: {} {}", form, filingDate);
        }
        // Add to manifest so future runs don't need to check S3
        try {
          cikManifestDir.mkdirs();
          synchronized (("manifest_" + cik).intern()) {
            String entryPrefix = cik + "|" + accession + "|";

            // Check if this entry already exists in the manifest (prevent duplicates)
            boolean alreadyExists = false;
            if (manifestFile.exists()) {
              try (BufferedReader reader = new BufferedReader(new FileReader(manifestFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                  if (line.startsWith(entryPrefix)) {
                    alreadyExists = true;
                    if (LOGGER.isDebugEnabled()) {
                      LOGGER.debug("Manifest entry already exists: {}", line);
                    }
                    break;
                  }
                }
              }
            }

            // Only write if entry doesn't already exist
            if (!alreadyExists) {
              // For cached files, mark as PROCESSED_WITH_VECTORS if vectorization is enabled,
              // even though we don't check if vectorized files exist (too expensive).
              // This prevents re-checking on every run.
              boolean vectorizationEnabled = false;
              if (currentOperand != null) {
                Map<String, Object> textSimilarityConfig = (Map<String, Object>) currentOperand.get("textSimilarity");
                vectorizationEnabled = textSimilarityConfig != null &&
                    Boolean.TRUE.equals(textSimilarityConfig.get("enabled"));
              }
              String fileTypes = vectorizationEnabled ? "PROCESSED_WITH_VECTORS" : "PROCESSED";
              String manifestEntry = entryPrefix + fileTypes + "|" + System.currentTimeMillis();

              // Write to manifest
              try (PrintWriter pw = new PrintWriter(new FileOutputStream(manifestFile, true))) {
                pw.println(manifestEntry);
              }

              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Added cached filing to manifest: {} {}", form, filingDate);
              }
            }
          }
        } catch (IOException e) {
          LOGGER.warn("Failed to add cached filing to manifest: {}", e.getMessage());
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
          storageProvider.writeFile(htmlPath, is);
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Downloaded HTML filing: {} {} ({})", form, filingDate, primaryDoc);
          }
          htmlExists = true;
          // Track downloaded file in manifest to avoid redundant S3 existence checks
          cacheManifest.markFileDownloaded(cik, accession, primaryDoc);
        } catch (Exception e) {
          LOGGER.debug("Could not download HTML: " + e.getMessage());
          return; // Can't proceed without HTML
        }

        // CRITICAL: After HTML download, check for inline XBRL to make XBRL download decision
        // This is the authoritative check - don't rely solely on submissions.json isInlineXBRL flag
        if (htmlExists) {
          confirmedInlineXbrl = checkHtmlForInlineXbrl(htmlPath);
          if (confirmedInlineXbrl) {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("HTML contains inline XBRL, will skip separate XBRL download: {} {}", form, filingDate);
            }
            // Mark the constructed XBRL filename as not found so we don't try again
            cacheManifest.markFileNotFound(cik, accession, xbrlDoc, "filing_uses_inline_xbrl");
          }
        }
      }

      // NOW make the XBRL download decision AFTER checking HTML
      // Download XBRL only if:
      // 1. XBRL file doesn't exist locally
      // 2. NOT already marked as not found in manifest
      // 3. NOT inline XBRL (per submissions.json OR HTML inspection)

      // Skip xbrlExists check entirely if we already know it's inline XBRL
      // For inline XBRL, there is no separate .xml file to check
      boolean xbrlExists = false; // Declare outside so it's available later in the method
      if (hasInlineXBRL || confirmedInlineXbrl) {
        if (LOGGER.isDebugEnabled()) {
          String source = confirmedInlineXbrl ? "HTML inspection" : "submissions.json";
          LOGGER.debug("Skipping XBRL download - filing has inline XBRL per {}: {} {}", source, form, filingDate);
        }
      } else {
        // Only check for separate XBRL file if NOT inline XBRL
        // Check manifest first to avoid S3 API call
        if (cacheManifest.isFileDownloaded(cik, accession, xbrlDoc)) {
          xbrlExists = true;
        } else if (!cacheManifest.isFileNotFound(cik, accession, xbrlDoc)) {
          // Only make S3 call if file status is unknown
          try {
            xbrlExists = storageProvider.exists(xbrlPath) && storageProvider.getMetadata(xbrlPath).getSize() > 0;
            if (xbrlExists) {
              // File exists but wasn't tracked in manifest - add it now
              cacheManifest.markFileDownloaded(cik, accession, xbrlDoc);
            }
          } catch (IOException e) {
            // File doesn't exist
          }
        }
        if (!xbrlExists && !cacheManifest.isFileNotFound(cik, accession, xbrlDoc)) {
          needXbrl = true;
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Will download XBRL file: {}", xbrlDoc);
          }
        }
      }

      // Download XBRL file if needed (only if not inline XBRL)
      if (needXbrl) {
        // Check if we already know this XBRL doesn't exist
        if (cacheManifest.isFileNotFound(cik, accession, xbrlDoc)) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Skipping XBRL download - already marked as not found in manifest: {}", xbrlDoc);
          }
        } else {
          if (isInsiderForm) {
            // For Forms 3/4/5, download the .txt file and extract the XML
            String txtUrl =
                String.format("https://www.sec.gov/Archives/edgar/data/%s/%s.txt", cik, accession); // Use hyphenated accession number for .txt files

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
                String xmlContent = "<?xml version=\"1.0\"?>\n"
  +
                    txtContent.substring(xmlStart, xmlEnd + "</ownershipDocument>".length());

                // Save the extracted XML
                storageProvider.writeFile(xbrlPath, xmlContent.getBytes(StandardCharsets.UTF_8));
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("Extracted ownership XML for Form {} {}", form, filingDate);
                }
                // Track downloaded file in manifest to avoid redundant S3 existence checks
                cacheManifest.markFileDownloaded(cik, accession, xbrlDoc);
              } else {
                LOGGER.warn("Could not find ownershipDocument in Form " + form + " .txt file");
                cacheManifest.markFileNotFound(cik, accession, xbrlDoc, "ownership_xml_not_in_txt_file");
              }
            } catch (Exception e) {
              LOGGER.warn("Failed to download/extract Form " + form + " ownership XML: " + e.getMessage());
              cacheManifest.markFileNotFound(cik, accession, xbrlDoc, "download_failed: " + e.getMessage());
            }
          } else {
            // For other forms, try FilingSummary.xml first for accurate filename
            String actualXbrlFilename = getXbrlFilenameFromSummary(provider, cik, accession, accessionPath);
            if (actualXbrlFilename != null && !actualXbrlFilename.equals(xbrlDoc)) {
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("FilingSummary.xml provided more accurate filename: {} (was: {})", actualXbrlFilename, xbrlDoc);
              }
              xbrlDoc = actualXbrlFilename;
              xbrlPath = storageProvider.resolvePath(accessionPath, xbrlDoc);
            }

            // Download the XBRL file
            String xbrlUrl =
                String.format("https://www.sec.gov/Archives/edgar/data/%s/%s/%s",
                cik, accessionClean, xbrlDoc);

            try (InputStream is = provider.openInputStream(xbrlUrl)) {
              storageProvider.writeFile(xbrlPath, is);
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Downloaded XBRL filing: {} {} ({})", form, filingDate, xbrlDoc);
              }
              // Track downloaded file in manifest to avoid redundant S3 existence checks
              cacheManifest.markFileDownloaded(cik, accession, xbrlDoc);
            } catch (Exception e) {
              // XBRL doesn't exist for this filing - mark in manifest to avoid retrying
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("XBRL not available for {} {}, will use HTML with inline XBRL", form, filingDate);
              }
              cacheManifest.markFileNotFound(cik, accession, xbrlDoc, "not_on_sec_server");
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Marked as not found in manifest: {}", xbrlDoc);
              }
            }
          }
        }
      }

      // INLINE PROCESSING: Convert XBRL/HTML to Parquet immediately after download
      // This works for both local and S3 cache since converter uses StorageProvider
      try {
        String fileToConvert = null;
        if (xbrlExists) {
          fileToConvert = xbrlPath;
        } else if (htmlExists && (confirmedInlineXbrl || hasInlineXBRL)) {
          fileToConvert = htmlPath;
        } else if (xbrlKnownNotFound && !confirmedInlineXbrl && !hasInlineXBRL) {
          // Special case: If this is vectorization reprocessing and filing is already PROCESSED,
          // we should upgrade to PROCESSED_WITH_VECTORS rather than marking as NO_XBRL
          // This happens when vectorization is enabled after files were already processed
          if (needsVectorizationReprocessing && LOGGER.isDebugEnabled()) {
            LOGGER.debug("Filing {} {} needs vectorization reprocessing but has no new XBRL - upgrading to PROCESSED_WITH_VECTORS", form, filingDate);
          }

          if (needsVectorizationReprocessing) {
            // Call addToManifest with empty outputFiles list to upgrade status
            // addToManifest will mark as PROCESSED_WITH_VECTORS when vectorization is enabled
            String govdataParquetDir = getGovDataParquetDir();
            if (govdataParquetDir != null) {
              String secParquetDirPath = storageProvider.resolvePath(govdataParquetDir, "source=sec");
              addToManifest(xbrlPath, secParquetDirPath, new ArrayList<>());
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Upgraded filing to PROCESSED_WITH_VECTORS: {} {}", form, filingDate);
              }
            }
          } else {
            // Filing has no XBRL data (no separate .xml file and no inline XBRL in HTML)
            // Add to manifest with NO_XBRL status to prevent re-checking on future runs
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Filing {} {} has no processable XBRL data - adding to manifest as NO_XBRL", form, filingDate);
            }
            try {
              cikManifestDir.mkdirs();
              synchronized (("manifest_" + cik).intern()) {
                String entryPrefix = cik + "|" + accession + "|";
                String manifestEntry = entryPrefix + "NO_XBRL|" + System.currentTimeMillis();

                // Check if entry already exists
                boolean alreadyExists = false;
                if (manifestFile.exists()) {
                  try (BufferedReader reader = new BufferedReader(new FileReader(manifestFile))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                      if (line.startsWith(entryPrefix)) {
                        alreadyExists = true;
                        break;
                      }
                    }
                  }
                }

                if (!alreadyExists) {
                  try (PrintWriter pw = new PrintWriter(new FileOutputStream(manifestFile, true))) {
                    pw.println(manifestEntry);
                  }
                  if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Added NO_XBRL entry to manifest: {}", manifestEntry);
                  }
                }
              }
            } catch (IOException e) {
              LOGGER.warn("Failed to add NO_XBRL entry to manifest: {}", e.getMessage());
            }
          }
          return; // No conversion possible or already upgraded
        }

        if (fileToConvert != null) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("INLINE CONVERSION: fileToConvert={} for filing {} {}", fileToConvert, form, filingDate);
          }

          // Get parquet directory
          String govdataParquetDir = getGovDataParquetDir();
          String secParquetDirPath = govdataParquetDir != null ? storageProvider.resolvePath(govdataParquetDir, "source=sec") : null;

          if (secParquetDirPath != null) {
            // Check if text similarity is enabled
            Map<String, Object> textSimilarityConfig = (Map<String, Object>) currentOperand.get("textSimilarity");
            boolean enableVectorization = textSimilarityConfig != null &&
                Boolean.TRUE.equals(textSimilarityConfig.get("enabled"));

            XbrlToParquetConverter converter = new XbrlToParquetConverter(this.storageProvider, enableVectorization);

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("INLINE CONVERSION: About to convert fileToConvert={} secParquetDirPath={} enableVectorization={}",
                  fileToConvert, secParquetDirPath, enableVectorization);
            }

            // Convert using String paths (works for both local and S3)
            List<java.io.File> outputFiles = converter.convertInternal(fileToConvert, secParquetDirPath, null);

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("INLINE CONVERSION: Conversion completed, outputFiles.size={}",
                  outputFiles != null ? outputFiles.size() : "null");
            }

            // Always call addToManifest, even with empty outputFiles
            // This ensures proper handling of vectorization reprocessing
            // addToManifest will mark as PROCESSED_WITH_VECTORS when vectorization is enabled,
            // even if no output files were created (e.g., 8-K with no financial data)
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("INLINE CONVERSION: Calling addToManifest with {} output files", outputFiles.size());
            }
            addToManifest(fileToConvert, secParquetDirPath, outputFiles);
            if (LOGGER.isDebugEnabled()) {
              if (!outputFiles.isEmpty()) {
                LOGGER.debug("Converted filing inline: {} {} ({} parquet files)", form, filingDate, outputFiles.size());
              } else {
                LOGGER.debug("Conversion completed with no output files, marked as processed: {} {}", form, filingDate);
              }
            }
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to convert filing {}: {}", accession, e.getMessage());

        // Write FAILED entry to manifest so we don't keep retrying this filing
        try {
          cikManifestDir.mkdirs();
          synchronized (("manifest_" + cik).intern()) {
            String manifestEntry = cik + "|" + accession + "|FAILED|" + System.currentTimeMillis();
            try (PrintWriter pw = new PrintWriter(new FileOutputStream(manifestFile, true))) {
              pw.println(manifestEntry);
            }
            LOGGER.debug("Added FAILED entry to manifest for {}: {}", accession, e.getMessage());
          }
        } catch (IOException ioe) {
          LOGGER.warn("Failed to add FAILED entry to manifest: {}", ioe.getMessage());
        }
      }

    } catch (Exception e) {
      LOGGER.warn("Failed to download filing " + accession + ": " + e.getMessage());

      // Write FAILED entry to manifest so we don't keep retrying this filing
      try {
        File cikManifestDir = new File(this.secOperatingDirectory, "cik=" + cik);
        File manifestFile = new File(cikManifestDir, "processed_filings.manifest");
        cikManifestDir.mkdirs();

        synchronized (("manifest_" + cik).intern()) {
          String manifestEntry = cik + "|" + accession + "|FAILED|" + System.currentTimeMillis();
          try (PrintWriter pw = new PrintWriter(new FileOutputStream(manifestFile, true))) {
            pw.println(manifestEntry);
          }
          LOGGER.debug("Added FAILED entry to manifest for {}: {}", accession, e.getMessage());
        }
      } catch (IOException ioe) {
        LOGGER.warn("Failed to add FAILED entry to manifest: {}", ioe.getMessage());
      }
    }
  }

  /**
   * Get XBRL instance document filename from FilingSummary.xml.
   *
   * @param provider HTTP storage provider
   * @param cik CIK of the company
   * @param accession Accession number
   * @param accessionPath Path to accession directory for this filing
   * @return XBRL instance document filename, or null if unavailable
   */
  private String getXbrlFilenameFromSummary(SecHttpStorageProvider provider, String cik,
      String accession, String accessionPath) {
    // Check cache first
    String cachedFilename = cacheManifest.getCachedFilingSummaryXbrlFilename(cik, accession);
    if (cachedFilename != null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Using cached XBRL filename from FilingSummary: {}", cachedFilename);
      }
      return cachedFilename;
    }

    // Check if we already know FilingSummary.xml doesn't exist
    if (cacheManifest.isFilingSummaryNotFound(cik, accession)) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("FilingSummary.xml known to not exist for {} {}", cik, accession);
      }
      return null;
    }

    // Download and parse FilingSummary.xml
    String accessionClean = accession.replace("-", "");
    String summaryUrl =
        String.format("https://www.sec.gov/Archives/edgar/data/%s/%s/FilingSummary.xml", cik, accessionClean);

    try {
      String summaryPath = storageProvider.resolvePath(accessionPath, "FilingSummary.xml");
      try (InputStream is = provider.openInputStream(summaryUrl)) {
        storageProvider.writeFile(summaryPath, is);
      }
      // Track downloaded file in manifest to avoid redundant S3 existence checks
      cacheManifest.markFileDownloaded(cik, accession, "FilingSummary.xml");

      // Parse XML to extract instance document filename
      String xbrlFilename = parseXbrlFilenameFromSummary(summaryPath);
      if (xbrlFilename != null) {
        // Cache the result
        cacheManifest.cacheFilingSummaryXbrlFilename(cik, accession, xbrlFilename);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Extracted XBRL filename from FilingSummary: {}", xbrlFilename);
        }
        return xbrlFilename;
      } else {
        LOGGER.debug("No XBRL instance document found in FilingSummary.xml for {} {}", cik, accession);
        cacheManifest.markFilingSummaryNotFound(cik, accession, "no_instance_document_in_summary");
        return null;
      }

    } catch (Exception e) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("FilingSummary.xml not available for {} {}: {}", cik, accession, e.getMessage());
      }
      cacheManifest.markFilingSummaryNotFound(cik, accession, "download_failed: " + e.getMessage());
      return null;
    }
  }

  /**
   * Parse XBRL instance document filename from FilingSummary.xml.
   *
   * @param summaryPath Path to the FilingSummary.xml file
   * @return Instance document filename, or null if not found
   */
  private String parseXbrlFilenameFromSummary(String summaryPath) {
    try {
      javax.xml.parsers.DocumentBuilderFactory factory = javax.xml.parsers.DocumentBuilderFactory.newInstance();
      javax.xml.parsers.DocumentBuilder builder = factory.newDocumentBuilder();
      try (InputStream is = storageProvider.openInputStream(summaryPath)) {
        org.w3c.dom.Document doc = builder.parse(is);

        // Look for <InstanceReport> element
        org.w3c.dom.NodeList instanceReports = doc.getElementsByTagName("InstanceReport");
        if (instanceReports.getLength() > 0) {
          org.w3c.dom.Node instanceReport = instanceReports.item(0);
          return instanceReport.getTextContent().trim();
        }

        // Fallback: look for <Report> with type="instance"
        org.w3c.dom.NodeList reports = doc.getElementsByTagName("Report");
        for (int i = 0; i < reports.getLength(); i++) {
          org.w3c.dom.Node report = reports.item(i);
          if (report.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {
            org.w3c.dom.Element reportElement = (org.w3c.dom.Element) report;
            String reportType = reportElement.getAttribute("type");
            if ("instance".equalsIgnoreCase(reportType)) {
              org.w3c.dom.NodeList htmlFileNames = reportElement.getElementsByTagName("HtmlFileName");
              if (htmlFileNames.getLength() > 0) {
                String htmlFileName = htmlFileNames.item(0).getTextContent().trim();
                // Convert .htm to .xml for XBRL instance
                return htmlFileName.replace(".htm", ".xml").replace(".html", ".xml");
              }
            }
          }
        }

        return null;
      }
    } catch (Exception e) {
      LOGGER.debug("Failed to parse FilingSummary.xml: {}", e.getMessage());
      return null;
    }
  }

  /**
   * Construct XBRL filename using ticker and report date (period end date) pattern.
   *
   * @param cik CIK of the company
   * @param reportDate Report/period end date (YYYY-MM-DD format) - NOT filing date
   * @return XBRL filename (e.g., "aapl-20181229.xml"), or null if unable to construct
   */
  private String constructXbrlFilename(String cik, String reportDate) {
    try {
      // Get ticker from CIK
      List<String> tickers = CikRegistry.getTickersForCik(cik);
      if (tickers.isEmpty()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("No ticker found for CIK {}", cik);
        }
        return null;
      }

      // Require reportDate for XBRL filename construction
      if (reportDate == null || reportDate.isEmpty()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("No report date available for XBRL filename construction");
        }
        return null;
      }

      // Use first ticker (most companies have one ticker)
      String ticker = tickers.get(0).toLowerCase();

      // Convert report date YYYY-MM-DD to YYYYMMDD
      String dateStr = reportDate.replace("-", "");

      String xbrlFilename = ticker + "-" + dateStr + ".xml";
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Constructed XBRL filename using report date {}: {}", reportDate, xbrlFilename);
      }
      return xbrlFilename;

    } catch (Exception e) {
      LOGGER.debug("Failed to construct XBRL filename for CIK {}: {}", cik, e.getMessage());
      return null;
    }
  }

  /**
   * Check if HTML file contains inline XBRL markers.
   *
   * @param htmlFile The HTML file to check
   * @return true if inline XBRL detected, false otherwise
   */
  private boolean checkHtmlForInlineXbrl(String htmlPath) {
    try {
      byte[] headerBytes = new byte[10240];
      try (InputStream is = storageProvider.openInputStream(htmlPath)) {
        int bytesRead = is.read(headerBytes);

        // Fully drain the remaining stream to avoid S3ObjectInputStream warnings
        // when the storage provider is S3 and returns raw S3ObjectInputStream
        byte[] drainBuffer = new byte[8192];
        while (is.read(drainBuffer) != -1) {
          // Just drain, don't process
        }

        if (bytesRead > 0) {
          String header = new String(headerBytes, 0, bytesRead);
          boolean hasInlineXbrl = header.contains("xmlns:ix=")
              || header.contains("http://www.xbrl.org/2013/inlineXBRL")
              || header.contains("<ix:")
              || header.contains("iXBRL");

          if (LOGGER.isDebugEnabled() && hasInlineXbrl) {
            String fileName = htmlPath.substring(htmlPath.lastIndexOf('/') + 1);
            LOGGER.debug("Detected inline XBRL in HTML file: {}", fileName);
          }
          return hasInlineXbrl;
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Failed to check HTML for inline XBRL: {}", e.getMessage());
    }
    return false;
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
      storageProvider.writeFile(outputFile.getAbsolutePath(), is);
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Downloaded inline XBRL (HTML): {} {} ({})", form, filingDate, primaryDoc);
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
      String secParquetDirPath = govdataParquetDir != null ? storageProvider.resolvePath(govdataParquetDir, "source=sec") : null;
      // Don't create File or call mkdirs() - parquet path may be S3
      // StorageProvider will create directories as needed when writing files

      if (!secRawDir.exists() || !secRawDir.isDirectory()) {
        LOGGER.warn("No sec-raw directory found: " + secRawDir);
        return;
      }

      // Load column metadata from sec-schema.json
      java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
          AbstractSecDataDownloader.loadTableColumns("filing_metadata");

      List<Map<String, Object>> dataList = new ArrayList<>();
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
              Map<String, Object> data = new HashMap<>();
              data.put("cik", cik);
              data.put("accession_number", accessionNumbers.get(i).asText());
              data.put("filing_type", forms.get(i).asText());
              data.put("filing_date", filingDates.get(i).asText());
              data.put("primary_document", primaryDocuments.get(i).asText(""));
              data.put("company_name", companyName);
              data.put("period_of_report", periodsOfReport != null && i < periodsOfReport.size()
                  ? periodsOfReport.get(i).asText("") : "");
              data.put("acceptance_datetime", acceptanceDatetimes != null && i < acceptanceDatetimes.size()
                  ? acceptanceDatetimes.get(i).asText("") : "");
              data.put("file_size", fileSizes != null && i < fileSizes.size()
                  ? fileSizes.get(i).asLong(0L) : 0L);

              String filingDate = filingDates.get(i).asText();
              int fiscalYear = filingDate.length() >= 4 ?
                  Integer.parseInt(filingDate.substring(0, 4)) : 0;
              data.put("fiscal_year", fiscalYear);

              dataList.add(data);
            }
          } catch (Exception e) {
            LOGGER.warn("Failed to process submissions for CIK " + cikDir.getName() + ": " + e.getMessage());
          }
        }
      }

      // Write consolidated SEC filings table
      String outputFilePath = storageProvider.resolvePath(secParquetDirPath, "sec_filings.parquet");
      // Use StorageProvider for parquet writing
      storageProvider.writeAvroParquet(outputFilePath, columns, dataList, "SecFiling", "SecFiling");
      LOGGER.info("Created SEC filings table with " + dataList.size() + " records: " + outputFilePath);

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
  private void createMockStockPrices(String baseDirPath, List<String> ciks, int startYear, int endYear) {
    try {
      // baseDirPath is the cache directory (sec-cache)
      // We need to create files in govdata-parquet/source=sec/stock_prices
      String govdataParquetDir = getGovDataParquetDir();
      String stockPricesDir = storageProvider.resolvePath(govdataParquetDir, "source=sec/stock_prices");
      LOGGER.debug("Creating mock stock prices in: {}", stockPricesDir);

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
          // Build relative path for parquet file - don't use File for S3 compatibility
          String relativePath =
              String.format("ticker=%s/year=%d/%s_%d_prices.parquet", ticker, year, ticker, year);
          String parquetFilePath = storageProvider.resolvePath(stockPricesDir, relativePath);
          LOGGER.debug("About to create/check file: {}", parquetFilePath);
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
  private void downloadStockPrices(String baseDirPath, List<String> ciks, int startYear, int endYear) {
    try {
      // Use the parquet directory for stock prices - same as other SEC data
      String govdataParquetDir = getGovDataParquetDir();
      if (govdataParquetDir == null || govdataParquetDir.isEmpty()) {
        LOGGER.warn("GOVDATA_PARQUET_DIR not set, skipping stock price download");
        return;
      }
      String basePath = storageProvider.resolvePath(govdataParquetDir, "source=sec");

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
        // Determine which stock price source to use
        // Priority: 1) operand, 2) STOCK_PRICE_SOURCE env var, 3) default to "stooq"
        String stockPriceSource = (String) currentOperand.get("stockPriceSource");
        if (stockPriceSource == null || stockPriceSource.isEmpty()) {
          stockPriceSource = System.getenv("STOCK_PRICE_SOURCE");
        }
        if (stockPriceSource == null || stockPriceSource.isEmpty()) {
          stockPriceSource = "stooq";  // Default to Stooq
        }
        stockPriceSource = stockPriceSource.toLowerCase();

        if ("stooq".equals(stockPriceSource)) {
          // Use Stooq downloader (default - no API key required)
          String stooqUsername = getEnvOrOperand("STOOQ_USERNAME", "stooqUsername");
          String stooqPassword = getEnvOrOperand("STOOQ_PASSWORD", "stooqPassword");

          LOGGER.info("Downloading stock prices for {} tickers using Stooq", tickerCikPairs.size());
          StooqDownloader downloader = new StooqDownloader(storageProvider, stooqUsername, stooqPassword);
          downloader.downloadStockPrices(basePath, tickerCikPairs, startYear, endYear);

        } else if ("alphavantage".equals(stockPriceSource)) {
          // Use Alpha Vantage downloader (requires API key)
          String apiKey = getEnvOrOperand("ALPHA_VANTAGE_KEY", "alphaVantageKey");

          if (apiKey == null || apiKey.isEmpty()) {
            LOGGER.warn("ALPHA_VANTAGE_KEY not found in environment, skipping stock price download");
            return;
          }

          LOGGER.info("Downloading stock prices for {} tickers using Alpha Vantage", tickerCikPairs.size());
          AlphaVantageDownloader downloader = new AlphaVantageDownloader(apiKey, storageProvider);
          downloader.downloadStockPrices(basePath, tickerCikPairs, startYear, endYear);

        } else {
          LOGGER.warn("Unknown stock price source: {}. Supported values: stooq, alphavantage", stockPriceSource);
        }
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
            String stockPriceDir = storageProvider.resolvePath(basePath, "stock_prices");
            String parquetPath =
                storageProvider.resolvePath(stockPriceDir, String.format("ticker=%s/year=%d/%s_%d.parquet", ticker, year, ticker, year));

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
   * Gets a configuration value from environment variable or operand.
   * Checks in order: 1) Environment variable, 2) TestEnvironmentLoader (for tests), 3) Operand
   *
   * @param envName Name of the environment variable
   * @param operandName Name of the operand key
   * @return The configuration value or null if not found
   */
  private String getEnvOrOperand(String envName, String operandName) {
    // First check system environment variable
    String value = System.getenv(envName);

    // If not found in system env, try TestEnvironmentLoader (for tests)
    if (value == null || value.isEmpty()) {
      try {
        Class<?> testEnvClass = Class.forName("org.apache.calcite.adapter.govdata.TestEnvironmentLoader");
        java.lang.reflect.Method getEnvMethod = testEnvClass.getMethod("getEnv", String.class);
        value = (String) getEnvMethod.invoke(null, envName);
      } catch (Exception e) {
        // TestEnvironmentLoader not available or error accessing it
      }
    }

    // Finally check operand
    if ((value == null || value.isEmpty()) && currentOperand != null) {
      Object operandValue = currentOperand.get(operandName);
      if (operandValue instanceof String) {
        value = (String) operandValue;
      }
    }

    return value;
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
        "S-8", "S8");              // Employee benefit plan registration
  }

  /**
   * Gets the TTL for submissions.json files in hours.
   * Default is 24 hours. Configure via SEC_SUBMISSIONS_TTL_HOURS environment variable.
   */
  private int getSubmissionsTtlHours() {
    String ttlStr = System.getenv("SEC_SUBMISSIONS_TTL_HOURS");
    if (ttlStr != null) {
      try {
        return Integer.parseInt(ttlStr);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid SEC_SUBMISSIONS_TTL_HOURS: {}, using default 24", ttlStr);
      }
    }
    return 24; // Default: 24 hours
  }

  /**
   * Bulk cleanup of all macOS metadata files in the parquet directory.
   * This is run at the end of processing to clean up any ._* files that were created.
   */
  private void cleanupAllMacOSMetadataFiles(String dirPath) {
    if (dirPath == null) {
      return;
    }

    // Use StorageProvider to check directory existence (works for both local and S3)
    try {
      if (!storageProvider.exists(dirPath)) {
        return;
      }
    } catch (IOException e) {
      LOGGER.debug("Could not check if parquet directory exists: {}", e.getMessage());
      return;
    }

    try {
      storageProvider.cleanupMacosMetadata(dirPath);
      LOGGER.info("Bulk cleanup: Removed macOS metadata files from " + dirPath);
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

  /**
   * Migrate legacy .notfound marker files to SecCacheManifest entries.
   * This allows us to deprecate the .notfound file mechanism in favor of manifest-based tracking.
   */
  private void migrateNotFoundMarkers(String secCacheDir) {
    try {
      if (!storageProvider.exists(secCacheDir)) {
        return;
      }
    } catch (IOException e) {
      return;
    }

    // Migration only works for local filesystem (uses java.nio.file.Files.walk)
    // For remote storage, .notfound markers were never used, so no migration needed
    java.io.File cacheRootFile = new java.io.File(secCacheDir);
    if (!cacheRootFile.exists()) {
      return;
    }

    int migratedCount = 0;
    int deletedCount = 0;

    try {
      java.nio.file.Files.walk(cacheRootFile.toPath())
          .filter(path -> path.toString().endsWith(".notfound"))
          .forEach(notFoundPath -> {
            try {
              File notFoundFile = notFoundPath.toFile();
              String fileName = notFoundFile.getName();
              String xbrlFileName = fileName.replace(".notfound", "");

              // Extract CIK and accession from path
              // Path structure: secCacheDir/CIK/ACCESSION/file.notfound
              File accessionDir = notFoundFile.getParentFile();
              File cikDir = accessionDir.getParentFile();

              if (cikDir != null && accessionDir != null) {
                String cik = cikDir.getName();
                String accession = accessionDir.getName();

                // Migrate to manifest
                cacheManifest.markFileNotFound(cik, accession, xbrlFileName, "migrated_from_notfound_marker");

                // Delete the old marker file
                if (notFoundFile.delete()) {
                  LOGGER.debug("Migrated and deleted .notfound marker: {}/{}/{}", cik, accession, xbrlFileName);
                } else {
                  LOGGER.warn("Failed to delete .notfound marker after migration: {}", notFoundPath);
                }
              }
            } catch (Exception e) {
              LOGGER.warn("Failed to migrate .notfound marker {}: {}", notFoundPath, e.getMessage());
            }
          });

      // Save manifest with migrated entries
      if (migratedCount > 0) {
        cacheManifest.save(secCacheDir);
        LOGGER.info("Migrated {} .notfound markers to manifest, deleted {} files", migratedCount, deletedCount);
      }
    } catch (IOException e) {
      LOGGER.warn("Error during .notfound marker migration: {}", e.getMessage());
    }
  }

}
