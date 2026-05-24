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
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Orchestrates document-based ETL processing for SEC filings and similar sources.
 *
 * <p>Unlike standard ETL pipelines that fetch JSON/CSV data and write to a single table,
 * document-based ETL downloads documents (XBRL, HTML) and extracts multiple tables
 * from each document.
 *
 * <h3>Document ETL Flow</h3>
 * <pre>
 * 1. Fetch metadata from source (e.g., SEC submissions.json)
 * 2. Parse metadata to enumerate documents
 * 3. For each document:
 *    a. Check cache - skip if already processed
 *    b. Download document (respecting rate limits)
 *    c. Invoke document converter (e.g., XbrlToParquetConverter)
 *    d. Converter produces multiple Parquet files (facts, contexts, etc.)
 * 4. Track conversion results
 * </pre>
 *
 * <h3>Multi-Table Output</h3>
 * <p>Document converters produce multiple output files with different schemas:
 * <ul>
 *   <li>{cik}_{accession}_facts.parquet - Financial line items</li>
 *   <li>{cik}_{accession}_metadata.parquet - Filing metadata</li>
 *   <li>{cik}_{accession}_contexts.parquet - XBRL contexts</li>
 *   <li>{cik}_{accession}_mda.parquet - MD&amp;A text sections</li>
 *   <li>{cik}_{accession}_relationships.parquet - XBRL relationships</li>
 * </ul>
 *
 * @see DocumentSource
 * @see HttpSourceConfig.DocumentSourceConfig
 */
public class DocumentETLProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(DocumentETLProcessor.class);

  private final HttpSourceConfig config;
  private final StorageProvider storageProvider;
  private final String outputDirectory;
  private final String cacheDirectory;
  private final FileConverter documentConverter;
  private final ProgressListener progressListener;
  private final ProcessedDocumentTracker documentTracker;
  private final FilingIndexProvider filingIndexProvider;

  // In-memory cache fallback to avoid repeated S3 exists() checks when no tracker provided
  private final Set<String> existsCache = new HashSet<String>();
  private final Set<String> notExistsCache = new HashSet<String>();

  // Per-CIK cache of accession → primaryDocument from data.sec.gov/submissions API.
  // Avoids duplicate submissions.json fetches when a CIK has multiple accessions in the same run.
  private final ConcurrentHashMap<String, Map<String, String>> cikSubmissionsCache =
      new ConcurrentHashMap<String, Map<String, String>>();

  // Limits diagnostic WARNs for submissions parse/miss — prevents log flooding.
  private final AtomicInteger submissionsParseDiagCount = new AtomicInteger(0);
  private final AtomicInteger submissionsMissDiagCount = new AtomicInteger(0);

  /**
   * Creates a new document ETL processor.
   *
   * @param config HTTP source configuration with document settings
   * @param storageProvider Storage provider for output files
   * @param outputDirectory Directory for converted Parquet files (can be S3 or local path)
   * @param cacheDirectory Cache directory for downloaded documents (can be S3 or local path)
   * @param documentConverter Converter for document processing
   */
  public DocumentETLProcessor(
      HttpSourceConfig config,
      StorageProvider storageProvider,
      String outputDirectory,
      String cacheDirectory,
      FileConverter documentConverter) {
    this(config, storageProvider, outputDirectory, cacheDirectory, documentConverter, null, null);
  }

  /**
   * Creates a new document ETL processor with progress listener.
   *
   * @param config HTTP source configuration with document settings
   * @param storageProvider Storage provider for output files
   * @param outputDirectory Directory for converted Parquet files (can be S3 or local path)
   * @param cacheDirectory Cache directory for downloaded documents (can be S3 or local path)
   * @param documentConverter Converter for document processing
   * @param progressListener Listener for progress updates
   */
  public DocumentETLProcessor(
      HttpSourceConfig config,
      StorageProvider storageProvider,
      String outputDirectory,
      String cacheDirectory,
      FileConverter documentConverter,
      ProgressListener progressListener) {
    this(config, storageProvider, outputDirectory, cacheDirectory, documentConverter,
        progressListener, null);
  }

  /**
   * Creates a new document ETL processor with progress listener and document tracker.
   *
   * @param config HTTP source configuration with document settings
   * @param storageProvider Storage provider for output files
   * @param outputDirectory Directory for converted Parquet files (can be S3 or local path)
   * @param cacheDirectory Cache directory for downloaded documents (can be S3 or local path)
   * @param documentConverter Converter for document processing
   * @param progressListener Listener for progress updates
   * @param documentTracker Tracker for processed documents (avoids S3 checks)
   */
  public DocumentETLProcessor(
      HttpSourceConfig config,
      StorageProvider storageProvider,
      String outputDirectory,
      String cacheDirectory,
      FileConverter documentConverter,
      ProgressListener progressListener,
      ProcessedDocumentTracker documentTracker) {
    this(config, storageProvider, outputDirectory, cacheDirectory, documentConverter,
        progressListener, documentTracker, null);
  }

  /**
   * Creates a new document ETL processor with all options.
   *
   * @param config HTTP source configuration with document settings
   * @param storageProvider Storage provider for output files
   * @param outputDirectory Directory for converted Parquet files (can be S3 or local path)
   * @param cacheDirectory Cache directory for downloaded documents (can be S3 or local path)
   * @param documentConverter Converter for document processing
   * @param progressListener Listener for progress updates
   * @param documentTracker Tracker for processed documents (avoids S3 checks)
   * @param filingIndexProvider Optional index provider for fast CIK pre-filtering
   */
  public DocumentETLProcessor(
      HttpSourceConfig config,
      StorageProvider storageProvider,
      String outputDirectory,
      String cacheDirectory,
      FileConverter documentConverter,
      ProgressListener progressListener,
      ProcessedDocumentTracker documentTracker,
      FilingIndexProvider filingIndexProvider) {
    this.config = config;
    this.storageProvider = storageProvider;
    this.outputDirectory = outputDirectory;
    this.cacheDirectory = cacheDirectory;
    this.documentConverter = documentConverter;
    this.progressListener = progressListener;
    this.documentTracker = documentTracker;
    this.filingIndexProvider = filingIndexProvider;
  }

  /**
   * Processes documents for a single entity (e.g., one company).
   *
   * @param entityVariables Variables identifying the entity (e.g., cik)
   * @return Processing result with statistics
   * @throws IOException If processing fails
   */
  public DocumentETLResult processEntity(Map<String, String> entityVariables) throws IOException {
    long startTime = System.currentTimeMillis();

    // Check index cache before hitting EDGAR API
    if (filingIndexProvider != null) {
      String cik = entityVariables.get("cik");
      String yearStr = entityVariables.get("year");
      if (cik != null && yearStr != null) {
        int year = Integer.parseInt(yearStr);
        List<String> filingTypes = config.getDocumentSource() != null
            ? config.getDocumentSource().getDocumentTypes() : null;
        FilingIndexProvider.CacheDecision decision =
            filingIndexProvider.checkCik(cik, year, filingTypes, documentTracker);
        if (decision == FilingIndexProvider.CacheDecision.SKIP) {
          LOGGER.debug("Index cache: skipping CIK {} year {} (no untracked filings)", cik, year);
          return new DocumentETLResult(0, 0, 0,
              Collections.<String>emptyList(), Collections.<String>emptyList(),
              System.currentTimeMillis() - startTime);
        }
      }
    }

    // Pass cache directory path directly to DocumentSource (supports S3 or local paths)
    DocumentSource documentSource = new DocumentSource(config, storageProvider, cacheDirectory);

    int documentsProcessed = 0;
    int documentsSkipped = 0;
    int documentsFailed = 0;
    List<String> outputFiles = new ArrayList<String>();
    List<String> errors = new ArrayList<String>();

    try {
      // Fetch metadata to enumerate documents
      String metadata = documentSource.fetchMetadata(entityVariables);
      LOGGER.debug("Fetched metadata for entity: {}", entityVariables);

      // Parse documents from metadata (includes pagination file fetching)
      Iterator<Map<String, String>> documents =
          parseDocuments(metadata, entityVariables, documentSource);

      while (documents.hasNext()) {
        Map<String, String> docVariables = documents.next();

        try {
          // Check if document already processed
          if (isAlreadyProcessed(docVariables)) {
            documentsSkipped++;
            continue;
          }

          List<String> converted =
              processDocumentWithRetry(docVariables, documentSource, outputDirectory);

          outputFiles.addAll(converted);
          documentsProcessed++;

          // Mark document as processed to avoid future S3 checks
          if (documentTracker != null && !converted.isEmpty()) {
            String cik = docVariables.get("cik");
            String accession = docVariables.get("accession");
            String form = docVariables.get("form");
            if (cik != null && accession != null) {
              documentTracker.markProcessed(cik, accession, form, converted);
            }
          }

          if (progressListener != null) {
            progressListener.onProgress(documentsProcessed, -1,
                "Processed: " + docVariables.get("document"));
          }

        } catch (IOException e) {
          documentsFailed++;
          String errorMsg = "Failed to process document " + docVariables + ": " + e.getMessage();
          LOGGER.warn(errorMsg, e);
          errors.add(errorMsg);

          // Continue processing remaining documents
        }
      }

    } catch (IOException e) {
      // Metadata fetch failed - this is fatal
      throw new IOException("Failed to fetch metadata for entity " + entityVariables, e);
    }

    long duration = System.currentTimeMillis() - startTime;

    return new DocumentETLResult(
        documentsProcessed,
        documentsSkipped,
        documentsFailed,
        outputFiles,
        errors,
        duration);
  }

  /**
   * Processes documents for multiple entities.
   *
   * @param entities List of entity variable maps
   * @return Aggregated processing result
   * @throws IOException If processing fails fatally
   */
  public DocumentETLResult processEntities(List<Map<String, String>> entities) throws IOException {
    prewarmExistsCache(extractYearsFromEntities(entities));
    long startTime = System.currentTimeMillis();
    int totalProcessed = 0;
    int totalSkipped = 0;
    int totalFailed = 0;
    List<String> allOutputFiles = new ArrayList<String>();
    List<String> allErrors = new ArrayList<String>();

    int entityCount = 0;
    int totalEntities = entities.size();
    for (Map<String, String> entityVariables : entities) {
      try {
        DocumentETLResult result = processEntity(entityVariables);
        totalProcessed += result.getDocumentsProcessed();
        totalSkipped += result.getDocumentsSkipped();
        totalFailed += result.getDocumentsFailed();
        allOutputFiles.addAll(result.getOutputFiles());
        allErrors.addAll(result.getErrors());

        LOGGER.info("Processed entity {}: {} documents, {} skipped, {} failed",
            entityVariables, result.getDocumentsProcessed(),
            result.getDocumentsSkipped(), result.getDocumentsFailed());

      } catch (IOException e) {
        totalFailed++;
        String errorMsg = "Failed to process entity " + entityVariables + ": " + e.getMessage();
        LOGGER.error(errorMsg);
        allErrors.add(errorMsg);
      }
      entityCount++;
      if (entityCount % 500 == 0) {
        LOGGER.info("ETL progress: {}/{} entities processed", entityCount, totalEntities);
      }
    }

    long duration = System.currentTimeMillis() - startTime;

    return new DocumentETLResult(
        totalProcessed,
        totalSkipped,
        totalFailed,
        allOutputFiles,
        allErrors,
        duration);
  }

  /**
   * Processes documents for multiple entities in parallel using a thread pool.
   *
   * <p>Each entity is submitted as an independent task. EDGAR rate limiting is
   * handled globally by {@link DocumentSource}, so concurrent threads collectively
   * respect the 10 req/sec limit regardless of thread count.
   *
   * @param entities List of entity variable maps
   * @param threadCount Number of parallel threads
   * @return Aggregated processing result
   * @throws IOException If processing fails fatally
   */
  public DocumentETLResult processEntitiesParallel(
      List<Map<String, String>> entities, int threadCount) throws IOException {
    prewarmExistsCache(extractYearsFromEntities(entities));
    long startTime = System.currentTimeMillis();
    final AtomicInteger totalProcessed = new AtomicInteger();
    final AtomicInteger totalSkipped = new AtomicInteger();
    final AtomicInteger totalFailed = new AtomicInteger();
    final AtomicInteger entityCount = new AtomicInteger();
    final int totalEntities = entities.size();
    final List<String> allOutputFiles = Collections.synchronizedList(new ArrayList<String>());
    final List<String> allErrors = Collections.synchronizedList(new ArrayList<String>());

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    List<Future<?>> futures = new ArrayList<Future<?>>();

    for (final Map<String, String> entityVariables : entities) {
      futures.add(
          executor.submit(new Callable<Void>() {
        @Override public Void call() {
          try {
            DocumentETLResult result = processEntity(entityVariables);
            totalProcessed.addAndGet(result.getDocumentsProcessed());
            totalSkipped.addAndGet(result.getDocumentsSkipped());
            totalFailed.addAndGet(result.getDocumentsFailed());
            allOutputFiles.addAll(result.getOutputFiles());
            allErrors.addAll(result.getErrors());
            LOGGER.info("Processed entity {}: {} documents, {} skipped, {} failed",
                entityVariables, result.getDocumentsProcessed(),
                result.getDocumentsSkipped(), result.getDocumentsFailed());
          } catch (Exception e) {
            totalFailed.incrementAndGet();
            allErrors.add("Failed: " + entityVariables + ": " + e.getMessage());
            LOGGER.error("Failed to process entity {}: {}", entityVariables, e.getMessage());
          }
          int count = entityCount.incrementAndGet();
          if (count % 500 == 0) {
            LOGGER.info("ETL progress: {}/{} entities processed", count, totalEntities);
          }
          return null;
        }
      }));
    }

    // Wait for all tasks to complete
    for (Future<?> f : futures) {
      try {
        f.get();
      } catch (Exception e) {
        LOGGER.error("Unexpected error waiting for entity task: {}", e.getMessage());
      }
    }
    executor.shutdown();

    long duration = System.currentTimeMillis() - startTime;

    return new DocumentETLResult(
        totalProcessed.get(),
        totalSkipped.get(),
        totalFailed.get(),
        allOutputFiles,
        allErrors,
        duration);
  }

  /**
   * Processes a single accession whose metadata is already known from the full-index cache.
   *
   * <p>Unlike {@link #processEntity}, this method fetches the filing {@code index.json} to
   * obtain the primary document (skipping the per-CIK {@code submissions.json} call) and
   * calls {@link ProcessedDocumentTracker#markProcessed} unconditionally — even for
   * zero-output filings — so the accession is never re-queued on subsequent runs.
   *
   * @param cik        Normalized 10-digit zero-padded CIK
   * @param accession  Accession number with dashes (e.g. {@code 0000320193-25-000006})
   * @param formType   Form type (e.g. {@code 10-K}, {@code 4})
   * @param filingDate ISO filing date (e.g. {@code 2025-01-15})
   * @return processing result
   */
  public DocumentETLResult processAccession(
      String cik, String accession, String formType, String filingDate) {
    long startTime = System.currentTimeMillis();

    if (documentTracker != null && documentTracker.isProcessed(cik, accession, formType)) {
      return new DocumentETLResult(0, 1, 0,
          Collections.<String>emptyList(), Collections.<String>emptyList(),
          System.currentTimeMillis() - startTime);
    }

    DocumentSource documentSource = new DocumentSource(config, storageProvider, cacheDirectory);

    String cikUrl = cik.replaceFirst("^0+", "");
    String accessionNoDashes = accession.replace("-", "");

    String primaryDocument;
    try {
      primaryDocument = fetchPrimaryDocumentFromSubmissions(documentSource, cik, accession);
    } catch (IOException e) {
      LOGGER.warn("Failed to fetch submissions.json for {}/{}: {}", cik, accession, e.getMessage());
      if (documentTracker != null) {
        documentTracker.markProcessed(cik, accession, formType,
            Collections.<String>emptyList());
      }
      return new DocumentETLResult(0, 0, 1,
          Collections.<String>emptyList(),
          Collections.singletonList(
              "submissions.json fetch failed for " + accession + ": " + e.getMessage()),
          System.currentTimeMillis() - startTime);
    }

    // For Form 3/4/5 (insider trading), EDGAR returns XSL-transformed path like
    // "xslF345X03/wf-form4_xxx.xml" — strip the prefix to get the raw XML filename.
    if (primaryDocument != null && (formType.equals("3") || formType.equals("4")
        || formType.equals("5") || formType.startsWith("3/")
        || formType.startsWith("4/") || formType.startsWith("5/"))) {
      if (primaryDocument.startsWith("xslF345X")) {
        int slashIdx = primaryDocument.indexOf('/');
        if (slashIdx > 0) {
          primaryDocument = primaryDocument.substring(slashIdx + 1);
        }
      }
    }

    if (primaryDocument == null || primaryDocument.isEmpty()) {
      LOGGER.debug("No primary document found for {}/{} form {}", cik, accession, formType);
      if (documentTracker != null) {
        documentTracker.markProcessed(cik, accession, formType,
            Collections.<String>emptyList());
      }
      return new DocumentETLResult(0, 1, 0,
          Collections.<String>emptyList(), Collections.<String>emptyList(),
          System.currentTimeMillis() - startTime);
    }

    Map<String, String> docVariables = new HashMap<String, String>();
    docVariables.put("cik", cik);
    docVariables.put("cik_url", cikUrl);
    docVariables.put("accession", accession);
    docVariables.put("accession_url", accessionNoDashes);
    docVariables.put("form", formType);
    docVariables.put("filingDate", filingDate);
    docVariables.put("document", primaryDocument);

    List<String> converted;
    List<String> errors = new ArrayList<String>();
    int processed = 0;
    int failed = 0;

    try {
      converted = processDocumentWithRetry(docVariables, documentSource, outputDirectory);
      processed = 1;
    } catch (IOException e) {
      converted = Collections.<String>emptyList();
      failed = 1;
      errors.add("Failed to process " + accession + ": " + e.getMessage());
      LOGGER.warn("Failed to process accession {}/{}: {}", cik, accession, e.getMessage());
    }

    // Always mark processed — zero-output and errors are valid terminal states
    if (documentTracker != null) {
      documentTracker.markProcessed(cik, accession, formType, converted);
    }

    return new DocumentETLResult(processed, 0, failed,
        converted, errors, System.currentTimeMillis() - startTime);
  }

  /**
   * Processes a list of accessions using the accession-centric path.
   *
   * @param accessions List of accessions to process
   * @return Aggregated processing result
   */
  public DocumentETLResult processAccessions(List<AccessionRef> accessions) {
    prewarmExistsCache(extractYearsFromAccessions(accessions));
    long startTime = System.currentTimeMillis();
    int totalProcessed = 0;
    int totalSkipped = 0;
    int totalFailed = 0;
    List<String> allOutputFiles = new ArrayList<String>();
    List<String> allErrors = new ArrayList<String>();

    int count = 0;
    int total = accessions.size();
    for (AccessionRef ref : accessions) {
      DocumentETLResult result =
          processAccession(ref.cik, ref.accession, ref.formType, ref.filingDate);
      totalProcessed += result.getDocumentsProcessed();
      totalSkipped += result.getDocumentsSkipped();
      totalFailed += result.getDocumentsFailed();
      allOutputFiles.addAll(result.getOutputFiles());
      allErrors.addAll(result.getErrors());
      count++;
      if (count % 500 == 0) {
        LOGGER.info("ETL progress: {}/{} accessions processed", count, total);
      }
    }

    return new DocumentETLResult(totalProcessed, totalSkipped, totalFailed,
        allOutputFiles, allErrors, System.currentTimeMillis() - startTime);
  }

  /**
   * Processes accessions in parallel using a thread pool.
   *
   * @param accessions  List of accessions to process
   * @param threadCount Number of parallel threads
   * @return Aggregated processing result
   */
  public DocumentETLResult processAccessionsParallel(
      List<AccessionRef> accessions, int threadCount) {
    prewarmExistsCache(extractYearsFromAccessions(accessions));
    long startTime = System.currentTimeMillis();
    final AtomicInteger totalProcessed = new AtomicInteger();
    final AtomicInteger totalSkipped = new AtomicInteger();
    final AtomicInteger totalFailed = new AtomicInteger();
    final AtomicInteger count = new AtomicInteger();
    final int total = accessions.size();
    final List<String> allOutputFiles =
        Collections.synchronizedList(new ArrayList<String>());
    final List<String> allErrors =
        Collections.synchronizedList(new ArrayList<String>());

    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    List<Future<?>> futures = new ArrayList<Future<?>>();

    for (final AccessionRef ref : accessions) {
      futures.add(
          executor.submit(new Callable<Void>() {
        @Override public Void call() {
          DocumentETLResult result =
              processAccession(ref.cik, ref.accession, ref.formType, ref.filingDate);
          totalProcessed.addAndGet(result.getDocumentsProcessed());
          totalSkipped.addAndGet(result.getDocumentsSkipped());
          totalFailed.addAndGet(result.getDocumentsFailed());
          allOutputFiles.addAll(result.getOutputFiles());
          allErrors.addAll(result.getErrors());
          int c = count.incrementAndGet();
          if (c % 500 == 0) {
            LOGGER.info("ETL progress: {}/{} accessions processed", c, total);
          }
          return null;
        }
      }));
    }

    for (Future<?> f : futures) {
      try {
        f.get();
      } catch (Exception e) {
        LOGGER.error("Unexpected error waiting for accession task: {}", e.getMessage());
      }
    }
    executor.shutdown();

    return new DocumentETLResult(totalProcessed.get(), totalSkipped.get(), totalFailed.get(),
        allOutputFiles, allErrors, System.currentTimeMillis() - startTime);
  }

  /**
   * Looks up the primary document filename for an accession via the EDGAR submissions API.
   *
   * <p>Fetches {@code https://data.sec.gov/submissions/CIK{cik}.json} and returns the
   * {@code primaryDocument} value whose {@code accessionNumber} matches the given accession.
   * Results are cached per CIK so that multiple accessions from the same company only require
   * one API call.
   *
   * @return primary document filename, or null if not found
   */
  private String fetchPrimaryDocumentFromSubmissions(
      DocumentSource documentSource, String cik, String accession) throws IOException {
    Map<String, String> byAccession = cikSubmissionsCache.get(cik);
    if (byAccession == null) {
      String submissionsUrl = "https://data.sec.gov/submissions/CIK" + cik + ".json";
      String submissionsJson = documentSource.fetchUrlContent(submissionsUrl);
      List<String> accessionNumbers = extractJsonArray(submissionsJson, "accessionNumber");
      List<String> primaryDocuments = extractJsonArray(submissionsJson, "primaryDocument");
      Map<String, String> map = new HashMap<String, String>();
      int count = Math.min(accessionNumbers.size(), primaryDocuments.size());
      for (int i = 0; i < count; i++) {
        map.put(accessionNumbers.get(i), primaryDocuments.get(i));
      }
      if (accessionNumbers.isEmpty() && submissionsParseDiagCount.get() < 5) {
        int diagIdx = submissionsParseDiagCount.getAndIncrement();
        LOGGER.warn("submissions.json parse [diag {}]: url={} responseLen={} "
            + "accessionNumbers=0 — first 200 chars of response: [{}]",
            diagIdx, submissionsUrl, submissionsJson.length(),
            submissionsJson.length() > 200 ? submissionsJson.substring(0, 200) : submissionsJson);
      }
      cikSubmissionsCache.putIfAbsent(cik, map);
      byAccession = cikSubmissionsCache.get(cik);
    }
    String result = byAccession.get(accession);
    if (result == null && submissionsMissDiagCount.get() < 5) {
      int mapSize = byAccession.size();
      if (mapSize > 0) {
        int diagIdx = submissionsMissDiagCount.getAndIncrement();
        String sampleKey = byAccession.keySet().iterator().hasNext()
            ? byAccession.keySet().iterator().next() : "(none)";
        LOGGER.warn("submissions.json miss [diag {}]: cik={} accession={} mapSize={} sampleKey={}",
            diagIdx, cik, accession, mapSize, sampleKey);
      }
    }
    return result;
  }

  private Set<String> extractYearsFromAccessions(List<AccessionRef> accessions) {
    Set<String> years = new HashSet<String>();
    for (AccessionRef ref : accessions) {
      if (ref.filingDate != null && ref.filingDate.length() >= 4) {
        years.add(ref.filingDate.substring(0, 4));
      }
    }
    return years;
  }

  private static final int DOC_MAX_RETRIES = 3;
  private static final long DOC_RETRY_INITIAL_DELAY_MS = 5000;

  /**
   * Downloads and converts a single document, retrying on transient errors.
   * Each retry waits with exponential backoff (5s, 10s, 20s).
   */
  private List<String> processDocumentWithRetry(
      Map<String, String> docVariables, DocumentSource documentSource,
      String outputDirectory) throws IOException {
    IOException lastException = null;
    for (int attempt = 0; attempt < DOC_MAX_RETRIES; attempt++) {
      try {
        String documentPath = documentSource.downloadDocument(docVariables);
        ConversionMetadata conversionMetadata = new ConversionMetadata(outputDirectory);
        for (Map.Entry<String, String> entry : docVariables.entrySet()) {
          conversionMetadata.setHint(entry.getKey(), entry.getValue());
        }
        return documentConverter.convert(documentPath, outputDirectory, conversionMetadata);
      } catch (IOException e) {
        lastException = e;
        if (attempt < DOC_MAX_RETRIES - 1 && isTransientError(e)) {
          long delay = DOC_RETRY_INITIAL_DELAY_MS * (1L << attempt);
          LOGGER.warn("Transient error processing {} - retrying in {}ms (attempt {}/{}): {}",
              docVariables.get("document"), delay, attempt + 1, DOC_MAX_RETRIES,
              e.getMessage());
          try {
            Thread.sleep(delay);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw e;
          }
        } else {
          throw e;
        }
      }
    }
    throw lastException;
  }

  /**
   * Returns whether an IOException represents a transient error worth retrying
   * at the document level. Covers HTTP 5xx, truncated streams, timeouts, and
   * connection resets.
   */
  private static boolean isTransientError(IOException e) {
    // Walk the cause chain — transient root causes are often wrapped
    Throwable t = e;
    while (t != null) {
      String msg = t.getMessage();
      if (msg != null) {
        String lower = msg.toLowerCase();
        if (lower.contains("http 50") // 500, 502, 503, 504
            || lower.contains("timed out")
            || lower.contains("timeout")
            || lower.contains("connection reset")
            || lower.contains("connection closed")
            || lower.contains("broken pipe")
            || lower.contains("premature end")
            || lower.contains("premature eof")
            || lower.contains("unexpected end")
            || lower.contains("end of zlib")
            || lower.contains("end of content-length")) {
          return true;
        }
      }
      if (t instanceof java.io.EOFException) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }

  /**
   * Parses document metadata to extract document list.
   * Backward-compatible overload without pagination support.
   *
   * @param metadataJson Raw metadata JSON
   * @param baseVariables Base variables (e.g., cik)
   * @return Iterator over document variable maps
   */
  protected Iterator<Map<String, String>> parseDocuments(
      String metadataJson, Map<String, String> baseVariables) {
    return parseDocuments(metadataJson, baseVariables, null);
  }

  /**
   * Parses document metadata to extract document list, including pagination files.
   *
   * <p>EDGAR's submissions.json returns filings in a "recent" section (limited to ~1000
   * entries) plus a "filings.files" array pointing to paginated history files. This method
   * parses the "recent" section and then fetches any pagination files whose date range
   * overlaps with the configured year range.
   *
   * @param metadataJson Raw metadata JSON
   * @param baseVariables Base variables (e.g., cik)
   * @param documentSource Document source for fetching pagination files (may be null)
   * @return Iterator over document variable maps
   */
  protected Iterator<Map<String, String>> parseDocuments(
      String metadataJson, Map<String, String> baseVariables,
      DocumentSource documentSource) {
    // Parse "recent" filings from main submissions.json
    List<Map<String, String>> allDocuments =
        new SecSubmissionsIterator(metadataJson, baseVariables, config).documents;

    // Extract and fetch pagination files if documentSource is available
    if (documentSource != null) {
      List<PaginationFileRef> paginationFiles = extractPaginationFiles(metadataJson);

      if (!paginationFiles.isEmpty()) {
        HttpSourceConfig.DocumentSourceConfig docConfig =
            config != null ? config.getDocumentSource() : null;
        Integer startYear = docConfig != null ? docConfig.getStartYear() : null;
        Integer endYear = docConfig != null ? docConfig.getEndYear() : null;
        String cik = baseVariables.get("cik");

        // Base URL for pagination files (same directory as submissions.json)
        String baseUrl = "https://data.sec.gov/submissions/";

        for (PaginationFileRef ref : paginationFiles) {
          // Skip pagination files outside the configured year range
          if (!ref.overlapsYearRange(startYear, endYear)) {
            LOGGER.debug("Skipping pagination file {} for CIK {} "
                    + "(range {}-{} outside {}-{})",
                ref.name, cik, ref.filingFrom, ref.filingTo, startYear, endYear);
            continue;
          }

          try {
            String paginationJson =
                fetchPaginationFile(documentSource, baseUrl + ref.name, ref.name, cik);

            // Parse pagination file through same iterator (same array structure)
            List<Map<String, String>> paginatedDocs =
                new SecSubmissionsIterator(paginationJson, baseVariables, config).documents;

            LOGGER.info("Fetched pagination file {} for CIK {}: {} filings "
                    + "(range {}-{})",
                ref.name, cik, paginatedDocs.size(), ref.filingFrom, ref.filingTo);

            allDocuments.addAll(paginatedDocs);

          } catch (IOException e) {
            LOGGER.warn("Failed to fetch pagination file {} for CIK {}: {}",
                ref.name, cik, e.getMessage());
          }
        }
      }
    }

    return allDocuments.iterator();
  }

  /**
   * Fetches a pagination file, using cached version from storage if available.
   */
  private String fetchPaginationFile(DocumentSource documentSource,
      String url, String filename, String cik) throws IOException {
    // Check cache first
    String cachePath =
        storageProvider.resolvePath(cacheDirectory, "submissions_pagination/" + filename);

    if (storageProvider.exists(cachePath)) {
      LOGGER.debug("Using cached pagination file: {}", cachePath);
      return readStorageFile(cachePath);
    }

    // Fetch from EDGAR
    LOGGER.info("Fetching pagination file: {} for CIK {}", filename, cik);
    String content = documentSource.fetchUrlContent(url);

    // Cache to storage for future use (pagination files are immutable)
    storageProvider.writeFile(cachePath, content.getBytes(StandardCharsets.UTF_8));

    return content;
  }

  /**
   * Reads a text file from the storage provider as a string.
   */
  private String readStorageFile(String path) throws IOException {
    StringBuilder sb = new StringBuilder();
    try (InputStream is = storageProvider.openInputStream(path);
         BufferedReader reader =
             new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line).append("\n");
      }
    }
    return sb.toString();
  }

  /**
   * Extracts pagination file references from EDGAR submissions.json.
   *
   * <p>The files array is located at "filings.files" and contains objects like:
   * <pre>{@code
   * {"name":"CIK0000070502-submissions-001.json",
   *  "filingCount":866,
   *  "filingFrom":"1994-01-18",
   *  "filingTo":"2017-10-06"}
   * }</pre>
   */
  List<PaginationFileRef> extractPaginationFiles(String json) {
    List<PaginationFileRef> refs = new ArrayList<PaginationFileRef>();

    // Find "files" array within "filings" object
    int filingsIdx = json.indexOf("\"filings\"");
    if (filingsIdx < 0) {
      return refs;
    }

    // Find "files" key after "filings"
    int filesIdx = json.indexOf("\"files\"", filingsIdx);
    if (filesIdx < 0) {
      return refs;
    }

    // Find the opening bracket of the files array
    int arrayStart = json.indexOf("[", filesIdx);
    if (arrayStart < 0) {
      return refs;
    }

    int arrayEnd = findMatchingBracket(json, arrayStart);
    if (arrayEnd < 0) {
      return refs;
    }

    String filesArray = json.substring(arrayStart + 1, arrayEnd);

    // Parse each object in the array
    int pos = 0;
    while (pos < filesArray.length()) {
      int objStart = filesArray.indexOf("{", pos);
      if (objStart < 0) {
        break;
      }
      int objEnd = findMatchingBracket(filesArray, objStart);
      if (objEnd < 0) {
        break;
      }

      String obj = filesArray.substring(objStart, objEnd + 1);

      String name = extractJsonStringField(obj, "name");
      String filingFrom = extractJsonStringField(obj, "filingFrom");
      String filingTo = extractJsonStringField(obj, "filingTo");
      int filingCount = extractJsonIntField(obj, "filingCount");

      if (name != null && !name.isEmpty()) {
        refs.add(new PaginationFileRef(name, filingFrom, filingTo, filingCount));
      }

      pos = objEnd + 1;
    }

    return refs;
  }

  /**
   * Extracts a string field value from a JSON object string.
   */
  static String extractJsonStringField(String json, String fieldName) {
    String key = "\"" + fieldName + "\"";
    int idx = json.indexOf(key);
    if (idx < 0) {
      return null;
    }
    // Skip past key and colon
    int colonIdx = json.indexOf(":", idx + key.length());
    if (colonIdx < 0) {
      return null;
    }
    int quoteStart = json.indexOf("\"", colonIdx + 1);
    if (quoteStart < 0) {
      return null;
    }
    int quoteEnd = json.indexOf("\"", quoteStart + 1);
    if (quoteEnd < 0) {
      return null;
    }
    return json.substring(quoteStart + 1, quoteEnd);
  }

  /**
   * Extracts an integer field value from a JSON object string.
   */
  static int extractJsonIntField(String json, String fieldName) {
    String key = "\"" + fieldName + "\"";
    int idx = json.indexOf(key);
    if (idx < 0) {
      return 0;
    }
    int colonIdx = json.indexOf(":", idx + key.length());
    if (colonIdx < 0) {
      return 0;
    }
    // Skip whitespace after colon
    int numStart = colonIdx + 1;
    while (numStart < json.length() && json.charAt(numStart) == ' ') {
      numStart++;
    }
    // Read digits
    int numEnd = numStart;
    while (numEnd < json.length()
        && (Character.isDigit(json.charAt(numEnd)) || json.charAt(numEnd) == '-')) {
      numEnd++;
    }
    if (numEnd == numStart) {
      return 0;
    }
    try {
      return Integer.parseInt(json.substring(numStart, numEnd));
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  /**
   * Finds the matching closing bracket/brace for an opening bracket/brace.
   * Returns -1 if not found.
   */
  static int findMatchingBracket(String json, int openPos) {
    char open = json.charAt(openPos);
    char close;
    if (open == '[') {
      close = ']';
    } else if (open == '{') {
      close = '}';
    } else {
      return -1;
    }

    int depth = 1;
    boolean inString = false;
    for (int i = openPos + 1; i < json.length(); i++) {
      char c = json.charAt(i);
      if (c == '\\' && inString) {
        i++; // skip escaped character
        continue;
      }
      if (c == '"') {
        inString = !inString;
        continue;
      }
      if (!inString) {
        if (c == open) {
          depth++;
        } else if (c == close) {
          depth--;
          if (depth == 0) {
            return i;
          }
        }
      }
    }
    return -1;
  }

  /**
   * Extracts a JSON string array by field name using simple string scanning (Java 8 compatible).
   * Returns values from the first occurrence of {@code "fieldName": [...]}.
   */
  static List<String> extractJsonArray(String json, String fieldName) {
    List<String> values = new ArrayList<String>();
    int start = json.indexOf("\"" + fieldName + "\"");
    if (start < 0) {
      return values;
    }
    start = json.indexOf("[", start);
    if (start < 0) {
      return values;
    }
    int end = json.indexOf("]", start);
    if (end < 0) {
      return values;
    }
    String arrayContent = json.substring(start + 1, end);
    int pos = 0;
    while (pos < arrayContent.length()) {
      int quoteStart = arrayContent.indexOf("\"", pos);
      if (quoteStart < 0) {
        break;
      }
      int quoteEnd = arrayContent.indexOf("\"", quoteStart + 1);
      if (quoteEnd < 0) {
        break;
      }
      values.add(arrayContent.substring(quoteStart + 1, quoteEnd));
      pos = quoteEnd + 1;
    }
    return values;
  }

  /**
   * Pre-warms the existsCache by listing all files in each output year partition.
   * Replaces per-filing storageProvider.exists() (Class A R2 HEAD ops) with a single
   * LIST per year. Called once before processing begins.
   *
   * @param years set of 4-digit year strings to list (e.g., {@code {"2025","2026"}})
   */
  private void prewarmExistsCache(Set<String> years) {
    if (documentTracker != null) {
      return;
    }
    for (String year : years) {
      String yearPrefix = outputDirectory + "/year=" + year;
      try {
        List<StorageProvider.FileEntry> entries = storageProvider.listFiles(yearPrefix, true);
        int count = 0;
        for (StorageProvider.FileEntry entry : entries) {
          if (!entry.isDirectory()) {
            existsCache.add(entry.getPath());
            count++;
          }
        }
        LOGGER.info("Pre-warmed existsCache with {} files from {}", count, yearPrefix);
      } catch (IOException e) {
        LOGGER.warn("Could not pre-warm existsCache for {}: {}", yearPrefix, e.getMessage());
      }
    }
  }

  /**
   * Extracts the unique year values from an entity list for existsCache pre-warming.
   */
  private Set<String> extractYearsFromEntities(List<Map<String, String>> entities) {
    Set<String> years = new HashSet<String>();
    for (Map<String, String> entity : entities) {
      String year = entity.get("year");
      if (year != null && !year.isEmpty()) {
        years.add(year);
      }
    }
    return years;
  }

  /**
   * Checks if a document has already been processed.
   * Uses documentTracker if available (no S3 checks), otherwise falls back to
   * in-memory cache and S3 exists checks.
   *
   * @param docVariables Document variables
   * @return true if already processed
   */
  protected boolean isAlreadyProcessed(Map<String, String> docVariables) {
    String cik = docVariables.get("cik");
    String accession = docVariables.get("accession");

    if (cik == null || accession == null) {
      return false;
    }

    // Use document tracker if available (no S3 checks needed)
    if (documentTracker != null) {
      String form = docVariables.get("form");
      return documentTracker.isProcessed(cik, accession, form);
    }

    // Fallback: check S3 for output files (slower)
    String year = extractYearFromAccession(accession);
    if (year == null) {
      return false;
    }

    String basePath = String.format("%s/year=%s/%s_%s", outputDirectory, year, cik, accession);
    String[] suffixes = {"_insider.parquet", "_facts.parquet", "_metadata.parquet"};

    try {
      for (String suffix : suffixes) {
        String fullPath = basePath + suffix;

        // Check in-memory cache first to avoid S3 calls
        if (existsCache.contains(fullPath)) {
          return true;
        }
        if (notExistsCache.contains(fullPath)) {
          continue;
        }

        // Not in cache - check storage and cache the result
        boolean exists = storageProvider.exists(fullPath);
        if (exists) {
          existsCache.add(fullPath);
          return true;
        } else {
          notExistsCache.add(fullPath);
        }
      }
      return false;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Extract 4-digit year from SEC accession number.
   * Accession format: XXXXXXXXXX-YY-NNNNNN where YY is 2-digit year.
   */
  private String extractYearFromAccession(String accession) {
    if (accession == null || accession.length() < 13) {
      return null;
    }
    // Find the year part after first dash
    int dashIndex = accession.indexOf('-');
    if (dashIndex < 0 || dashIndex + 3 > accession.length()) {
      return null;
    }
    String twoDigitYear = accession.substring(dashIndex + 1, dashIndex + 3);
    try {
      int yy = Integer.parseInt(twoDigitYear);
      // Assume 00-50 is 2000-2050, 51-99 is 1951-1999
      int year = yy <= 50 ? 2000 + yy : 1900 + yy;
      return String.valueOf(year);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * Progress listener for document processing.
   */
  public interface ProgressListener {
    /**
     * Called when progress is made.
     *
     * @param processed Number of documents processed so far
     * @param total Total number of documents (-1 if unknown)
     * @param message Progress message
     */
    void onProgress(int processed, int total, String message);
  }

  /** Descriptor for one accession in the accession-centric ETL path. */
  public static class AccessionRef {
    public final String cik;
    public final String accession;
    public final String formType;
    public final String filingDate;

    public AccessionRef(String cik, String accession, String formType, String filingDate) {
      this.cik = cik;
      this.accession = accession;
      this.formType = formType;
      this.filingDate = filingDate;
    }

    @Override public String toString() {
      return cik + "/" + accession + "(" + formType + ")";
    }
  }

  /**
   * Result of document ETL processing.
   */
  public static class DocumentETLResult {
    private final int documentsProcessed;
    private final int documentsSkipped;
    private final int documentsFailed;
    private final List<String> outputFiles;
    private final List<String> errors;
    private final long durationMs;

    public DocumentETLResult(int documentsProcessed, int documentsSkipped,
        int documentsFailed, List<String> outputFiles,
        List<String> errors, long durationMs) {
      this.documentsProcessed = documentsProcessed;
      this.documentsSkipped = documentsSkipped;
      this.documentsFailed = documentsFailed;
      this.outputFiles = outputFiles;
      this.errors = errors;
      this.durationMs = durationMs;
    }

    public int getDocumentsProcessed() {
      return documentsProcessed;
    }

    public int getDocumentsSkipped() {
      return documentsSkipped;
    }

    public int getDocumentsFailed() {
      return documentsFailed;
    }

    public List<String> getOutputFiles() {
      return outputFiles;
    }

    public List<String> getErrors() {
      return errors;
    }

    public long getDurationMs() {
      return durationMs;
    }

    public boolean isSuccess() {
      return documentsFailed == 0;
    }

    @Override public String toString() {
      return String.format(
          "DocumentETLResult{processed=%d, skipped=%d, failed=%d, outputs=%d, duration=%dms}",
          documentsProcessed, documentsSkipped, documentsFailed,
          outputFiles.size(), durationMs);
    }
  }

  /**
   * Reference to a pagination file from EDGAR's submissions.json "filings.files" array.
   */
  static class PaginationFileRef {
    final String name;
    final String filingFrom;
    final String filingTo;
    final int filingCount;

    PaginationFileRef(String name, String filingFrom, String filingTo, int filingCount) {
      this.name = name;
      this.filingFrom = filingFrom;
      this.filingTo = filingTo;
      this.filingCount = filingCount;
    }

    /**
     * Checks if this pagination file's date range overlaps with the given year range.
     * Returns true if either bound is null (no filtering), or if the ranges overlap.
     */
    boolean overlapsYearRange(Integer startYear, Integer endYear) {
      // No year filtering configured - always include
      if (startYear == null && endYear == null) {
        return true;
      }
      // Parse years from filing dates (format: YYYY-MM-DD)
      int fileFromYear = parseYear(filingFrom);
      int fileToYear = parseYear(filingTo);

      // If we can't parse dates, include the file to be safe
      if (fileFromYear < 0 || fileToYear < 0) {
        return true;
      }

      // Check overlap: file range [fileFromYear, fileToYear] vs config [startYear, endYear]
      if (endYear != null && fileFromYear > endYear) {
        return false;
      }
      if (startYear != null && fileToYear < startYear) {
        return false;
      }
      return true;
    }

    private static int parseYear(String date) {
      if (date == null || date.length() < 4) {
        return -1;
      }
      try {
        return Integer.parseInt(date.substring(0, 4));
      } catch (NumberFormatException e) {
        return -1;
      }
    }

    @Override public String toString() {
      return "PaginationFileRef{name='" + name
          + "', filingFrom='" + filingFrom
          + "', filingTo='" + filingTo
          + "', filingCount=" + filingCount + "}";
    }
  }

  /**
   * Iterator over SEC EDGAR submissions.json documents.
   */
  private static class SecSubmissionsIterator implements Iterator<Map<String, String>> {

    @SuppressWarnings("UnusedVariable")
    private final String cik;
    private final HttpSourceConfig config;
    private final List<Map<String, String>> documents;
    private int index = 0;

    SecSubmissionsIterator(String metadataJson, Map<String, String> baseVariables,
        HttpSourceConfig httpConfig) {
      this.cik = baseVariables.get("cik");
      this.config = httpConfig;
      this.documents = parseSubmissions(metadataJson, baseVariables);
    }

    private List<Map<String, String>> parseSubmissions(String json,
        Map<String, String> baseVariables) {
      List<Map<String, String>> result = new ArrayList<Map<String, String>>();

      // Get document source config for filtering
      HttpSourceConfig.DocumentSourceConfig docConfig =
          config != null ? config.getDocumentSource() : null;
      List<String> documentTypes = docConfig != null ? docConfig.getDocumentTypes() : null;

      try {
        // Parse JSON manually for Java 8 compatibility
        // Look for filings object with recent/files arrays

        // Extract accessionNumber array
        List<String> accessionNumbers = extractJsonArray(json, "accessionNumber");
        List<String> forms = extractJsonArray(json, "form");
        List<String> filingDates = extractJsonArray(json, "filingDate");
        List<String> primaryDocuments = extractJsonArray(json, "primaryDocument");

        int count =
            Math.min(
                accessionNumbers.size(), Math.min(forms.size(),
                Math.min(filingDates.size(), primaryDocuments.size())));

        // Get year range for filtering
        Integer startYear = docConfig != null ? docConfig.getStartYear() : null;
        Integer endYear = docConfig != null ? docConfig.getEndYear() : null;

        for (int i = 0; i < count; i++) {
          String form = forms.get(i);
          String filingDate = filingDates.get(i);

          // Filter by year range if configured
          if (startYear != null || endYear != null) {
            try {
              int filingYear = Integer.parseInt(filingDate.substring(0, 4));
              if (startYear != null && filingYear < startYear) {
                continue;
              }
              if (endYear != null && filingYear > endYear) {
                continue;
              }
            } catch (Exception e) {
              // If date parsing fails, skip this filing
              LOGGER.debug("Skipping filing with unparseable date: {}", filingDate);
              continue;
            }
          }

          // Filter by document types if configured
          if (documentTypes != null && !documentTypes.isEmpty()) {
            boolean matches = false;
            for (String type : documentTypes) {
              if (form.equalsIgnoreCase(type) || form.startsWith(type + "/")) {
                matches = true;
                break;
              }
            }
            if (!matches) {
              continue;
            }
          }

          Map<String, String> doc = new HashMap<String, String>();
          doc.putAll(baseVariables);
          String accession = accessionNumbers.get(i);
          doc.put("accession", accession);
          // SEC URLs require accession without dashes
          doc.put("accession_url", accession.replace("-", ""));
          // SEC URLs require CIK without leading zeros
          String cik = baseVariables.get("cik");
          if (cik != null) {
            doc.put("cik_url", cik.replaceFirst("^0+", ""));
          }
          doc.put("form", form);
          doc.put("filingDate", filingDates.get(i));

          // For Form 3/4/5 (insider trading), SEC returns XSL-transformed path
          // like "xslF345X03/wf-form4_xxx.xml" but we need raw XML at "wf-form4_xxx.xml"
          String document = primaryDocuments.get(i);
          if (form != null && (form.equals("3") || form.equals("4") || form.equals("5")
              || form.startsWith("3/") || form.startsWith("4/") || form.startsWith("5/"))) {
            if (document.startsWith("xslF345X")) {
              // Strip XSL prefix: "xslF345X03/wf-form4_xxx.xml" -> "wf-form4_xxx.xml"
              int slashIdx = document.indexOf('/');
              if (slashIdx > 0) {
                document = document.substring(slashIdx + 1);
                LOGGER.debug("Stripped XSL prefix from Form {} document: {}", form, document);
              }
            }
          }
          doc.put("document", document);

          result.add(doc);
        }

      } catch (Exception e) {
        LOGGER.warn("Failed to parse submissions JSON: {}", e.getMessage());
      }

      return result;
    }

    private List<String> extractJsonArray(String json, String fieldName) {
      return DocumentETLProcessor.extractJsonArray(json, fieldName);
    }

    @Override public boolean hasNext() {
      return index < documents.size();
    }

    @Override public Map<String, String> next() {
      return documents.get(index++);
    }

    @Override public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
