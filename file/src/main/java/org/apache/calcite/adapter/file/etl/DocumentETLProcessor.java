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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.converters.FileConverter;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
    this(config, storageProvider, outputDirectory, cacheDirectory, documentConverter, null);
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
    this.config = config;
    this.storageProvider = storageProvider;
    this.outputDirectory = outputDirectory;
    this.cacheDirectory = cacheDirectory;
    this.documentConverter = documentConverter;
    this.progressListener = progressListener;
  }

  /**
   * Processes documents for a single entity (e.g., one company).
   *
   * @param entityVariables Variables identifying the entity (e.g., cik)
   * @return Processing result with statistics
   * @throws IOException If processing fails
   */
  public DocumentETLResult processEntity(Map<String, String> entityVariables) throws IOException {
    // Pass cache directory path directly to DocumentSource (supports S3 or local paths)
    DocumentSource documentSource = new DocumentSource(config, storageProvider, cacheDirectory);

    long startTime = System.currentTimeMillis();
    int documentsProcessed = 0;
    int documentsSkipped = 0;
    int documentsFailed = 0;
    List<String> outputFiles = new ArrayList<String>();
    List<String> errors = new ArrayList<String>();

    try {
      // Fetch metadata to enumerate documents
      String metadata = documentSource.fetchMetadata(entityVariables);
      LOGGER.debug("Fetched metadata for entity: {}", entityVariables);

      // Parse documents from metadata
      Iterator<Map<String, String>> documents =
          parseDocuments(metadata, entityVariables);

      while (documents.hasNext()) {
        Map<String, String> docVariables = documents.next();

        try {
          // Check if document already processed
          if (isAlreadyProcessed(docVariables)) {
            documentsSkipped++;
            continue;
          }

          // Download document - returns storage provider path (can be S3)
          String documentPath = documentSource.downloadDocument(docVariables);

          // Convert document using String-based method for S3 compatibility
          ConversionMetadata conversionMetadata = new ConversionMetadata(outputDirectory);
          List<String> converted = documentConverter.convert(
              documentPath,
              outputDirectory,
              conversionMetadata);

          outputFiles.addAll(converted);
          documentsProcessed++;

          if (progressListener != null) {
            progressListener.onProgress(documentsProcessed, -1,
                "Processed: " + docVariables.get("document"));
          }

        } catch (IOException e) {
          documentsFailed++;
          String errorMsg = "Failed to process document " + docVariables + ": " + e.getMessage();
          LOGGER.warn(errorMsg);
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
    long startTime = System.currentTimeMillis();
    int totalProcessed = 0;
    int totalSkipped = 0;
    int totalFailed = 0;
    List<String> allOutputFiles = new ArrayList<String>();
    List<String> allErrors = new ArrayList<String>();

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
        // Continue with next entity
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
   * Parses document metadata to extract document list.
   *
   * <p>This method handles SEC EDGAR submissions.json format.
   * Override or extend for other metadata formats.
   *
   * @param metadataJson Raw metadata JSON
   * @param baseVariables Base variables (e.g., cik)
   * @return Iterator over document variable maps
   */
  protected Iterator<Map<String, String>> parseDocuments(
      String metadataJson, Map<String, String> baseVariables) {
    // Parse SEC EDGAR submissions.json format
    // Returns iterator of document variables with accession, form type, etc.
    return new SecSubmissionsIterator(metadataJson, baseVariables, config);
  }

  /**
   * Checks if a document has already been processed.
   *
   * @param docVariables Document variables
   * @return true if already processed
   */
  protected boolean isAlreadyProcessed(Map<String, String> docVariables) {
    // Check for existence of output files for this document
    String cik = docVariables.get("cik");
    String accession = docVariables.get("accession");

    if (cik == null || accession == null) {
      return false;
    }

    // Extract year from accession number (format: XXXXXXXXXX-YY-NNNNNN where YY is 2-digit year)
    // Examples: 0000320193-24-000132 -> 2024, 0001104659-23-014712 -> 2023
    String year = extractYearFromAccession(accession);
    if (year == null) {
      return false;
    }

    // Check for specific parquet files using direct path (no glob)
    // Try common output types: _insider (Form 3/4/5), _facts (10-K/10-Q), _metadata
    String basePath = String.format("%s/year=%s/%s_%s", outputDirectory, year, cik, accession);
    String[] suffixes = {"_insider.parquet", "_facts.parquet", "_metadata.parquet"};

    try {
      for (String suffix : suffixes) {
        if (storageProvider.exists(basePath + suffix)) {
          return true;
        }
      }
      return false;
    } catch (Exception e) {
      // If check fails, assume not processed
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
   * Iterator over SEC EDGAR submissions.json documents.
   */
  private static class SecSubmissionsIterator implements Iterator<Map<String, String>> {

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

        int count = Math.min(accessionNumbers.size(),
            Math.min(forms.size(),
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
      List<String> values = new ArrayList<String>();

      // Simple JSON array extraction for field: ["value1", "value2", ...]
      String pattern = "\"" + fieldName + "\"\\s*:\\s*\\[";
      int start = json.indexOf(pattern.replace("\\s*", ""));
      if (start < 0) {
        // Try without strict matching
        start = json.indexOf("\"" + fieldName + "\"");
        if (start < 0) {
          return values;
        }
        start = json.indexOf("[", start);
        if (start < 0) {
          return values;
        }
      } else {
        start = json.indexOf("[", start);
      }

      int end = json.indexOf("]", start);
      if (end < 0) {
        return values;
      }

      String arrayContent = json.substring(start + 1, end);

      // Extract string values
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
