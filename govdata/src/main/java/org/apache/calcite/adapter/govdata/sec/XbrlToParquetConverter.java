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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.converters.FileConverter;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.text.StringEscapeUtils;

import org.jsoup.Jsoup;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * Converter for XBRL files to Parquet format.
 * Implements the file adapter's FileConverter interface for integration
 * with FileConversionManager.
 */
public class XbrlToParquetConverter implements FileConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(XbrlToParquetConverter.class);

  // Global lock for complete single-threading of all vectorization operations to ensure 100% thread safety
  private static final Object GLOBAL_VECTORIZATION_LOCK = new Object();

  private final StorageProvider storageProvider;
  private final boolean enableVectorization;

  // Override metadata for non-XBRL filings (e.g. 8-K plain HTML) is passed
  // via ConversionMetadata hints instead of mutable instance fields, making
  // this converter stateless and safe for concurrent use by multiple threads.

  public XbrlToParquetConverter(StorageProvider storageProvider) {
    this(storageProvider, false);
  }

  public XbrlToParquetConverter(StorageProvider storageProvider, boolean enableVectorization) {
    this.storageProvider = storageProvider;
    this.enableVectorization = enableVectorization;
  }

  /**
   * Read all bytes from an InputStream.
   * Helper method for Java 8 compatibility (Files.readAllBytes with InputStream is Java 9+).
   */
  private byte[] readAllBytes(InputStream is) throws IOException {
    java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
    byte[] data = new byte[8192];
    int bytesRead;
    while ((bytesRead = is.read(data, 0, data.length)) != -1) {
      buffer.write(data, 0, bytesRead);
    }
    return buffer.toByteArray();
  }

  // HTML tag removal pattern
  private static final Pattern HTML_TAG_PATTERN = Pattern.compile("<[^>]+>");
  private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");

  @Override public boolean canConvert(String sourceFormat, String targetFormat) {
    return ("xbrl".equalsIgnoreCase(sourceFormat) || "xml".equalsIgnoreCase(sourceFormat)
        || "html".equalsIgnoreCase(sourceFormat) || "htm".equalsIgnoreCase(sourceFormat))
        && "parquet".equalsIgnoreCase(targetFormat);
  }

  @Override public List<String> convert(String sourcePath, String targetDirectory,
      ConversionMetadata metadata) throws IOException {
    // Extract hints from metadata if available (populated by DocumentETLProcessor)
    // Pass explicit metadata for all filing types when hints are present,
    // so the converter can distinguish actual filing date from period end date
    if (metadata != null && metadata.getHint("form") != null) {
      return convertInternal(sourcePath, targetDirectory, metadata,
          metadata.getHint("cik"), metadata.getHint("form"),
          metadata.getHint("filingDate"), metadata.getHint("accession"));
    }
    return convertInternal(sourcePath, targetDirectory, metadata);
  }

  /**
   * Overloaded convertInternal that accepts explicit metadata for non-XBRL filings (e.g. 8-K).
   * Passes overrides via metadata hints (thread-safe, no mutable instance state).
   */
  public List<String> convertInternal(String sourceFilePath, String targetDirectoryPath,
      ConversionMetadata metadata, String cik, String filingType, String filingDate,
      String accession) throws IOException {
    if (metadata == null) {
      metadata = new ConversionMetadata(targetDirectoryPath);
    }
    if (cik != null) {
      metadata.setHint("cik", cik);
    }
    if (filingType != null) {
      metadata.setHint("form", filingType);
    }
    if (filingDate != null) {
      metadata.setHint("filingDate", filingDate);
    }
    if (accession != null) {
      metadata.setHint("accession", accession);
    }
    return convertInternal(sourceFilePath, targetDirectoryPath, metadata);
  }

  /**
   * Converts XBRL/HTML file to Parquet format using String paths (S3-compatible).
   *
   * @param sourceFilePath Path to source XBRL/HTML file (can be local or S3 path)
   * @param targetDirectoryPath Path to target directory (can be local or S3 URI)
   * @param metadata Conversion metadata tracker
   * @return List of created file paths (supports S3 URIs)
   * @throws IOException If conversion fails
   */
  public List<String> convertInternal(String sourceFilePath, String targetDirectoryPath,
      ConversionMetadata metadata) throws IOException {
    LOGGER.debug(" XbrlToParquetConverter.convert() START for file: " + sourceFilePath);
    List<String> outputFiles = new ArrayList<>();

    // Extract accession from metadata if available
    String accession = null;
    if (metadata != null && metadata.getAllConversions() != null) {
      for (ConversionMetadata.ConversionRecord record : metadata.getAllConversions().values()) {
        if (sourceFilePath.equals(record.originalFile)) {
          // We stored accession in sourceFile field
          accession = record.sourceFile;
          break;
        }
      }
    }

    // Fallback: extract accession from source path if not in metadata
    // Path pattern: s3://bucket/sec/{cik}/{accession}/{filename} or /path/sec/{cik}/{accession}/{filename}
    if (accession == null || accession.isEmpty()) {
      accession = extractAccessionFromPath(sourceFilePath);
    }

    // Extract filename from path (works for both local and S3 paths)
    String fileName = sourceFilePath.substring(sourceFilePath.lastIndexOf('/') + 1).toLowerCase();

    try {
      // Fast path: 8-K filings are plain HTML — skip all XBRL parsing attempts
      String hintFilingType = metadata != null ? metadata.getHint("form") : null;
      if (hintFilingType != null && is8KFiling(hintFilingType)) {
        LOGGER.debug("8-K filing detected, skipping XBRL parsing: {}", fileName);
        return process8KHtml(sourceFilePath, targetDirectoryPath, metadata);
      }

      // Fast path: 13F-HR filings have structured XML information tables
      if (hintFilingType != null && is13FFiling(hintFilingType)) {
        LOGGER.debug("13F filing detected: {}", fileName);
        return process13FForm(sourceFilePath, targetDirectoryPath, metadata);
      }

      // Fast path: SC 13D/G filings are HTML with beneficial ownership data
      if (hintFilingType != null && is13DGFiling(hintFilingType)) {
        LOGGER.debug("13D/G filing detected: {}", fileName);
        return process13DGForm(sourceFilePath, targetDirectoryPath, metadata);
      }

      // Fast path: Forms 3/4/5 are XML ownership documents — parse directly as XML
      // bypassing the HTML/inline-XBRL detection path which would fail on xslF345X HTML files
      if (hintFilingType != null && isInsiderForm(null, hintFilingType)) {
        LOGGER.info("Insider form {} detected, parsing XML directly: {}", hintFilingType, fileName);
        return processInsiderXmlForm(sourceFilePath, targetDirectoryPath, metadata);
      }

      Document doc = null;
      boolean isInlineXbrl = false;

      // Check if it's an HTML file (potential inline XBRL)
      if (fileName.endsWith(".htm") || fileName.endsWith(".html")) {
        // Try to parse as inline XBRL (post-2019 filings embed ix: tags in HTML)
        doc = parseInlineXbrl(sourceFilePath);
        if (doc != null) {
          isInlineXbrl = true;
          LOGGER.debug("Successfully parsed inline XBRL from HTML: {}", fileName);
        } else {
          // Pre-2019 filings: HTML has no inline XBRL — look for companion .xml
          LOGGER.debug("No inline XBRL in {}; searching for companion XBRL instance",
              fileName);
          String xbrlPath = findCompanionXbrlFile(sourceFilePath);
          if (xbrlPath != null) {
            LOGGER.info("Found companion XBRL instance: {}",
                xbrlPath.substring(xbrlPath.lastIndexOf('/') + 1));
            try {
              DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
              factory.setNamespaceAware(true);
              DocumentBuilder builder = factory.newDocumentBuilder();
              try (InputStream is = sanitizeXmlStream(
                  storageProvider.openInputStream(xbrlPath))) {
                doc = builder.parse(is);
              }
            } catch (org.xml.sax.SAXParseException e) {
              LOGGER.info("Strict XML parse failed for {}: {} — falling back to JSoup",
                  xbrlPath.substring(xbrlPath.lastIndexOf('/') + 1), e.getMessage());
              doc = parseWithJsoupFallback(xbrlPath);
            }
          }
        }
      }

      // If not HTML, try traditional XBRL/XML parsing
      if (doc == null && !fileName.endsWith(".htm") && !fileName.endsWith(".html")) {
        try {
          DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
          factory.setNamespaceAware(true);
          DocumentBuilder builder = factory.newDocumentBuilder();
          try (InputStream is = sanitizeXmlStream(
              storageProvider.openInputStream(sourceFilePath))) {
            doc = builder.parse(is);
          }
        } catch (org.xml.sax.SAXParseException e) {
          // Malformed XML (unclosed tags, HTML entities, etc.) — fall back to JSoup
          LOGGER.info("Strict XML parse failed for {}: {} — falling back to JSoup",
              fileName, e.getMessage());
          doc = parseWithJsoupFallback(sourceFilePath);
        }
      }

      // If all parsing attempts failed, return empty
      if (doc == null) {
        LOGGER.warn("No XBRL data found for: {}", fileName);
        return outputFiles;
      }

      // Debug: Check document structure after successful parsing
      LOGGER.debug(" Document parsing succeeded for: " + fileName);
      LOGGER.debug(" Document has root element: " + (doc.getDocumentElement() != null ? doc.getDocumentElement().getNodeName() : "null"));
      LOGGER.debug(" Document child nodes count: " + doc.getChildNodes().getLength());
      if (doc.getDocumentElement() != null) {
        LOGGER.debug(" Root element has child nodes count: " + doc.getDocumentElement().getChildNodes().getLength());
      }

      // Extract filing metadata
      String cik = extractCik(doc, sourceFilePath);
      String filingType = extractFilingType(doc, sourceFilePath);
      String periodEndDate = extractPeriodEndDate(doc, sourceFilePath);

      // Compute actual filing date: prefer SEC submission date from EDGAR (metadata hint),
      // fall back to period end date extracted from document for standalone conversion
      String hintFilingDate = metadata != null ? metadata.getHint("filingDate") : null;
      String actualFilingDate = (hintFilingDate != null) ? hintFilingDate : periodEndDate;

      LOGGER.debug(" Extracted metadata for " + fileName + " - CIK: " + cik + ", Filing Type: " + filingType
          + ", Period End: " + periodEndDate + ", Filing Date: " + actualFilingDate);

      // Skip conversion if we couldn't extract required metadata
      if (cik == null || cik.equals("0000000000")) {
        LOGGER.warn("DEBUG: Skipping conversion due to invalid or missing CIK: " + fileName + " (extracted CIK: " + cik + ")");
        return outputFiles; // Return empty list
      }

      // Validate period end date - must be present (needed for fiscal year logic)
      if (periodEndDate == null) {
        LOGGER.warn("DEBUG: Skipping conversion - could not extract period end date from: " + fileName);
        return outputFiles; // Skip conversion
      }

      // Validate period end date format and year
      if (periodEndDate.length() >= 4) {
        try {
          int year = Integer.parseInt(periodEndDate.substring(0, 4));
          if (year < 1934 || year > java.time.Year.now().getValue()) {
            LOGGER.warn("Invalid year " + year + " in period end date " + periodEndDate + " for " + fileName);
            return outputFiles; // Skip conversion
          }
        } catch (NumberFormatException e) {
          LOGGER.warn("Invalid period end date format: " + periodEndDate + " for " + fileName);
          return outputFiles; // Skip conversion
        }
      } else {
        LOGGER.warn("Period end date too short: " + periodEndDate + " for " + fileName);
        return outputFiles; // Skip conversion
      }

      // Check if this is a Form 3, 4, or 5 (insider trading forms)
      if (isInsiderForm(doc, filingType)) {
        LOGGER.debug("Processing as insider form: " + fileName);
        return convertInsiderForm(doc, sourceFilePath, targetDirectoryPath, cik, filingType, actualFilingDate, accession);
      }

      LOGGER.debug("Not an insider form, proceeding to facts extraction: " + fileName);

      // Check if this is an 8-K filing with potential earnings exhibits
      if (is8KFiling(filingType)) {
        List<String> extraFiles = extract8KExhibits(sourceFilePath, targetDirectoryPath, cik, filingType, actualFilingDate, accession, false);
        outputFiles.addAll(extraFiles);
      }

      // Create partitioned output path - BUILD RELATIVE PATHS
      // Year-only partitioning for optimal 128MB file sizes
      String partitionYear = getPartitionYear(filingType, actualFilingDate, periodEndDate, doc);

      // Build RELATIVE partition path (relative to targetDirectoryPath which already includes source=sec)
      // Uses year-only partitioning - CIK/filing_type filtering done via Parquet/Iceberg statistics
      String relativePartitionPath = String.format("year=%s", partitionYear);

      // For local filesystem only, create actual directories (S3 doesn't need directory creation)
      if (!targetDirectoryPath.contains("://")) {
        java.nio.file.Path partitionPath = Paths.get(targetDirectoryPath, relativePartitionPath);
        Files.createDirectories(partitionPath);
      }

      // Use accession for file naming if available, otherwise fall back to filing date
      String uniqueId = (accession != null && !accession.isEmpty()) ? accession : actualFilingDate;

      // Build FULL paths for all outputs (StorageProvider needs absolute paths)
      String factsPath = storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_facts.parquet", cik, uniqueId));
      String metadataPath = storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_metadata.parquet", cik, uniqueId));
      String contextsPath = storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_contexts.parquet", cik, uniqueId));
      String mdaPath = storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_mda.parquet", cik, uniqueId));
      String relationshipsPath = storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_relationships.parquet", cik, uniqueId));

      // Convert financial facts to Parquet (returns extracted data for reuse by vectorization)
      List<Map<String, Object>> factsData;
      LOGGER.debug(" Starting facts.parquet generation for: " + fileName + " -> " + factsPath);
      try {
        factsData = writeFactsToParquet(doc, factsPath, cik, filingType, actualFilingDate, accession, sourceFilePath);
        // Paths are already full paths from storageProvider.resolvePath()
        outputFiles.add(factsPath);
        LOGGER.debug(" Successfully created facts.parquet: " + factsPath);
      } catch (Exception e) {
        LOGGER.error("Exception during facts.parquet creation for {}: {}", fileName, e.getMessage());
        throw e;
      }

      // Write filing metadata
      writeMetadataToParquet(doc, metadataPath, cik, filingType, actualFilingDate, accession, sourceFilePath);
      outputFiles.add(metadataPath);

      // Convert contexts to Parquet
      writeContextsToParquet(doc, contextsPath, cik, filingType, actualFilingDate, accession);
      outputFiles.add(contextsPath);

      // Extract MD&A ONCE and use for both mda_sections and vectorized_chunks
      List<Map<String, Object>> mdaData = extractMDAData(doc, cik, filingType, actualFilingDate, accession, sourceFilePath);
      writeMDAToParquetFromData(mdaData, mdaPath);
      outputFiles.add(mdaPath);

      // Extract and write XBRL relationships
      LOGGER.debug(" Starting relationships.parquet generation for: " + fileName + " -> " + relationshipsPath);
      try {
        writeRelationshipsToParquet(doc, relationshipsPath, cik, accession, filingType, actualFilingDate, sourceFilePath);
        outputFiles.add(relationshipsPath);
        LOGGER.debug(" Successfully created relationships.parquet: " + relationshipsPath);
      } catch (Exception e) {
        LOGGER.error("Exception during relationships.parquet creation for {}: {}", fileName, e.getMessage());
        throw e;
      }

      // Create vectorized chunks with contextual enrichment if enabled
      // Uses the SAME mdaData extracted above for consistency
      if (enableVectorization) {
        String chunksPath =
            storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_chunks.parquet", cik, uniqueId));
        writeVectorizedChunksToParquet(doc, chunksPath, cik, filingType, actualFilingDate, sourceFilePath, mdaData, factsData);
        outputFiles.add(chunksPath);
      }

      // Metadata is updated by FileConversionManager after successful conversion

      LOGGER.info("Converted XBRL to Parquet: " + fileName +
          " -> " + outputFiles.size() + " files");

    } catch (Exception e) {
      throw new IOException("Failed to convert XBRL to Parquet", e);
    }

    return outputFiles;
  }

  private String extractCik(Document doc, String sourcePath) {
    // For ownership documents (Form 3/4/5), extract issuerCik
    NodeList issuerCiks = doc.getElementsByTagName("issuerCik");
    if (issuerCiks.getLength() > 0) {
      String cik = issuerCiks.item(0).getTextContent().trim();
      // Pad to 10 digits
      while (cik.length() < 10) {
        cik = "0" + cik;
      }
      return cik;
    }

    // Try to extract from XBRL context
    NodeList contexts = doc.getElementsByTagNameNS("*", "context");
    for (int i = 0; i < contexts.getLength(); i++) {
      Element context = (Element) contexts.item(i);
      NodeList identifiers = context.getElementsByTagNameNS("*", "identifier");
      if (identifiers.getLength() > 0) {
        String identifier = identifiers.item(0).getTextContent();
        if (identifier.matches("\\d{10}")) {
          return identifier;
        }
      }
    }

    // Fall back to directory structure parsing
    // If file is in structure: /CIK/ACCESSION/file.xml
    String parentPath = sourcePath.substring(0, sourcePath.lastIndexOf('/'));
    if (!parentPath.isEmpty()) {
      String grandparentPath = parentPath.substring(0, parentPath.lastIndexOf('/'));
      if (!grandparentPath.isEmpty()) {
        String dirName = grandparentPath.substring(grandparentPath.lastIndexOf('/') + 1);
        if (dirName.matches("\\d+")) {
          // Pad to 10 digits
          while (dirName.length() < 10) {
            dirName = "0" + dirName;
          }
          return dirName;
        }
      }
    }

    // Fall back to filename parsing
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    if (filename.matches("\\d{10}_.*")) {
      return filename.substring(0, 10);
    }

    // Last resort - but log warning
    LOGGER.warn("Could not extract CIK from " + filename + ", skipping conversion");
    return null; // Return null to indicate failure
  }

  private String extractFilingType(Document doc, String sourcePath) {
    // For ownership documents (Form 3/4/5), extract documentType
    NodeList docTypes = doc.getElementsByTagName("documentType");
    if (docTypes.getLength() > 0) {
      String docType = docTypes.item(0).getTextContent().trim();
      // Return just the number for forms 3, 4, 5
      if (docType.equals("3") || docType.equals("4") || docType.equals("5")) {
        return docType;
      }
      if (docType.startsWith("3/") || docType.startsWith("4/") || docType.startsWith("5/")) {
        return docType.substring(0, 1); // Just return "3", "4", or "5"
      }
    }

    // Try to extract from document type - check both with and without namespace
    NodeList documentTypes = doc.getElementsByTagNameNS("*", "DocumentType");
    if (documentTypes.getLength() > 0) {
      String docType = documentTypes.item(0).getTextContent().trim();
      // Normalize the filing type (remove hyphens for consistency)
      return docType.replace("-", "");
    }

    // Also check for dei:DocumentType (common in inline XBRL)
    NodeList deiDocTypes = doc.getElementsByTagName("dei:DocumentType");
    if (deiDocTypes.getLength() > 0) {
      String docType = deiDocTypes.item(0).getTextContent().trim();
      return docType.replace("-", "");
    }

    // Also check with ix: prefix for inline XBRL (note the capital N in nonNumeric)
    NodeList ixDocTypes = doc.getElementsByTagName("ix:nonNumeric");
    for (int i = 0; i < ixDocTypes.getLength(); i++) {
      Element elem = (Element) ixDocTypes.item(i);
      if ("dei:DocumentType".equals(elem.getAttribute("name"))) {
        String docType = elem.getTextContent().trim();
        return docType.replace("-", "");
      }
    }

    // Fall back to filename parsing
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    if (filename.contains("10K") || filename.contains("10-K")) return "10K";
    if (filename.contains("10Q") || filename.contains("10-Q")) return "10Q";
    if (filename.contains("8K") || filename.contains("8-K")) return "8K";

    return "UNKNOWN";
  }

  /**
   * Extract date from inline XBRL content using regex patterns.
   * This is a fallback for complex inline XBRL structures.
   */
  private String extractDateFromInlineXBRL(String htmlPath) {
    try {
      String content;
      try (InputStream is = storageProvider.openInputStream(htmlPath)) {
        content = new String(readAllBytes(is), java.nio.charset.StandardCharsets.UTF_8);
      }

      // Look for xbrli:endDate patterns directly in the content
      Pattern endDatePattern = Pattern.compile("<xbrli:endDate>(\\d{4}-\\d{2}-\\d{2})</xbrli:endDate>");
      Matcher matcher = endDatePattern.matcher(content);

      String latestDate = null;
      while (matcher.find()) {
        String date = matcher.group(1);
        if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
          // Keep track of the latest date
          if (latestDate == null || date.compareTo(latestDate) > 0) {
            latestDate = date;
          }
        }
      }

      if (latestDate != null) {
        return latestDate;
      }

      // Try instant dates as well
      Pattern instantPattern = Pattern.compile("<xbrli:instant>(\\d{4}-\\d{2}-\\d{2})</xbrli:instant>");
      matcher = instantPattern.matcher(content);

      while (matcher.find()) {
        String date = matcher.group(1);
        if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
          if (latestDate == null || date.compareTo(latestDate) > 0) {
            latestDate = date;
          }
        }
      }

      return latestDate;

    } catch (IOException e) {
      LOGGER.debug("Failed to extract date from inline XBRL: " + e.getMessage());
    }

    return null;
  }

  /**
   * Extract date from HTML filing metadata.
   */
  private String extractDateFromHTML(String htmlPath) {
    try {
      org.jsoup.nodes.Document doc;
      try (InputStream is = storageProvider.openInputStream(htmlPath)) {
        doc = Jsoup.parse(is, "UTF-8", "");
      }

      // Look for SEC header metadata
      // Pattern: "CONFORMED PERIOD OF REPORT: 20240930"
      Elements elements = doc.getElementsMatchingOwnText("CONFORMED PERIOD OF REPORT:");
      for (org.jsoup.nodes.Element elem : elements) {
        String text = elem.text();
        Pattern pattern = Pattern.compile("CONFORMED PERIOD OF REPORT:\\s*(\\d{8})");
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) {
          String dateStr = matcher.group(1);
          // Convert YYYYMMDD to YYYY-MM-DD
          if (dateStr.length() == 8) {
            return dateStr.substring(0, 4) + "-" + dateStr.substring(4, 6) + "-" + dateStr.substring(6, 8);
          }
        }
      }

      // Look for FILED AS OF DATE pattern
      elements = doc.getElementsMatchingOwnText("FILED AS OF DATE:");
      for (org.jsoup.nodes.Element elem : elements) {
        String text = elem.text();
        Pattern pattern = Pattern.compile("FILED AS OF DATE:\\s*(\\d{8})");
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) {
          String dateStr = matcher.group(1);
          // Convert YYYYMMDD to YYYY-MM-DD
          if (dateStr.length() == 8) {
            return dateStr.substring(0, 4) + "-" + dateStr.substring(4, 6) + "-" + dateStr.substring(6, 8);
          }
        }
      }

      // Look for inline XBRL elements with date
      elements = doc.select("[name*=DocumentPeriodEndDate], [name*=PeriodEndDate]");
      for (org.jsoup.nodes.Element elem : elements) {
        String date = elem.text().trim();
        if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
          return date;
        }
        // Handle format like "Sep. 30, 2024" or "September 30, 2024"
        if (date.matches("\\w+\\.?\\s+\\d{1,2},\\s+\\d{4}")) {
          try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("[MMMM][MMM][.][ ]d, yyyy", Locale.ENGLISH);
            LocalDate parsedDate = LocalDate.parse(date.replaceAll("\\.", ""), formatter);
            return parsedDate.format(DateTimeFormatter.ISO_LOCAL_DATE);
          } catch (Exception e) {
            // Ignore parse errors
          }
        }
      }

    } catch (IOException e) {
      LOGGER.warn("Failed to parse HTML file for date extraction: " + e.getMessage());
    }

    return null;
  }

  private String extractPeriodEndDate(Document doc, String sourcePath) {
    // For ownership documents (Form 3/4/5), extract periodOfReport
    NodeList periodOfReports = doc.getElementsByTagName("periodOfReport");
    if (periodOfReports.getLength() > 0) {
      String date = periodOfReports.item(0).getTextContent().trim();
      // Validate date format (should be YYYY-MM-DD); also strip timezone offset (e.g. 2026-03-18-05:00)
      if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
        return date;
      }
      if (date.length() > 10 && date.substring(0, 10).matches("\\d{4}-\\d{2}-\\d{2}")) {
        return date.substring(0, 10);
      }
      // Normalize compact YYYYMMDD format (used by some older SEC filings)
      if (date.matches("\\d{8}")) {
        return date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6, 8);
      }
    }

    // Try to extract from document period end date (standard XBRL)
    NodeList periodEnds = doc.getElementsByTagNameNS("*", "DocumentPeriodEndDate");
    if (periodEnds.getLength() > 0) {
      String date = periodEnds.item(0).getTextContent().trim();
      if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
        return date;
      }
    }

    // Try to extract from dei:DocumentPeriodEndDate (inline XBRL)
    NodeList deiPeriodEnds = doc.getElementsByTagNameNS("*", "dei:DocumentPeriodEndDate");
    if (deiPeriodEnds.getLength() == 0) {
      // Try without namespace prefix
      deiPeriodEnds = doc.getElementsByTagName("dei:DocumentPeriodEndDate");
    }
    if (deiPeriodEnds.getLength() > 0) {
      String date = deiPeriodEnds.item(0).getTextContent().trim();
      if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
        return date;
      }
    }

    // Try filename-based date extraction before context scan.
    // SEC EDGAR mandates inline XBRL filenames as {ticker}-{YYYYMMDD}.htm
    // where the date is the period end date. This is more reliable than
    // scanning XBRL contexts which can contain forward-looking dates
    // (lease maturities, debt maturities, pension horizons, etc.)
    String filenameFromPath = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    String filenameDateStr = extractFilingDateFromFilename(filenameFromPath);
    // extractFilingDateFromFilename returns "2024-01-01" as a hardcoded default — only use
    // it if it actually matched a date in the filename (not the default)
    boolean hasFilenameDate = filenameDateStr != null
        && !filenameDateStr.equals("2024-01-01");
    if (hasFilenameDate) {
      LOGGER.debug("Extracted period end date from filename: " + filenameDateStr);
      return filenameDateStr;
    }

    // Try to extract from XBRL contexts - check both xbrli:context and context elements.
    // Cap at current year to avoid picking up forward-looking dates from XBRL contexts
    // (e.g., lease/debt maturities extending years into the future).
    int maxYear = java.time.Year.now().getValue();
    NodeList contexts = doc.getElementsByTagName("xbrli:context");
    if (contexts.getLength() == 0) {
      contexts = doc.getElementsByTagNameNS("*", "context");
    }
    if (contexts.getLength() == 0) {
      contexts = doc.getElementsByTagName("context");
    }

    // Look for the latest valid period end date in contexts, capped at current year
    String latestDate = null;
    for (int i = 0; i < contexts.getLength(); i++) {
      Node context = contexts.item(i);

      // Check for xbrli:endDate elements (common in inline XBRL like DEF 14A)
      NodeList endDates = ((Element) context).getElementsByTagName("xbrli:endDate");
      if (endDates.getLength() == 0) {
        endDates = ((Element) context).getElementsByTagNameNS("*", "endDate");
      }
      if (endDates.getLength() > 0) {
        String date = endDates.item(0).getTextContent().trim();
        if (date.matches("\\d{4}-\\d{2}-\\d{2}")
            && Integer.parseInt(date.substring(0, 4)) <= maxYear) {
          if (latestDate == null || date.compareTo(latestDate) > 0) {
            latestDate = date;
          }
        }
      }

      // Also check for instant dates
      NodeList instants = ((Element) context).getElementsByTagName("xbrli:instant");
      if (instants.getLength() == 0) {
        instants = ((Element) context).getElementsByTagNameNS("*", "instant");
      }
      if (instants.getLength() > 0) {
        String date = instants.item(0).getTextContent().trim();
        if (date.matches("\\d{4}-\\d{2}-\\d{2}")
            && Integer.parseInt(date.substring(0, 4)) <= maxYear) {
          if (latestDate == null || date.compareTo(latestDate) > 0) {
            latestDate = date;
          }
        }
      }
    }

    // Return the latest date found from contexts
    if (latestDate != null) {
      return latestDate;
    }

    // For HTML/inline XBRL files, try to parse from HTML metadata
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    if (filename.endsWith(".htm") || filename.endsWith(".html")) {
      String htmlDate = extractDateFromHTML(sourcePath);
      if (htmlDate != null) {
        return htmlDate;
      }
    }

    // Try to extract date from inline XBRL directly from file content as last resort
    if (filename.endsWith(".htm") || filename.endsWith(".html")) {
      String inlineDate = extractDateFromInlineXBRL(sourcePath);
      if (inlineDate != null) {
        return inlineDate;
      }
    }

    // Insider form fallback: use latest transactionDate or signatureDate across all transactions.
    // Forms 3/4/5 may lack periodOfReport or XBRL metadata but always have transaction/signature dates.
    String insiderFallback = extractInsiderFormPeriodDate(doc);
    if (insiderFallback != null) {
      LOGGER.debug("Extracted period end date from insider transaction/signature date: " + insiderFallback);
      return insiderFallback;
    }

    // Return null if no date found - let the caller handle this
    LOGGER.warn("Could not extract period end date from: " + filename);
    return null;
  }

  private String extractInsiderFormPeriodDate(Document doc) {
    String latest = null;
    // Scan nonDerivativeTransaction, derivativeTransaction, nonDerivativeHolding, derivativeHolding
    String[] txnTags = {"nonDerivativeTransaction", "derivativeTransaction",
        "nonDerivativeHolding", "derivativeHolding"};
    for (String tag : txnTags) {
      NodeList nodes = doc.getElementsByTagName(tag);
      for (int i = 0; i < nodes.getLength(); i++) {
        Element el = (Element) nodes.item(i);
        String date = getElementText(el, "transactionDate", "value");
        if (date != null && date.matches("\\d{4}-\\d{2}-\\d{2}")) {
          if (latest == null || date.compareTo(latest) > 0) {
            latest = date;
          }
        }
        // YYYYMMDD compact format
        if (date != null && date.matches("\\d{8}")) {
          String normalized = date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6, 8);
          if (latest == null || normalized.compareTo(latest) > 0) {
            latest = normalized;
          }
        }
      }
    }
    if (latest != null) {
      return latest;
    }
    // Last resort: signatureDate on the ownershipDocument
    NodeList sigDates = doc.getElementsByTagName("signatureDate");
    for (int i = 0; i < sigDates.getLength(); i++) {
      String date = sigDates.item(i).getTextContent().trim();
      // Strip timezone offset (e.g. 2026-03-18-05:00 → 2026-03-18)
      if (date.length() > 10 && date.substring(0, 10).matches("\\d{4}-\\d{2}-\\d{2}")) {
        date = date.substring(0, 10);
      }
      if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
        if (latest == null || date.compareTo(latest) > 0) {
          latest = date;
        }
      }
      if (date.matches("\\d{8}")) {
        String normalized = date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6, 8);
        if (latest == null || normalized.compareTo(latest) > 0) {
          latest = normalized;
        }
      }
    }
    return latest;
  }

  private List<Map<String, Object>> writeFactsToParquet(Document doc, String outputPath,
      String cik, String filingType, String filingDate, String accession, String sourcePath) throws IOException {

    LOGGER.debug("writeFactsToParquet called for " + outputPath +
                " (CIK: " + cik + ", Type: " + filingType + ", Date: " + filingDate + ")");

    // Load column metadata from sec-schema.json
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractSecDataDownloader.loadTableColumns("financial_line_items");

    // Extract all fact elements
    List<Map<String, Object>> dataList = new ArrayList<>();
    NodeList allElements = doc.getElementsByTagName("*");

    LOGGER.debug("Found " + allElements.getLength() + " total XML elements in document");

    int elementsWithContextRef = 0;

    for (int i = 0; i < allElements.getLength(); i++) {
      Element element = (Element) allElements.item(i);

      // Identify potential fact elements more broadly
      // Original approach: only elements with contextRef
      boolean isFactElement = element.hasAttribute("contextRef");

      // Enhanced approach for 2024-01-01 filings: also check elements that look like financial facts
      if (!isFactElement) {
        // Check if element has financial-related attributes or naming patterns
        String tagName = element.getTagName();
        String localName = element.getLocalName();

        // Look for XBRL fact patterns without contextRef
        if (tagName != null && (
            // Common financial concept namespaces
            tagName.contains("us-gaap:") ||
            tagName.contains("dei:") ||
            tagName.contains("gaap:") ||
            // Or has numeric content that could be a fact
            (element.getTextContent() != null &&
             element.getTextContent().trim().matches("^[0-9,.-]+$")) ||
            // Or has a 'name' attribute (inline XBRL without contextRef)
            element.hasAttribute("name"))) {
          isFactElement = true;
        }
      }

      if (isFactElement) {
        if (element.hasAttribute("contextRef")) {
          elementsWithContextRef++;
        }

        Map<String, Object> data = new HashMap<>();
        // Required data columns
        data.put("cik", cik);
        data.put("accession_number", accession != null ? accession : cik + "-" + filingDate);
        data.put("filing_date", filingDate);

        // Extract year for Iceberg partitioning
        int year = 0;
        if (filingDate != null && filingDate.length() >= 4) {
          try {
            year = Integer.parseInt(filingDate.substring(0, 4));
          } catch (NumberFormatException e) {
            LOGGER.warn("Failed to parse year from filing date: {}", filingDate);
          }
        }
        data.put("year", year);

        // For inline XBRL, concept is in 'name' attribute; for regular XBRL, it's the element name
        String concept;
        if (element.hasAttribute("name")) {
          // Inline XBRL: concept is in the 'name' attribute
          concept = element.getAttribute("name");
          // Remove namespace prefix if present (e.g., "us-gaap:NetIncomeLoss" -> "NetIncomeLoss")
          if (concept != null && concept.contains(":")) {
            concept = concept.substring(concept.indexOf(":") + 1);
          }
        } else {
          // Regular XBRL: concept is the element's local name or tag name
          concept = element.getLocalName();
          if (concept == null) {
            concept = element.getTagName();
            // Remove namespace prefix if present
            if (concept != null && concept.contains(":")) {
              concept = concept.substring(concept.indexOf(":") + 1);
            }
          }
        }

        // Skip if concept is null or empty
        if (concept == null || concept.isEmpty()) {
          continue;
        }
        data.put("concept", concept);
        data.put("context_ref", element.getAttribute("contextRef"));
        data.put("unit_ref", element.getAttribute("unitRef"));

        // Get raw text content
        String rawValue = element.getTextContent().trim();

        // For TextBlocks and narrative content, preserve more formatting
        boolean isTextBlock =
                             concept != null && (concept.contains("TextBlock") ||
                             concept.contains("Disclosure") ||
                             concept.contains("Policy"));

        String cleanValue;
        String fullText = null;

        if (isTextBlock) {
          // For text blocks, preserve paragraph structure
          fullText = preserveTextBlockFormatting(rawValue);
          // Still provide a shorter cleaned version for the value field
          cleanValue = extractFirstParagraph(fullText);
        } else {
          // For regular facts, clean normally
          cleanValue = cleanHtmlText(rawValue);
        }

        data.put("value", cleanValue);
        data.put("full_text", fullText);

        // Extract footnote references (e.g., "See Note 14")
        String footnoteRefs = extractFootnoteReferences(rawValue);
        data.put("footnote_refs", footnoteRefs);

        // Store element ID for relationship tracking
        // Generate synthetic ID if element lacks one (required for primary key)
        String elementId = element.getAttribute("id");
        if (elementId == null || elementId.isEmpty()) {
          // Generate synthetic ID from concept + context_ref + hash
          String conceptName = concept != null ? concept : "";
          String ctxRef = element.getAttribute("contextRef");
          ctxRef = ctxRef != null ? ctxRef : "";
          elementId = "gen_" + Math.abs((conceptName + "_" + ctxRef + "_" + dataList.size()).hashCode());
        }
        data.put("element_id", elementId);

        // Try to parse as numeric (using cleaned value)
        try {
          double numValue =
              Double.parseDouble(cleanValue.replaceAll(",", ""));
          data.put("numeric_value", numValue);
        } catch (NumberFormatException e) {
          data.put("numeric_value", null);
        }

        // Set period_start and period_end to null for now (will be populated from contexts)
        data.put("period_start", null);
        data.put("period_end", null);
        data.put("is_instant", false);

        dataList.add(data);
      }
    }

    LOGGER.debug("Found " + elementsWithContextRef + " elements with contextRef attributes");
    LOGGER.debug("Extracted " + dataList.size() + " valid fact records (including enhanced extraction for elements without contextRef)");

    if (dataList.isEmpty()) {
      LOGGER.warn("No facts extracted from XBRL document for " + filingDate + " despite enhanced extraction - document may not contain recognizable financial facts");
      LOGGER.warn("DEBUG: Document analysis - total elements: " + allElements.getLength() +
                     ", elements with contextRef: " + elementsWithContextRef);

      // Additional diagnostic info for inline XBRL documents
      NodeList ixElements = doc.getElementsByTagNameNS("http://www.xbrl.org/2013/inlineXBRL", "*");
      NodeList usGaapElements = doc.getElementsByTagName("*");
      int usGaapCount = 0;
      int deiCount = 0;
      for (int i = 0; i < usGaapElements.getLength(); i++) {
        Element elem = (Element) usGaapElements.item(i);
        String tagName = elem.getTagName();
        if (tagName.contains("us-gaap:")) usGaapCount++;
        if (tagName.contains("dei:")) deiCount++;
      }

      LOGGER.warn("Inline XBRL elements: {}, us-gaap elements: {}, dei elements: {}",
          ixElements.getLength(), usGaapCount, deiCount);
    }

    // Use consolidated StorageProvider method for Parquet writing
    try {
      LOGGER.debug("Writing facts to parquet file: " + outputPath);
      LOGGER.debug(" About to write " + dataList.size() + " fact records using storageProvider.writeAvroParquet");

      // Check if we have any records to write
      if (dataList.isEmpty()) {
        LOGGER.warn("DEBUG: No fact records to write - creating empty parquet file for cache validation");
        // Still need to create the file for cache validation
      }

      storageProvider.writeAvroParquet(outputPath, columns, dataList, "XbrlFact", "XbrlFact");
      LOGGER.info("Successfully wrote " + dataList.size() + " facts to " + outputPath);

    } catch (Exception e) {
      LOGGER.error("Failed to write facts parquet file for {} (CIK: {}): {}", filingDate, cik, e.getMessage());
      throw new IOException("Failed to write facts to parquet: " + e.getMessage(), e);
    }
    return dataList;
  }

  private void writeMetadataToParquet(Document doc, String outputPath,
      String cik, String filingType, String filingDate, String accession, String sourcePath) throws IOException {

    // Load column metadata from sec-schema.json
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractSecDataDownloader.loadTableColumns("filing_metadata");

    List<Map<String, Object>> dataList = new ArrayList<>();
    Map<String, Object> data = new HashMap<>();

    // Use passed accession, or try to extract from filename, or generate fallback
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    String accessionNumber = accession;
    if (accessionNumber == null || accessionNumber.isEmpty()) {
      accessionNumber = extractAccessionNumber(filename);
    }
    if (accessionNumber == null || accessionNumber.isEmpty()) {
      // Generate fallback accession number from cik and filing date
      accessionNumber = cik + "-" + filingDate.replace("-", "").substring(2);
    }
    data.put("cik", cik);
    data.put("accession_number", accessionNumber);
    data.put("filing_type", filingType);
    data.put("filing_date", filingDate);

    // Extract year for Iceberg partitioning
    int year = 0;
    if (filingDate != null && filingDate.length() >= 4) {
      try {
        year = Integer.parseInt(filingDate.substring(0, 4));
      } catch (NumberFormatException e) {
        LOGGER.warn("Failed to parse year from filing date: {}", filingDate);
      }
    }
    data.put("year", year);

    // Extract company name — try DEI elements first, then form-specific elements
    String companyName = extractDeiValue(doc, "EntityRegistrantName", "RegistrantName");
    if (companyName == null) {
      // Form 4: <issuer><issuerName>
      companyName = getElementText(doc, "issuerName");
    }
    if (companyName == null) {
      // 13F: <filingManager><name> or <companyName>
      companyName = getElementText(doc, "filingManager");
      if (companyName == null) {
        companyName = getElementText(doc, "companyName");
      }
    }
    data.put("company_name", companyName);

    String stateOfIncorp =
                                           extractDeiValue(doc, "EntityIncorporationStateCountryCode", "StateOrCountryOfIncorporation");
    data.put("state_of_incorporation", stateOfIncorp);

    String fiscalYearEnd = extractDeiValue(doc, "CurrentFiscalYearEndDate", "FiscalYearEnd");
    data.put("fiscal_year_end", fiscalYearEnd);

    String businessAddress = extractDeiValue(doc, "EntityAddressAddressLine1", "BusinessAddress");
    data.put("business_address", businessAddress);

    String mailingAddress = extractDeiValue(doc, "EntityAddressMailingAddressLine1", "MailingAddress");
    data.put("mailing_address", mailingAddress);

    String phone = extractDeiValue(doc, "EntityPhoneNumber", "Phone");
    data.put("phone", phone);

    String docType = extractDeiValue(doc, "DocumentType", "FormType");
    data.put("document_type", docType);

    String periodEnd = extractDeiValue(doc, "DocumentPeriodEndDate", "PeriodEndDate");
    data.put("period_end_date", periodEnd);
    data.put("period_of_report", periodEnd);
    data.put("primary_document", filename);

    // Try to extract SIC code
    String sicStr = extractDeiValue(doc, "EntityStandardIndustrialClassificationCode", "SicCode");
    data.put("sic_code", sicStr);

    String irsNumber = extractDeiValue(doc, "EntityTaxIdentificationNumber", "IrsNumber");
    data.put("irs_number", irsNumber);

    // Set remaining fields to null/defaults
    data.put("acceptance_datetime", null);
    data.put("file_size", null);
    data.put("fiscal_year", null);

    dataList.add(data);

    // Use consolidated StorageProvider method for Parquet writing
    storageProvider.writeAvroParquet(outputPath, columns, dataList, "FilingMetadata", "FilingMetadata");
    LOGGER.info("Successfully wrote " + dataList.size() + " metadata records to " + outputPath);
  }

  private String extractDeiValue(Document doc, String... possibleTags) {
    // Try to find DEI elements in the XML document
    for (String tag : possibleTags) {
      // Try with dei: namespace
      NodeList nodes = doc.getElementsByTagNameNS("*", tag);
      if (nodes.getLength() > 0) {
        String text = nodes.item(0).getTextContent();
        if (text != null && !text.trim().isEmpty()) {
          return text.trim();
        }
      }

      // Try without namespace
      nodes = doc.getElementsByTagName(tag);
      if (nodes.getLength() > 0) {
        String text = nodes.item(0).getTextContent();
        if (text != null && !text.trim().isEmpty()) {
          return text.trim();
        }
      }

      // Try with dei: prefix
      nodes = doc.getElementsByTagName("dei:" + tag);
      if (nodes.getLength() > 0) {
        String text = nodes.item(0).getTextContent();
        if (text != null && !text.trim().isEmpty()) {
          return text.trim();
        }
      }
    }
    return null;
  }

  private String extractAccessionNumber(String filename) {
    // Extract accession number from filename like "0000320193-24-000123.xml"
    if (filename != null) {
      // Remove file extension
      String nameWithoutExt = filename.replaceAll("\\.[^.]+$", "");
      // Look for pattern with dashes
      if (nameWithoutExt.matches("\\d{10}-\\d{2}-\\d{6}")) {
        return nameWithoutExt;
      }
      // Try extracting from path segments
      String[] parts = nameWithoutExt.split("[/_]");
      for (String part : parts) {
        if (part.matches("\\d{10}-\\d{2}-\\d{6}")) {
          return part;
        }
      }
    }
    return null;
  }

  /**
   * Extract year from filing date string (YYYY-MM-DD format).
   * Returns 0 if parsing fails.
   */
  private int extractYearFromDate(String filingDate) {
    if (filingDate != null && filingDate.length() >= 4) {
      try {
        return Integer.parseInt(filingDate.substring(0, 4));
      } catch (NumberFormatException e) {
        LOGGER.warn("Failed to parse year from filing date: {}", filingDate);
      }
    }
    return 0;
  }

  private void writeContextsToParquet(Document doc, String outputPath,
      String cik, String filingType, String filingDate, String accession) throws IOException {

    // Load column metadata from sec-schema.json
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractSecDataDownloader.loadTableColumns("filing_contexts");

    // Extract context elements
    // Use getElementsByTagName (not getElementsByTagNameNS) because inline XBRL parsing
    // creates context elements without a namespace, and some DOM implementations don't
    // return null-namespace elements when using getElementsByTagNameNS("*", ...)
    List<Map<String, Object>> dataList = new ArrayList<>();
    NodeList contexts = doc.getElementsByTagName("context");

    for (int i = 0; i < contexts.getLength(); i++) {
      Element context = (Element) contexts.item(i);
      Map<String, Object> data = new HashMap<>();

      // Required data columns (cik and accession_number are in schema)
      data.put("cik", cik);
      data.put("accession_number", accession != null ? accession : cik + "-" + filingDate);
      data.put("filing_date", filingDate);

      // Extract year for Iceberg partitioning
      int year = 0;
      if (filingDate != null && filingDate.length() >= 4) {
        try {
          year = Integer.parseInt(filingDate.substring(0, 4));
        } catch (NumberFormatException e) {
          LOGGER.warn("Failed to parse year from filing date: {}", filingDate);
        }
      }
      data.put("year", year);
      data.put("context_id", context.getAttribute("id"));

      // Extract entity information
      // Use getElementsByTagName (not getElementsByTagNameNS) since inline XBRL parsing
      // creates elements without namespaces
      NodeList identifiers = context.getElementsByTagName("identifier");
      if (identifiers.getLength() > 0) {
        Element identifier = (Element) identifiers.item(0);
        data.put("entity_identifier", identifier.getTextContent());
        data.put("entity_scheme", identifier.getAttribute("scheme"));
      } else {
        // Fallback to CIK since entity_identifier is required (nullable: false)
        data.put("entity_identifier", cik);
        data.put("entity_scheme", "http://www.sec.gov/CIK");
      }

      // Extract period information
      NodeList startDates = context.getElementsByTagName("startDate");
      NodeList endDates = context.getElementsByTagName("endDate");
      NodeList instants = context.getElementsByTagName("instant");

      if (startDates.getLength() > 0) {
        data.put("period_start", startDates.item(0).getTextContent());
      } else {
        data.put("period_start", null);
      }
      if (endDates.getLength() > 0) {
        data.put("period_end", endDates.item(0).getTextContent());
      } else {
        data.put("period_end", null);
      }
      if (instants.getLength() > 0) {
        data.put("period_instant", instants.item(0).getTextContent());
      } else {
        data.put("period_instant", null);
      }

      // Set segment and scenario to null for now
      data.put("segment", null);
      data.put("scenario", null);

      dataList.add(data);
    }

    // Only write file if there's data - empty parquet files cause DuckDB union_by_name issues
    if (!dataList.isEmpty()) {
      storageProvider.writeAvroParquet(outputPath, columns, dataList, "XbrlContext", "XbrlContext");
      LOGGER.info("Successfully wrote " + dataList.size() + " context records to " + outputPath);
    } else {
      LOGGER.debug("Skipping empty contexts file: " + outputPath);
    }
  }

  @Override public String getSourceFormat() {
    return "xbrl";
  }

  @Override public String getTargetFormat() {
    return "parquet";
  }

  /**
   * Clean HTML tags and entities from text content.
   * This is essential for footnotes, MD&A, risk factors, and other narrative text in XBRL.
   */
  private String cleanHtmlText(String text) {
    if (text == null || text.isEmpty()) {
      return text;
    }

    // First, unescape HTML entities (&amp; &lt; &gt; &nbsp; etc.)
    String unescaped = StringEscapeUtils.unescapeHtml4(text);

    // Remove HTML tags
    String withoutTags = HTML_TAG_PATTERN.matcher(unescaped).replaceAll(" ");

    // Normalize whitespace (multiple spaces/tabs/newlines to single space)
    String normalized = WHITESPACE_PATTERN.matcher(withoutTags).replaceAll(" ");

    // Final trim
    return normalized.trim();
  }

  /**
   * Preserve formatting for TextBlock content while removing HTML.
   * Maintains paragraph breaks and list structure.
   */
  private String preserveTextBlockFormatting(String text) {
    if (text == null || text.isEmpty()) {
      return text;
    }

    // Parse HTML content if present
    if (text.contains("<") && text.contains(">")) {
      org.jsoup.nodes.Document doc = Jsoup.parseBodyFragment(text);

      // Convert <p> tags to double newlines
      doc.select("p").append("\n\n");

      // Convert <br> to single newline
      doc.select("br").append("\n");

      // Convert list items to bullet points
      doc.select("li").prepend("• ");

      // Get text with preserved structure
      String formatted = doc.text();

      // Clean up excessive newlines
      formatted = formatted.replaceAll("\n{3,}", "\n\n");

      return formatted.trim();
    }

    // If no HTML, just unescape entities
    return StringEscapeUtils.unescapeHtml4(text).trim();
  }

  /**
   * Extract first paragraph or first 500 chars for summary.
   */
  private String extractFirstParagraph(String text) {
    if (text == null || text.isEmpty()) {
      return text;
    }

    // Find first paragraph break
    int paragraphEnd = text.indexOf("\n\n");
    if (paragraphEnd > 0 && paragraphEnd < 500) {
      return text.substring(0, paragraphEnd).trim();
    }

    // Otherwise, return first 500 chars
    return text.length() > 500 ?
        text.substring(0, 497) + "..." :
        text;
  }

  /**
   * Extract footnote references from text.
   * Looks for patterns like "See Note 14", "(Note 3)", "Refer to Note 2", etc.
   */
  private String extractFootnoteReferences(String text) {
    if (text == null || text.isEmpty()) {
      return null;
    }

    Pattern footnotePattern =
        Pattern.compile("(?:See|Refer to|Reference|\\()?\\s*Note[s]?\\s+(\\d+[A-Za-z]?(?:\\s*[,&]\\s*\\d+[A-Za-z]?)*)",
        Pattern.CASE_INSENSITIVE);

    Matcher matcher = footnotePattern.matcher(text);
    Set<String> references = new HashSet<>();

    while (matcher.find()) {
      String noteRefs = matcher.group(1);
      // Split on comma or ampersand for multiple references
      String[] notes = noteRefs.split("[,&]");
      for (String note : notes) {
        references.add("Note " + note.trim());
      }
    }

    return references.isEmpty() ? null : String.join("; ", references);
  }

  /**
   * Finds inline XBRL elements (ix:nonNumeric, ix:nonFraction, etc.) in a Jsoup document.
   * Works with both XML-parsed and HTML-parsed documents.
   */
  private org.jsoup.select.Elements findInlineXbrlElements(
      org.jsoup.nodes.Document jsoupDoc) {
    org.jsoup.select.Elements ixElements = new org.jsoup.select.Elements();

    // Try CSS namespace selectors (work in XML mode)
    try {
      ixElements.addAll(jsoupDoc.select("ix|nonNumeric, ix|nonFraction"));
    } catch (Exception e) {
      // Pipe notation may not work in HTML parser mode
    }

    // Try lowercase variants
    if (ixElements.isEmpty()) {
      try {
        ixElements.addAll(jsoupDoc.select("ix|nonnumeric, ix|nonfraction"));
      } catch (Exception e) {
        // Expected in HTML parser mode
      }
    }

    // Scan all elements for ix: prefixed tags with contextRef (most robust)
    for (org.jsoup.nodes.Element elem : jsoupDoc.getAllElements()) {
      String tagName = elem.tagName().toLowerCase(Locale.ROOT);
      if (tagName.startsWith("ix:")
          && (elem.hasAttr("contextRef") || elem.hasAttr("contextref"))) {
        ixElements.add(elem);
      }
    }

    return ixElements;
  }

  /**
   * Finds and downloads a companion XBRL instance document (.xml) from SEC EDGAR.
   * Pre-2019 SEC filings store XBRL data in separate .xml files alongside the HTML.
   * Uses FilingSummary.xml (same approach as SecSchemaFactory) to identify the instance doc.
   */
  private String findCompanionXbrlFile(String htmlPath) {
    try {
      String cik = extractCikFromPath(htmlPath);
      String parentDir = htmlPath.substring(0, htmlPath.lastIndexOf('/'));
      String accessionDir =
          parentDir.substring(parentDir.lastIndexOf('/') + 1);

      if (cik == null || accessionDir.isEmpty()) {
        LOGGER.debug("Cannot extract CIK/accession from path: {}",
            htmlPath);
        return null;
      }

      String cikNumeric = cik.replaceFirst("^0+", "");
      String baseUrl = String.format(
          "https://www.sec.gov/Archives/edgar/data/%s/%s",
          cikNumeric, accessionDir);

      // Strategy 1: FilingSummary.xml (reliable when present)
      String xbrlFileName = null;
      String summaryXml = downloadFile(baseUrl + "/FilingSummary.xml");
      if (summaryXml != null) {
        xbrlFileName = parseXbrlFilenameFromSummary(summaryXml);
      }

      // Strategy 2: Parse filing index page for XBRL instance doc
      if (xbrlFileName == null) {
        String indexHtml = downloadFile(baseUrl + "/");
        if (indexHtml != null) {
          xbrlFileName = findXbrlInstanceInIndex(indexHtml);
        }
      }

      if (xbrlFileName == null) {
        LOGGER.debug("No companion XBRL found for {}/{}",
            cikNumeric, accessionDir);
        return null;
      }

      // Download the XBRL instance file from EDGAR
      LOGGER.info("Downloading companion XBRL: {}", xbrlFileName);
      String xbrlContent = downloadFile(baseUrl + "/" + xbrlFileName);
      if (xbrlContent == null) {
        LOGGER.warn("Failed to download XBRL file: {}", xbrlFileName);
        return null;
      }

      // Store alongside the HTML file via storageProvider
      String xbrlPath = storageProvider.resolvePath(
          parentDir, xbrlFileName);
      storageProvider.writeFile(xbrlPath,
          xbrlContent.getBytes(StandardCharsets.UTF_8));

      return xbrlPath;

    } catch (Exception e) {
      LOGGER.debug("Failed to find companion XBRL for {}: {}",
          htmlPath, e.getMessage());
      return null;
    }
  }

  /**
   * Parses an EDGAR filing index page to find the XBRL instance document.
   * Looks for .xml files with type "EX-101.INS" or similar XBRL patterns,
   * filtering out taxonomy linkbase files.
   */
  private String findXbrlInstanceInIndex(String indexHtml) {
    org.jsoup.nodes.Document doc = Jsoup.parse(indexHtml);
    org.jsoup.select.Elements rows = doc.select("table tr");

    String bestCandidate = null;

    for (org.jsoup.nodes.Element row : rows) {
      org.jsoup.select.Elements cells = row.select("td");
      if (cells.size() < 3) {
        continue;
      }

      // Find link and type text from cells
      String docType = "";
      String fileName = null;

      for (org.jsoup.nodes.Element cell : cells) {
        org.jsoup.nodes.Element link = cell.selectFirst("a");
        if (link != null && fileName == null) {
          String href = link.attr("href");
          if (href.endsWith(".xml") || href.endsWith(".htm")
              || href.endsWith(".txt")) {
            fileName = href.substring(
                href.lastIndexOf('/') + 1);
          }
        }
        String text = cell.text().trim().toLowerCase(Locale.ROOT);
        if (text.contains("ex-101") || text.contains("xbrl")) {
          docType = text;
        }
      }

      if (fileName == null || !fileName.endsWith(".xml")) {
        continue;
      }

      String nameLower = fileName.toLowerCase(Locale.ROOT);

      // Skip taxonomy/linkbase files
      if (nameLower.contains("_lab") || nameLower.contains("_cal")
          || nameLower.contains("_def") || nameLower.contains("_pre")
          || nameLower.endsWith(".xsd")) {
        continue;
      }
      // Skip R-viewer and MetaLinks files
      if (nameLower.matches("r\\d+\\.xml")
          || nameLower.equals("metalinks.json")
          || nameLower.equals("filingsummary.xml")) {
        continue;
      }

      // Best match: explicit XBRL instance type
      if (docType.contains("ex-101.ins")
          || docType.contains("xbrl instance")) {
        return fileName;
      }

      // Track first non-linkbase .xml as fallback
      if (bestCandidate == null) {
        bestCandidate = fileName;
      }
    }

    return bestCandidate;
  }

  /**
   * Parse XBRL instance document filename from FilingSummary.xml content.
   * Mirrors the logic in SecSchemaFactory.parseXbrlFilenameFromSummary.
   */
  private String parseXbrlFilenameFromSummary(String summaryXml) {
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.parse(
          new ByteArrayInputStream(
              summaryXml.getBytes(StandardCharsets.UTF_8)));

      // Look for <InstanceReport> element
      NodeList instanceReports =
          doc.getElementsByTagName("InstanceReport");
      if (instanceReports.getLength() > 0) {
        return instanceReports.item(0).getTextContent().trim();
      }

      // Fallback: look for <Report> with type="instance"
      NodeList reports = doc.getElementsByTagName("Report");
      for (int i = 0; i < reports.getLength(); i++) {
        Node report = reports.item(i);
        if (report.getNodeType() == Node.ELEMENT_NODE) {
          Element reportElement = (Element) report;
          String reportType = reportElement.getAttribute("type");
          if ("instance".equalsIgnoreCase(reportType)) {
            NodeList htmlFileNames =
                reportElement.getElementsByTagName("HtmlFileName");
            if (htmlFileNames.getLength() > 0) {
              String htmlFileName =
                  htmlFileNames.item(0).getTextContent().trim();
              return htmlFileName.replace(".htm", ".xml")
                  .replace(".html", ".xml");
            }
          }
        }
      }

      return null;
    } catch (Exception e) {
      LOGGER.debug("Failed to parse FilingSummary.xml: {}",
          e.getMessage());
      return null;
    }
  }

  /**
   * Parse inline XBRL from HTML file.
   * Inline XBRL uses ix: namespace tags embedded in HTML.
   */
  private Document parseInlineXbrl(String htmlPath) {
    String fileName = htmlPath.substring(htmlPath.lastIndexOf('/') + 1);
    LOGGER.debug("========== PARSE INLINE XBRL START for: " + fileName + " ==========");
    LOGGER.debug("parseInlineXbrl START for: " + htmlPath);
    try {
      // Read HTML file via StorageProvider
      LOGGER.debug("Reading HTML file...");
      String html;
      try (InputStream is = storageProvider.openInputStream(htmlPath)) {
        byte[] bytes = readAllBytes(is);
        html = new String(bytes, StandardCharsets.UTF_8);
      }
      LOGGER.debug("HTML file size: " + html.length() + " bytes");

      // Try XML parser first (preserves namespace prefixes), then HTML parser as fallback
      // HTML files from SEC filings may contain entities (e.g. &nbsp;) and unclosed tags
      // that are valid HTML but invalid XML, causing the XML parser to produce an incomplete tree
      org.jsoup.nodes.Document jsoupDoc =
          Jsoup.parse(html, "", org.jsoup.parser.Parser.xmlParser());

      org.jsoup.select.Elements ixElements = findInlineXbrlElements(jsoupDoc);

      if (ixElements.isEmpty()) {
        // XML parser failed to find elements - retry with lenient HTML parser
        LOGGER.debug("XML parser found no ix: elements in {}, retrying with HTML parser",
            fileName);
        jsoupDoc = Jsoup.parse(html);
        ixElements = findInlineXbrlElements(jsoupDoc);
      }

      if (ixElements.isEmpty()) {
        LOGGER.debug("No inline XBRL elements found in: " + fileName);
        return null;
      }

      LOGGER.debug("========== FOUND " + ixElements.size() + " INLINE XBRL ELEMENTS in: " + fileName + " ==========");
      LOGGER.debug("Found " + ixElements.size() + " inline XBRL elements in: " + fileName);

      // Debug: Log detailed information about found elements
      if (ixElements.size() > 0) {
        LOGGER.debug(" Sample of found inline XBRL elements:");
        int sampleCount = Math.min(5, ixElements.size());
        for (int i = 0; i < sampleCount; i++) {
          org.jsoup.nodes.Element elem = ixElements.get(i);
          LOGGER.debug(" Element " + (i+1) + ": tag=" + elem.tagName() +
                     ", name=" + elem.attr("name") +
                     ", contextRef=" + elem.attr("contextRef") +
                     ", text=" + elem.text().substring(0, Math.min(100, elem.text().length())));
        }
      }

      // Create a new XML document from inline XBRL elements
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setNamespaceAware(true);
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.newDocument();

      // Create root element
      Element root = doc.createElement("xbrl");
      doc.appendChild(root);

      // Extract and convert inline XBRL facts to standard XBRL format
      int factsAdded = 0;
      int factsSkippedNoContextRef = 0;
      int factsSkippedNoName = 0;

      for (org.jsoup.nodes.Element ixElement : ixElements) {
        String name = ixElement.attr("name");

        // Try multiple ways to extract contextRef (case-insensitive)
        String contextRef = ixElement.attr("contextref");
        if (contextRef == null || contextRef.isEmpty()) {
          contextRef = ixElement.attr("contextRef");
        }
        if (contextRef == null || contextRef.isEmpty()) {
          contextRef = ixElement.attr("CONTEXTREF");
        }
        if (contextRef == null || contextRef.isEmpty()) {
          // Also check for attributes with namespaces
          for (org.jsoup.nodes.Attribute attr : ixElement.attributes()) {
            String attrName = attr.getKey().toLowerCase();
            if (attrName.equals("contextref") || attrName.endsWith(":contextref")) {
              contextRef = attr.getValue();
              break;
            }
          }
        }

        // Extract name more robustly
        if (name == null || name.isEmpty()) {
          String tagName = ixElement.tagName();
          // For ix:nonFraction and ix:nonNumeric elements, try name attribute again
          if (tagName.equals("ix:nonfraction") || tagName.equals("ix:nonnumeric") ||
              tagName.equals("ix:nonNumeric") || tagName.equals("ix:nonFraction")) {
            name = ixElement.attr("name");
          } else if (tagName.startsWith("ix:")) {
            // Use the tag name itself as concept
            name = "us-gaap:" + tagName.substring(3);
          } else {
            // For elements found via contextRef selector, try to extract from other attributes
            for (org.jsoup.nodes.Attribute attr : ixElement.attributes()) {
              String attrName = attr.getKey().toLowerCase();
              if (attrName.equals("name") || attrName.endsWith(":name")) {
                name = attr.getValue();
                break;
              }
            }

            // If still no name, skip this element
            if (name == null || name.isEmpty()) {
              continue;
            }
          }
        }

        if (name == null || name.isEmpty()) {
          factsSkippedNoName++;
          continue;
        }

        String value = ixElement.text().trim();

        // Create fact element with proper namespace (more lenient - only require contextRef)
        if (contextRef != null && !contextRef.isEmpty()) {
          // Extract namespace and local name
          String namespace = "us-gaap";
          String localName = name;
          if (name.contains(":")) {
            String[] parts = name.split(":", 2);
            namespace = parts[0];
            localName = parts[1];
          }

          Element fact =
              doc.createElementNS("http://fasb.org/us-gaap/2024",
              namespace + ":" + localName);
          fact.setAttribute("contextRef", contextRef);

          // Handle numeric values - remove commas and formatting
          if (ixElement.hasAttr("scale") || ixElement.hasAttr("decimals")) {
            value = value.replaceAll("[,$()]", "").trim();
            if (value.startsWith("(") && value.endsWith(")")) {
              value = "-" + value.substring(1, value.length() - 1);
            }
          }

          fact.setTextContent(value);
          root.appendChild(fact);
          factsAdded++;
        } else {
          factsSkippedNoContextRef++;
        }
      }

      // Extract contexts from HTML (both ix:context and xbrli:context)
      org.jsoup.select.Elements contexts = new org.jsoup.select.Elements();
      contexts.addAll(jsoupDoc.getElementsByTag("ix:context"));
      contexts.addAll(jsoupDoc.getElementsByTag("xbrli:context"));
      contexts.addAll(jsoupDoc.select("[id^='c'], [id^='C']"));
      Map<String, Element> contextMap = new HashMap<>();
      for (org.jsoup.nodes.Element context : contexts) {
        Element xmlContext = doc.createElement("context");
        xmlContext.setAttribute("id", context.attr("id"));

        // Extract entity, period, etc from context
        org.jsoup.nodes.Element entity = context.selectFirst("[*|entity]");
        if (entity != null) {
          Element xmlEntity = doc.createElement("entity");
          org.jsoup.nodes.Element identifier = entity.selectFirst("[*|identifier]");
          if (identifier != null) {
            Element xmlIdentifier = doc.createElement("identifier");
            xmlIdentifier.setAttribute("scheme", identifier.attr("scheme"));
            xmlIdentifier.setTextContent(identifier.text());
            xmlEntity.appendChild(xmlIdentifier);
          }
          xmlContext.appendChild(xmlEntity);
        }

        // Extract period
        org.jsoup.nodes.Element period = context.selectFirst("[*|period]");
        if (period != null) {
          Element xmlPeriod = doc.createElement("period");
          org.jsoup.nodes.Element instant = period.selectFirst("[*|instant]");
          if (instant != null) {
            Element xmlInstant = doc.createElement("instant");
            xmlInstant.setTextContent(instant.text());
            xmlPeriod.appendChild(xmlInstant);
          }
          org.jsoup.nodes.Element startDate = period.selectFirst("[*|startDate]");
          if (startDate != null) {
            Element xmlStart = doc.createElement("startDate");
            xmlStart.setTextContent(startDate.text());
            xmlPeriod.appendChild(xmlStart);
          }
          org.jsoup.nodes.Element endDate = period.selectFirst("[*|endDate]");
          if (endDate != null) {
            Element xmlEnd = doc.createElement("endDate");
            xmlEnd.setTextContent(endDate.text());
            xmlPeriod.appendChild(xmlEnd);
          }
          xmlContext.appendChild(xmlPeriod);
        }

        root.appendChild(xmlContext);
      }

      // Log the summary
      LOGGER.debug("Processed inline XBRL from " + fileName);
      LOGGER.debug("  Found " + ixElements.size() + " inline XBRL elements");
      LOGGER.debug("  Added " + factsAdded + " facts to XML document");
      LOGGER.debug("  Skipped " + factsSkippedNoName + " elements (no name), " +
                  factsSkippedNoContextRef + " elements (no contextRef)");

      if (factsAdded == 0) {
        LOGGER.warn("No valid facts extracted from inline XBRL in " + fileName);
        return null;
      }

      // Debug: Log conversion summary
      LOGGER.debug(" parseInlineXbrl conversion summary for " + fileName + ":");
      LOGGER.debug(" - Facts added: " + factsAdded);
      LOGGER.debug(" - Facts skipped (no contextRef): " + factsSkippedNoContextRef);
      LOGGER.debug(" - Facts skipped (no name): " + factsSkippedNoName);
      LOGGER.debug(" - Final document has " + doc.getDocumentElement().getChildNodes().getLength() + " child elements");

      return doc;

    } catch (Exception e) {
      LOGGER.warn("Exception in parseInlineXbrl: " + e.getMessage());
      // Re-throw to let caller handle and track with full stack trace
      throw new RuntimeException("Failed to parse inline XBRL from " + fileName, e);
    }
  }


  /**
   * Extract MD&A data from document. This is the SINGLE source of truth for MD&A text.
   * Used by both mda_sections table and vectorized_chunks for embeddings.
   */
  private List<Map<String, Object>> extractMDAData(Document doc,
      String cik, String filingType, String filingDate, String accession, String sourcePath) {

    List<Map<String, Object>> dataList = new ArrayList<>();
    String accessionNumber = accession != null ? accession : cik + "-" + filingDate;
    SemanticTextChunker chunker = SemanticTextChunker.forMDA();

    // 1. Extract MD&A from HTML using semantic chunking
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    if (filename.endsWith(".htm") || filename.endsWith(".html")) {
      extractMDAWithChunker(sourcePath, dataList, cik, accessionNumber, filingDate, chunker);
    }

    // 2. Also extract from XBRL TextBlocks (if present)
    NodeList allElements = doc.getElementsByTagName("*");
    for (int i = 0; i < allElements.getLength(); i++) {
      Element element = (Element) allElements.item(i);

      if (element.hasAttribute("contextRef")) {
        String concept = extractConceptName(element);

        if (isMDAConcept(concept)) {
          String text = element.getTextContent().trim();
          if (!text.isEmpty()) {
            List<SemanticTextChunker.Chunk> chunks = chunker.chunkPlainText(text);
            for (SemanticTextChunker.Chunk chunk : chunks) {
              Map<String, Object> data = new HashMap<>();
              data.put("cik", cik);
              data.put("accession_number", accessionNumber);
              data.put("filing_date", filingDate);
              // Extract year for Iceberg partitioning
              int year = 0;
              if (filingDate != null && filingDate.length() >= 4) {
                try {
                  year = Integer.parseInt(filingDate.substring(0, 4));
                } catch (NumberFormatException e) {
                  // ignore
                }
              }
              data.put("year", year);
              data.put("section", "XBRL MD&A");
              data.put("subsection", concept);
              data.put("paragraph_number", dataList.size() + 1);
              data.put("paragraph_text", chunk.getText());
              data.put("footnote_refs", formatFootnoteRefs(chunk.getFootnoteRefs()));
              dataList.add(data);
            }
          }
        }
      }
    }

    LOGGER.info("Extracted {} MD&A paragraphs from {}", dataList.size(), sourcePath);
    return dataList;
  }

  /**
   * Write pre-extracted MD&A data to Parquet file.
   */
  private void writeMDAToParquetFromData(List<Map<String, Object>> mdaData, String outputPath)
      throws IOException {
    if (!mdaData.isEmpty()) {
      java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
          AbstractSecDataDownloader.loadTableColumns("mda_sections");
      storageProvider.writeAvroParquet(outputPath, columns, mdaData, "MDASection", "MDASection");
      LOGGER.info("Wrote {} MD&A chunks to {}", mdaData.size(), outputPath);
    } else {
      LOGGER.debug("Skipping empty MD&A file: {}", outputPath);
    }
  }

  /**
   * Formats footnote references list as comma-separated string.
   */
  private String formatFootnoteRefs(List<String> refs) {
    if (refs == null || refs.isEmpty()) {
      return null;
    }
    return String.join(", ", refs);
  }

  /**
   * Extract MD&A from HTML using semantic chunking.
   * Finds Item 7 and Item 7A sections and extracts content using optimal chunk sizes.
   */
  private void extractMDAWithChunker(String htmlPath, List<Map<String, Object>> dataList,
      String cik, String accessionNumber, String filingDate, SemanticTextChunker chunker) {
    try {
      org.jsoup.nodes.Document doc;
      try (InputStream is = storageProvider.openInputStream(htmlPath)) {
        doc = Jsoup.parse(is, "UTF-8", "");
      }

      // Find Item 7 and Item 7A sections
      List<MDASection> sections = findMDASections(doc);

      for (MDASection section : sections) {
        // Use semantic chunker to extract content
        List<SemanticTextChunker.Chunk> chunks = chunker.chunkFromElement(
            section.startElement,
            "(?i)item\\s*(8|9)\\b"  // Stop at Item 8 or 9
        );

        for (SemanticTextChunker.Chunk chunk : chunks) {
          Map<String, Object> data = new HashMap<>();
          data.put("cik", cik);
          data.put("accession_number", accessionNumber);
          data.put("filing_date", filingDate);
          // Extract year for Iceberg partitioning
          int year = 0;
          if (filingDate != null && filingDate.length() >= 4) {
            try {
              year = Integer.parseInt(filingDate.substring(0, 4));
            } catch (NumberFormatException e) {
              // ignore
            }
          }
          data.put("year", year);
          data.put("section", section.sectionName);
          data.put("subsection", chunk.getContentType().name());
          data.put("paragraph_number", dataList.size() + 1);
          data.put("paragraph_text", chunk.getText());
          data.put("footnote_refs", formatFootnoteRefs(chunk.getFootnoteRefs()));
          dataList.add(data);
        }
      }

      // If no structured sections found, try aggressive text extraction
      if (dataList.isEmpty()) {
        extractMDAAggressively(doc, dataList, cik, accessionNumber, filingDate, chunker);
      }

    } catch (Exception e) {
      LOGGER.warn("Failed to extract MD&A from HTML using chunker: " + e.getMessage());
    }
  }

  /**
   * Helper class to hold MD&A section info.
   */
  private static class MDASection {
    final String sectionName;
    final org.jsoup.nodes.Element startElement;

    MDASection(String sectionName, org.jsoup.nodes.Element startElement) {
      this.sectionName = sectionName;
      this.startElement = startElement;
    }
  }

  /**
   * Finds MD&A sections (Item 7 and Item 7A) in the document.
   */
  private List<MDASection> findMDASections(org.jsoup.nodes.Document doc) {
    List<MDASection> sections = new ArrayList<>();

    // Strategy 1: Look for Item 7 text in various formats
    org.jsoup.select.Elements elements = doc.select("*:matchesOwn((?i)item\\s*7[A]?\\b)");

    // Strategy 2: Also look for Management's Discussion and Analysis directly
    if (elements.isEmpty()) {
      elements = doc.select("*:matchesOwn((?i)management.{0,5}discussion.{0,5}analysis)");
    }

    // Strategy 3: Look for specific HTML patterns common in SEC filings
    if (elements.isEmpty()) {
      elements = doc.select("td:matchesOwn((?i)item\\s*7), div:matchesOwn((?i)item\\s*7)");
    }

    for (org.jsoup.nodes.Element element : elements) {
      String text = element.text();

      // Validate this is actually Item 7 or 7A (not Item 17, 27, etc.)
      if (!text.matches("(?i).*item\\s*7[A]?\\b.*") ||
          text.matches("(?i).*item\\s*[1-6]?7[0-9].*")) {
        continue;
      }

      // Skip table of contents entries
      if (text.length() < 100 &&
          (text.matches("(?i).*page.*") || text.matches(".*\\d+$"))) {
        continue;
      }

      String sectionName = text.contains("7A") || text.contains("7a") ? "Item 7A" : "Item 7";

      // Find content start - may be this element or a sibling
      org.jsoup.nodes.Element contentStart = findContentStart(element);
      if (contentStart != null) {
        sections.add(new MDASection(sectionName, contentStart));
      }
    }

    return sections;
  }

  /**
   * Finds the actual content start element (may be after a header).
   */
  private org.jsoup.nodes.Element findContentStart(org.jsoup.nodes.Element headerElement) {
    // If the element itself has substantial content, use it
    if (headerElement.text().length() > 200) {
      return headerElement;
    }

    // Look at parent's next sibling
    org.jsoup.nodes.Element parent = headerElement.parent();
    if (parent != null) {
      org.jsoup.nodes.Element nextSibling = parent.nextElementSibling();
      if (nextSibling != null && nextSibling.text().length() > 200) {
        return nextSibling;
      }
    }

    // Try direct next sibling
    org.jsoup.nodes.Element nextSibling = headerElement.nextElementSibling();
    if (nextSibling != null && nextSibling.text().length() > 200) {
      return nextSibling;
    }

    // Fall back to the header element itself
    return headerElement;
  }

  /**
   * Aggressive MD&A extraction when structured sections aren't found.
   * Searches for MD&A-related content throughout the document.
   */
  private void extractMDAAggressively(org.jsoup.nodes.Document doc, List<Map<String, Object>> dataList,
      String cik, String accessionNumber, String filingDate, SemanticTextChunker chunker) {

    org.jsoup.select.Elements textBlocks = doc.select("div, p, td");
    boolean inMDA = false;
    String currentSection = "";
    StringBuilder contentBuffer = new StringBuilder();

    for (org.jsoup.nodes.Element block : textBlocks) {
      String text = block.text();

      // Check if we're entering MD&A section
      if (text.matches("(?i).*item\\s*7[^0-9].*management.*discussion.*") ||
          text.matches("(?i).*management.*discussion.*analysis.*")) {
        inMDA = true;
        currentSection = "Item 7";
        continue;
      }

      // Check if we're entering Item 7A
      if (text.matches("(?i).*item\\s*7A.*")) {
        // Flush any accumulated content
        if (contentBuffer.length() > 0 && !currentSection.isEmpty()) {
          addChunkedContent(contentBuffer.toString(), currentSection, dataList,
              cik, accessionNumber, filingDate, chunker);
          contentBuffer = new StringBuilder();
        }
        inMDA = true;
        currentSection = "Item 7A";
        continue;
      }

      // Check if we're leaving MD&A section
      if (inMDA && text.matches("(?i).*item\\s*[89]\\b.*")) {
        break;
      }

      // Accumulate content if we're in MD&A
      if (inMDA && !text.isEmpty()) {
        if (contentBuffer.length() > 0) {
          contentBuffer.append("\n\n");
        }
        contentBuffer.append(text);
      }
    }

    // Flush remaining content
    if (contentBuffer.length() > 0 && !currentSection.isEmpty()) {
      addChunkedContent(contentBuffer.toString(), currentSection, dataList,
          cik, accessionNumber, filingDate, chunker);
    }
  }

  /**
   * Helper to add chunked content to the data list.
   */
  private void addChunkedContent(String content, String section, List<Map<String, Object>> dataList,
      String cik, String accessionNumber, String filingDate, SemanticTextChunker chunker) {

    List<SemanticTextChunker.Chunk> chunks = chunker.chunkPlainText(content);
    for (SemanticTextChunker.Chunk chunk : chunks) {
      Map<String, Object> data = new HashMap<>();
      data.put("cik", cik);
      data.put("accession_number", accessionNumber);
      data.put("filing_date", filingDate);
      // Extract year for Iceberg partitioning
      int year = 0;
      if (filingDate != null && filingDate.length() >= 4) {
        try {
          year = Integer.parseInt(filingDate.substring(0, 4));
        } catch (NumberFormatException e) {
          // ignore
        }
      }
      data.put("year", year);
      data.put("section", section);
      data.put("subsection", "General");
      data.put("paragraph_number", dataList.size() + 1);
      data.put("paragraph_text", chunk.getText());
      data.put("footnote_refs", formatFootnoteRefs(chunk.getFootnoteRefs()));
      dataList.add(data);
    }
  }

  /**
   * Extract MD&A from HTML file by looking for Item 7 and Item 7A sections.
   * @deprecated Use extractMDAWithChunker instead for semantic chunking.
   * Enhanced to handle inline XBRL documents where Item 7 may be embedded in tags.
   */
  private void extractMDAFromHTML(String htmlPath,
      List<Map<String, Object>> dataList, String cik, String accessionNumber, String filingDate) {
    try {
      org.jsoup.nodes.Document doc;
      try (InputStream is = storageProvider.openInputStream(htmlPath)) {
        doc = Jsoup.parse(is, "UTF-8", "");
      }

      // Strategy 1: Look for Item 7 text in various formats
      // Match Item 7, Item 7., ITEM 7, Item 7A, etc.
      org.jsoup.select.Elements sections = doc.select("*:matchesOwn((?i)item\\s*7[A]?\\b)");

      // Strategy 2: Also look for Management's Discussion and Analysis directly
      if (sections.isEmpty()) {
        sections = doc.select("*:matchesOwn((?i)management.{0,5}discussion.{0,5}analysis)");
      }

      // Strategy 3: Look for specific HTML patterns common in SEC filings
      if (sections.isEmpty()) {
        // Look for table cells or divs that might contain Item 7
        sections = doc.select("td:matchesOwn((?i)item\\s*7), div:matchesOwn((?i)item\\s*7)");
      }

      for (org.jsoup.nodes.Element section : sections) {
        String sectionText = section.text();

        // Check if this is actually Item 7 or 7A (not Item 17, 27, etc.)
        if (!sectionText.matches("(?i).*item\\s*7[A]?\\b.*") ||
            sectionText.matches("(?i).*item\\s*[1-6]?7[0-9].*")) {
          continue;
        }

        // Check if this is just a table of contents reference
        if (sectionText.length() < 100 &&
            (sectionText.matches("(?i).*page.*") || sectionText.matches(".*\\d+$"))) {
          continue;
        }

        String sectionName = sectionText.contains("7A") || sectionText.contains("7a") ? "Item 7A" : "Item 7";

        // Try to find the actual content
        org.jsoup.nodes.Element contentStart = section;

        // If this element is small, it might just be a header - look for larger content
        if (section.text().length() < 200) {
          // Look at parent and siblings for actual content
          org.jsoup.nodes.Element parent = section.parent();
          if (parent != null) {
            // Check next siblings of parent
            org.jsoup.nodes.Element nextSibling = parent.nextElementSibling();
            if (nextSibling != null && nextSibling.text().length() > 200) {
              contentStart = nextSibling;
            }
          }

          // Also try direct next sibling
          org.jsoup.nodes.Element nextSibling = section.nextElementSibling();
          if (nextSibling != null && nextSibling.text().length() > 200) {
            contentStart = nextSibling;
          }
        }

        // Extract content
        extractMDAContent(contentStart, sectionName, dataList, cik, accessionNumber, filingDate);
      }

      // If we still didn't find any MD&A, try a more aggressive approach
      if (dataList.isEmpty()) {
        // Look for large text blocks that mention key MD&A terms
        org.jsoup.select.Elements textBlocks = doc.select("div, p, td");
        boolean inMDA = false;
        String currentSection = "";

        for (org.jsoup.nodes.Element block : textBlocks) {
          String text = block.text();

          // Check if we're entering MD&A section
          if (text.matches("(?i).*item\\s*7[^0-9].*management.*discussion.*") ||
              text.matches("(?i).*management.*discussion.*analysis.*")) {
            inMDA = true;
            currentSection = "Item 7";
            continue;
          }

          // Check if we're entering Item 7A
          if (text.matches("(?i).*item\\s*7A.*")) {
            inMDA = true;
            currentSection = "Item 7A";
            continue;
          }

          // Check if we're leaving MD&A section
          if (inMDA && text.matches("(?i).*item\\s*[89]\\b.*")) {
            break;
          }

          // Extract content if we're in MD&A
          if (inMDA && text.length() > 100) {
            extractTextAsParagraphs(text, currentSection, dataList, cik, accessionNumber, filingDate);
          }
        }
      }

    } catch (Exception e) {
      LOGGER.warn("Failed to extract MD&A from HTML: " + e.getMessage());
    }
  }

  /**
   * Extract MD&A content starting from a given element.
   */
  private void extractMDAContent(org.jsoup.nodes.Element startElement, String sectionName,
      List<Map<String, Object>> dataList, String cik, String accessionNumber, String filingDate) {

    String subsection = "Overview";
    int paragraphNum = 1;
    org.jsoup.nodes.Element current = startElement;
    int emptyCount = 0;

    while (current != null && !isNextItem(current.text()) && emptyCount < 5) {
      String text = current.text().trim();

      // Skip empty elements but don't give up immediately
      if (text.isEmpty()) {
        emptyCount++;
        current = current.nextElementSibling();
        continue;
      }
      emptyCount = 0;

      // Check for subsection headers
      if (isSubsectionHeader(current)) {
        subsection = cleanSubsectionName(text);
        current = current.nextElementSibling();
        continue;
      }

      // Extract meaningful paragraphs
      if (text.length() > 100 && !text.matches("(?i).*page\\s*\\d+.*")) {
        // Split very long blocks into paragraphs
        String[] paragraphs = text.split("(?<=[.!?])\\s+(?=[A-Z])");

        for (String paragraph : paragraphs) {
          if (paragraph.trim().length() > 50) {
            Map<String, Object> data = new HashMap<>();
            data.put("cik", cik);
            data.put("accession_number", accessionNumber);
            data.put("filing_date", filingDate);
            data.put("section", sectionName);
            data.put("subsection", subsection);
            data.put("paragraph_number", paragraphNum++);
            data.put("paragraph_text", paragraph.trim());
            data.put("footnote_refs", extractFootnoteReferences(paragraph));
            dataList.add(data);
          }
        }
      }

      current = current.nextElementSibling();
    }
  }

  /**
   * Extract text as paragraphs for aggressive MD&A extraction.
   */
  private void extractTextAsParagraphs(String text, String sectionName,
      List<Map<String, Object>> dataList, String cik, String accessionNumber, String filingDate) {

    // Split into sentences or natural paragraphs
    String[] paragraphs = text.split("(?<=[.!?])\\s+(?=[A-Z])");

    // Group sentences into reasonable paragraph sizes
    StringBuilder currentParagraph = new StringBuilder();
    int paragraphNum = dataList.size() + 1;

    for (String sentence : paragraphs) {
      currentParagraph.append(sentence).append(" ");

      // Create a paragraph every few sentences or at natural breaks
      if (currentParagraph.length() > 300 || sentence.endsWith(".")) {
        String paragraphText = currentParagraph.toString().trim();
        if (paragraphText.length() > 100) {
          Map<String, Object> data = new HashMap<>();
          data.put("cik", cik);
          data.put("accession_number", accessionNumber);
          data.put("filing_date", filingDate);
          data.put("section", sectionName);
          data.put("subsection", "General");
          data.put("paragraph_number", paragraphNum++);
          data.put("paragraph_text", paragraphText);
          data.put("footnote_refs", extractFootnoteReferences(paragraphText));
          dataList.add(data);

          currentParagraph = new StringBuilder();
        }
      }
    }

    // Add any remaining text
    String remaining = currentParagraph.toString().trim();
    if (remaining.length() > 100) {
      Map<String, Object> data = new HashMap<>();
      data.put("cik", cik);
      data.put("accession_number", accessionNumber);
      data.put("filing_date", filingDate);
      data.put("year", extractYearFromDate(filingDate));
      data.put("section", sectionName);
      data.put("subsection", "General");
      data.put("paragraph_number", paragraphNum);
      data.put("paragraph_text", remaining);
      data.put("footnote_refs", extractFootnoteReferences(remaining));
      dataList.add(data);
    }
  }

  /**
   * Check if text indicates the start of the next Item section.
   */
  private boolean isNextItem(String text) {
    return text != null && text.matches(".*Item\\s+[89]\\b.*");
  }

  /**
   * Check if element is a subsection header.
   */
  private boolean isSubsectionHeader(org.jsoup.nodes.Element element) {
    return element.tagName().matches("h[2-4]") ||
           (element.tagName().equals("p") &&
            element.text().matches("^[A-Z][A-Z\\s]{2,50}$"));
  }

  /**
   * Clean subsection name for storage.
   */
  private String cleanSubsectionName(String text) {
    return text.replaceAll("\\s+", " ").trim();
  }

  /**
   * Check if concept name is MD&A related.
   */
  private boolean isMDAConcept(String concept) {
    return concept != null && (
        concept.contains("ManagementDiscussionAnalysis") ||
        concept.contains("MDA") ||
        concept.contains("OperatingResults") ||
        concept.contains("LiquidityCapitalResources") ||
        concept.contains("CriticalAccountingPolicies"));
  }

  /**
   * Extract paragraphs from text block.
   */
  private void extractParagraphs(String text, String concept,
      List<Map<String, Object>> dataList, String cik, String accessionNumber, String filingDate) {

    // Split into paragraphs
    String[] paragraphs = text.split("\\n\\n+");

    for (int i = 0; i < paragraphs.length; i++) {
      String paragraph = paragraphs[i].trim();
      if (paragraph.length() > 50) {  // Skip very short paragraphs
        Map<String, Object> data = new HashMap<>();
        data.put("cik", cik);
        data.put("accession_number", accessionNumber);
        data.put("filing_date", filingDate);
        data.put("year", extractYearFromDate(filingDate));
        data.put("section", "XBRL MD&A");
        data.put("subsection", concept);
        data.put("paragraph_number", i + 1);
        data.put("paragraph_text", paragraph);
        data.put("footnote_refs", extractFootnoteReferences(paragraph));
        dataList.add(data);
      }
    }
  }

  /**
   * Extract concept name from element.
   */
  private String extractConceptName(Element element) {
    if (element.hasAttribute("name")) {
      String concept = element.getAttribute("name");
      if (concept != null && concept.contains(":")) {
        concept = concept.substring(concept.indexOf(":") + 1);
      }
      return concept;
    }
    return element.getLocalName();
  }

  /**
   * Write XBRL relationships to Parquet.
   * Captures presentation, calculation, and definition linkbases.
   *
   * NOTE: Modern SEC filings use inline XBRL (iXBRL) where relationships are not embedded
   * in the main document but are provided in separate linkbase files (*.xml) referenced
   * from the XSD schema. Since we currently only download the main HTML filing document,
   * we cannot extract relationships from inline XBRL filings. This would require:
   * 1. Downloading the XSD schema file referenced in the document
   * 2. Parsing the XSD to find linkbase file references
   * 3. Downloading and parsing each linkbase file (calculation, presentation, definition)
   *
   * For now, this method will create empty relationship files for inline XBRL to satisfy
   * cache validation requirements.
   *
   * TODO: Implement full linkbase download functionality:
   * - Extract XSD href from: <link:schemaRef xlink:type="simple" xlink:href="aapl-20230930.xsd">
   * - Download XSD from: https://www.sec.gov/Archives/edgar/data/{CIK}/{ACCESSION}/{XSD_FILE}
   * - Parse XSD for linkbaseRef elements pointing to linkbase files
   * - Download each linkbase file (e.g., aapl-20230930_cal.xml, aapl-20230930_pre.xml)
   * - Parse linkbase XML for arc elements defining relationships
   * - Convert relationships to Parquet records with proper linkbase_type classification
   */
  private void writeRelationshipsToParquet(Document doc, String outputPath,
      String cik, String accession, String filingType, String filingDate, String sourcePath) throws IOException {

    LOGGER.debug(String.format("DEBUG: writeRelationshipsToParquet START for CIK %s filing type %s date %s", cik, filingType, filingDate));
    LOGGER.debug(String.format("DEBUG: Document is null? %s", doc == null));
    if (doc != null && doc.getDocumentElement() != null) {
      LOGGER.debug(String.format("DEBUG: Document root element: %s", doc.getDocumentElement().getNodeName()));
      LOGGER.debug(String.format("DEBUG: Document has %d child nodes", doc.getChildNodes().getLength()));
    }

    // Load schema from metadata
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractSecDataDownloader.loadTableColumns("xbrl_relationships");

    List<Map<String, Object>> dataList = new ArrayList<>();

    // Only extract traditional arc relationships if document was successfully parsed
    if (doc != null) {
      // Look for linkbase arcs in the document
      // These define relationships between concepts
      NodeList arcs = doc.getElementsByTagNameNS("*", "arc");
      LOGGER.debug(String.format("DEBUG: Found %d arc elements with namespace wildcard", arcs.getLength()));

      // Also try without namespace
      if (arcs.getLength() == 0) {
        arcs = doc.getElementsByTagName("arc");
        LOGGER.debug(String.format("DEBUG: Found %d arc elements without namespace", arcs.getLength()));
      }

      // Also try with common linkbase prefixes
      if (arcs.getLength() == 0) {
        arcs = doc.getElementsByTagName("link:arc");
        LOGGER.debug(String.format("DEBUG: Found %d link:arc elements", arcs.getLength()));
      }

      // Log what elements we DO have at root level
      if (arcs.getLength() == 0 && doc.getDocumentElement() != null) {
        NodeList allElems = doc.getDocumentElement().getChildNodes();
        LOGGER.debug(String.format("DEBUG: Document has %d root child elements:", allElems.getLength()));
        for (int i = 0; i < Math.min(10, allElems.getLength()); i++) {
          Node node = allElems.item(i);
          if (node.getNodeType() == Node.ELEMENT_NODE) {
            LOGGER.debug(String.format("DEBUG: Child element %d: %s", i, node.getNodeName()));
          }
        }
      }
    for (int i = 0; i < arcs.getLength(); i++) {
      Element arc = (Element) arcs.item(i);

      Map<String, Object> data = new HashMap<>();
      data.put("cik", cik);
      data.put("accession_number", accession);
      data.put("filing_date", filingDate);
      // Extract year for Iceberg partitioning
      int year = 0;
      if (filingDate != null && filingDate.length() >= 4) {
        try {
          year = Integer.parseInt(filingDate.substring(0, 4));
        } catch (NumberFormatException e) {
          // ignore
        }
      }
      data.put("year", year);

      // Determine linkbase type from namespace or arc role
      String arcRole = arc.getAttribute("arcrole");
      String linkbaseType = determineLinkbaseType(arcRole);
      data.put("linkbase_type", linkbaseType);
      data.put("arc_role", arcRole);

      // Get from and to concepts
      String from = arc.getAttribute("from");
      String to = arc.getAttribute("to");
      data.put("from_concept", cleanConceptName(from));
      data.put("to_concept", cleanConceptName(to));

      // Get weight for calculation linkbase
      String weight = arc.getAttribute("weight");
      if (weight != null && !weight.isEmpty()) {
        try {
          data.put("weight", Double.parseDouble(weight));
        } catch (NumberFormatException e) {
          data.put("weight", null);
        }
      }

      // Get order for presentation linkbase
      String order = arc.getAttribute("order");
      if (order != null && !order.isEmpty()) {
        try {
          data.put("order", (int) Double.parseDouble(order));
        } catch (NumberFormatException e) {
          data.put("order", null);
        }
      }

      // Get preferred label
      data.put("preferred_label", arc.getAttribute("preferredLabel"));

      dataList.add(data);
      LOGGER.debug(String.format("DEBUG: Added arc relationship from %s to %s", from, to));
    }
    LOGGER.debug(String.format("DEBUG: Total arc-based relationships extracted: %d", dataList.size()));
    } else {
      LOGGER.debug(" Document is null, skipping arc extraction");
    }

    // Also extract relationships from inline XBRL if present
    int beforeInlineCount = dataList.size();
    LOGGER.debug(String.format("DEBUG: Before inline extraction, have %d relationships", beforeInlineCount));
    extractInlineXBRLRelationships(doc, columns, dataList, cik, accession, filingDate, sourcePath);
    int inlineRelationships = dataList.size() - beforeInlineCount;
    LOGGER.debug(String.format("DEBUG: After inline extraction, extracted %d inline relationships, total now %d", inlineRelationships, dataList.size()));

    // Only write file if there's data - empty parquet files cause DuckDB union_by_name issues
    try {
      LOGGER.debug(" About to write " + dataList.size() + " relationship records to " + outputPath);

      if (!dataList.isEmpty()) {
        storageProvider.writeAvroParquet(outputPath, columns, dataList, "XbrlRelationship", "xbrl_relationships");
        LOGGER.info(
            String.format("Wrote %d relationships (%d arc-based, %d inline) to %s",
            dataList.size(), beforeInlineCount, inlineRelationships, outputPath));
      } else {
        // Skip empty files - inline XBRL filings typically have no relationships since
        // they're in separate linkbase files that we don't currently download
        LOGGER.debug(String.format("Skipping empty relationships file for CIK %s filing type %s (expected for inline XBRL)", cik, filingType));
      }

    } catch (Exception e) {
      LOGGER.error("Failed to write relationships parquet file for {} (CIK: {}): {}", filingDate, cik, e.getMessage());
      throw new IOException("Failed to write relationships to parquet: " + e.getMessage(), e);
    }
  }

  /**
   * Determine linkbase type from arc role.
   */
  private String determineLinkbaseType(String arcRole) {
    if (arcRole == null) return "unknown";

    if (arcRole.contains("parent-child") || arcRole.contains("presentation")) {
      return "presentation";
    } else if (arcRole.contains("summation") || arcRole.contains("calculation")) {
      return "calculation";
    } else if (arcRole.contains("dimension") || arcRole.contains("definition")) {
      return "definition";
    }

    return "other";
  }

  /**
   * Clean concept name by removing namespace prefix.
   */
  private String cleanConceptName(String concept) {
    if (concept == null) return null;
    if (concept.contains(":")) {
      return concept.substring(concept.indexOf(":") + 1);
    }
    return concept;
  }

  /**
   * Extract relationships from inline XBRL structure.
   */
  private void extractInlineXBRLRelationships(Document doc,
      java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns,
      List<Map<String, Object>> dataList, String cik, String accession, String filingDate, String sourcePath) {

    String fileName = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    LOGGER.debug(" extractInlineXBRLRelationships START");
    LOGGER.debug(String.format("DEBUG: Document is null? %s", doc == null));
    LOGGER.debug(String.format("DEBUG: Source file is: %s", fileName));

    // First, try to download and parse external linkbase files
    // These contain the actual relationship definitions for inline XBRL
    try {
      downloadAndParseLinkbases(sourcePath, columns, dataList, cik, accession, filingDate);
    } catch (Exception e) {
      LOGGER.debug("Failed to download/parse linkbase files: " + e.getMessage());
    }

    // In inline XBRL, relationships are often implicit in the document structure
    // We extract multiple types of relationships:
    // 1. Parent-child from nesting
    // 2. Calculation relationships from summation contexts
    // 3. Dimensional relationships

    // Track unique relationships to avoid duplicates
    Set<String> processedRelationships = new HashSet<>();

    // CRITICAL FIX: The doc passed here is a transformed document that doesn't contain
    // ix:relationship or ix:footnote elements. We need to parse the original HTML file
    // to find these inline XBRL relationship elements.
    Document originalHtmlDoc = null;
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    if (filename.endsWith(".htm") || filename.endsWith(".html")) {
      try {
        LOGGER.debug("Parsing original HTML file to extract inline XBRL relationships: " + filename);
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        try (InputStream is = storageProvider.openInputStream(sourcePath)) {
          originalHtmlDoc = builder.parse(is);
        }
        LOGGER.debug("Successfully parsed original HTML file for relationship extraction");
      } catch (Exception e) {
        LOGGER.warn("Failed to parse original HTML file for relationships: " + e.getMessage());
      }
    }

    // Use the original HTML document if available, otherwise fall back to the transformed doc
    Document searchDoc = (originalHtmlDoc != null) ? originalHtmlDoc : doc;

    // Look for ix:relationship elements (Inline XBRL 1.1 standard)
    NodeList ixRelationships = searchDoc.getElementsByTagNameNS("http://www.xbrl.org/2013/inlineXBRL", "relationship");
    LOGGER.debug(String.format("Found %d ix:relationship elements", ixRelationships.getLength()));

    for (int i = 0; i < ixRelationships.getLength(); i++) {
      Element ixRel = (Element) ixRelationships.item(i);
      String arcrole = ixRel.getAttribute("arcrole");
      String fromRefs = ixRel.getAttribute("fromRefs");  // FIXED: Use correct plural attribute
      String toRefs = ixRel.getAttribute("toRefs");      // FIXED: Use correct plural attribute
      String order = ixRel.getAttribute("order");
      String weight = ixRel.getAttribute("weight");

      LOGGER.debug(
          String.format("Processing ix:relationship %d: arcrole=%s, fromRefs=%s, toRefs=%s",
          i, arcrole, fromRefs, toRefs));

      if (arcrole != null && !arcrole.isEmpty() && fromRefs != null && !fromRefs.isEmpty() &&
          toRefs != null && !toRefs.isEmpty()) {

        // Split space-separated fromRefs and toRefs (inline XBRL can have multiple refs)
        String[] fromRefArray = fromRefs.trim().split("\\s+");
        String[] toRefArray = toRefs.trim().split("\\s+");

        // Create relationships for each fromRef to each toRef combination
        for (String fromRef : fromRefArray) {
          for (String toRef : toRefArray) {
            String relationshipKey = fromRef + "->" + toRef + ":" + arcrole;

            if (!processedRelationships.contains(relationshipKey)) {
              Map<String, Object> data = new HashMap<>();
              data.put("cik", cik);
              data.put("accession_number", accession);
              data.put("filing_date", filingDate);
              data.put("year", extractYearFromDate(filingDate));
              data.put("linkbase_type", determineLinkbaseType(arcrole));
              data.put("arc_role", arcrole);
              data.put("from_concept", cleanConceptName(fromRef));
              data.put("to_concept", cleanConceptName(toRef));
              data.put("weight", weight != null && !weight.isEmpty() ? Double.parseDouble(weight) : null);
              data.put("order", order != null && !order.isEmpty() ? (int) Double.parseDouble(order) : i);
              data.put("preferred_label", ixRel.getAttribute("preferredLabel"));
              dataList.add(data);
              processedRelationships.add(relationshipKey);
              LOGGER.debug("Added ix:relationship: " + relationshipKey);
            }
          }
        }
      } else {
        LOGGER.debug(
            String.format("Skipping ix:relationship %d: missing required attributes (arcrole=%s, fromRefs=%s, toRefs=%s)",
            i, arcrole, fromRefs, toRefs));
      }
    }

    // Look for ix:footnote elements tied to facts
    NodeList ixFootnotes = searchDoc.getElementsByTagNameNS("http://www.xbrl.org/2013/inlineXBRL", "footnote");
    LOGGER.debug(String.format("Found %d ix:footnote elements", ixFootnotes.getLength()));

    for (int i = 0; i < ixFootnotes.getLength(); i++) {
      Element footnote = (Element) ixFootnotes.item(i);
      String footnoteRole = footnote.getAttribute("footnoteRole");
      String id = footnote.getAttribute("id");

      if (id != null && !id.isEmpty()) {
        // Look for elements that reference this footnote
        NodeList allElems = searchDoc.getElementsByTagName("*");
        for (int j = 0; j < allElems.getLength(); j++) {
          Element elem = (Element) allElems.item(j);
          String footnoteRefs = elem.getAttribute("footnoteRefs");

          if (footnoteRefs != null && footnoteRefs.contains(id)) {
            String concept = extractConceptName(elem);
            if (concept != null) {
              String relationshipKey = concept + "-footnote->" + id;

              if (!processedRelationships.contains(relationshipKey)) {
                Map<String, Object> data = new HashMap<>();
                data.put("cik", cik);
                data.put("accession_number", accession);
                data.put("filing_date", filingDate);
                data.put("year", extractYearFromDate(filingDate));
                data.put("linkbase_type", "reference");
                data.put("arc_role", "http://www.xbrl.org/2009/arcrole/fact-explanatoryFact");
                data.put("from_concept", cleanConceptName(concept));
                data.put("to_concept", "footnote_" + id);
                data.put("weight", null);
                data.put("order", i * 1000 + j);
                data.put("preferred_label", footnoteRole);
                dataList.add(data);
                processedRelationships.add(relationshipKey);
                LOGGER.debug("Added fact-footnote relationship: " + relationshipKey);
              }
            }
          }
        }
      }
    }

    // Look for ix:nonNumeric and ix:nonFraction elements that define structure
    NodeList ixElements = doc.getElementsByTagNameNS("http://www.xbrl.org/2013/inlineXBRL", "*");
    LOGGER.debug(String.format("Found %d inline XBRL elements", ixElements.getLength()));

    // Extract relationships from table structures (common in inline XBRL)
    NodeList tables = doc.getElementsByTagName("table");
    for (int t = 0; t < tables.getLength(); t++) {
      Element table = (Element) tables.item(t);

      // Look for XBRL concepts within table rows
      NodeList rows = table.getElementsByTagName("tr");
      String lastParentConcept = null;

      for (int r = 0; r < rows.getLength(); r++) {
        NodeList cells = ((Element) rows.item(r)).getElementsByTagName("*");

        for (int c = 0; c < cells.getLength(); c++) {
          Element cell = (Element) cells.item(c);

          if (cell.hasAttribute("contextRef") || cell.hasAttribute("name")) {
            String concept = extractConceptName(cell);
            if (concept != null) {
              // Check if this appears to be a child of the previous concept
              if (lastParentConcept != null && !concept.equals(lastParentConcept)) {
                String relationshipKey = lastParentConcept + "->" + concept;

                if (!processedRelationships.contains(relationshipKey)) {
                  Map<String, Object> data = new HashMap<>();
                  data.put("cik", cik);
                  data.put("accession_number", accession);
                  data.put("filing_date", filingDate);
                  data.put("year", extractYearFromDate(filingDate));
                  data.put("linkbase_type", "presentation");
                  data.put("arc_role", "table-structure");
                  data.put("from_concept", cleanConceptName(lastParentConcept));
                  data.put("to_concept", cleanConceptName(concept));
                  data.put("weight", null);
                  data.put("order", r * 100 + c);  // Row-column based ordering
                  data.put("preferred_label", cell.getAttribute("preferredLabel"));
                  dataList.add(data);
                  processedRelationships.add(relationshipKey);
                }
              }

              // Update parent if this looks like a section header
              if (cell.getTagName().matches("th|h\\d") ||
                  cell.getAttribute("class").contains("header")) {
                lastParentConcept = concept;
              }
            }
          }
        }
      }
    }

    // Extract calculation relationships from elements with calculation attributes
    NodeList allElements = doc.getElementsByTagName("*");
    for (int i = 0; i < allElements.getLength(); i++) {
      Element element = (Element) allElements.item(i);

      // Look for calculation weight indicators
      if (element.hasAttribute("calculationWeight") ||
          element.hasAttribute("data-calculation-parent")) {
        String concept = extractConceptName(element);
        String parentConcept = element.getAttribute("data-calculation-parent");
        String weight = element.getAttribute("calculationWeight");

        if (concept != null && parentConcept != null && !parentConcept.isEmpty()) {
          String relationshipKey = parentConcept + "-calc->" + concept;

          if (!processedRelationships.contains(relationshipKey)) {
            Map<String, Object> data = new HashMap<>();
            data.put("cik", cik);
            data.put("accession_number", accession);
            data.put("filing_date", filingDate);
            data.put("year", extractYearFromDate(filingDate));
            data.put("linkbase_type", "calculation");
            data.put("arc_role", "summation-item");
            data.put("from_concept", cleanConceptName(parentConcept));
            data.put("to_concept", cleanConceptName(concept));
            data.put("weight", weight.isEmpty() ? 1.0 : Double.parseDouble(weight));
            data.put("order", i);
            data.put("preferred_label", element.getAttribute("preferredLabel"));
            dataList.add(data);
            processedRelationships.add(relationshipKey);
          }
        }
      }
    }

    LOGGER.debug(String.format("Extracted %d unique inline XBRL relationships", processedRelationships.size()));
  }

  /**
   * Download and parse linkbase files referenced from inline XBRL schema.
   */
  private void downloadAndParseLinkbases(String htmlPath,
      java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns,
      List<Map<String, Object>> dataList, String cik, String accession, String filingDate) throws Exception {

    LOGGER.debug("Looking for linkbase references in inline XBRL document");

    // Parse the HTML file to look for schema references
    LOGGER.debug("Parsing HTML file for schema references: " + htmlPath);
    Document doc;
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setNamespaceAware(true);
      DocumentBuilder builder = factory.newDocumentBuilder();
      try (InputStream is = storageProvider.openInputStream(htmlPath)) {
        doc = builder.parse(is);
      }
      LOGGER.debug("Successfully parsed HTML file, proceeding with schema reference search");
    } catch (Exception e) {
      LOGGER.debug("Failed to parse HTML file: " + e.getMessage());
      return;
    }

    // Find schemaRef element that points to XSD file
    // First try with the correct linkbase namespace
    NodeList schemaRefs = doc.getElementsByTagNameNS("http://www.xbrl.org/2003/linkbase", "schemaRef");
    LOGGER.debug("Found " + schemaRefs.getLength() + " schemaRef elements in linkbase namespace");

    if (schemaRefs.getLength() == 0) {
      // Try with prefixed tag name (more common in inline XBRL)
      schemaRefs = doc.getElementsByTagName("link:schemaRef");
      LOGGER.debug("Found " + schemaRefs.getLength() + " link:schemaRef elements");
    }
    if (schemaRefs.getLength() == 0) {
      // Try looking inside ix:references elements
      NodeList references = doc.getElementsByTagName("ix:references");
      LOGGER.debug("Found " + references.getLength() + " ix:references elements");
      for (int i = 0; i < references.getLength(); i++) {
        Element referencesEl = (Element) references.item(i);
        NodeList childSchemaRefs = referencesEl.getElementsByTagName("link:schemaRef");
        LOGGER.debug("Found " + childSchemaRefs.getLength() + " link:schemaRef children in ix:references[" + i + "]");
        if (childSchemaRefs.getLength() > 0) {
          schemaRefs = childSchemaRefs;
          break;
        }
      }
    }

    if (schemaRefs.getLength() == 0) {
      LOGGER.debug("No schemaRef found in inline XBRL - no linkbases to download");
      return;
    }

    Element schemaRef = (Element) schemaRefs.item(0);
    String xsdHref = schemaRef.getAttribute("xlink:href");
    if (xsdHref == null || xsdHref.isEmpty()) {
      xsdHref = schemaRef.getAttribute("href");
    }

    if (xsdHref == null || xsdHref.isEmpty()) {
      LOGGER.debug("No XSD href found in schemaRef");
      return;
    }

    LOGGER.debug("Found XSD reference: " + xsdHref);

    // Extract base URL from source file path or document URL
    String baseUrl = extractBaseUrl(htmlPath, xsdHref);
    if (baseUrl == null) {
      LOGGER.warn("Could not determine base URL for linkbase downloads");
      return;
    }

    // Download and parse XSD to find linkbase references
    String xsdUrl = resolveUrl(baseUrl, xsdHref);
    LOGGER.debug("Downloading XSD from: " + xsdUrl);

    String xsdContent = downloadFile(xsdUrl);
    if (xsdContent == null) {
      LOGGER.debug("Failed to download XSD file from: " + xsdUrl + " - continuing with inline XBRL relationship processing");
      // For inline XBRL documents, XSD files often don't exist on the server
      // We can still extract relationships from the inline document structure
      return;
    }

    // Parse XSD to find linkbaseRef elements
    DocumentBuilderFactory xsdFactory = DocumentBuilderFactory.newInstance();
    xsdFactory.setNamespaceAware(true);
    DocumentBuilder xsdBuilder = xsdFactory.newDocumentBuilder();
    Document xsdDoc = xsdBuilder.parse(new ByteArrayInputStream(xsdContent.getBytes(StandardCharsets.UTF_8)));

    // Find linkbaseRef elements
    NodeList linkbaseRefs = xsdDoc.getElementsByTagNameNS("http://www.xbrl.org/2003/linkbase", "linkbaseRef");
    if (linkbaseRefs.getLength() == 0) {
      linkbaseRefs = xsdDoc.getElementsByTagName("link:linkbaseRef");
    }

    LOGGER.debug("Found " + linkbaseRefs.getLength() + " linkbase references in XSD");

    // Process each linkbase file
    for (int i = 0; i < linkbaseRefs.getLength(); i++) {
      Element linkbaseRef = (Element) linkbaseRefs.item(i);
      String linkbaseHref = linkbaseRef.getAttribute("xlink:href");
      if (linkbaseHref == null || linkbaseHref.isEmpty()) {
        linkbaseHref = linkbaseRef.getAttribute("href");
      }

      if (linkbaseHref == null || linkbaseHref.isEmpty()) {
        continue;
      }

      String role = linkbaseRef.getAttribute("xlink:role");
      if (role == null || role.isEmpty()) {
        role = linkbaseRef.getAttribute("role");
      }

      // Determine linkbase type from role
      String linkbaseType = determineLinkbaseTypeFromRole(role);

      LOGGER.debug("Processing " + linkbaseType + " linkbase: " + linkbaseHref);

      // Download linkbase file
      String linkbaseUrl = resolveUrl(baseUrl, linkbaseHref);
      String linkbaseContent = downloadFile(linkbaseUrl);

      if (linkbaseContent == null) {
        LOGGER.warn("Failed to download linkbase from: " + linkbaseUrl);
        continue;
      }

      // Parse linkbase and extract relationships
      try {
        DocumentBuilderFactory linkbaseFactory = DocumentBuilderFactory.newInstance();
        linkbaseFactory.setNamespaceAware(true);
        DocumentBuilder linkbaseBuilder = linkbaseFactory.newDocumentBuilder();
        Document linkbaseDoc = linkbaseBuilder.parse(new ByteArrayInputStream(linkbaseContent.getBytes(StandardCharsets.UTF_8)));
        extractLinkbaseRelationships(linkbaseDoc, columns, dataList, cik, accession, filingDate, linkbaseType);
      } catch (Exception e) {
        LOGGER.warn("Failed to parse linkbase " + linkbaseHref + ": " + e.getMessage());
      }
    }
  }

  /**
   * Extract relationships from a linkbase document.
   */
  private void extractLinkbaseRelationships(Document linkbaseDoc,
      java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns,
      List<Map<String, Object>> dataList, String cik, String accession, String filingDate, String linkbaseType) {

    // Find arc elements in the linkbase
    NodeList arcs = linkbaseDoc.getElementsByTagName("*");
    int relationshipCount = 0;

    for (int i = 0; i < arcs.getLength(); i++) {
      Element element = (Element) arcs.item(i);
      String tagName = element.getTagName();

      // Look for arc elements (calculationArc, presentationArc, definitionArc, etc.)
      if (tagName.endsWith("Arc") || tagName.contains(":arc")) {
        String from = element.getAttribute("xlink:from");
        String to = element.getAttribute("xlink:to");

        if (from == null || from.isEmpty()) {
          from = element.getAttribute("from");
        }
        if (to == null || to.isEmpty()) {
          to = element.getAttribute("to");
        }

        if (from != null && !from.isEmpty() && to != null && !to.isEmpty()) {
          Map<String, Object> data = new HashMap<>();
          data.put("cik", cik);
          data.put("accession_number", accession);
          data.put("filing_date", filingDate);
          data.put("year", extractYearFromDate(filingDate));
          data.put("linkbase_type", linkbaseType);

          // Get arc role
          String arcRole = element.getAttribute("xlink:arcrole");
          if (arcRole == null || arcRole.isEmpty()) {
            arcRole = element.getAttribute("arcrole");
          }
          data.put("arc_role", arcRole);

          // Clean concept names
          data.put("from_concept", cleanConceptName(from));
          data.put("to_concept", cleanConceptName(to));

          // Get weight for calculation linkbase
          String weight = element.getAttribute("weight");
          if (weight != null && !weight.isEmpty()) {
            try {
              data.put("weight", Double.parseDouble(weight));
            } catch (NumberFormatException e) {
              data.put("weight", null);
            }
          } else {
            data.put("weight", null);
          }

          // Get order for presentation linkbase
          String order = element.getAttribute("order");
          if (order != null && !order.isEmpty()) {
            try {
              data.put("order", (int) Double.parseDouble(order));
            } catch (NumberFormatException e) {
              data.put("order", null);
            }
          } else {
            data.put("order", null);
          }

          // Get preferred label
          String preferredLabel = element.getAttribute("preferredLabel");
          data.put("preferred_label", preferredLabel);

          dataList.add(data);
          relationshipCount++;
        }
      }
    }

    LOGGER.debug("Extracted " + relationshipCount + " relationships from " + linkbaseType + " linkbase");
  }

  /**
   * Determine linkbase type from role URI.
   */
  private String determineLinkbaseTypeFromRole(String role) {
    if (role == null) return "unknown";

    if (role.contains("calculation")) {
      return "calculation";
    } else if (role.contains("presentation")) {
      return "presentation";
    } else if (role.contains("definition")) {
      return "definition";
    } else if (role.contains("label")) {
      return "label";
    } else if (role.contains("reference")) {
      return "reference";
    }

    return "other";
  }

  /**
   * Extract base URL from file path or filing information.
   */
  private String extractBaseUrl(String htmlPath, String xsdHref) {
    // Try to extract CIK and accession from file path
    // File path pattern: /path/to/cache/sec/{CIK}/{ACCESSION_NUMBER}/filename.htm

    // Look for SEC EDGAR file path pattern (CIK is 10 digits, accession is variable)
    Pattern pathPattern = Pattern.compile("/sec/([0-9]{10})/([^/]+)/[^/]+\\.htm");
    Matcher pathMatcher = pathPattern.matcher(htmlPath);

    if (pathMatcher.find()) {
      String cik = pathMatcher.group(1);
      String accession = pathMatcher.group(2);
      // Convert accession number to EDGAR format (remove dashes if present)
      String edgarAccession = accession.replace("-", "");
      return "https://www.sec.gov/Archives/edgar/data/" + cik + "/" + edgarAccession + "/";
    }

    // Try to read the HTML file and look for EDGAR URLs in content
    try {
      String content;
      try (InputStream is = storageProvider.openInputStream(htmlPath)) {
        byte[] bytes = readAllBytes(is);
        content = new String(bytes, StandardCharsets.UTF_8);
      }

      // Look for EDGAR URLs in HTML content or comments
      Pattern urlPattern = Pattern.compile("https?://[^\\s\"']+edgar/data/[0-9]+/[^\\s\"'/]+/");
      Matcher urlMatcher = urlPattern.matcher(content);
      if (urlMatcher.find()) {
        return urlMatcher.group();
      }

    } catch (Exception e) {
      LOGGER.debug("Could not read HTML file to extract base URL: " + e.getMessage());
    }

    // Default to SEC EDGAR base URL structure
    // This is a fallback - in production we'd need better context
    return "https://www.sec.gov/Archives/edgar/data/";
  }

  /**
   * Resolve relative URL against base URL.
   */
  private String resolveUrl(String baseUrl, String relativeUrl) {
    if (relativeUrl.startsWith("http://") || relativeUrl.startsWith("https://")) {
      return relativeUrl;
    }

    if (!baseUrl.endsWith("/")) {
      baseUrl += "/";
    }

    if (relativeUrl.startsWith("/")) {
      // Absolute path - extract base domain
      try {
        URI baseUri = URI.create(baseUrl);
        return baseUri.getScheme() + "://" + baseUri.getHost() + relativeUrl;
      } catch (Exception e) {
        return baseUrl + relativeUrl;
      }
    }

    return baseUrl + relativeUrl;
  }

  /**
   * Parses a malformed XML/XBRL file using JSoup's lenient HTML parser, then
   * converts to a W3C DOM Document. Used as a fallback when strict XML parsing
   * fails due to unclosed tags, bare ampersands, HTML entities, etc.
   *
   * @param filePath Path to the file (local or S3)
   * @return W3C DOM Document, or null if parsing fails
   */
  private Document parseWithJsoupFallback(String filePath) {
    try {
      String content;
      try (InputStream is = storageProvider.openInputStream(filePath)) {
        byte[] bytes = readAllBytes(is);
        content = new String(bytes, StandardCharsets.UTF_8);
      }

      // Try JSoup XML parser first (preserves namespace prefixes)
      org.jsoup.nodes.Document jsoupDoc =
          Jsoup.parse(content, "", org.jsoup.parser.Parser.xmlParser());

      // If XML parser produces a near-empty tree, retry with HTML parser
      if (jsoupDoc.children().size() <= 1
          && jsoupDoc.getElementsByTag("xbrl").isEmpty()) {
        jsoupDoc = Jsoup.parse(content);
      }

      // Convert JSoup document to W3C DOM
      org.jsoup.helper.W3CDom w3cDom = new org.jsoup.helper.W3CDom();
      Document doc = w3cDom.fromJsoup(jsoupDoc);

      LOGGER.info("JSoup fallback parsed {} successfully ({} child nodes)",
          filePath.substring(filePath.lastIndexOf('/') + 1),
          doc.getDocumentElement() != null
              ? doc.getDocumentElement().getChildNodes().getLength() : 0);
      return doc;
    } catch (Exception e) {
      LOGGER.warn("JSoup fallback also failed for {}: {}",
          filePath.substring(filePath.lastIndexOf('/') + 1), e.getMessage());
      return null;
    }
  }

  /**
   * Sanitizes an XML input stream by escaping bare {@code &} characters that are
   * not part of valid entity references. Some legacy SEC EDGAR filings (especially
   * .TXT format) contain unescaped ampersands like "Smith & Jones" which cause
   * SAXParseException during XML parsing.
   *
   * <p>Valid entity references ({@code &amp;}, {@code &lt;}, {@code &gt;},
   * {@code &quot;}, {@code &apos;}, {@code &#NNN;}, {@code &#xHHH;}) are preserved.
   */
  private InputStream sanitizeXmlStream(InputStream is) throws IOException {
    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
    byte[] buf = new byte[8192];
    int n;
    while ((n = is.read(buf)) != -1) {
      baos.write(buf, 0, n);
    }
    is.close();

    String content = baos.toString(StandardCharsets.UTF_8.name());

    // Replace bare '&' that are NOT followed by a valid entity reference:
    //   &amp; &lt; &gt; &quot; &apos; &#digits; &#xhex;
    // The negative lookahead matches '&' not followed by word-chars+semicolon or #
    String sanitized = content.replaceAll("&(?!(?:[a-zA-Z][a-zA-Z0-9]*|#[0-9]+|#x[0-9a-fA-F]+);)", "&amp;");

    if (!sanitized.equals(content)) {
      int fixes = sanitized.length() - content.length();
      // Each bare '&' becomes '&amp;' — 4 extra chars per fix
      LOGGER.info("Sanitized {} bare '&' characters in XML input", fixes / 4);
    }

    return new ByteArrayInputStream(sanitized.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Download file from URL with retry and exponential backoff for transient errors.
   */
  private String downloadFile(String urlString) {
    int maxRetries = 3;
    long initialDelayMs = 1000;

    for (int attempt = 0; attempt < maxRetries; attempt++) {
      HttpURLConnection conn = null;
      try {
        URI uri = URI.create(urlString);
        URL url = uri.toURL();
        conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(30000);
        conn.setReadTimeout(30000);
        // SEC.gov requires proper User-Agent identifying automated tools with contact info
        // Format: "Company Name admin@email.com" - see: https://www.sec.gov/os/accessing-edgar-data
        String userAgent = "Apache Calcite SEC Adapter apache-calcite@apache.org";
        conn.setRequestProperty("User-Agent", userAgent);
        conn.setRequestProperty("Accept",
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
        conn.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
        conn.setRequestProperty("DNT", "1");
        conn.setRequestProperty("Connection", "keep-alive");
        conn.setRequestProperty("Upgrade-Insecure-Requests", "1");

        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Attempting XSD download with User-Agent: {}", userAgent);
        }

        int responseCode = conn.getResponseCode();

        // Retryable HTTP status codes
        if (responseCode == 429 || responseCode == 500 || responseCode == 502
            || responseCode == 503 || responseCode == 504) {
          if (attempt < maxRetries - 1) {
            long delay = initialDelayMs * (1L << attempt);
            LOGGER.warn("HTTP {} from {} - retrying in {}ms (attempt {}/{})",
                responseCode, urlString, delay, attempt + 1, maxRetries);
            sleepQuietly(delay);
            continue;
          }
          LOGGER.warn("HTTP {} from {} after {} attempts - giving up",
              responseCode, urlString, maxRetries);
          return null;
        }

        if (responseCode != HttpURLConnection.HTTP_OK) {
          String responseMessage = conn.getResponseMessage();
          LOGGER.warn("HTTP {} ({}) for URL: {}", responseCode, responseMessage, urlString);
          return null;
        }

        try (BufferedReader reader =
            new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
          StringBuilder content = new StringBuilder();
          String line;
          while ((line = reader.readLine()) != null) {
            content.append(line).append("\n");
          }
          return content.toString();
        }
      } catch (Exception e) {
        String msg = e.getMessage();
        boolean retryable = isRetryableException(msg);
        if (attempt < maxRetries - 1 && retryable) {
          long delay = initialDelayMs * (1L << attempt);
          LOGGER.warn("Download of {} failed: {} - retrying in {}ms (attempt {}/{})",
              urlString, msg, delay, attempt + 1, maxRetries);
          sleepQuietly(delay);
        } else {
          LOGGER.warn("Failed to download {}: {}", urlString, msg);
          return null;
        }
      } finally {
        if (conn != null) {
          conn.disconnect();
        }
      }
    }
    LOGGER.warn("Failed to download {} after {} attempts", urlString, maxRetries);
    return null;
  }

  /**
   * Returns whether an exception message indicates a transient/retryable error.
   */
  private static boolean isRetryableException(String msg) {
    if (msg == null) {
      return true;
    }
    String lower = msg.toLowerCase();
    return lower.contains("connection reset")
        || lower.contains("tls")
        || lower.contains("ssl")
        || lower.contains("handshake")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("broken pipe")
        || lower.contains("connection refused")
        || lower.contains("no route to host")
        || lower.contains("network is unreachable")
        || lower.contains("unexpected end of stream");
  }

  /**
   * Sleeps for the specified duration, restoring interrupt flag if interrupted.
   */
  private static void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Helper method to write records using StorageProvider's consolidated Parquet writing.
   */
  private void writeRecordsToParquet(List<GenericRecord> records, Schema schema,
      String outputPath, String recordType) throws IOException {

    if (records.isEmpty()) {
      LOGGER.debug("No " + recordType + " records to write for " + outputPath + " - creating empty file with schema");
    }

    LOGGER.debug("Writing " + records.size() + " " + recordType + " records to " + outputPath);

    // Use StorageProvider's consolidated Parquet writing method
    storageProvider.writeAvroParquet(outputPath, schema, records, recordType);

    LOGGER.info("Successfully wrote " + records.size() + " " + recordType + " records to " + outputPath);
  }

  /**
   * Overloaded method for backward compatibility with existing calls.
   */
  private void writeRecordsToParquet(List<GenericRecord> records, Schema schema,
      String outputPath) throws IOException {
    writeRecordsToParquet(records, schema, outputPath, "records");
  }

  /**
   * Check if this is an insider trading form (Form 3, 4, or 5).
   */
  private boolean isInsiderForm(Document doc, String filingType) {
    // Check filing type first
    if (filingType != null && (filingType.equals("3") || filingType.equals("4")
        || filingType.equals("5") || filingType.startsWith("3/")
        || filingType.startsWith("4/") || filingType.startsWith("5/"))) {
      return true;
    }

    // Also check for ownershipDocument root element
    if (doc == null) {
      return false;
    }
    NodeList ownershipDocs = doc.getElementsByTagName("ownershipDocument");
    return ownershipDocs.getLength() > 0;
  }

  /**
   * Parse a Form 3/4/5 XML file directly and delegate to convertInsiderForm.
   * Bypasses the HTML/inline-XBRL detection path so that ownership XML documents
   * are never mistaken for plain HTML pages.
   */
  private List<String> processInsiderXmlForm(String sourceFilePath, String targetDirectoryPath,
      ConversionMetadata metadata) throws IOException {
    String fileName = sourceFilePath.substring(sourceFilePath.lastIndexOf('/') + 1);
    String accession = metadata != null ? metadata.getHint("accession") : null;
    if (accession == null || accession.isEmpty()) {
      accession = extractAccessionFromPath(sourceFilePath);
    }
    Document doc;
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setNamespaceAware(true);
      DocumentBuilder builder = factory.newDocumentBuilder();
      try (InputStream is = sanitizeXmlStream(storageProvider.openInputStream(sourceFilePath))) {
        doc = builder.parse(is);
      }
    } catch (org.xml.sax.SAXParseException e) {
      LOGGER.info("XML parse failed for insider form {}: {} — falling back to JSoup", fileName, e.getMessage());
      doc = parseWithJsoupFallback(sourceFilePath);
    } catch (Exception e) {
      LOGGER.warn("Failed to parse insider form {}: {}", fileName, e.getMessage());
      return new ArrayList<String>();
    }
    if (doc == null) {
      LOGGER.warn("No parseable document found for insider form: {}", fileName);
      return new ArrayList<String>();
    }
    String cik = extractCik(doc, sourceFilePath);
    if (cik == null || cik.equals("0000000000")) {
      LOGGER.warn("Could not extract CIK from insider form: {}", fileName);
      return new ArrayList<String>();
    }
    String filingType = extractFilingType(doc, sourceFilePath);
    if (filingType == null || filingType.equals("UNKNOWN")) {
      String hintForm = metadata != null ? metadata.getHint("form") : null;
      filingType = hintForm != null ? hintForm : "4";
    }
    String periodEndDate = extractPeriodEndDate(doc, sourceFilePath);
    String hintFilingDate = metadata != null ? metadata.getHint("filingDate") : null;
    String actualFilingDate = (hintFilingDate != null) ? hintFilingDate : periodEndDate;
    if (actualFilingDate == null) {
      actualFilingDate = String.valueOf(java.time.Year.now().getValue()) + "-01-01";
    }
    LOGGER.info("Processing insider form {} CIK={} period={} accession={}",
        filingType, cik, periodEndDate, accession);
    return convertInsiderForm(doc, sourceFilePath, targetDirectoryPath, cik, filingType, actualFilingDate, accession);
  }

  /**
   * Convert Form 3/4/5 insider trading forms to Parquet.
   */
  private List<String> convertInsiderForm(Document doc, String sourcePath, String targetDirectoryPath,
      String cik, String filingType, String filingDate, String accession) throws IOException {
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    LOGGER.debug(" convertInsiderForm() START for " + filename + " - CIK: " + cik + ", Filing Type: " + filingType + ", Date: " + filingDate);
    List<String> outputFiles = new ArrayList<>();

    try {
      // Validate and parse year from filing date
      int year;
      try {
        year = Integer.parseInt(filingDate.substring(0, 4));
        // Sanity check - SEC filings shouldn't be from before 1934 or in the future
        if (year < 1934 || year > java.time.Year.now().getValue()) {
          LOGGER.warn("Invalid year " + year + " for filing date " + filingDate + ", using current year");
          year = java.time.Year.now().getValue();
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to parse year from filing date: " + filingDate + ", using current year");
        year = java.time.Year.now().getValue();
      }

      // Year-only partitioning for optimal 128MB file sizes
      // For insider forms (3/4/5), filing date is used for partition year (not 10-K/10-Q)
      String partitionYear = getPartitionYear(filingType, filingDate, filingDate, doc);

      // Build RELATIVE partition path (relative to targetDirectoryPath which already includes source=sec)
      // Uses year-only partitioning - CIK/filing_type filtering done via Parquet/Iceberg statistics
      String relativePartitionPath = String.format("year=%s", partitionYear);

      // Extract insider transactions
      List<Map<String, Object>> transactions = extractInsiderTransactions(doc, cik, filingType, filingDate, accession);
      LOGGER.debug(" Extracted " + transactions.size() + " insider transactions from " + filename);

      // Always write insider.parquet file (even if empty) to indicate processing completed
      // This prevents unnecessary reprocessing during cache validation
      String uniqueId = (accession != null && !accession.isEmpty()) ? accession : filingDate;
      String outputPath =
          storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_insider.parquet", cik, uniqueId));

      if (!transactions.isEmpty()) {
        LOGGER.debug(" Writing " + transactions.size() + " transactions to parquet file: " + outputPath);
      } else {
        LOGGER.debug(" Creating empty insider parquet file (no transactions found): " + outputPath);
      }

      java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns = loadInsiderTransactionColumns();
      storageProvider.writeAvroParquet(outputPath, columns, transactions, "InsiderTransaction", "insider_transactions");
      LOGGER.debug(" Successfully wrote insider transactions parquet file: " + outputPath);

      // CRITICAL: Add insider file to outputFiles so addToManifest() can detect it
      outputFiles.add(outputPath);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Added insider file to outputFiles: {}", outputPath);
      }

      LOGGER.info("Converted Form " + filingType + " to insider transactions: "
          + transactions.size() + " records");

      // Write filing_metadata for insider forms
      String metadataPath = storageProvider.resolvePath(targetDirectoryPath,
          relativePartitionPath + "/" + String.format("%s_%s_metadata.parquet", cik, uniqueId));
      writeMetadataToParquet(doc, metadataPath, cik, filingType, filingDate, accession, sourcePath);
      outputFiles.add(metadataPath);

      // Create vectorized chunks for insider forms if text similarity is enabled
      // Note: For now, we're creating a minimal vectorized file for insider forms
      // This could be enhanced to vectorize transaction narratives or remarks
      if (enableVectorization) {
        // Reuse uniqueId from above - build FULL path with StorageProvider
        String chunksPath =
            storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_chunks.parquet", cik, uniqueId));

        try {
          writeInsiderVectorizedChunksToParquet(doc, chunksPath, cik, filingType, filingDate, sourcePath, accession);
          // CRITICAL: Add chunks file to outputFiles so addToManifest() can detect it
          outputFiles.add(chunksPath);
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Added chunks file to outputFiles: {}", chunksPath);
          }
        } catch (Exception ve) {
          LOGGER.warn("Failed to create vectorized chunks for insider form: " + ve.getMessage());
        }
      }

    } catch (Exception e) {
      LOGGER.warn("Failed to convert insider form: " + e.getMessage());
    }

    return outputFiles;
  }

  /**
   * Extract insider transactions from Form 3/4/5.
   */
  private List<Map<String, Object>> extractInsiderTransactions(Document doc, String cik,
      String filingType, String filingDate, String accession) {
    List<Map<String, Object>> dataList = new ArrayList<>();
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns = loadInsiderTransactionColumns();

    // Handle multiple reporting owners properly
    NodeList reportingOwners = doc.getElementsByTagName("reportingOwner");

    // For multi-owner filings, create records for each reporting owner with all transactions
    // For single-owner filings, this will work as before
    if (reportingOwners.getLength() > 0) {
      for (int ownerIndex = 0; ownerIndex < reportingOwners.getLength(); ownerIndex++) {
        Element reportingOwner = (Element) reportingOwners.item(ownerIndex);

        // Extract this reporting owner's information
        String reportingPersonCik = getElementText(reportingOwner, "rptOwnerCik");
        String reportingPersonName = getElementText(reportingOwner, "rptOwnerName");

        // Extract this reporting owner's relationship
        boolean isDirector = "true".equals(getElementText(reportingOwner, "isDirector")) || "1".equals(getElementText(reportingOwner, "isDirector"));
        boolean isOfficer = "true".equals(getElementText(reportingOwner, "isOfficer")) || "1".equals(getElementText(reportingOwner, "isOfficer"));
        boolean isTenPercentOwner = "true".equals(getElementText(reportingOwner, "isTenPercentOwner")) || "1".equals(getElementText(reportingOwner, "isTenPercentOwner"));
        String officerTitle = getElementText(reportingOwner, "officerTitle");

        // Process all transaction and holding types for this reporting owner
        int sizeBefore = dataList.size();
        addNonDerivativeTransactions(doc, cik, filingType, filingDate, accession, reportingPersonCik, reportingPersonName,
            isDirector, isOfficer, isTenPercentOwner, officerTitle, columns, dataList);
        addNonDerivativeHoldings(doc, cik, filingType, filingDate, accession, reportingPersonCik, reportingPersonName,
            isDirector, isOfficer, isTenPercentOwner, officerTitle, columns, dataList);
        addDerivativeTransactions(doc, cik, filingType, filingDate, accession, reportingPersonCik, reportingPersonName,
            isDirector, isOfficer, isTenPercentOwner, officerTitle, columns, dataList);
        addDerivativeHoldings(doc, cik, filingType, filingDate, accession, reportingPersonCik, reportingPersonName,
            isDirector, isOfficer, isTenPercentOwner, officerTitle, columns, dataList);

        // If no transactions or holdings were found (e.g., Form 3 with noSecuritiesOwned),
        // create an initial appointment record to preserve the insider relationship data
        if (dataList.size() == sizeBefore) {
          addInitialAppointmentRecord(cik, filingType, filingDate, accession,
              reportingPersonCik, reportingPersonName, isDirector, isOfficer,
              isTenPercentOwner, officerTitle, dataList);
        }
      }
    } else {
      // Fallback for documents without explicit reporting owner structure
      // Use the old method for extracting reporting owner information globally
      String reportingPersonCik = getElementText(doc, "rptOwnerCik");
      String reportingPersonName = getElementText(doc, "rptOwnerName");
      boolean isDirector = "1".equals(getElementText(doc, "isDirector"));
      boolean isOfficer = "1".equals(getElementText(doc, "isOfficer"));
      boolean isTenPercentOwner = "1".equals(getElementText(doc, "isTenPercentOwner"));
      String officerTitle = getElementText(doc, "officerTitle");

      addNonDerivativeTransactions(doc, cik, filingType, filingDate, accession, reportingPersonCik, reportingPersonName,
          isDirector, isOfficer, isTenPercentOwner, officerTitle, columns, dataList);
      addNonDerivativeHoldings(doc, cik, filingType, filingDate, accession, reportingPersonCik, reportingPersonName,
          isDirector, isOfficer, isTenPercentOwner, officerTitle, columns, dataList);
      addDerivativeTransactions(doc, cik, filingType, filingDate, accession, reportingPersonCik, reportingPersonName,
          isDirector, isOfficer, isTenPercentOwner, officerTitle, columns, dataList);
      addDerivativeHoldings(doc, cik, filingType, filingDate, accession, reportingPersonCik, reportingPersonName,
          isDirector, isOfficer, isTenPercentOwner, officerTitle, columns, dataList);

      if (dataList.isEmpty()) {
        addInitialAppointmentRecord(cik, filingType, filingDate, accession,
            reportingPersonCik, reportingPersonName, isDirector, isOfficer,
            isTenPercentOwner, officerTitle, dataList);
      }
    }

    return dataList;
  }

  /**
   * Add non-derivative transactions for a specific reporting owner.
   */
  private void addNonDerivativeTransactions(Document doc, String cik, String filingType, String filingDate,
      String accession, String reportingPersonCik, String reportingPersonName, boolean isDirector, boolean isOfficer,
      boolean isTenPercentOwner, String officerTitle,
      java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns,
      List<Map<String, Object>> dataList) {

    NodeList nonDerivTrans = doc.getElementsByTagName("nonDerivativeTransaction");
    for (int i = 0; i < nonDerivTrans.getLength(); i++) {
      Element trans = (Element) nonDerivTrans.item(i);

      Map<String, Object> data = new HashMap<>();
      data.put("accession_number", accession);
      data.put("cik", cik);
      data.put("filing_date", filingDate);
      data.put("filing_type", filingType);
      // Extract year for Iceberg partitioning
      data.put("year", extractYearFromDate(filingDate));
      data.put("reporting_person_cik", reportingPersonCik);
      data.put("reporting_person_name", reportingPersonName);
      data.put("is_director", isDirector);
      data.put("is_officer", isOfficer);
      data.put("is_ten_percent_owner", isTenPercentOwner);
      data.put("officer_title", officerTitle);

      // Transaction details
      data.put("transaction_date", getElementText(trans, "transactionDate", "value"));
      data.put("transaction_code", getElementText(trans, "transactionCode"));
      data.put("security_title", getElementText(trans, "securityTitle", "value"));

      String shares = getElementText(trans, "transactionShares", "value");
      data.put("shares_transacted", shares != null ? Double.parseDouble(shares) : null);

      String price = getElementText(trans, "transactionPricePerShare", "value");
      data.put("price_per_share", price != null ? Double.parseDouble(price) : null);

      String sharesAfter = getElementText(trans, "sharesOwnedFollowingTransaction", "value");
      data.put("shares_owned_after", sharesAfter != null ? Double.parseDouble(sharesAfter) : null);

      String acquiredDisposed = getElementText(trans, "transactionAcquiredDisposedCode", "value");
      data.put("acquired_disposed_code", acquiredDisposed);

      String ownership = getElementText(trans, "directOrIndirectOwnership", "value");
      data.put("ownership_type", ownership);

      // Extract footnotes if any
      NodeList footnoteIds = trans.getElementsByTagName("footnoteId");
      StringBuilder footnotes = new StringBuilder();
      for (int j = 0; j < footnoteIds.getLength(); j++) {
        String id = ((Element) footnoteIds.item(j)).getAttribute("id");
        String footnoteText = getFootnoteText(doc, id);
        if (footnoteText != null) {
          if (footnotes.length() > 0) footnotes.append(" | ");
          footnotes.append(footnoteText);
        }
      }
      data.put("footnotes", footnotes.length() > 0 ? footnotes.toString() : null);

      dataList.add(data);
    }
  }

  /**
   * Add non-derivative holdings for a specific reporting owner.
   */
  private void addNonDerivativeHoldings(Document doc, String cik, String filingType, String filingDate,
      String accession, String reportingPersonCik, String reportingPersonName, boolean isDirector, boolean isOfficer,
      boolean isTenPercentOwner, String officerTitle,
      java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns,
      List<Map<String, Object>> dataList) {

    NodeList holdings = doc.getElementsByTagName("nonDerivativeHolding");
    for (int i = 0; i < holdings.getLength(); i++) {
      Element holding = (Element) holdings.item(i);

      Map<String, Object> data = new HashMap<>();
      data.put("accession_number", accession);
      data.put("cik", cik);
      data.put("filing_date", filingDate);
      data.put("filing_type", filingType);
      // Extract year for Iceberg partitioning
      data.put("year", extractYearFromDate(filingDate));
      data.put("reporting_person_cik", reportingPersonCik);
      data.put("reporting_person_name", reportingPersonName);
      data.put("is_director", isDirector);
      data.put("is_officer", isOfficer);
      data.put("is_ten_percent_owner", isTenPercentOwner);
      data.put("officer_title", officerTitle);

      // Holding details (no transaction)
      data.put("transaction_date", null);
      data.put("transaction_code", "H"); // H for holding
      data.put("security_title", getElementText(holding, "securityTitle", "value"));
      data.put("shares_transacted", null);
      data.put("price_per_share", null);

      String shares = getElementText(holding, "sharesOwnedFollowingTransaction", "value");
      data.put("shares_owned_after", shares != null ? Double.parseDouble(shares) : null);

      data.put("acquired_disposed_code", null);

      String ownership = getElementText(holding, "directOrIndirectOwnership", "value");
      data.put("ownership_type", ownership);

      String natureOfOwnership = getElementText(holding, "natureOfOwnership", "value");
      data.put("footnotes", natureOfOwnership);

      dataList.add(data);
    }
  }

  /**
   * Add derivative transactions for a specific reporting owner.
   */
  private void addDerivativeTransactions(Document doc, String cik, String filingType, String filingDate,
      String accession, String reportingPersonCik, String reportingPersonName, boolean isDirector, boolean isOfficer,
      boolean isTenPercentOwner, String officerTitle,
      java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns,
      List<Map<String, Object>> dataList) {

    NodeList derivTrans = doc.getElementsByTagName("derivativeTransaction");
    for (int i = 0; i < derivTrans.getLength(); i++) {
      Element trans = (Element) derivTrans.item(i);

      Map<String, Object> data = new HashMap<>();
      data.put("accession_number", accession);
      data.put("cik", cik);
      data.put("filing_date", filingDate);
      data.put("filing_type", filingType);
      // Extract year for Iceberg partitioning
      data.put("year", extractYearFromDate(filingDate));
      data.put("reporting_person_cik", reportingPersonCik);
      data.put("reporting_person_name", reportingPersonName);
      data.put("is_director", isDirector);
      data.put("is_officer", isOfficer);
      data.put("is_ten_percent_owner", isTenPercentOwner);
      data.put("officer_title", officerTitle);

      // Transaction details for derivatives
      data.put("transaction_date", getElementText(trans, "transactionDate", "value"));
      data.put("transaction_code", getElementText(trans, "transactionCode"));

      // For derivatives, append " (Derivative)" to distinguish from non-derivative
      String secTitle = getElementText(trans, "securityTitle", "value");
      data.put("security_title", secTitle != null ? secTitle + " (Derivative)" : "Option/Warrant (Derivative)");

      String shares = getElementText(trans, "transactionShares", "value");
      data.put("shares_transacted", shares != null ? Double.parseDouble(shares) : null);

      // For derivatives, use conversion/exercise price if available
      String price = getElementText(trans, "transactionPricePerShare", "value");
      if (price == null || price.isEmpty()) {
        price = getElementText(trans, "conversionOrExercisePrice", "value");
      }
      data.put("price_per_share", price != null && !price.isEmpty() ? Double.parseDouble(price) : null);

      String sharesAfter = getElementText(trans, "sharesOwnedFollowingTransaction", "value");
      data.put("shares_owned_after", sharesAfter != null ? Double.parseDouble(sharesAfter) : null);

      String acquiredDisposed = getElementText(trans, "transactionAcquiredDisposedCode", "value");
      data.put("acquired_disposed_code", acquiredDisposed);

      String ownership = getElementText(trans, "directOrIndirectOwnership", "value");
      data.put("ownership_type", ownership);

      // Extract footnotes if any
      NodeList footnoteIds = trans.getElementsByTagName("footnoteId");
      StringBuilder footnotes = new StringBuilder();
      for (int j = 0; j < footnoteIds.getLength(); j++) {
        String id = ((Element) footnoteIds.item(j)).getAttribute("id");
        String footnoteText = getFootnoteText(doc, id);
        if (footnoteText != null) {
          if (footnotes.length() > 0) footnotes.append(" | ");
          footnotes.append(footnoteText);
        }
      }

      // Also add exercise date and expiration date if available
      String exerciseDate = getElementText(trans, "exerciseDate", "value");
      String expirationDate = getElementText(trans, "expirationDate", "value");
      if (exerciseDate != null || expirationDate != null) {
        if (footnotes.length() > 0) footnotes.append(" | ");
        if (exerciseDate != null) footnotes.append("Exercise: ").append(exerciseDate);
        if (expirationDate != null) {
          if (exerciseDate != null) footnotes.append(", ");
          footnotes.append("Expires: ").append(expirationDate);
        }
      }

      data.put("footnotes", footnotes.length() > 0 ? footnotes.toString() : null);

      dataList.add(data);
    }
  }

  /**
   * Add derivative holdings for a specific reporting owner.
   */
  private void addDerivativeHoldings(Document doc, String cik, String filingType, String filingDate,
      String accession, String reportingPersonCik, String reportingPersonName, boolean isDirector, boolean isOfficer,
      boolean isTenPercentOwner, String officerTitle,
      java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns,
      List<Map<String, Object>> dataList) {

    NodeList derivHoldings = doc.getElementsByTagName("derivativeHolding");
    for (int i = 0; i < derivHoldings.getLength(); i++) {
      Element holding = (Element) derivHoldings.item(i);

      Map<String, Object> data = new HashMap<>();
      data.put("accession_number", accession);
      data.put("cik", cik);
      data.put("filing_date", filingDate);
      data.put("filing_type", filingType);
      // Extract year for Iceberg partitioning
      data.put("year", extractYearFromDate(filingDate));
      data.put("reporting_person_cik", reportingPersonCik);
      data.put("reporting_person_name", reportingPersonName);
      data.put("is_director", isDirector);
      data.put("is_officer", isOfficer);
      data.put("is_ten_percent_owner", isTenPercentOwner);
      data.put("officer_title", officerTitle);

      // Holding details for derivatives
      data.put("transaction_date", null);
      data.put("transaction_code", "H"); // H for holding

      String secTitle = getElementText(holding, "securityTitle", "value");
      data.put("security_title", secTitle != null ? secTitle + " (Derivative)" : "Option/Warrant Holding (Derivative)");

      data.put("shares_transacted", null);

      // For derivative holdings, get conversion/exercise price
      String price = getElementText(holding, "conversionOrExercisePrice", "value");
      data.put("price_per_share", price != null && !price.isEmpty() ? Double.parseDouble(price) : null);

      String shares = getElementText(holding, "sharesOwnedFollowingTransaction", "value");
      data.put("shares_owned_after", shares != null ? Double.parseDouble(shares) : null);

      data.put("acquired_disposed_code", null);

      String ownership = getElementText(holding, "directOrIndirectOwnership", "value");
      data.put("ownership_type", ownership);

      // Add expiration date info to footnotes
      StringBuilder footnotes = new StringBuilder();
      String expirationDate = getElementText(holding, "expirationDate", "value");
      if (expirationDate != null) {
        footnotes.append("Expires: ").append(expirationDate);
      }
      String natureOfOwnership = getElementText(holding, "natureOfOwnership", "value");
      if (natureOfOwnership != null) {
        if (footnotes.length() > 0) footnotes.append(" | ");
        footnotes.append(natureOfOwnership);
      }
      data.put("footnotes", footnotes.length() > 0 ? footnotes.toString() : null);

      dataList.add(data);
    }
  }

  /**
   * Creates an initial appointment record for insiders with no securities owned.
   *
   * <p>Form 3 filings with {@code <noSecuritiesOwned>} indicate a newly appointed insider
   * who holds zero shares. This method preserves the insider relationship data
   * (name, CIK, role flags, officer title) that would otherwise be lost.
   *
   * <p>The record uses {@code transaction_code = "I"} (Initial appointment, no holdings)
   * with {@code shares_owned_after = 0} to distinguish from holdings ({@code "H"})
   * and transactions ({@code "P"}, {@code "S"}, etc.).
   */
  private void addInitialAppointmentRecord(String cik, String filingType, String filingDate,
      String accession, String reportingPersonCik, String reportingPersonName,
      boolean isDirector, boolean isOfficer, boolean isTenPercentOwner, String officerTitle,
      List<Map<String, Object>> dataList) {

    Map<String, Object> data = new HashMap<>();
    data.put("accession_number", accession);
    data.put("cik", cik);
    data.put("filing_date", filingDate);
    data.put("filing_type", filingType);
    data.put("year", extractYearFromDate(filingDate));
    data.put("reporting_person_cik", reportingPersonCik);
    data.put("reporting_person_name", reportingPersonName);
    data.put("is_director", isDirector);
    data.put("is_officer", isOfficer);
    data.put("is_ten_percent_owner", isTenPercentOwner);
    data.put("officer_title", officerTitle);
    data.put("transaction_date", filingDate);
    data.put("transaction_code", "I"); // I = Initial appointment, no securities owned
    data.put("security_title", null);
    data.put("shares_transacted", null);
    data.put("price_per_share", null);
    data.put("shares_owned_after", 0.0);
    data.put("acquired_disposed_code", null);
    data.put("ownership_type", null);
    data.put("footnotes", "No securities beneficially owned at time of initial filing");

    dataList.add(data);
  }

  /**
   * Load schema for insider transactions from metadata.
   */
  private java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> loadInsiderTransactionColumns() {
    return AbstractSecDataDownloader.loadTableColumns("insider_transactions");
  }

  /**
   * Helper to get element text with optional nested element.
   */
  private String getElementText(Element parent, String tagName, String nestedTag) {
    NodeList elements = parent.getElementsByTagName(tagName);
    if (elements.getLength() > 0) {
      Element elem = (Element) elements.item(0);
      if (nestedTag != null) {
        NodeList nested = elem.getElementsByTagName(nestedTag);
        if (nested.getLength() > 0) {
          return nested.item(0).getTextContent().trim();
        }
      } else {
        return elem.getTextContent().trim();
      }
    }
    return null;
  }

  /**
   * Helper to get element text.
   */
  private String getElementText(Document doc, String tagName) {
    NodeList elements = doc.getElementsByTagName(tagName);
    if (elements.getLength() > 0) {
      return elements.item(0).getTextContent().trim();
    }
    return null;
  }

  /**
   * Helper to get element text from parent.
   */
  private String getElementText(Element parent, String tagName) {
    return getElementText(parent, tagName, null);
  }

  /**
   * Get footnote text by ID.
   */
  private String getFootnoteText(Document doc, String footnoteId) {
    NodeList footnotes = doc.getElementsByTagName("footnote");
    for (int i = 0; i < footnotes.getLength(); i++) {
      Element footnote = (Element) footnotes.item(i);
      if (footnoteId.equals(footnote.getAttribute("id"))) {
        return footnote.getTextContent().trim();
      }
    }
    return null;
  }

  /**
   * Write Parquet file using StorageProvider's consolidated method.
   */
  private void writeParquetFile(List<GenericRecord> records, Schema schema, String outputPath)
      throws IOException {
    // Complete single-threading of all vectorization operations to ensure 100% thread safety
    synchronized (GLOBAL_VECTORIZATION_LOCK) {
      storageProvider.writeAvroParquet(outputPath, schema, records, "vectorized");
    }
  }


  /**
   * Process an 8-K filing as plain HTML (no XBRL required).
   * Reads override metadata from ConversionMetadata hints with path-based fallbacks.
   */
  private List<String> process8KHtml(String sourceFilePath, String targetDirectoryPath,
      ConversionMetadata metadata) throws IOException {
    List<String> outputFiles = new ArrayList<>();

    String hintCik = metadata != null ? metadata.getHint("cik") : null;
    String hintForm = metadata != null ? metadata.getHint("form") : null;
    String hintDate = metadata != null ? metadata.getHint("filingDate") : null;
    String hintAccession = metadata != null ? metadata.getHint("accession") : null;

    String cik = hintCik != null ? hintCik : extractCikFromPath(sourceFilePath);
    String filingType = hintForm != null ? hintForm : "8-K";
    String filingDate = hintDate;
    String accession = hintAccession != null ? hintAccession
        : extractAccessionFromPath(sourceFilePath);

    if (cik == null || cik.equals("0000000000")) {
      LOGGER.warn("Skipping 8-K HTML processing - invalid CIK from: {}", sourceFilePath);
      return outputFiles;
    }
    if (filingDate == null) {
      LOGGER.warn("Skipping 8-K HTML processing - no filing date for: {}", sourceFilePath);
      return outputFiles;
    }

    LOGGER.info("Processing 8-K as plain HTML: cik={}, date={}, accession={}", cik, filingDate, accession);

    List<String> extraFiles = extract8KExhibits(
        sourceFilePath, targetDirectoryPath, cik, filingType, filingDate, accession, true);
    outputFiles.addAll(extraFiles);

    return outputFiles;
  }

  /**
   * Check if this is an 8-K filing.
   */
  private boolean is8KFiling(String filingType) {
    return filingType != null && (filingType.equals("8-K") || filingType.equals("8K")
        || filingType.startsWith("8-K/"));
  }

  /**
   * Check if this is a 13F-HR filing (institutional holdings).
   */
  private boolean is13FFiling(String filingType) {
    return filingType != null && (filingType.equals("13F-HR")
        || filingType.startsWith("13F-HR/"));
  }

  /**
   * Check if this is a Schedule 13D or 13G filing (beneficial ownership).
   */
  private boolean is13DGFiling(String filingType) {
    return filingType != null && (filingType.startsWith("SC 13D")
        || filingType.startsWith("SC 13G"));
  }

  /**
   * Extract ALL item sections from 8-K HTML into vectorized_chunks format.
   * Parses headers like "Item 1.01", "Item 5.02", "Item 8.01" and captures
   * text between each header and the next (or SIGNATURES).
   */
  private List<Map<String, Object>> extract8KItems(String fileContent,
      String cik, String filingDate, String accession, int year) {
    List<Map<String, Object>> chunks = new ArrayList<>();
    Pattern itemPattern = Pattern.compile("Item\\s+(\\d+\\.\\d+)", Pattern.CASE_INSENSITIVE);

    org.jsoup.nodes.Document doc = Jsoup.parse(fileContent);
    doc.select("script, style").remove();
    String bodyText = doc.body() != null ? doc.body().text() : doc.text();

    // Find all item header positions
    Matcher matcher = itemPattern.matcher(bodyText);
    List<int[]> itemPositions = new ArrayList<>(); // [start, end, -] pairs
    List<String> itemNumbers = new ArrayList<>();
    while (matcher.find()) {
      itemPositions.add(new int[]{matcher.start(), matcher.end()});
      itemNumbers.add(matcher.group(1));
    }

    // Find SIGNATURES position as end boundary
    int sigPos = bodyText.toUpperCase().indexOf("SIGNATURES");
    if (sigPos < 0) {
      sigPos = bodyText.length();
    }

    int sequence = 0;
    Set<String> boilerplate = new HashSet<>();
    boilerplate.add("FORWARD-LOOKING STATEMENTS");
    boilerplate.add("Safe Harbor");
    boilerplate.add("forward-looking statements");

    for (int i = 0; i < itemPositions.size(); i++) {
      String itemNumber = itemNumbers.get(i);
      int textStart = itemPositions.get(i)[1];
      int textEnd = (i + 1 < itemPositions.size()) ? itemPositions.get(i + 1)[0] : sigPos;
      if (textStart >= textEnd) {
        continue;
      }

      String sectionText = bodyText.substring(textStart, textEnd).trim();
      // Split into paragraphs by sentence boundaries or double spaces
      String[] parts = sectionText.split("(?<=\\.)\\s{2,}|\\n\\s*\\n");

      int paraSeq = 0;
      for (String part : parts) {
        String para = part.trim();
        if (para.length() < 50) {
          continue;
        }
        // Skip boilerplate
        boolean isBoilerplate = false;
        for (String bp : boilerplate) {
          if (para.contains(bp)) {
            isBoilerplate = true;
            break;
          }
        }
        if (isBoilerplate) {
          continue;
        }

        Map<String, Object> chunk = new HashMap<>();
        chunk.put("cik", cik);
        chunk.put("accession_number", accession);
        chunk.put("year", year);
        chunk.put("chunk_id", "item_" + itemNumber + "_" + paraSeq);
        chunk.put("source_type", "8k_item");
        chunk.put("section", "Item " + itemNumber);
        chunk.put("sequence", sequence++);
        chunk.put("filing_date", filingDate);
        chunk.put("chunk_text", para);
        chunk.put("enriched_text", para);
        chunk.put("content_type", "paragraph");
        chunk.put("financial_concepts", null);

        chunks.add(chunk);
        paraSeq++;
      }
    }

    return chunks;
  }

  /**
   * Write filing_metadata record for an 8-K accession.
   * Follows the pattern of writeMetadataToParquet() but without XBRL DEI fields.
   */
  private void write8KMetadata(String fileContent, String outputPath,
      String cik, String filingType, String filingDate, String accession,
      String sourcePath) throws IOException {

    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractSecDataDownloader.loadTableColumns("filing_metadata");

    Map<String, Object> data = new HashMap<>();
    data.put("cik", cik);
    data.put("accession_number", accession);
    data.put("filing_type", filingType);
    data.put("filing_date", filingDate);

    int year = 0;
    if (filingDate != null && filingDate.length() >= 4) {
      try {
        year = Integer.parseInt(filingDate.substring(0, 4));
      } catch (NumberFormatException e) {
        LOGGER.warn("Failed to parse year from filing date: {}", filingDate);
      }
    }
    data.put("year", year);

    // Extract company name from HTML <title> tag if available
    org.jsoup.nodes.Document doc = Jsoup.parse(fileContent);
    org.jsoup.nodes.Element titleEl = doc.selectFirst("title");
    data.put("company_name", titleEl != null ? titleEl.text().trim() : null);

    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    data.put("primary_document", filename);
    data.put("document_type", filingType);

    // DEI fields not available from plain HTML (no XBRL)
    data.put("state_of_incorporation", null);
    data.put("fiscal_year_end", null);
    data.put("business_address", null);
    data.put("mailing_address", null);
    data.put("phone", null);
    data.put("period_end_date", null);
    data.put("period_of_report", null);
    data.put("sic_code", null);
    data.put("irs_number", null);
    data.put("acceptance_datetime", null);
    data.put("file_size", null);
    data.put("fiscal_year", null);

    List<Map<String, Object>> dataList = new ArrayList<>();
    dataList.add(data);

    storageProvider.writeAvroParquet(outputPath, columns, dataList, "FilingMetadata", "FilingMetadata");
    LOGGER.info("Wrote 8-K filing metadata to {}", outputPath);
  }

  /**
   * Extract 8-K exhibits (particularly 99.1 and 99.2 for earnings).
   */
  /**
   * Write vectorized chunks for insider forms (Form 3/4/5).
   * Creates minimal vectors for remarks and transaction descriptions.
   * Uses the vectorized_chunks schema for consistency.
   */
  private void writeInsiderVectorizedChunksToParquet(Document doc, String outputPath,
      String cik, String filingType, String filingDate, String sourcePath, String accession) throws IOException {

    // Load schema from metadata - use vectorized_chunks for consistency
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractSecDataDownloader.loadTableColumns("vectorized_chunks");

    List<Map<String, Object>> dataList = new ArrayList<>();
    int sequence = 0;

    // Extract year from filing date for Iceberg partitioning
    int year = 0;
    if (filingDate != null && filingDate.length() >= 4) {
      try {
        year = Integer.parseInt(filingDate.substring(0, 4));
      } catch (NumberFormatException e) {
        LOGGER.warn("Could not parse year from filing date: {}", filingDate);
      }
    }

    // Extract remarks and footnotes from insider forms
    NodeList remarks = doc.getElementsByTagName("remarks");
    NodeList footnotes = doc.getElementsByTagName("footnote");

    // Process remarks
    for (int i = 0; i < remarks.getLength(); i++) {
      String remarkText = remarks.item(i).getTextContent().trim();
      if (!remarkText.isEmpty() && remarkText.length() > 20) {
        Map<String, Object> data = new HashMap<>();
        data.put("cik", cik);
        data.put("accession_number", accession);
        data.put("year", year);  // Required for Iceberg partitioning
        data.put("chunk_id", "remark_" + i);
        data.put("source_type", "insider_remark");
        data.put("section", "Form " + filingType);
        data.put("sequence", sequence++);
        data.put("filing_date", filingDate);
        data.put("chunk_text", remarkText);
        data.put("enriched_text", remarkText); // No enrichment for simple remarks
        // embedding computed by DuckDB at materialization time
        data.put("content_type", "remark");
        data.put("financial_concepts", null);

        dataList.add(data);
      }
    }

    // Process footnotes
    for (int i = 0; i < footnotes.getLength(); i++) {
      String footnoteText = footnotes.item(i).getTextContent().trim();
      if (!footnoteText.isEmpty() && footnoteText.length() > 20) {
        Map<String, Object> data = new HashMap<>();
        data.put("cik", cik);
        data.put("accession_number", accession);
        data.put("year", year);  // Required for Iceberg partitioning
        data.put("chunk_id", "footnote_" + i);
        data.put("source_type", "insider_footnote");
        data.put("section", "Form " + filingType);
        data.put("sequence", sequence++);
        data.put("filing_date", filingDate);
        data.put("chunk_text", footnoteText);
        data.put("enriched_text", footnoteText); // No enrichment for simple footnotes
        // embedding computed by DuckDB at materialization time
        data.put("content_type", "footnote");
        data.put("financial_concepts", null);

        dataList.add(data);
      }
    }

    // Always write the file, even if empty, to satisfy cache validation
    storageProvider.writeAvroParquet(outputPath, columns, dataList, "VectorizedChunk", "vectorized_chunks");
    if (!dataList.isEmpty()) {
      LOGGER.info("Wrote " + dataList.size() + " vectorized insider chunks to " + outputPath);
    } else {
      LOGGER.info("Created empty chunks file (no content > 20 chars) for " + outputPath);
    }
  }

  private List<String> extract8KExhibits(String sourcePath, String targetDirectoryPath,
      String cik, String filingType, String filingDate, String accession,
      boolean writeMetadata) {
    List<String> outputFiles = new ArrayList<>();

    try {
      // Parse the 8-K filing to find exhibits
      String fileContent;
      try (InputStream is = storageProvider.openInputStream(sourcePath)) {
        fileContent = new String(readAllBytes(is), java.nio.charset.StandardCharsets.UTF_8);
      }

      // Validate and parse year
      int year;
      try {
        year = Integer.parseInt(filingDate.substring(0, 4));
        if (year < 1934 || year > java.time.Year.now().getValue()) {
          LOGGER.warn("Invalid year " + year + " for filing date " + filingDate);
          year = java.time.Year.now().getValue();
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to parse year from filing date: " + filingDate);
        year = java.time.Year.now().getValue();
      }

      String partitionYear = filingDate.substring(0, 4);
      String relativePartitionPath = String.format("year=%s", partitionYear);
      String uniqueId = (accession != null && !accession.isEmpty()) ? accession : filingDate;

      // 1. Write filing_metadata for 8-K accession (skip when XBRL path handles it with richer DEI data)
      if (writeMetadata) {
        String metadataPath = storageProvider.resolvePath(targetDirectoryPath,
            relativePartitionPath + "/" + String.format("%s_%s_metadata.parquet", cik, uniqueId));
        write8KMetadata(fileContent, metadataPath, cik, filingType, filingDate, accession, sourcePath);
        outputFiles.add(metadataPath);
      }

      // 2. Existing earnings extraction (unchanged)
      List<Map<String, Object>> earningsRecords = new ArrayList<>();
      java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> earningsColumns = loadEarningsTranscriptColumns();

      // Check if this file contains exhibit content directly
      if (fileContent.contains("EX-99.1") || fileContent.contains("EX-99.2")) {
        earningsRecords.addAll(extractEarningsFromExhibit(fileContent, cik, filingType, filingDate, accession));
      }

      // Also check for earnings-related content patterns
      if (fileContent.toLowerCase().contains("financial results")
          || fileContent.toLowerCase().contains("earnings release")
          || fileContent.toLowerCase().contains("conference call")) {

        List<String> paragraphs = extractEarningsParagraphs(fileContent);

        for (int i = 0; i < paragraphs.size(); i++) {
          Map<String, Object> data = new HashMap<>();
          data.put("accession_number", accession);
          data.put("cik", cik);
          data.put("filing_date", filingDate);
          data.put("year", year);
          data.put("filing_type", filingType);
          data.put("exhibit_number", detectExhibitNumber(fileContent));
          data.put("section_type", detectSectionType(paragraphs.get(i)));
          data.put("paragraph_number", i + 1);
          data.put("paragraph_text", paragraphs.get(i));
          data.put("speaker_name", extractSpeaker(paragraphs.get(i)));
          data.put("speaker_role", extractSpeakerRole(paragraphs.get(i)));

          earningsRecords.add(data);
        }
      }

      // 3. Write earnings_transcripts (unchanged - only when earnings content found)
      if (!earningsRecords.isEmpty()) {
        String earningsPath = storageProvider.resolvePath(targetDirectoryPath,
            relativePartitionPath + "/" + String.format("%s_%s_earnings.parquet", cik, uniqueId));

        storageProvider.writeAvroParquet(earningsPath, earningsColumns, earningsRecords, "EarningsTranscript", "earnings_transcripts");
        outputFiles.add(earningsPath);

        LOGGER.info("Extracted " + earningsRecords.size() + " earnings paragraphs from 8-K");
      }

      // 4. Extract ALL item sections + merge with earnings chunks
      if (enableVectorization) {
        // Build earnings chunks list (without writing separately)
        List<Map<String, Object>> allChunks = new ArrayList<>();

        // Convert earnings records to chunk format
        int sequence = 0;
        for (Map<String, Object> earnings : earningsRecords) {
          String paragraphText = (String) earnings.get("paragraph_text");
          if (paragraphText == null || paragraphText.length() < 20) {
            continue;
          }
          Map<String, Object> chunk = new HashMap<>();
          chunk.put("cik", cik);
          chunk.put("accession_number", accession);
          chunk.put("year", year);
          chunk.put("chunk_id", "earnings_" + earnings.get("paragraph_number"));
          chunk.put("source_type", "earnings");
          chunk.put("section", earnings.get("section_type"));
          chunk.put("sequence", sequence++);
          chunk.put("filing_date", filingDate);
          chunk.put("chunk_text", paragraphText);
          chunk.put("enriched_text", paragraphText);
          chunk.put("content_type", "paragraph");
          chunk.put("financial_concepts", null);
          chunk.put("exhibit_number", earnings.get("exhibit_number"));
          chunk.put("speaker_name", earnings.get("speaker_name"));
          chunk.put("speaker_role", earnings.get("speaker_role"));
          chunk.put("paragraph_number", earnings.get("paragraph_number"));
          allChunks.add(chunk);
        }

        // Extract all 8-K item sections
        List<Map<String, Object>> itemChunks = extract8KItems(fileContent, cik, filingDate, accession, year);

        // Dedup: if earnings covered Item 2.02, skip item chunks for that section
        // (earnings version has richer metadata like speaker/exhibit)
        Set<String> earningsSections = new HashSet<>();
        if (!earningsRecords.isEmpty()) {
          earningsSections.add("Item 2.02"); // Results of Operations - typically the earnings item
        }

        for (Map<String, Object> itemChunk : itemChunks) {
          String section = (String) itemChunk.get("section");
          if (earningsSections.contains(section)) {
            continue; // Earnings version takes priority
          }
          // Re-sequence to follow earnings chunks
          itemChunk.put("sequence", sequence++);
          allChunks.add(itemChunk);
        }

        // 5. Write combined chunks parquet
        if (!allChunks.isEmpty()) {
          java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> chunkColumns =
              AbstractSecDataDownloader.loadTableColumns("vectorized_chunks");
          String chunksPath = storageProvider.resolvePath(targetDirectoryPath,
              relativePartitionPath + "/" + String.format("%s_%s_chunks.parquet", cik, uniqueId));
          storageProvider.writeAvroParquet(chunksPath, chunkColumns, allChunks, "VectorizedChunk", "vectorized_chunks");
          outputFiles.add(chunksPath);
          LOGGER.info("Wrote " + allChunks.size() + " combined chunks (earnings + items) to " + chunksPath);
        }
      }

    } catch (Exception e) {
      LOGGER.warn("Failed to extract 8-K exhibits: " + e.getMessage());
    }

    return outputFiles;
  }

  /**
   * Write earnings records to vectorized_chunks for semantic search.
   * Links back to earnings_transcripts via exhibit_number, paragraph_number.
   */
  private void writeEarningsVectorizedChunks(List<Map<String, Object>> earningsRecords,
      String outputPath, String cik, String filingDate, String accession, int year) throws IOException {

    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractSecDataDownloader.loadTableColumns("vectorized_chunks");

    List<Map<String, Object>> chunksList = new ArrayList<>();
    int sequence = 0;

    for (Map<String, Object> earnings : earningsRecords) {
      String paragraphText = (String) earnings.get("paragraph_text");
      if (paragraphText == null || paragraphText.length() < 20) {
        continue;
      }

      Map<String, Object> chunk = new HashMap<>();
      chunk.put("cik", cik);
      chunk.put("accession_number", accession);
      chunk.put("year", year);
      chunk.put("chunk_id", "earnings_" + earnings.get("paragraph_number"));
      chunk.put("source_type", "earnings");
      chunk.put("section", earnings.get("section_type"));
      chunk.put("sequence", sequence++);
      chunk.put("filing_date", filingDate);
      chunk.put("chunk_text", paragraphText);
      chunk.put("enriched_text", paragraphText); // Basic enrichment - could enhance later
      chunk.put("content_type", "paragraph");
      chunk.put("financial_concepts", null);
      // Earnings-specific columns for linking back
      chunk.put("exhibit_number", earnings.get("exhibit_number"));
      chunk.put("speaker_name", earnings.get("speaker_name"));
      chunk.put("speaker_role", earnings.get("speaker_role"));
      chunk.put("paragraph_number", earnings.get("paragraph_number"));

      chunksList.add(chunk);
    }

    storageProvider.writeAvroParquet(outputPath, columns, chunksList, "VectorizedChunk", "vectorized_chunks");

    if (!chunksList.isEmpty()) {
      LOGGER.info("Wrote " + chunksList.size() + " earnings chunks to vectorized_chunks: " + outputPath);
    }
  }

  /**
   * Extract earnings content from exhibit text.
   */
  private List<Map<String, Object>> extractEarningsFromExhibit(String exhibitContent,
      String cik, String filingType, String filingDate, String accession) {
    List<Map<String, Object>> dataList = new ArrayList<>();
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns = loadEarningsTranscriptColumns();

    // Parse as HTML to extract text content
    org.jsoup.nodes.Document doc = Jsoup.parse(exhibitContent);

    // Remove script and style elements
    doc.select("script, style").remove();

    // Extract paragraphs
    Elements paragraphs = doc.select("p, div");

    int paragraphNum = 0;
    for (org.jsoup.nodes.Element para : paragraphs) {
      String text = para.text().trim();

      // Skip empty or very short paragraphs
      if (text.length() < 50) continue;

      // Skip boilerplate
      if (text.contains("FORWARD-LOOKING STATEMENTS")
          || text.contains("Safe Harbor")
          || text.contains("Copyright")) continue;

      paragraphNum++;

      Map<String, Object> data = new HashMap<>();
      data.put("accession_number", accession);
      data.put("cik", cik);
      data.put("filing_date", filingDate);
      data.put("year", Integer.parseInt(filingDate.substring(0, 4)));
      data.put("filing_type", filingType);
      data.put("exhibit_number", detectExhibitNumber(exhibitContent));
      data.put("section_type", detectSectionType(text));
      data.put("paragraph_number", paragraphNum);
      data.put("paragraph_text", text);
      data.put("speaker_name", extractSpeaker(text));
      data.put("speaker_role", extractSpeakerRole(text));

      dataList.add(data);
    }

    return dataList;
  }

  /**
   * Extract earnings-related paragraphs from text.
   */
  private List<String> extractEarningsParagraphs(String content) {
    List<String> paragraphs = new ArrayList<>();

    // Parse as HTML
    org.jsoup.nodes.Document doc = Jsoup.parse(content);

    // Look for earnings-related sections
    Elements relevantElements = doc.select("p, div");

    boolean inEarningsSection = false;
    for (org.jsoup.nodes.Element elem : relevantElements) {
      String text = elem.text().trim();

      // Start capturing when we find earnings indicators
      if (text.contains("financial results") || text.contains("earnings")
          || text.contains("revenue") || text.contains("quarter")) {
        inEarningsSection = true;
      }

      // Capture relevant paragraphs
      if (inEarningsSection && text.length() > 50) {
        // Skip legal boilerplate
        if (!text.contains("forward-looking") && !text.contains("Safe Harbor")) {
          paragraphs.add(text);
        }
      }

      // Stop at certain sections
      if (text.contains("SIGNATURES") || text.contains("EXHIBIT INDEX")) {
        break;
      }
    }

    return paragraphs;
  }

  /**
   * Detect exhibit number from content.
   */
  private String detectExhibitNumber(String content) {
    if (content.contains("EX-99.1") || content.contains("Exhibit 99.1")) {
      return "99.1";
    } else if (content.contains("EX-99.2") || content.contains("Exhibit 99.2")) {
      return "99.2";
    }
    return null;
  }

  /**
   * Detect section type from paragraph content.
   */
  private String detectSectionType(String text) {
    String lowerText = text.toLowerCase();

    if (lowerText.contains("prepared remarks") || lowerText.contains("opening remarks")) {
      return "prepared_remarks";
    } else if (lowerText.contains("question") || lowerText.contains("answer")) {
      return "q_and_a";
    } else if (lowerText.contains("financial results") || lowerText.contains("earnings")) {
      return "earnings_release";
    } else if (lowerText.contains("conference call") || lowerText.contains("transcript")) {
      return "transcript";
    }

    return "other";
  }

  /**
   * Extract speaker name from paragraph (for transcripts).
   */
  private String extractSpeaker(String text) {
    // Look for patterns like "Name - Title:" or "Name:"
    Pattern speakerPattern = Pattern.compile("^([A-Z][a-z]+ [A-Z][a-z]+)\\s*[-:]");
    Matcher matcher = speakerPattern.matcher(text);

    if (matcher.find()) {
      return matcher.group(1);
    }

    return null;
  }

  /**
   * Extract speaker role from paragraph.
   */
  private String extractSpeakerRole(String text) {
    // Look for patterns like "Name - CEO:" or "Name, Chief Executive Officer:"
    Pattern rolePattern = Pattern.compile("[-,]\\s*([^:]+):");
    Matcher matcher = rolePattern.matcher(text);

    if (matcher.find()) {
      String role = matcher.group(1).trim();

      // Normalize common roles
      if (role.contains("Chief Executive") || role.contains("CEO")) {
        return "CEO";
      } else if (role.contains("Chief Financial") || role.contains("CFO")) {
        return "CFO";
      } else if (role.contains("Analyst")) {
        return "Analyst";
      } else if (role.contains("Operator")) {
        return "Operator";
      }

      return role;
    }

    return null;
  }

  /**
   * Create schema for earnings transcripts.
   */
  private java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> loadEarningsTranscriptColumns() {
    return AbstractSecDataDownloader.loadTableColumns("earnings_transcripts");
  }

  /**
   * Write vectorized chunks with contextual enrichment to Parquet.
   * Creates individual vectors for footnotes and MD&A paragraphs with relationships.
   * Text is enriched with context tags and normalized for consistent embeddings.
   * Embeddings are NOT stored here - they are computed by DuckDB at materialization time.
   *
   * @param mdaData Pre-extracted MD&A data from extractMDAData() - single source of truth
   */
  private void writeVectorizedChunksToParquet(Document doc, String outputPath,
      String cik, String filingType, String filingDate, String sourcePath,
      List<Map<String, Object>> mdaData, List<Map<String, Object>> factsData) throws IOException {

    // Load schema from metadata (now vectorized_chunks)
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractSecDataDownloader.loadTableColumns("vectorized_chunks");

    List<Map<String, Object>> dataList = new ArrayList<>();

    // Extract period end date for text normalization context
    String periodEnd = extractPeriodEndDateFromDocument(doc);
    if (periodEnd == null) {
      periodEnd = filingDate; // Fall back to filing date
    }

    // Extract footnotes as TextBlobs
    List<SecTextVectorizer.TextBlob> footnoteBlobs = extractFootnoteBlobs(doc);

    // Convert pre-extracted MDA data to TextBlobs (uses SAME text as mda_sections)
    LOGGER.info("VECTORIZATION: Converting {} MDA sections to blobs", mdaData != null ? mdaData.size() : 0);
    List<SecTextVectorizer.TextBlob> mdaBlobs = convertMDADataToBlobs(mdaData);
    LOGGER.info("VECTORIZATION: Created {} MDA blobs", mdaBlobs.size());

    // Build relationship map (who references whom)
    LOGGER.info("VECTORIZATION: Building reference map from {} footnotes and {} MDA blobs",
        footnoteBlobs.size(), mdaBlobs.size());
    Map<String, List<String>> references = buildReferenceMap(footnoteBlobs, mdaBlobs);
    LOGGER.info("VECTORIZATION: Built reference map with {} entries", references.size());

    // Build financial facts map from already-extracted facts data (avoids redundant DOM traversal)
    Map<String, SecTextVectorizer.FinancialFact> facts = new HashMap<>();
    for (Map<String, Object> row : factsData) {
      String concept = (String) row.get("concept");
      Object numericValue = row.get("numeric_value");
      if (concept != null && numericValue instanceof Number) {
        String period = row.get("period_end") != null
            ? (String) row.get("period_end") : "unknown";
        facts.put(concept, new SecTextVectorizer.FinancialFact(
            concept, ((Number) numericValue).doubleValue(), period));
      }
    }
    LOGGER.info("VECTORIZATION: Built {} financial facts from extracted data", facts.size());

    // Create vectorizer instance
    SecTextVectorizer vectorizer = new SecTextVectorizer();

    // Generate individual enriched chunks with filing context for text normalization
    // filingDate and periodEnd allow TextNormalizer to resolve relative dates
    LOGGER.info("VECTORIZATION: Creating individual chunks with filingDate={}, periodEnd={}", filingDate, periodEnd);
    List<SecTextVectorizer.ContextualChunk> individualChunks =
        vectorizer.createIndividualChunks(footnoteBlobs, mdaBlobs,
            new ArrayList<>(), references, facts, filingDate, periodEnd);
    LOGGER.info("VECTORIZATION: Created {} individual chunks", individualChunks.size());

    List<SecTextVectorizer.ContextualChunk> allChunks = individualChunks;
    LOGGER.info("Total chunks for vectorization: {}", allChunks.size());

    // Extract accession number from output path (e.g., CIK_ACCESSION_chunks.parquet)
    String accessionNumber = null;
    String filename = outputPath.substring(outputPath.lastIndexOf('/') + 1);
    if (filename.contains("_")) {
      String[] parts = filename.replace("_chunks.parquet", "").split("_");
      if (parts.length >= 2) {
        accessionNumber = parts[1];
      }
    }

    // Extract year from filing date for Iceberg partitioning
    int year = 0;
    if (filingDate != null && filingDate.length() >= 4) {
      try {
        year = Integer.parseInt(filingDate.substring(0, 4));
      } catch (NumberFormatException e) {
        LOGGER.warn("Could not parse year from filing date: {}", filingDate);
      }
    }

    // Convert chunks to Parquet records
    int sequence = 0;
    for (SecTextVectorizer.ContextualChunk chunk : allChunks) {
      Map<String, Object> data = new HashMap<>();

      // Required identifiers for Iceberg materialization
      data.put("cik", cik);
      data.put("accession_number", accessionNumber != null ? accessionNumber : cik + "-" + filingDate);
      data.put("year", year);

      // Core identifiers
      data.put("chunk_id", chunk.originalBlobId != null ? chunk.originalBlobId : chunk.context);
      data.put("source_type", chunk.blobType);
      data.put("section", chunk.metadata.get("parent_section"));
      data.put("sequence", sequence++);
      data.put("filing_date", filingDate);

      // Text columns - chunk_text is original before normalization, enriched_text is normalized
      // The applyNormalization method stores original enriched text in metadata
      String originalEnrichedText = chunk.metadata.containsKey("original_enriched_text") ?
          (String) chunk.metadata.get("original_enriched_text") : chunk.text;
      data.put("chunk_text", truncateText(originalEnrichedText, 32000));
      data.put("enriched_text", truncateText(chunk.text, 32000));  // This is the normalized text

      // Content type classification
      data.put("content_type", inferContentType(chunk.text));

      // Financial concepts
      if (chunk.metadata.containsKey("financial_concepts")) {
        @SuppressWarnings("unchecked")
        List<String> concepts = (List<String>) chunk.metadata.get("financial_concepts");
        data.put("financial_concepts", String.join(",", concepts));
      }

      // Note: embedding column is NOT set here - it's a computed column
      // that will be populated by DuckDB quackformers at materialization time:
      // expression: "embed_jina(enriched_text)::FLOAT[768]"

      dataList.add(data);
    }

    // Always write file, even if empty (zero rows) to ensure cache consistency
    storageProvider.writeAvroParquet(outputPath, columns, dataList, "VectorizedChunk", "vectorized_chunks");
    LOGGER.info("Successfully wrote " + dataList.size() + " vectorized chunks to " + outputPath);
  }

  /**
   * Infer content type from text structure.
   *
   * @param text The text to analyze
   * @return Content type: paragraph, table, list, heading, or mixed
   */
  private String inferContentType(String text) {
    if (text == null || text.isEmpty()) {
      return "paragraph";
    }

    // Check for table indicators (multiple columns, alignment)
    if (text.contains("\t") || text.matches(".*\\|.*\\|.*")
        || (text.contains("$") && text.split("\\$").length > 3)) {
      return "table";
    }

    // Check for list indicators
    if (text.matches("(?s).*^\\s*[•\\-\\*\\d]+[.\\)]\\s+.*")
        || text.contains("\n• ") || text.contains("\n- ") || text.contains("\n* ")) {
      return "list";
    }

    // Check for heading indicators (short, ends without period, possibly all caps)
    if (text.length() < 100 && !text.endsWith(".") && !text.endsWith(",")) {
      String trimmed = text.trim();
      if (trimmed.equals(trimmed.toUpperCase()) || trimmed.matches("^(Item|Note|Part)\\s+\\d+.*")) {
        return "heading";
      }
    }

    // Check if mixed content (contains multiple types)
    int indicators = 0;
    if (text.contains("\t") || text.contains("|")) indicators++;
    if (text.contains("\n• ") || text.contains("\n- ")) indicators++;
    if (text.split("\n").length > 5) indicators++;
    if (indicators >= 2) {
      return "mixed";
    }

    return "paragraph";
  }

  /**
   * Extract footnotes as TextBlob objects for vectorization.
   */
  private List<SecTextVectorizer.TextBlob> extractFootnoteBlobs(Document doc) {
    List<SecTextVectorizer.TextBlob> blobs = new ArrayList<>();

    // Extract footnotes from both traditional XBRL and inline XBRL (iXBRL) TextBlock elements
    // These contain actual narrative text in XBRL filings
    NodeList allElements = doc.getElementsByTagName("*");
    int footnoteCounter = 0;

    // Track seen text content to deduplicate across traditional XBRL and inline XBRL.
    // In iXBRL filings (post-2020), the same TextBlock content appears both as a
    // traditional XBRL element AND as an ix:nonNumeric wrapper, causing every
    // footnote to be extracted twice. We deduplicate by concept+text hash.
    java.util.Set<Long> seenTextHashes = new java.util.HashSet<>();
    int duplicatesSkipped = 0;

    for (int i = 0; i < allElements.getLength(); i++) {
      Element element = (Element) allElements.item(i);
      String localName = element.getLocalName();
      String namespaceURI = element.getNamespaceURI();

      String concept = null;
      String text = null;

      // Check for traditional XBRL TextBlock elements
      if (localName != null && localName.endsWith("TextBlock")) {
        text = element.getTextContent().trim();
        concept = extractConceptName(element);
      }
      // Check for inline XBRL (iXBRL) TextBlock elements
      else if ("nonNumeric".equals(localName) && namespaceURI != null
               && namespaceURI.contains("xbrl")) {
        String name = element.getAttribute("name");
        if (name != null && name.contains("TextBlock")) {
          // For inline XBRL, the text content is in the element
          text = element.getTextContent().trim();
          // Extract concept from the name attribute (e.g., "us-gaap:RevenueRecognitionPolicyTextBlock")
          concept = name.contains(":") ? name.substring(name.indexOf(":") + 1) : name;
        }
      }
      // Check for traditional XBRL link:footnote elements (pre-2019 companion XBRL)
      // These are inside <link:footnoteLink> and contain XHTML content
      else if ("footnote".equals(localName) && namespaceURI != null
               && namespaceURI.contains("xbrl")) {
        text = element.getTextContent().trim();
        String footnoteId = element.getAttribute("id");
        concept = "FootnoteAnnotation"
            + (footnoteId != null && !footnoteId.isEmpty() ? "_" + footnoteId : "");
      }

      // Process extracted text if found
      // link:footnote elements use a lower threshold (50 chars) since they are
      // explicit footnote annotations that may be shorter than TextBlock narratives
      int minLength = "FootnoteAnnotation".equals(concept)
          || (concept != null && concept.startsWith("FootnoteAnnotation_")) ? 50 : 200;
      if (text != null && text.length() > minLength && !text.startsWith("<")) {
        // Deduplicate: hash concept name + text length + first/last 200 chars.
        // Using concept + text signature avoids O(n) string comparison while
        // correctly deduplicating the same content from traditional vs inline XBRL.
        String conceptKey = concept != null ? concept : "";
        String textSig = text.length() <= 400 ? text
            : text.substring(0, 200) + text.substring(text.length() - 200);
        long hash = conceptKey.hashCode() * 31L + textSig.hashCode();
        if (!seenTextHashes.add(hash)) {
          duplicatesSkipped++;
          continue;
        }

        String contextRef = element.getAttribute("contextRef");

        // Determine footnote section from concept name
        String parentSection = determineFootnoteSection(concept);

        if (parentSection != null) {
          String id = "footnote_" + (++footnoteCounter);

          // Create footnote blob with metadata
          Map<String, String> attributes = new HashMap<>();
          attributes.put("concept", concept != null ? concept : "");
          attributes.put("contextRef", contextRef != null ? contextRef : "");

          SecTextVectorizer.TextBlob blob =
              new SecTextVectorizer.TextBlob(id, "footnote", text, parentSection, null, attributes);
          blobs.add(blob);

          LOGGER.debug("Extracted footnote: " + id + " from concept: " + concept);
        }
      }
    }

    if (duplicatesSkipped > 0) {
      LOGGER.info("Deduplicated {} duplicate footnotes (iXBRL dual extraction)", duplicatesSkipped);
    }
    LOGGER.info("Extracted " + blobs.size() + " footnote blobs from XBRL");
    return blobs;
  }

  /**
   * Determine the section for a footnote based on its concept name.
   */
  private String determineFootnoteSection(String concept) {
    if (concept == null) return "Notes to Financial Statements";

    String lowerConcept = concept.toLowerCase();

    // Map common footnote concepts to sections
    if (lowerConcept.contains("accountingpolicy") || lowerConcept.contains("significantaccounting")) {
      return "Significant Accounting Policies";
    } else if (lowerConcept.contains("revenue")) {
      return "Revenue Recognition";
    } else if (lowerConcept.contains("debt") || lowerConcept.contains("borrowing")) {
      return "Debt and Credit Facilities";
    } else if (lowerConcept.contains("equity") || lowerConcept.contains("stock")) {
      return "Stockholders Equity";
    } else if (lowerConcept.contains("segment")) {
      return "Segment Information";
    } else if (lowerConcept.contains("commitment") || lowerConcept.contains("contingenc")) {
      return "Commitments and Contingencies";
    } else if (lowerConcept.contains("acquisition") || lowerConcept.contains("businesscombination")) {
      return "Business Combinations";
    } else if (lowerConcept.contains("incometax") || lowerConcept.contains("tax")) {
      return "Income Taxes";
    } else if (lowerConcept.contains("pension") || lowerConcept.contains("retirement")) {
      return "Employee Benefits";
    } else if (lowerConcept.contains("derivative") || lowerConcept.contains("hedge")) {
      return "Derivatives and Hedging";
    } else if (lowerConcept.contains("fairvalue")) {
      return "Fair Value Measurements";
    } else if (lowerConcept.contains("lease")) {
      return "Leases";
    } else if (lowerConcept.contains("textblock") || lowerConcept.contains("disclosure")) {
      return "Notes to Financial Statements";
    }

    return "Notes to Financial Statements";  // Default section
  }

  /**
   * Convert pre-extracted MDA data to TextBlobs for vectorization.
   * This ensures vectorized_chunks uses the EXACT same text as mda_sections.
   */
  private List<SecTextVectorizer.TextBlob> convertMDADataToBlobs(List<Map<String, Object>> mdaData) {
    List<SecTextVectorizer.TextBlob> blobs = new ArrayList<>();

    int counter = 0;
    // Pending header text to propagate as the subsection of subsequent content paragraphs.
    // Section headers (e.g., "Item 7. Management's Discussion and Analysis...")
    // should not be standalone chunks — they become the subsection in the
    // [HIERARCHY: Management Discussion and Analysis > <header text>] tag.
    String pendingHeader = null;
    int headersAbsorbed = 0;

    for (Map<String, Object> data : mdaData) {
      String text = (String) data.get("paragraph_text");
      if (text == null || text.length() <= 50) {
        continue;
      }
      String subsection = (String) data.getOrDefault("subsection", "PARAGRAPH");

      // Detect standalone section headers: either classified as HEADING by the chunker,
      // or matching SEC Item header patterns (Item 7, Item 7A, etc.)
      if ("HEADING".equals(subsection) || isSectionHeader(text)) {
        // Accumulate headers (there can be multiple, e.g., "Item 7" followed by
        // "Management's Discussion and Analysis of Financial Condition and Results of Operations")
        if (pendingHeader != null) {
          pendingHeader = pendingHeader + " — " + text.trim();
        } else {
          pendingHeader = text.trim();
        }
        continue;
      }

      // Normal content paragraph — use pending header as subsection if available.
      // This flows into the existing [HIERARCHY: parentSection > subsection] tag
      // in vectorizeMDAParagraph(), giving the embedding model section context.
      String effectiveSubsection;
      if (pendingHeader != null) {
        effectiveSubsection = pendingHeader;
        headersAbsorbed++;
        // Don't clear pendingHeader — it applies to ALL paragraphs in this section
        // until the next header replaces it
      } else {
        effectiveSubsection = subsection;
      }

      String id = "mda_para_" + (++counter);

      Map<String, String> attributes = new HashMap<>();
      attributes.put("source", "mda_sections");

      SecTextVectorizer.TextBlob blob =
          new SecTextVectorizer.TextBlob(id, "mda_paragraph", text,
              "Management Discussion and Analysis", effectiveSubsection, attributes);
      blobs.add(blob);
    }

    // Trailing header with no following content is dropped (no content to contextualize)

    if (headersAbsorbed > 0) {
      LOGGER.info("Absorbed {} section headers into [HIERARCHY:] subsections", headersAbsorbed);
    }
    LOGGER.debug("Converted {} MDA records to {} TextBlobs", mdaData.size(), blobs.size());
    return blobs;
  }

  /**
   * Detects standalone section headers in SEC filings.
   * Matches patterns like "Item 7.", "ITEM 7A.", "Management's Discussion and Analysis...",
   * "Results of Operations", etc.
   */
  private static final java.util.regex.Pattern SEC_ITEM_PATTERN =
      java.util.regex.Pattern.compile(
          "(?i)^\\s*(item\\s+\\d+[a-z]?\\.?\\s*[-—]?\\s*)?"
          + "(management.{0,5}discussion|results\\s+of\\s+operations"
          + "|financial\\s+condition|liquidity\\s+and\\s+capital"
          + "|quantitative\\s+and\\s+qualitative|critical\\s+accounting"
          + "|risk\\s+factors|controls\\s+and\\s+procedures).*$");

  private boolean isSectionHeader(String text) {
    if (text == null) {
      return false;
    }
    String trimmed = text.trim();
    // Short text that matches Item N pattern or known section titles
    if (trimmed.length() > 300) {
      return false;  // Real content, not a header
    }
    // Check for "Item N" prefix
    if (trimmed.matches("(?i)^\\s*item\\s+\\d+[a-z]?\\.?\\s*$")) {
      return true;
    }
    // Check for known SEC section header patterns
    return SEC_ITEM_PATTERN.matcher(trimmed).matches();
  }

  /**
   * Detect MD&A subsection from paragraph content.
   */
  private String detectMDASubsection(String text) {
    if (text == null || text.length() < 20) return null;

    String lower = text.toLowerCase();

    // Check for common MD&A subsections
    if (lower.contains("overview") || lower.contains("executive summary")) {
      return "Overview";
    } else if (lower.contains("results") && lower.contains("operation")) {
      return "Results of Operations";
    } else if (lower.contains("liquidity") && lower.contains("capital")) {
      return "Liquidity and Capital Resources";
    } else if (lower.contains("liquidity")) {
      return "Liquidity";
    } else if (lower.contains("capital") && (lower.contains("resource") || lower.contains("cash flow"))) {
      return "Capital Resources";
    } else if (lower.contains("critical") && lower.contains("accounting")) {
      return "Critical Accounting Policies";
    } else if (lower.contains("risk factor") || lower.contains("uncertainties")) {
      return "Risk Factors";
    } else if (lower.contains("outlook") || lower.contains("guidance")) {
      return "Outlook";
    } else if (lower.contains("segment") || lower.contains("geographic")) {
      return "Segment Analysis";
    }

    return null;  // No specific subsection detected
  }

  // Method already exists above - removing duplicate

  /**
   * Check if text is boilerplate that should be skipped.
   */
  private boolean isBoilerplate(String text) {
    if (text == null || text.isEmpty()) return true;
    String lower = text.toLowerCase();
    return lower.contains("forward-looking statements") ||
           lower.contains("safe harbor") ||
           lower.contains("table of contents") ||
           lower.contains("exhibit index") ||
           lower.startsWith("page ") ||
           (lower.length() < 20 && lower.matches("\\d+"));
  }

  /**
   * Build a map of footnote references from MD&A paragraphs.
   */
  private Map<String, List<String>> buildReferenceMap(
      List<SecTextVectorizer.TextBlob> footnotes,
      List<SecTextVectorizer.TextBlob> mdaBlobs) {

    Map<String, List<String>> references = new HashMap<>();

    // Check each MD&A paragraph for footnote references
    for (SecTextVectorizer.TextBlob mdaBlob : mdaBlobs) {
      java.util.regex.Pattern pattern =
          java.util.regex.Pattern.compile("(?:Note|Footnote)\\s+(\\d+[A-Za-z]?)", java.util.regex.Pattern.CASE_INSENSITIVE);
      java.util.regex.Matcher matcher = pattern.matcher(mdaBlob.text);

      while (matcher.find()) {
        String footnoteRef = "footnote_" + matcher.group(1);

        // Add MD&A paragraph to the footnote's reference list
        references.computeIfAbsent(footnoteRef, k -> new ArrayList<>()).add(mdaBlob.id);
      }
    }

    return references;
  }


  /**
   * Generate a simple embedding for short text (used for insider forms).
   * This is a placeholder implementation that creates deterministic vectors.
   */
  private List<Float> generateSimpleEmbedding(String text, int dimension) {
    List<Float> embedding = new ArrayList<>(dimension);

    // Simple hash-based approach for deterministic embeddings
    int hash = text.hashCode();
    java.util.Random random = new java.util.Random(hash);

    for (int i = 0; i < dimension; i++) {
      embedding.add(random.nextFloat() * 2.0f - 1.0f); // Range [-1, 1]
    }

    // Normalize the vector
    float sum = 0;
    for (float val : embedding) {
      sum += val * val;
    }
    float norm = (float) Math.sqrt(sum);
    if (norm > 0) {
      for (int i = 0; i < embedding.size(); i++) {
        embedding.set(i, embedding.get(i) / norm);
      }
    }

    return embedding;
  }

  /**
   * Convert object to JSON string.
   */
  private String toJsonString(Object obj) {
    // Simple JSON conversion - in production would use Jackson or Gson
    if (obj instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) obj;
      StringBuilder json = new StringBuilder("{");
      boolean first = true;
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        if (!first) json.append(",");
        json.append("\"").append(entry.getKey()).append("\":");
        if (entry.getValue() instanceof List) {
          json.append("[");
          List<?> list = (List<?>) entry.getValue();
          for (int i = 0; i < list.size(); i++) {
            if (i > 0) json.append(",");
            json.append("\"").append(list.get(i)).append("\"");
          }
          json.append("]");
        } else {
          json.append("\"").append(entry.getValue()).append("\"");
        }
        first = false;
      }
      json.append("}");
      return json.toString();
    }
    return obj != null ? obj.toString() : null;
  }

  /**
   * Truncate text to fit Parquet string limits.
   */
  private String truncateText(String text, int maxLength) {
    if (text == null || text.length() <= maxLength) {
      return text;
    }
    return text.substring(0, maxLength - 3) + "...";
  }

  private String extractCikFromPath(String path) {
    // Extract CIK from path like "/sec/0000320193/000032019323000106/"
    Pattern pattern = Pattern.compile("/(\\d{10})/");
    Matcher matcher = pattern.matcher(path);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }

  private String extractFilingTypeFromPath(String sourcePath) {
    // Try to determine filing type from filename and parent directory
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1).toLowerCase();
    String parentPath = sourcePath.substring(0, sourcePath.lastIndexOf('/'));
    String parentName = parentPath.substring(parentPath.lastIndexOf('/') + 1).toLowerCase();

    // Check filename patterns
    if (filename.contains("10k") || filename.contains("10-k") || parentName.contains("10k")) {
      return "10-K";
    } else if (filename.contains("10q") || filename.contains("10-q") || parentName.contains("10q")) {
      return "10-Q";
    } else if (filename.contains("8k") || filename.contains("8-k") ||
               filename.contains("_8k") || parentName.contains("8k")) {
      return "8-K";
    } else if (filename.contains("def14a") || parentName.contains("def14a")) {
      return "DEF14A";
    }

    // Try to infer from filename patterns
    // Files like "aapl-20240928.htm" are usually 10-K/10-Q based on date
    if (filename.matches(".*-20\\d{6}\\.htm.*")) {
      // Check if it's end of September (Q4 - likely 10-K)
      if (filename.contains("0930") || filename.contains("0928")) {
        return "10-K";
      } else if (filename.contains("0331") || filename.contains("0330") ||
                 filename.contains("0630") || filename.contains("0629") ||
                 filename.contains("1231") || filename.contains("1230")) {
        return "10-Q";
      }
    }

    // Default to 8-K for other HTML files as they're most common
    return "8-K";
  }

  private String extractFilingDateFromFilename(String filename) {
    // Extract date from filename like "aapl-20230930.htm" or "msft-20240630.htm"
    // Also handle 8-K names like "ny20007635x4_8k.htm" or "d447690d8k.htm"
    Pattern pattern = Pattern.compile("(20\\d{6})");  // Look for dates starting with 20
    Matcher matcher = pattern.matcher(filename);
    if (matcher.find()) {
      String dateStr = matcher.group(1);
      // Convert YYYYMMDD to YYYY-MM-DD
      if (dateStr.length() == 8) {
        String year = dateStr.substring(0, 4);
        String month = dateStr.substring(4, 6);
        String day = dateStr.substring(6, 8);
        // Validate the date components
        try {
          int y = Integer.parseInt(year);
          int m = Integer.parseInt(month);
          int d = Integer.parseInt(day);
          if (y >= 2000 && y <= 2030 && m >= 1 && m <= 12 && d >= 1 && d <= 31) {
            return year + "-" + month + "-" + day;
          }
        } catch (NumberFormatException e) {
          // Invalid date format
        }
      }
    }

    // If no valid date found, try to use current year
    // This is a fallback for files without dates in the name
    return "2024-01-01";  // Default date for filing
  }

  private String extractAccessionFromPath(String path) {
    // Extract accession from path like "/sec/0000320193/000032019323000106/"
    Pattern pattern = Pattern.compile("/(\\d{18})/");
    Matcher matcher = pattern.matcher(path);
    if (matcher.find()) {
      String acc = matcher.group(1);
      // Format as 0000320193-23-000106
      return acc.substring(0, 10) + "-" + acc.substring(10, 12) + "-" + acc.substring(12);
    }
    return null;
  }

  private void createEnhancedMetadata(String sourcePath, String targetDirectoryPath, String cik,
      String filingType, String filingDate, String accession, Map<String, String> companyInfo) throws IOException {
    // Build partition path - uses year-only partitioning
    String partitionYear = filingDate.substring(0, 4); // For metadata without doc context, use filing year
    String relativePartitionPath = String.format("year=%s", partitionYear);

    // Build metadata file path with FULL path from StorageProvider
    String metadataPath =
        storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_metadata.parquet", cik, (accession != null && !accession.isEmpty()) ? accession : filingDate));

    // Load schema from metadata
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractSecDataDownloader.loadTableColumns("filing_metadata");

    List<Map<String, Object>> dataList = new ArrayList<>();
    Map<String, Object> data = new HashMap<>();
    data.put("accession_number", accession != null ? accession : cik + "-" + filingDate);
    data.put("filing_date", filingDate);
    data.put("company_name", companyInfo.get("company_name"));
    data.put("state_of_incorporation", companyInfo.get("state_of_incorporation"));
    data.put("fiscal_year_end", companyInfo.get("fiscal_year_end"));
    data.put("sic_code", companyInfo.get("sic_code"));
    data.put("irs_number", companyInfo.get("irs_number"));
    data.put("business_address", companyInfo.get("business_address"));
    data.put("mailing_address", companyInfo.get("mailing_address"));
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    data.put("filing_url", filename);
    dataList.add(data);

    // Use consolidated StorageProvider method for Parquet writing
    storageProvider.writeAvroParquet(metadataPath, columns, dataList, "FilingMetadata", "filing_metadata");
  }

  private void createMinimalMetadata(String sourcePath, String targetDirectoryPath, String cik,
      String filingType, String filingDate, String accession) throws IOException {
    // Build partition path - uses year-only partitioning
    String partitionYear = filingDate.substring(0, 4); // For metadata without doc context, use filing year
    String relativePartitionPath = String.format("year=%s", partitionYear);

    // Build metadata file path with FULL path from StorageProvider
    String metadataPath =
        storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_metadata.parquet", cik, (accession != null && !accession.isEmpty()) ? accession : filingDate));

    // Load schema from metadata
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractSecDataDownloader.loadTableColumns("filing_metadata");

    List<Map<String, Object>> dataList = new ArrayList<>();
    Map<String, Object> data = new HashMap<>();
    data.put("accession_number", accession != null ? accession : cik + "-" + filingDate);
    data.put("filing_date", filingDate);
    // We don't have company info from inline XBRL that failed to parse
    data.put("company_name", null);
    data.put("state_of_incorporation", null);
    data.put("fiscal_year_end", null);
    data.put("sic_code", null);
    data.put("irs_number", null);
    data.put("business_address", null);
    data.put("mailing_address", null);
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    data.put("filing_url", filename);
    dataList.add(data);

    // Use consolidated StorageProvider method for Parquet writing
    storageProvider.writeAvroParquet(metadataPath, columns, dataList, "FilingMetadata", "filing_metadata");
  }

  /**
   * Determine the appropriate year for partitioning based on filing type.
   * For 10-K/10-Q and their amendments, use fiscal year (from period end date).
   * For all other filings, use actual filing year (SEC submission date).
   *
   * @param filingType SEC form type (e.g., "10-K", "10-Q/A")
   * @param actualFilingDate SEC submission date (YYYY-MM-DD)
   * @param periodEndDate Period end date extracted from document (YYYY-MM-DD), may be null
   * @param doc Parsed XBRL document for fallback extraction
   */
  private String getPartitionYear(String filingType, String actualFilingDate,
      String periodEndDate, Document doc) {
    String normalizedType = filingType.replace("-", "").replace("/", "");

    // For 10-K/10-Q and their amendments (10-KA, 10-QA), use fiscal year from period end date
    if ("10Q".equals(normalizedType) || "10K".equals(normalizedType)
        || "10QA".equals(normalizedType) || "10KA".equals(normalizedType)
        || "10KSB".equals(normalizedType) || "10KSBA".equals(normalizedType)
        || "10QSB".equals(normalizedType) || "10QSBA".equals(normalizedType)) {
      // Prefer periodEndDate passed from caller (extracted by extractPeriodEndDate)
      if (periodEndDate != null && periodEndDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
        return periodEndDate.substring(0, 4);
      }
      // Fall back to document extraction
      String docPeriodEnd = extractPeriodEndDateFromDocument(doc);
      if (docPeriodEnd != null && docPeriodEnd.matches("\\d{4}-\\d{2}-\\d{2}")) {
        return docPeriodEnd.substring(0, 4);
      }
    }

    // For all other filing types, use actual filing year (SEC submission date)
    return actualFilingDate.substring(0, 4);
  }

  /**
   * Extract period end date from XBRL document using a heuristic chain.
   * Used by getPartitionYear() and writeVectorizedChunksToParquet() for fiscal year
   * determination and text normalization context.
   *
   * <p>Heuristic order:
   * 1. DocumentPeriodEndDate DEI element (most authoritative)
   * 2. Collect all endDate values, dedup, sort descending, pick latest where year <= current year
   */
  private String extractPeriodEndDateFromDocument(Document doc) {
    try {
      // 1. Try DocumentPeriodEndDate DEI element (most authoritative)
      NodeList deiNodes = doc.getElementsByTagNameNS("*", "DocumentPeriodEndDate");
      if (deiNodes.getLength() > 0) {
        String date = deiNodes.item(0).getTextContent().trim();
        if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
          return date;
        }
      }

      // Also try with dei: prefix (inline XBRL)
      deiNodes = doc.getElementsByTagName("dei:DocumentPeriodEndDate");
      if (deiNodes.getLength() > 0) {
        String date = deiNodes.item(0).getTextContent().trim();
        if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
          return date;
        }
      }

      // 2. Collect all endDate values, dedup, pick latest where year <= current year
      int maxYear = java.time.Year.now().getValue();
      Set<String> allDates = new HashSet<>();

      // Try multiple tag name variants
      String[] tagNames = {"endDate", "xbrli:endDate"};
      for (String tagName : tagNames) {
        NodeList endDateNodes = doc.getElementsByTagName(tagName);
        for (int i = 0; i < endDateNodes.getLength(); i++) {
          String date = endDateNodes.item(i).getTextContent().trim();
          if (date.matches("\\d{4}-\\d{2}-\\d{2}")
              && Integer.parseInt(date.substring(0, 4)) <= maxYear) {
            allDates.add(date);
          }
        }
      }

      // Also try namespace-aware lookup
      NodeList nsEndDates = doc.getElementsByTagNameNS("*", "endDate");
      for (int i = 0; i < nsEndDates.getLength(); i++) {
        String date = nsEndDates.item(i).getTextContent().trim();
        if (date.matches("\\d{4}-\\d{2}-\\d{2}")
            && Integer.parseInt(date.substring(0, 4)) <= maxYear) {
          allDates.add(date);
        }
      }

      // Sort descending and pick latest
      if (!allDates.isEmpty()) {
        List<String> sorted = new ArrayList<>(allDates);
        java.util.Collections.sort(sorted, java.util.Collections.reverseOrder());
        return sorted.get(0);
      }

      // Try period elements with endDate as fallback
      NodeList periodNodes = doc.getElementsByTagName("period");
      for (int i = 0; i < periodNodes.getLength(); i++) {
        Element periodElement = (Element) periodNodes.item(i);
        NodeList endDates = periodElement.getElementsByTagName("endDate");
        if (endDates.getLength() > 0) {
          String date = endDates.item(0).getTextContent().trim();
          if (date.matches("\\d{4}-\\d{2}-\\d{2}")
              && Integer.parseInt(date.substring(0, 4)) <= maxYear) {
            return date;
          }
        }
      }

      return null;
    } catch (Exception e) {
      LOGGER.warn("Error extracting period end date: " + e.getMessage());
      return null;
    }
  }

  // =========================================================================
  // 13F-HR Processing — Institutional Holdings
  // =========================================================================

  /**
   * Process a 13F-HR filing. Parses the informationTable XML to extract
   * per-security holdings with share counts, market value, and voting authority.
   */
  private List<String> process13FForm(String sourceFilePath, String targetDirectoryPath,
      ConversionMetadata metadata) throws IOException {
    List<String> outputFiles = new ArrayList<>();

    String hintCik = metadata != null ? metadata.getHint("cik") : null;
    String hintForm = metadata != null ? metadata.getHint("form") : null;
    String hintDate = metadata != null ? metadata.getHint("filingDate") : null;
    String hintAccession = metadata != null ? metadata.getHint("accession") : null;

    String cik = hintCik != null ? hintCik : extractCikFromPath(sourceFilePath);
    String filingType = hintForm != null ? hintForm : "13F-HR";
    String filingDate = hintDate;
    String accession = hintAccession != null ? hintAccession
        : extractAccessionFromPath(sourceFilePath);

    if (cik == null || cik.equals("0000000000")) {
      LOGGER.warn("Skipping 13F processing - invalid CIK from: {}", sourceFilePath);
      return outputFiles;
    }
    if (filingDate == null) {
      LOGGER.warn("Skipping 13F processing - no filing date for: {}", sourceFilePath);
      return outputFiles;
    }

    LOGGER.info("Processing 13F-HR: cik={}, date={}, accession={}", cik, filingDate, accession);

    try {
      int year;
      try {
        year = Integer.parseInt(filingDate.substring(0, 4));
        if (year < 1934 || year > java.time.Year.now().getValue()) {
          year = java.time.Year.now().getValue();
        }
      } catch (Exception e) {
        year = java.time.Year.now().getValue();
      }

      String partitionYear = filingDate.substring(0, 4);
      String relativePartitionPath = String.format("year=%s", partitionYear);
      String uniqueId = (accession != null && !accession.isEmpty()) ? accession : filingDate;

      // Parse the primary document (cover page) for metadata
      Document primaryDoc = null;
      String fileName = sourceFilePath.substring(sourceFilePath.lastIndexOf('/') + 1).toLowerCase();

      if (fileName.endsWith(".xml")) {
        try {
          DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
          factory.setNamespaceAware(true);
          DocumentBuilder builder = factory.newDocumentBuilder();
          try (InputStream is = sanitizeXmlStream(storageProvider.openInputStream(sourceFilePath))) {
            primaryDoc = builder.parse(is);
          }
        } catch (Exception e) {
          LOGGER.info("XML parse failed for 13F {}: {} — trying JSoup", fileName, e.getMessage());
          primaryDoc = parseWithJsoupFallback(sourceFilePath);
        }
      }

      if (primaryDoc == null) {
        LOGGER.warn("Could not parse 13F primary doc XML: {}", sourceFilePath);
        return outputFiles;
      }

      // The primary_doc.xml is just the cover page — the actual holdings are in
      // a separate XML file (typically InformationTableOutput.xml or infotable.xml).
      // We need to download it from EDGAR.
      Document infoTableDoc = download13FInfoTable(cik, accession);

      List<Map<String, Object>> holdings;
      if (infoTableDoc != null) {
        holdings = extract13FHoldings(infoTableDoc, cik, filingType, filingDate, accession, year);
        // Extract manager name from primary doc if not in info table
        if (!holdings.isEmpty()) {
          String managerName = getElementText(primaryDoc, "filingManager");
          if (managerName == null) {
            managerName = getElementText(primaryDoc, "name");
          }
          if (managerName != null) {
            for (Map<String, Object> h : holdings) {
              if (h.get("manager_name") == null) {
                h.put("manager_name", managerName);
              }
            }
          }
        }
      } else {
        LOGGER.warn("Could not download 13F information table for cik={}, accession={}", cik, accession);
        holdings = new ArrayList<>();
      }

      // Write 13f.parquet
      String outputPath = storageProvider.resolvePath(targetDirectoryPath,
          relativePartitionPath + "/" + String.format("%s_%s_13f.parquet", cik, uniqueId));

      java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
          AbstractSecDataDownloader.loadTableColumns("institutional_holdings");
      storageProvider.writeAvroParquet(outputPath, columns, holdings,
          "InstitutionalHolding", "institutional_holdings");
      outputFiles.add(outputPath);

      LOGGER.info("Converted 13F-HR to institutional holdings: {} records", holdings.size());

      // Write filing_metadata using primary doc (has company info, period, etc.)
      String metadataPath = storageProvider.resolvePath(targetDirectoryPath,
          relativePartitionPath + "/" + String.format("%s_%s_metadata.parquet", cik, uniqueId));
      writeMetadataToParquet(primaryDoc, metadataPath, cik, filingType, filingDate, accession, sourceFilePath);
      outputFiles.add(metadataPath);

    } catch (Exception e) {
      LOGGER.warn("Failed to process 13F form: " + e.getMessage());
    }

    return outputFiles;
  }

  /**
   * Downloads the 13F information table XML from EDGAR.
   *
   * <p>A 13F-HR filing consists of two documents:
   * <ul>
   *   <li>primary_doc.xml — cover page with manager info and report period</li>
   *   <li>InformationTableOutput.xml — the actual per-security holdings</li>
   * </ul>
   *
   * <p>This method fetches the filing index page and locates the information
   * table document, then downloads and parses it.
   */
  private Document download13FInfoTable(String cik, String accession) {
    if (cik == null || accession == null) {
      return null;
    }

    try {
      String cikNumeric = cik.replaceFirst("^0+", "");
      String accessionNoDash = accession.replace("-", "");
      String baseUrl = String.format(
          "https://www.sec.gov/Archives/edgar/data/%s/%s", cikNumeric, accessionNoDash);

      // Try common info table filenames directly (faster than parsing index)
      String[] infoTableNames = {
          "InformationTableOutput.xml",
          "infotable.xml",
          "InfoTable.xml",
          "information_table.xml"
      };

      for (String name : infoTableNames) {
        String content = downloadFile(baseUrl + "/" + name);
        if (content != null && content.contains("infoTable")) {
          LOGGER.info("Downloaded 13F info table: {}/{}", accession, name);
          DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
          factory.setNamespaceAware(true);
          DocumentBuilder builder = factory.newDocumentBuilder();
          try (InputStream is = sanitizeXmlStream(
              new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)))) {
            return builder.parse(is);
          }
        }
      }

      // Fallback: parse the formatted filing index page to find info table document.
      // The formatted index ({accession}-index.html) has 5 columns including document Type,
      // while the raw directory listing (/) only has 3 columns (name, size, date).
      String indexHtml = downloadFile(baseUrl + "/" + accession + "-index.html");
      if (indexHtml == null) {
        // Try raw directory listing as last resort
        indexHtml = downloadFile(baseUrl + "/");
      }
      if (indexHtml != null) {
        String infoTableFile = find13FInfoTableInIndex(indexHtml);
        if (infoTableFile != null) {
          String content = downloadFile(baseUrl + "/" + infoTableFile);
          if (content != null) {
            LOGGER.info("Downloaded 13F info table from index: {}/{}", accession, infoTableFile);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            try (InputStream is = sanitizeXmlStream(
                new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)))) {
              return builder.parse(is);
            }
          }
        }
      }

      LOGGER.warn("No 13F information table found for {}/{}", cikNumeric, accessionNoDash);
      return null;

    } catch (Exception e) {
      LOGGER.warn("Failed to download 13F info table for {}: {}", accession, e.getMessage());
      return null;
    }
  }

  /**
   * Finds the 13F information table filename in an EDGAR filing index page.
   *
   * <p>The formatted index ({accession}-index.html) has 5 columns:
   * Seq, Description, Document, Type, Size. The Type column contains
   * "INFORMATION TABLE" for the info table XML.
   *
   * <p>The raw directory listing (/) has 3 columns: name, size, date.
   * For the raw listing, we match by filename patterns.
   */
  private String find13FInfoTableInIndex(String indexHtml) {
    org.jsoup.nodes.Document doc = Jsoup.parse(indexHtml);
    org.jsoup.select.Elements rows = doc.select("table tr");

    for (org.jsoup.nodes.Element row : rows) {
      org.jsoup.select.Elements cells = row.select("td");
      if (cells.size() < 3) {
        continue;
      }

      // Check document type if present (formatted index has 5 columns)
      if (cells.size() > 3) {
        String type = cells.get(3).text().trim().toLowerCase();
        if (type.contains("information table")) {
          // Get the XML link from the Document column (index 2)
          // The formatted index has both .html (XSL-transformed) and .xml versions
          // We want the raw .xml file
          org.jsoup.select.Elements links = cells.get(2).select("a");
          if (!links.isEmpty()) {
            String href = links.first().attr("href");
            int lastSlash = href.lastIndexOf('/');
            if (lastSlash >= 0) {
              href = href.substring(lastSlash + 1);
            }
            // Skip XSL-transformed HTML versions, keep raw XML
            if (href.endsWith(".xml")) {
              return href;
            }
          }
        }
      }

      // Fallback: match by filename in any column (works for raw directory listings)
      for (org.jsoup.nodes.Element cell : cells) {
        org.jsoup.select.Elements links = cell.select("a");
        for (org.jsoup.nodes.Element link : links) {
          String href = link.attr("href");
          int lastSlash = href.lastIndexOf('/');
          String filename = lastSlash >= 0 ? href.substring(lastSlash + 1) : href;
          String lower = filename.toLowerCase();
          if (lower.endsWith(".xml")
              && (lower.contains("infotable") || lower.contains("information"))) {
            return filename;
          }
        }
      }
    }

    return null;
  }

  /**
   * Extract holdings from a 13F-HR XML informationTable.
   *
   * <p>13F XML structure:
   * <pre>{@code
   * <informationTable>
   *   <infoTable>
   *     <nameOfIssuer>APPLE INC</nameOfIssuer>
   *     <titleOfClass>COM</titleOfClass>
   *     <cusip>037833100</cusip>
   *     <value>150000</value>
   *     <shrsOrPrnAmt>
   *       <sshPrnamt>1000</sshPrnamt>
   *       <sshPrnamtType>SH</sshPrnamtType>
   *     </shrsOrPrnAmt>
   *     <investmentDiscretion>SOLE</investmentDiscretion>
   *     <votingAuthority>
   *       <Sole>1000</Sole>
   *       <Shared>0</Shared>
   *       <None>0</None>
   *     </votingAuthority>
   *     <putCall>...</putCall>
   *   </infoTable>
   * </informationTable>
   * }</pre>
   */
  private List<Map<String, Object>> extract13FHoldings(Document doc, String cik,
      String filingType, String filingDate, String accession, int year) {
    List<Map<String, Object>> dataList = new ArrayList<>();

    // Extract manager info from the primary doc (headerData or coverPage)
    String managerName = getElementText(doc, "filingManager");
    if (managerName == null) {
      managerName = getElementText(doc, "name");
    }
    String managerCik = cik;

    // Extract report period
    String reportPeriod = getElementText(doc, "reportCalendarOrQuarter");
    if (reportPeriod == null) {
      reportPeriod = getElementText(doc, "periodOfReport");
    }

    // Find infoTable entries (handles both namespaced and non-namespaced)
    NodeList entries = doc.getElementsByTagName("infoTable");
    if (entries.getLength() == 0) {
      entries = doc.getElementsByTagNameNS("*", "infoTable");
    }

    for (int i = 0; i < entries.getLength(); i++) {
      Element entry = (Element) entries.item(i);

      Map<String, Object> data = new HashMap<>();
      data.put("cik", cik);
      data.put("accession_number", accession);
      data.put("filing_date", filingDate);
      data.put("year", year);
      data.put("filing_type", filingType);
      data.put("manager_name", managerName);
      data.put("manager_cik", managerCik);
      data.put("report_period", reportPeriod);

      data.put("issuer_name", getElementText(entry, "nameOfIssuer"));
      data.put("title_of_class", getElementText(entry, "titleOfClass"));
      data.put("cusip", getElementText(entry, "cusip"));

      String value = getElementText(entry, "value");
      data.put("value_thousands", parseDoubleOrNull(value));

      // Shares or principal amount
      data.put("shares_or_principal",
          parseDoubleOrNull(getElementText(entry, "sshPrnamt")));
      data.put("shares_or_principal_type",
          getElementText(entry, "sshPrnamtType"));

      data.put("investment_discretion",
          getElementText(entry, "investmentDiscretion"));

      // Voting authority
      NodeList votingAuth = entry.getElementsByTagName("votingAuthority");
      if (votingAuth.getLength() == 0) {
        votingAuth = entry.getElementsByTagNameNS("*", "votingAuthority");
      }
      if (votingAuth.getLength() > 0) {
        Element va = (Element) votingAuth.item(0);
        data.put("voting_authority_sole", parseDoubleOrNull(getElementText(va, "Sole")));
        data.put("voting_authority_shared", parseDoubleOrNull(getElementText(va, "Shared")));
        data.put("voting_authority_none", parseDoubleOrNull(getElementText(va, "None")));
      } else {
        data.put("voting_authority_sole", null);
        data.put("voting_authority_shared", null);
        data.put("voting_authority_none", null);
      }

      data.put("put_call", getElementText(entry, "putCall"));

      dataList.add(data);
    }

    return dataList;
  }

  // =========================================================================
  // 13D/G Processing — Beneficial Ownership
  // =========================================================================

  /**
   * Process a Schedule 13D or 13G filing. Extracts beneficial ownership data
   * from the HTML cover page and Item 4 (purpose of transaction) for vectorization.
   */
  private List<String> process13DGForm(String sourceFilePath, String targetDirectoryPath,
      ConversionMetadata metadata) throws IOException {
    List<String> outputFiles = new ArrayList<>();

    String hintCik = metadata != null ? metadata.getHint("cik") : null;
    String hintForm = metadata != null ? metadata.getHint("form") : null;
    String hintDate = metadata != null ? metadata.getHint("filingDate") : null;
    String hintAccession = metadata != null ? metadata.getHint("accession") : null;

    String cik = hintCik != null ? hintCik : extractCikFromPath(sourceFilePath);
    String filingType = hintForm != null ? hintForm : "SC 13D";
    String filingDate = hintDate;
    String accession = hintAccession != null ? hintAccession
        : extractAccessionFromPath(sourceFilePath);

    if (cik == null || cik.equals("0000000000")) {
      LOGGER.warn("Skipping 13D/G processing - invalid CIK from: {}", sourceFilePath);
      return outputFiles;
    }
    if (filingDate == null) {
      LOGGER.warn("Skipping 13D/G processing - no filing date for: {}", sourceFilePath);
      return outputFiles;
    }

    LOGGER.info("Processing {}: cik={}, date={}, accession={}", filingType, cik, filingDate, accession);

    try {
      int year;
      try {
        year = Integer.parseInt(filingDate.substring(0, 4));
        if (year < 1934 || year > java.time.Year.now().getValue()) {
          year = java.time.Year.now().getValue();
        }
      } catch (Exception e) {
        year = java.time.Year.now().getValue();
      }

      String partitionYear = filingDate.substring(0, 4);
      String relativePartitionPath = String.format("year=%s", partitionYear);
      String uniqueId = (accession != null && !accession.isEmpty()) ? accession : filingDate;

      // Read file content
      String fileContent;
      try (InputStream is = storageProvider.openInputStream(sourceFilePath)) {
        fileContent = new String(readAllBytes(is), java.nio.charset.StandardCharsets.UTF_8);
      }

      // Try XML parse first (some 13D/G are structured XML)
      Document xmlDoc = null;
      String fileName = sourceFilePath.substring(sourceFilePath.lastIndexOf('/') + 1).toLowerCase();
      if (fileName.endsWith(".xml")) {
        try {
          DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
          factory.setNamespaceAware(true);
          DocumentBuilder builder = factory.newDocumentBuilder();
          try (InputStream is = sanitizeXmlStream(
              new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8)))) {
            xmlDoc = builder.parse(is);
          }
        } catch (Exception e) {
          LOGGER.debug("XML parse failed for 13D/G, using HTML path: {}", e.getMessage());
        }
      }

      // Extract structured ownership data
      List<Map<String, Object>> ownershipRecords = extract13DGOwnership(
          fileContent, xmlDoc, cik, filingType, filingDate, accession, year);
      LOGGER.debug("Extracted {} ownership records from {}", ownershipRecords.size(), fileName);

      // Write 13dg.parquet
      String outputPath = storageProvider.resolvePath(targetDirectoryPath,
          relativePartitionPath + "/" + String.format("%s_%s_13dg.parquet", cik, uniqueId));

      java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
          AbstractSecDataDownloader.loadTableColumns("beneficial_ownership");
      storageProvider.writeAvroParquet(outputPath, columns, ownershipRecords,
          "BeneficialOwnership", "beneficial_ownership");
      outputFiles.add(outputPath);

      LOGGER.info("Converted {} to beneficial ownership: {} records", filingType, ownershipRecords.size());

      // Write filing_metadata
      if (xmlDoc != null) {
        String metadataPath = storageProvider.resolvePath(targetDirectoryPath,
            relativePartitionPath + "/" + String.format("%s_%s_metadata.parquet", cik, uniqueId));
        writeMetadataToParquet(xmlDoc, metadataPath, cik, filingType, filingDate, accession, sourceFilePath);
        outputFiles.add(metadataPath);
      } else {
        // Write 8K-style metadata from HTML
        String metadataPath = storageProvider.resolvePath(targetDirectoryPath,
            relativePartitionPath + "/" + String.format("%s_%s_metadata.parquet", cik, uniqueId));
        write8KMetadata(fileContent, metadataPath, cik, filingType, filingDate, accession, sourceFilePath);
        outputFiles.add(metadataPath);
      }

      // Extract Item 4 (purpose of transaction) for vectorized_chunks
      if (enableVectorization) {
        List<Map<String, Object>> chunks = extract13DGItems(fileContent, cik, filingDate, accession, year);
        if (!chunks.isEmpty()) {
          java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> chunkColumns =
              AbstractSecDataDownloader.loadTableColumns("vectorized_chunks");
          String chunksPath = storageProvider.resolvePath(targetDirectoryPath,
              relativePartitionPath + "/" + String.format("%s_%s_chunks.parquet", cik, uniqueId));
          storageProvider.writeAvroParquet(chunksPath, chunkColumns, chunks,
              "VectorizedChunk", "vectorized_chunks");
          outputFiles.add(chunksPath);
          LOGGER.info("Wrote {} vectorized chunks from 13D/G items", chunks.size());
        }
      }

    } catch (Exception e) {
      LOGGER.warn("Failed to process 13D/G form: " + e.getMessage());
    }

    return outputFiles;
  }

  /**
   * Extract beneficial ownership data from Schedule 13D/G filing.
   * Handles both HTML (text parsing) and XML structured formats.
   */
  private List<Map<String, Object>> extract13DGOwnership(String fileContent, Document xmlDoc,
      String cik, String filingType, String filingDate, String accession, int year) {
    List<Map<String, Object>> dataList = new ArrayList<>();

    // Parse HTML for text extraction
    org.jsoup.nodes.Document htmlDoc = Jsoup.parse(fileContent);
    htmlDoc.select("script, style").remove();
    String bodyText = htmlDoc.body() != null ? htmlDoc.body().text() : htmlDoc.text();

    // Extract subject company from cover page
    String subjectCompany = extractPatternValue(bodyText,
        "(?i)Name of Issuer[:\\s]+([^\\n]+?)(?:\\s{2,}|$)");
    String subjectCik = null;

    // Extract title of class and CUSIP
    String titleOfClass = extractPatternValue(bodyText,
        "(?i)Title of Class[^:]*[:\\s]+([^\\n]+?)(?:\\s{2,}|$)");
    String cusip = extractPatternValue(bodyText,
        "(?i)CUSIP[^:]*[:\\s]+([A-Za-z0-9]{6,9})");

    // Extract date of event
    String dateOfEvent = extractPatternValue(bodyText,
        "(?i)Date of Event[^:]*[:\\s]+(\\d{1,2}/\\d{1,2}/\\d{2,4}|\\w+ \\d{1,2},? \\d{4})");

    // Extract type of reporting person
    String typeOfReportingPerson = extractPatternValue(bodyText,
        "(?i)Type of Reporting Person[^:]*[:\\s]+([A-Z]{2})");

    // Extract source of funds
    String sourceOfFunds = extractPatternValue(bodyText,
        "(?i)Source of Funds[^:]*[:\\s]+([A-Z]{2})");

    // Extract percent of class
    Double percentOfClass = null;
    String percentStr = extractPatternValue(bodyText,
        "(?i)Percent of Class[^:]*[:\\s]+([\\d.]+)\\s*%?");
    if (percentStr != null) {
      percentOfClass = parseDoubleOrNull(percentStr);
    }

    // Extract aggregate amount beneficially owned
    Double sharesBeneficiallyOwned = null;
    String sharesStr = extractPatternValue(bodyText,
        "(?i)Aggregate Amount Beneficially Owned[^:]*[:\\s]+([\\d,]+)");
    if (sharesStr != null) {
      sharesBeneficiallyOwned = parseDoubleOrNull(sharesStr.replace(",", ""));
    }

    // Extract voting/dispositive power
    Double soleVoting = parseDoubleOrNull(extractPatternValue(bodyText,
        "(?i)Sole Voting Power[^:]*[:\\s]+([\\d,]+)"));
    Double sharedVoting = parseDoubleOrNull(extractPatternValue(bodyText,
        "(?i)Shared Voting Power[^:]*[:\\s]+([\\d,]+)"));
    Double soleDispositive = parseDoubleOrNull(extractPatternValue(bodyText,
        "(?i)Sole Dispositive Power[^:]*[:\\s]+([\\d,]+)"));
    Double sharedDispositive = parseDoubleOrNull(extractPatternValue(bodyText,
        "(?i)Shared Dispositive Power[^:]*[:\\s]+([\\d,]+)"));

    // Extract filer name(s) — look for reporting person on cover page
    String filerName = extractPatternValue(bodyText,
        "(?i)(?:Name of Reporting Person|REPORTING PERSON)[^:]*[:\\s]+([^\\n]+?)(?:\\s{2,}|$)");

    // Extract purpose of transaction (Item 4)
    String purpose = extract13DGPurposeText(bodyText);

    Map<String, Object> data = new HashMap<>();
    data.put("cik", cik);
    data.put("accession_number", accession);
    data.put("filing_date", filingDate);
    data.put("year", year);
    data.put("filing_type", filingType);
    data.put("subject_company", subjectCompany);
    data.put("subject_cik", subjectCik);
    data.put("filer_name", filerName);
    data.put("filer_cik", cik); // In EDGAR, the filer's CIK is the filing CIK
    data.put("date_of_event", dateOfEvent);
    data.put("title_of_class", titleOfClass);
    data.put("cusip", cusip);
    data.put("percent_of_class", percentOfClass);
    data.put("shares_beneficially_owned", sharesBeneficiallyOwned);
    data.put("sole_voting_power", soleVoting != null ? soleVoting : null);
    data.put("shared_voting_power", sharedVoting != null ? sharedVoting : null);
    data.put("sole_dispositive_power", soleDispositive != null ? soleDispositive : null);
    data.put("shared_dispositive_power", sharedDispositive != null ? sharedDispositive : null);
    data.put("type_of_reporting_person", typeOfReportingPerson);
    data.put("source_of_funds", sourceOfFunds);
    data.put("purpose_of_transaction", purpose);

    dataList.add(data);
    return dataList;
  }

  /**
   * Extract Item 4 (Purpose of Transaction) text from a 13D/G filing body.
   * Also extracts Item 3 (Source and Amount of Funds) and Item 6 (Contracts).
   */
  private String extract13DGPurposeText(String bodyText) {
    // Try to find Item 4 boundaries
    Pattern item4Start = Pattern.compile(
        "(?i)Item\\s+4\\.?\\s*[-—.]?\\s*Purpose of Transaction", Pattern.CASE_INSENSITIVE);
    Matcher m4 = item4Start.matcher(bodyText);
    if (!m4.find()) {
      return null;
    }

    int start = m4.end();

    // End at Item 5 or SIGNATURES
    Pattern item5Start = Pattern.compile(
        "(?i)Item\\s+5\\.?\\s*[-—.]?\\s*Interest", Pattern.CASE_INSENSITIVE);
    Matcher m5 = item5Start.matcher(bodyText);
    int end;
    if (m5.find(start)) {
      end = m5.start();
    } else {
      int sigPos = bodyText.toUpperCase().indexOf("SIGNATURES", start);
      end = sigPos > 0 ? sigPos : Math.min(start + 5000, bodyText.length());
    }

    String purpose = bodyText.substring(start, end).trim();
    // Truncate very long purpose text
    if (purpose.length() > 5000) {
      purpose = purpose.substring(0, 5000) + "...";
    }
    return purpose.isEmpty() ? null : purpose;
  }

  /**
   * Extract item sections from 13D/G HTML for vectorized_chunks.
   * Similar to extract8KItems but with 13D/G-specific item numbers.
   */
  private List<Map<String, Object>> extract13DGItems(String fileContent,
      String cik, String filingDate, String accession, int year) {
    List<Map<String, Object>> chunks = new ArrayList<>();
    Pattern itemPattern = Pattern.compile(
        "Item\\s+(\\d+)\\.?\\s*[-—.]?\\s*([^\\n]{5,80})", Pattern.CASE_INSENSITIVE);

    org.jsoup.nodes.Document doc = Jsoup.parse(fileContent);
    doc.select("script, style").remove();
    String bodyText = doc.body() != null ? doc.body().text() : doc.text();

    // Find all item header positions
    Matcher matcher = itemPattern.matcher(bodyText);
    List<int[]> itemPositions = new ArrayList<>();
    List<String> itemNumbers = new ArrayList<>();
    while (matcher.find()) {
      String itemNum = matcher.group(1);
      // 13D/G items are 1-10
      try {
        int num = Integer.parseInt(itemNum);
        if (num >= 1 && num <= 10) {
          itemPositions.add(new int[]{matcher.start(), matcher.end()});
          itemNumbers.add(itemNum);
        }
      } catch (NumberFormatException e) {
        // skip
      }
    }

    // Find SIGNATURES position as end boundary
    int sigPos = bodyText.toUpperCase().indexOf("SIGNATURES");
    if (sigPos < 0) {
      sigPos = bodyText.length();
    }

    int sequence = 0;
    Set<String> boilerplate = new HashSet<>();
    boilerplate.add("FORWARD-LOOKING STATEMENTS");
    boilerplate.add("Safe Harbor");

    for (int i = 0; i < itemPositions.size(); i++) {
      String itemNumber = itemNumbers.get(i);
      int textStart = itemPositions.get(i)[1];
      int textEnd = (i + 1 < itemPositions.size()) ? itemPositions.get(i + 1)[0] : sigPos;
      if (textStart >= textEnd) {
        continue;
      }

      String sectionText = bodyText.substring(textStart, textEnd).trim();
      String[] parts = sectionText.split("(?<=\\.)\\s{2,}|\\n\\s*\\n");

      int paraSeq = 0;
      for (String part : parts) {
        String para = part.trim();
        if (para.length() < 50) {
          continue;
        }
        boolean isBoilerplate = false;
        for (String bp : boilerplate) {
          if (para.contains(bp)) {
            isBoilerplate = true;
            break;
          }
        }
        if (isBoilerplate) {
          continue;
        }

        Map<String, Object> chunk = new HashMap<>();
        chunk.put("cik", cik);
        chunk.put("accession_number", accession);
        chunk.put("year", year);
        chunk.put("chunk_id", "13dg_item_" + itemNumber + "_" + paraSeq);
        chunk.put("source_type", "13dg_item");
        chunk.put("section", "Item " + itemNumber);
        chunk.put("sequence", sequence++);
        chunk.put("filing_date", filingDate);
        chunk.put("chunk_text", para);
        chunk.put("enriched_text", para);
        chunk.put("content_type", "paragraph");
        chunk.put("financial_concepts", null);

        chunks.add(chunk);
        paraSeq++;
      }
    }

    return chunks;
  }

  /**
   * Extract a value from text using a regex pattern with a capture group.
   */
  private String extractPatternValue(String text, String patternStr) {
    try {
      Pattern pattern = Pattern.compile(patternStr);
      Matcher matcher = pattern.matcher(text);
      if (matcher.find()) {
        return matcher.group(1).trim();
      }
    } catch (Exception e) {
      LOGGER.trace("Pattern extraction failed: {}", e.getMessage());
    }
    return null;
  }

  /**
   * Parse a string as Double, returning null on failure.
   */
  private Double parseDoubleOrNull(String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    try {
      return Double.parseDouble(value.replace(",", ""));
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
