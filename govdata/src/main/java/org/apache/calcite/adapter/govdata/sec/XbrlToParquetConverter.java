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

import org.apache.calcite.adapter.file.converters.FileConverter;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
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
import java.io.File;
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

  @Override public List<File> convert(File sourceFile, File targetDirectory,
      ConversionMetadata metadata) throws IOException {
    // Delegate to String-based method for S3 compatibility
    return convertInternal(sourceFile.getAbsolutePath(), targetDirectory.getAbsolutePath(), metadata);
  }

  /**
   * Converts XBRL/HTML file to Parquet format using String paths (S3-compatible).
   *
   * @param sourceFilePath Path to source XBRL/HTML file (local filesystem only)
   * @param targetDirectoryPath Path to target directory (may be S3 URI)
   * @param metadata Conversion metadata tracker
   * @return List of created files (File objects are placeholders for S3 paths)
   * @throws IOException If conversion fails
   */
  public List<File> convertInternal(String sourceFilePath, String targetDirectoryPath,
      ConversionMetadata metadata) throws IOException {
    LOGGER.debug(" XbrlToParquetConverter.convert() START for file: " + sourceFilePath);
    List<File> outputFiles = new ArrayList<>();

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

    // Extract filename from path (works for both local and S3 paths)
    String fileName = sourceFilePath.substring(sourceFilePath.lastIndexOf('/') + 1).toLowerCase();

    try {
      Document doc = null;
      boolean isInlineXbrl = false;

      // Check if it's an HTML file (potential inline XBRL)
      if (fileName.endsWith(".htm") || fileName.endsWith(".html")) {
        // Try to parse as inline XBRL using StorageProvider
        LOGGER.debug("Attempting to parse inline XBRL from: " + fileName);
        doc = parseInlineXbrl(sourceFilePath);
        if (doc != null) {
          isInlineXbrl = true;
          LOGGER.debug("Successfully parsed inline XBRL from HTML: " + fileName);
        } else {
          LOGGER.warn("Failed to parse inline XBRL from HTML: " + fileName);
        }
      }

      // If not inline XBRL or parsing failed, try traditional XBRL
      if (doc == null && !fileName.endsWith(".htm") && !fileName.endsWith(".html")) {
        // Parse traditional XBRL/XML file using StorageProvider
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        try (InputStream is = storageProvider.openInputStream(sourceFilePath)) {
          doc = builder.parse(is);
        }
      }

      // For inline XBRL files that failed initial parse, try specialized inline XBRL parsing
      if (doc == null && (fileName.endsWith(".htm") || fileName.endsWith(".html"))) {
        LOGGER.debug("Standard XML parsing failed for HTML file, attempting inline XBRL extraction: " + fileName);
        try {
          // Parse as HTML and extract inline XBRL elements
          doc = parseInlineXbrl(sourceFilePath);
          if (doc != null) {
            LOGGER.debug("Successfully extracted inline XBRL data from: " + fileName);
          }
        } catch (Exception e) {
          LOGGER.warn("Inline XBRL extraction failed: " + e.getMessage());
          // Get the full stack trace for debugging
          java.io.StringWriter sw = new java.io.StringWriter();
          e.printStackTrace(new java.io.PrintWriter(sw));
          String fullStackTrace = sw.toString();

          // Track the actual exception with stack trace
          trackParsingError(sourceFilePath, "inline_xbrl_extraction_exception",
              "Exception: " + e.getClass().getName() + " - " + e.getMessage() + "\nStack trace:\n"
  + fullStackTrace);
        }

        // If still no document, don't create minimal parquet files
        // Let the parsing failure be properly reported and track for offline review
        if (doc == null) {
          LOGGER.warn("Failed to extract XBRL data from inline XBRL file - parsing error for: " + fileName);

          // Only track generic failure if we didn't already track the exception
          // (this happens when parseInlineXbrl returns null without throwing)
          trackParsingError(sourceFilePath, "inline_xbrl_returned_null",
              "parseInlineXbrl() returned null without throwing an exception");

          // Return empty output files list to indicate failure
          return outputFiles;
        }
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
      String filingDate = extractFilingDate(doc, sourceFilePath);

      LOGGER.debug(" Extracted metadata for " + fileName + " - CIK: " + cik + ", Filing Type: " + filingType + ", Date: " + filingDate);

      // Skip conversion if we couldn't extract required metadata
      if (cik == null || cik.equals("0000000000")) {
        LOGGER.warn("DEBUG: Skipping conversion due to invalid or missing CIK: " + fileName + " (extracted CIK: " + cik + ")");
        return outputFiles; // Return empty list
      }

      // Validate filing date - must be present
      if (filingDate == null) {
        LOGGER.warn("DEBUG: Skipping conversion - could not extract filing date from: " + fileName);
        return outputFiles; // Skip conversion
      }

      // Validate filing date format and year
      if (filingDate.length() >= 4) {
        try {
          int year = Integer.parseInt(filingDate.substring(0, 4));
          if (year < 1934 || year > java.time.Year.now().getValue()) {
            LOGGER.warn("Invalid year " + year + " in filing date " + filingDate + " for " + fileName);
            return outputFiles; // Skip conversion
          }
        } catch (NumberFormatException e) {
          LOGGER.warn("Invalid filing date format: " + filingDate + " for " + fileName);
          return outputFiles; // Skip conversion
        }
      } else {
        LOGGER.warn("Filing date too short: " + filingDate + " for " + fileName);
        return outputFiles; // Skip conversion
      }

      // Check if this is a Form 3, 4, or 5 (insider trading forms)
      if (isInsiderForm(doc, filingType)) {
        LOGGER.debug("Processing as insider form: " + fileName);
        return convertInsiderForm(doc, sourceFilePath, targetDirectoryPath, cik, filingType, filingDate, accession);
      }

      LOGGER.debug("Not an insider form, proceeding to facts extraction: " + fileName);

      // Check if this is an 8-K filing with potential earnings exhibits
      if (is8KFiling(filingType)) {
        List<File> extraFiles = extract8KExhibits(sourceFilePath, targetDirectoryPath, cik, filingType, filingDate, accession);
        outputFiles.addAll(extraFiles);
      }

      // Create partitioned output path - BUILD RELATIVE PATHS
      // Normalize filing type: remove hyphens and slashes
      String normalizedFilingType = filingType.replace("-", "").replace("/", "");
      String partitionYear = getPartitionYear(filingType, filingDate, doc);

      // Build RELATIVE partition path (relative to targetDirectoryPath which already includes source=sec)
      String relativePartitionPath =
          String.format("cik=%s/filing_type=%s/year=%s", cik, normalizedFilingType, partitionYear);

      // For local filesystem only, create actual directories (S3 doesn't need directory creation)
      if (!targetDirectoryPath.contains("://")) {
        java.nio.file.Path partitionPath = Paths.get(targetDirectoryPath, relativePartitionPath);
        Files.createDirectories(partitionPath);
      }

      // Use accession for file naming if available, otherwise fall back to filing date
      String uniqueId = (accession != null && !accession.isEmpty()) ? accession : filingDate;

      // Build FULL paths for all outputs (StorageProvider needs absolute paths)
      String factsPath = storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_facts.parquet", cik, uniqueId));
      String metadataPath = storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_metadata.parquet", cik, uniqueId));
      String contextsPath = storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_contexts.parquet", cik, uniqueId));
      String mdaPath = storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_mda.parquet", cik, uniqueId));
      String relationshipsPath = storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_relationships.parquet", cik, uniqueId));

      // Convert financial facts to Parquet
      LOGGER.debug(" Starting facts.parquet generation for: " + fileName + " -> " + factsPath);
      try {
        writeFactsToParquet(doc, factsPath, cik, filingType, filingDate, sourceFilePath);
        // Paths are already full paths from storageProvider.resolvePath()
        outputFiles.add(new File(factsPath));
        LOGGER.debug(" Successfully created facts.parquet: " + factsPath);
      } catch (Exception e) {
        LOGGER.error("DEBUG: Exception during facts.parquet creation for " + fileName + ": " + e.getMessage());
        trackConversionError(sourceFilePath, "facts_parquet_exception", "Exception: " + e.getClass().getName() + " - " + e.getMessage());
        throw e;
      }

      // Write filing metadata
      writeMetadataToParquet(doc, metadataPath, cik, filingType, filingDate, sourceFilePath);
      outputFiles.add(new File(metadataPath));

      // Convert contexts to Parquet
      writeContextsToParquet(doc, contextsPath, cik, filingType, filingDate);
      outputFiles.add(new File(contextsPath));

      // Extract and write MD&A
      writeMDAToParquet(doc, mdaPath, cik, filingType, filingDate, sourceFilePath);
      outputFiles.add(new File(mdaPath));

      // Extract and write XBRL relationships
      LOGGER.debug(" Starting relationships.parquet generation for: " + fileName + " -> " + relationshipsPath);
      try {
        writeRelationshipsToParquet(doc, relationshipsPath, cik, filingType, filingDate, sourceFilePath);
        outputFiles.add(new File(relationshipsPath));
        LOGGER.debug(" Successfully created relationships.parquet: " + relationshipsPath);
      } catch (Exception e) {
        LOGGER.error("DEBUG: Exception during relationships.parquet creation for " + fileName + ": " + e.getMessage());
        trackConversionError(sourceFilePath, "relationships_parquet_exception", "Exception: " + e.getClass().getName() + " - " + e.getMessage());
        throw e;
      }

      // Create vectorized blobs with contextual enrichment if enabled
      if (enableVectorization) {
        String vectorizedPath =
            storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_vectorized.parquet", cik, uniqueId));
        writeVectorizedBlobsToParquet(doc, vectorizedPath, cik, filingType, filingDate, sourceFilePath);
        outputFiles.add(new File(vectorizedPath));
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

  private String extractFilingDate(Document doc, String sourcePath) {
    // For ownership documents (Form 3/4/5), extract periodOfReport
    NodeList periodOfReports = doc.getElementsByTagName("periodOfReport");
    if (periodOfReports.getLength() > 0) {
      String date = periodOfReports.item(0).getTextContent().trim();
      // Validate date format (should be YYYY-MM-DD)
      if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
        return date;
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

    // Try to extract from XBRL contexts - check both xbrli:context and context elements
    NodeList contexts = doc.getElementsByTagName("xbrli:context");
    if (contexts.getLength() == 0) {
      contexts = doc.getElementsByTagNameNS("*", "context");
    }
    if (contexts.getLength() == 0) {
      contexts = doc.getElementsByTagName("context");
    }

    // Look for the first valid period end date in contexts
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
        if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
          // Keep track of the latest date found
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
        if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
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

    // Return null if no date found - let the caller handle this
    LOGGER.warn("Could not extract filing date from: " + filename);
    return null;
  }

  private void writeFactsToParquet(Document doc, String outputPath,
      String cik, String filingType, String filingDate, String sourcePath) throws IOException {

    LOGGER.debug("writeFactsToParquet called for " + outputPath +
                " (CIK: " + cik + ", Type: " + filingType + ", Date: " + filingDate + ")");

    // Create Avro schema for facts
    // NOTE: Partition columns (cik, filing_type, year) are NOT included in the file
    // They are encoded in the directory structure for Hive-style partitioning
    Schema schema = SchemaBuilder.record("XbrlFact")
        .fields()
        .name("filing_date").doc("Date of the SEC filing (ISO 8601 format)").type().stringType().noDefault()
        .name("concept").doc("XBRL concept name (e.g., 'us-gaap:Assets', 'dei:EntityRegistrantName')").type().stringType().noDefault()
        .name("context_ref").doc("Reference to context element defining the reporting period and entity").type().nullable().stringType().noDefault()
        .name("unit_ref").doc("Reference to unit element defining measurement units (e.g., 'USD', 'shares')").type().nullable().stringType().noDefault()
        .name("value").doc("Text value of the fact element").type().nullable().stringType().noDefault()
        .name("full_text").doc("Full text content for TextBlock elements (narrative disclosures)").type().nullable().stringType().noDefault()
        .name("numeric_value").doc("Numeric value for monetary and numeric facts").type().nullable().doubleType().noDefault()
        .name("period_start").doc("Start date of the reporting period (ISO 8601 format)").type().nullable().stringType().noDefault()
        .name("period_end").doc("End date of the reporting period (ISO 8601 format)").type().nullable().stringType().noDefault()
        .name("is_instant").doc("Whether this is an instant-in-time fact (true) or duration fact (false)").type().nullable().booleanType().noDefault()
        .name("footnote_refs").doc("Comma-separated list of footnote references").type().nullable().stringType().noDefault()
        .name("element_id").doc("Unique element identifier for linking to other elements").type().nullable().stringType().noDefault()
        .name("type").doc("Table type identifier for DuckDB catalog").type().stringType().noDefault()
        .endRecord();

    // Extract all fact elements
    List<GenericRecord> records = new ArrayList<>();
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

        GenericRecord record = new GenericData.Record(schema);
        // Partition columns (cik, filing_type) are NOT added - they're in the directory path
        record.put("filing_date", filingDate);

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
        record.put("concept", concept);
        record.put("context_ref", element.getAttribute("contextRef"));
        record.put("unit_ref", element.getAttribute("unitRef"));

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

        record.put("value", cleanValue);
        record.put("full_text", fullText);

        // Extract footnote references (e.g., "See Note 14")
        String footnoteRefs = extractFootnoteReferences(rawValue);
        record.put("footnote_refs", footnoteRefs);

        // Store element ID for relationship tracking
        String elementId = element.getAttribute("id");
        record.put("element_id", elementId.isEmpty() ? null : elementId);

        // Try to parse as numeric (using cleaned value)
        try {
          double numValue =
              Double.parseDouble(cleanValue.replaceAll(",", ""));
          record.put("numeric_value", numValue);
        } catch (NumberFormatException e) {
          record.put("numeric_value", null);
        }

        // Set type field for DuckDB table identification
        record.put("type", "financial_line_items");

        records.add(record);
      }
    }

    LOGGER.debug("Found " + elementsWithContextRef + " elements with contextRef attributes");
    LOGGER.debug("Extracted " + records.size() + " valid fact records (including enhanced extraction for elements without contextRef)");

    if (records.isEmpty()) {
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

      LOGGER.warn("DEBUG: Inline XBRL elements: " + ixElements.getLength() +
                     ", us-gaap elements: " + usGaapCount +
                     ", dei elements: " + deiCount);

      trackConversionError(sourcePath, "no_facts_extracted",
                         "No recognizable facts found despite " + allElements.getLength() + " elements");
    }

    // Use consolidated StorageProvider method for Parquet writing
    try {
      LOGGER.debug("Writing facts to parquet file: " + outputPath);
      LOGGER.debug(" About to write " + records.size() + " fact records using storageProvider.writeAvroParquet");

      // Check if we have any records to write
      if (records.isEmpty()) {
        LOGGER.warn("DEBUG: No fact records to write - creating empty parquet file for cache validation");
        // Still need to create the file for cache validation
      }

      storageProvider.writeAvroParquet(outputPath, schema, records, "facts");
      LOGGER.info("Successfully wrote " + records.size() + " facts to " + outputPath);

    } catch (Exception e) {
      LOGGER.error("Failed to write facts parquet file for " + filingDate + " (CIK: " + cik + "): " + e.getClass().getName() + " - " + e.getMessage());
      trackConversionError(sourcePath, "facts_write_exception",
                         "Exception: " + e.getClass().getName() + " - " + e.getMessage());
      throw new IOException("Failed to write facts to parquet: " + e.getMessage(), e);
    }
  }

  private void writeMetadataToParquet(Document doc, String outputPath,
      String cik, String filingType, String filingDate, String sourcePath) throws IOException {

    // Create Avro schema for filing metadata
    // NOTE: Partition columns (cik, filing_type, year) are NOT included in the file
    Schema schema = SchemaBuilder.record("FilingMetadata")
        .fields()
        .name("accession_number").doc("SEC accession number (unique filing identifier)").type().stringType().noDefault()
        .name("filing_date").doc("Date of the SEC filing (ISO 8601 format)").type().stringType().noDefault()
        .name("company_name").doc("Legal name of the registrant company").type().nullable().stringType().noDefault()
        .name("state_of_incorporation").doc("State or jurisdiction of incorporation").type().nullable().stringType().noDefault()
        .name("fiscal_year_end").doc("Fiscal year end date (MMDD format)").type().nullable().stringType().noDefault()
        .name("business_address").doc("Physical business address of the company").type().nullable().stringType().noDefault()
        .name("mailing_address").doc("Mailing address for correspondence").type().nullable().stringType().noDefault()
        .name("phone").doc("Company phone number").type().nullable().stringType().noDefault()
        .name("document_type").doc("Type of filing document (e.g., '10-K', '10-Q', '8-K')").type().nullable().stringType().noDefault()
        .name("period_end_date").doc("Period end date for the reporting period").type().nullable().stringType().noDefault()
        .name("sic_code").doc("Standard Industrial Classification code").type().nullable().longType().noDefault()
        .name("irs_number").doc("IRS Employer Identification Number (EIN)").type().nullable().stringType().noDefault()
        .name("type").doc("Table type identifier for DuckDB catalog").type().stringType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    GenericRecord record = new GenericData.Record(schema);

    // Extract accession number from filename
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    String accessionNumber = extractAccessionNumber(filename);
    record.put("accession_number", accessionNumber != null ? accessionNumber : "");
    record.put("filing_date", filingDate);

    // Extract DEI (Document and Entity Information) elements
    // Try different namespaces and formats
    String companyName = extractDeiValue(doc, "EntityRegistrantName", "RegistrantName");
    record.put("company_name", companyName);

    String stateOfIncorp =
                                           extractDeiValue(doc, "EntityIncorporationStateCountryCode", "StateOrCountryOfIncorporation");
    record.put("state_of_incorporation", stateOfIncorp);

    String fiscalYearEnd = extractDeiValue(doc, "CurrentFiscalYearEndDate", "FiscalYearEnd");
    record.put("fiscal_year_end", fiscalYearEnd);

    String businessAddress = extractDeiValue(doc, "EntityAddressAddressLine1", "BusinessAddress");
    record.put("business_address", businessAddress);

    String mailingAddress = extractDeiValue(doc, "EntityAddressMailingAddressLine1", "MailingAddress");
    record.put("mailing_address", mailingAddress);

    String phone = extractDeiValue(doc, "EntityPhoneNumber", "Phone");
    record.put("phone", phone);

    String docType = extractDeiValue(doc, "DocumentType", "FormType");
    record.put("document_type", docType);

    String periodEnd = extractDeiValue(doc, "DocumentPeriodEndDate", "PeriodEndDate");
    record.put("period_end_date", periodEnd);

    // Try to extract SIC code
    String sicStr = extractDeiValue(doc, "EntityStandardIndustrialClassificationCode", "SicCode");
    if (sicStr != null && !sicStr.isEmpty()) {
      try {
        record.put("sic_code", Long.parseLong(sicStr));
      } catch (NumberFormatException e) {
        record.put("sic_code", null);
      }
    } else {
      record.put("sic_code", null);
    }

    String irsNumber = extractDeiValue(doc, "EntityTaxIdentificationNumber", "IrsNumber");
    record.put("irs_number", irsNumber);

    // Set type field for DuckDB table identification
    record.put("type", "filing_metadata");

    records.add(record);

    // Use consolidated StorageProvider method for Parquet writing
    storageProvider.writeAvroParquet(outputPath, schema, records, "metadata");
    LOGGER.info("Successfully wrote " + records.size() + " metadata records to " + outputPath);
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

  private void writeContextsToParquet(Document doc, String outputPath,
      String cik, String filingType, String filingDate) throws IOException {

    // Create Avro schema for contexts
    // NOTE: Partition columns (cik, filing_type, year) are NOT included in the file
    Schema schema = SchemaBuilder.record("XbrlContext")
        .fields()
        .name("filing_date").doc("Date of the SEC filing (ISO 8601 format)").type().stringType().noDefault()
        .name("context_id").doc("Unique identifier for this context element").type().stringType().noDefault()
        .name("entity_identifier").doc("Entity identifier (typically CIK number)").type().nullable().stringType().noDefault()
        .name("entity_scheme").doc("Entity identifier scheme (e.g., 'http://www.sec.gov/CIK')").type().nullable().stringType().noDefault()
        .name("period_start").doc("Start date of the reporting period (ISO 8601 format)").type().nullable().stringType().noDefault()
        .name("period_end").doc("End date of the reporting period (ISO 8601 format)").type().nullable().stringType().noDefault()
        .name("period_instant").doc("Instant date for point-in-time facts (ISO 8601 format)").type().nullable().stringType().noDefault()
        .name("segment").doc("Segment dimension information (XML fragment)").type().nullable().stringType().noDefault()
        .name("scenario").doc("Scenario dimension information (XML fragment)").type().nullable().stringType().noDefault()
        .name("type").doc("Table type identifier for DuckDB catalog").type().stringType().noDefault()
        .endRecord();

    // Extract context elements
    List<GenericRecord> records = new ArrayList<>();
    NodeList contexts = doc.getElementsByTagNameNS("*", "context");

    for (int i = 0; i < contexts.getLength(); i++) {
      Element context = (Element) contexts.item(i);
      GenericRecord record = new GenericData.Record(schema);

      // Partition columns (cik, filing_type) are NOT added - they're in the directory path
      record.put("filing_date", filingDate);
      record.put("context_id", context.getAttribute("id"));

      // Extract entity information
      NodeList identifiers = context.getElementsByTagNameNS("*", "identifier");
      if (identifiers.getLength() > 0) {
        Element identifier = (Element) identifiers.item(0);
        record.put("entity_identifier", identifier.getTextContent());
        record.put("entity_scheme", identifier.getAttribute("scheme"));
      }

      // Extract period information
      NodeList startDates = context.getElementsByTagNameNS("*", "startDate");
      NodeList endDates = context.getElementsByTagNameNS("*", "endDate");
      NodeList instants = context.getElementsByTagNameNS("*", "instant");

      if (startDates.getLength() > 0) {
        record.put("period_start", startDates.item(0).getTextContent());
      }
      if (endDates.getLength() > 0) {
        record.put("period_end", endDates.item(0).getTextContent());
      }
      if (instants.getLength() > 0) {
        record.put("period_instant", instants.item(0).getTextContent());
      }

      // Set type field for DuckDB table identification
      record.put("type", "filing_contexts");

      records.add(record);
    }

    // Use consolidated StorageProvider method for Parquet writing
    storageProvider.writeAvroParquet(outputPath, schema, records, "contexts");
    LOGGER.info("Successfully wrote " + records.size() + " context records to " + outputPath);
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

      // Parse with Jsoup - use XML parser to preserve namespace prefixes
      LOGGER.debug("Calling Jsoup.parse with XML parser for namespace support...");
      org.jsoup.nodes.Document jsoupDoc = Jsoup.parse(html, "", org.jsoup.parser.Parser.xmlParser());
      LOGGER.debug("Jsoup.parse completed");
      // XML syntax already set by xmlParser

      // Look for inline XBRL elements - JSoup handles namespace prefixes as part of the tag name
      org.jsoup.select.Elements ixElements = new org.jsoup.select.Elements();

      // Collect all ix: prefixed elements (these are inline XBRL elements)
      // Use pipe notation for namespace selectors in XML mode
      try {
        LOGGER.debug("Trying selector with pipe notation: ix|nonNumeric, ix|nonFraction");
        ixElements.addAll(jsoupDoc.select("ix|nonNumeric, ix|nonFraction"));
      } catch (Exception e) {
        LOGGER.warn("Selector with pipe notation failed: " + e.getMessage());
      }

      // Also try lowercase variants
      if (ixElements.isEmpty()) {
        try {
          LOGGER.debug("Trying lowercase selectors");
          ixElements.addAll(jsoupDoc.select("ix|nonnumeric, ix|nonfraction"));
        } catch (Exception e) {
          LOGGER.warn("Lowercase selector failed: " + e.getMessage());
        }
      }

      // Debug: Log what we're finding
      LOGGER.debug(" Initial selector found " + ixElements.size() + " elements");

      // Debug: Check what tag names JSoup is seeing
      int debugCount = 0;
      for (org.jsoup.nodes.Element elem : jsoupDoc.getAllElements()) {
        if (elem.tagName().startsWith("ix:") && debugCount < 5) {
          LOGGER.debug(" Found tag with name: " + elem.tagName());
          debugCount++;
        }
      }

      // Also look for elements with contextRef attribute (case insensitive)
      for (org.jsoup.nodes.Element elem : jsoupDoc.getAllElements()) {
        if (elem.hasAttr("contextRef") || elem.hasAttr("contextref")) {
          if (elem.tagName().startsWith("ix:")) {
            ixElements.add(elem);
          }
        }
      }

      if (ixElements.isEmpty()) {
        // No inline XBRL found
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
   * Extract and write MD&A (Management Discussion & Analysis) to Parquet.
   * Each paragraph becomes a separate record for better querying.
   */
  private void writeMDAToParquet(Document doc, String outputPath,
      String cik, String filingType, String filingDate, String sourcePath) throws IOException {

    // Create schema for MD&A paragraphs
    Schema schema = SchemaBuilder.record("MDASection")
        .fields()
        .name("filing_date").doc("Date of the SEC filing (ISO 8601 format)").type().stringType().noDefault()
        .name("section").doc("Item section identifier (e.g., 'Item 7', 'Item 7A')").type().stringType().noDefault()
        .name("subsection").doc("Subsection name (e.g., 'Overview', 'Results of Operations')").type().stringType().noDefault()
        .name("paragraph_number").doc("Sequential paragraph number within the subsection").type().intType().noDefault()
        .name("paragraph_text").doc("Full text content of the paragraph").type().stringType().noDefault()
        .name("footnote_refs").doc("Comma-separated list of footnote references").type().nullable().stringType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();

    // Look for MD&A content in different ways
    // 1. Look for Item 7 and Item 7A in inline XBRL
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    if (filename.endsWith(".htm") || filename.endsWith(".html")) {
      extractMDAFromHTML(sourcePath, schema, records, filingDate);
    }

    // 2. Look for MD&A-related TextBlocks in XBRL
    NodeList allElements = doc.getElementsByTagName("*");
    for (int i = 0; i < allElements.getLength(); i++) {
      Element element = (Element) allElements.item(i);

      if (element.hasAttribute("contextRef")) {
        String concept = extractConceptName(element);

        // Check if this is an MD&A-related concept
        if (isMDAConcept(concept)) {
          String text = element.getTextContent().trim();
          if (text.length() > 50) {  // Skip if just a title
            extractParagraphs(text, concept, schema, records, filingDate);
          }
        }
      }
    }

    // Always write file, even if empty (zero rows) to ensure cache consistency
    storageProvider.writeAvroParquet(outputPath, schema, records, "mda");
    LOGGER.info("Successfully wrote " + records.size() + " MD&A paragraphs to " + outputPath);
  }

  /**
   * Extract MD&A from HTML file by looking for Item 7 and Item 7A sections.
   * Enhanced to handle inline XBRL documents where Item 7 may be embedded in tags.
   */
  private void extractMDAFromHTML(String htmlPath, Schema schema,
      List<GenericRecord> records, String filingDate) {
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
        extractMDAContent(contentStart, sectionName, schema, records, filingDate);
      }

      // If we still didn't find any MD&A, try a more aggressive approach
      if (records.isEmpty()) {
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
            extractTextAsParagraphs(text, currentSection, schema, records, filingDate);
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
      Schema schema, List<GenericRecord> records, String filingDate) {

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
            GenericRecord record = new GenericData.Record(schema);
            record.put("filing_date", filingDate);
            record.put("section", sectionName);
            record.put("subsection", subsection);
            record.put("paragraph_number", paragraphNum++);
            record.put("paragraph_text", paragraph.trim());
            record.put("footnote_refs", extractFootnoteReferences(paragraph));
            records.add(record);
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
      Schema schema, List<GenericRecord> records, String filingDate) {

    // Split into sentences or natural paragraphs
    String[] paragraphs = text.split("(?<=[.!?])\\s+(?=[A-Z])");

    // Group sentences into reasonable paragraph sizes
    StringBuilder currentParagraph = new StringBuilder();
    int paragraphNum = records.size() + 1;

    for (String sentence : paragraphs) {
      currentParagraph.append(sentence).append(" ");

      // Create a paragraph every few sentences or at natural breaks
      if (currentParagraph.length() > 300 || sentence.endsWith(".")) {
        String paragraphText = currentParagraph.toString().trim();
        if (paragraphText.length() > 100) {
          GenericRecord record = new GenericData.Record(schema);
          record.put("filing_date", filingDate);
          record.put("section", sectionName);
          record.put("subsection", "General");
          record.put("paragraph_number", paragraphNum++);
          record.put("paragraph_text", paragraphText);
          record.put("footnote_refs", extractFootnoteReferences(paragraphText));
          records.add(record);

          currentParagraph = new StringBuilder();
        }
      }
    }

    // Add any remaining text
    String remaining = currentParagraph.toString().trim();
    if (remaining.length() > 100) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("filing_date", filingDate);
      record.put("section", sectionName);
      record.put("subsection", "General");
      record.put("paragraph_number", paragraphNum);
      record.put("paragraph_text", remaining);
      record.put("footnote_refs", extractFootnoteReferences(remaining));
      records.add(record);
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
  private void extractParagraphs(String text, String concept, Schema schema,
      List<GenericRecord> records, String filingDate) {

    // Split into paragraphs
    String[] paragraphs = text.split("\\n\\n+");

    for (int i = 0; i < paragraphs.length; i++) {
      String paragraph = paragraphs[i].trim();
      if (paragraph.length() > 50) {  // Skip very short paragraphs
        GenericRecord record = new GenericData.Record(schema);
        record.put("filing_date", filingDate);
        record.put("section", "XBRL MD&A");
        record.put("subsection", concept);
        record.put("paragraph_number", i + 1);
        record.put("paragraph_text", paragraph);
        record.put("footnote_refs", extractFootnoteReferences(paragraph));
        records.add(record);
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
      String cik, String filingType, String filingDate, String sourcePath) throws IOException {

    LOGGER.debug(String.format("DEBUG: writeRelationshipsToParquet START for CIK %s filing type %s date %s", cik, filingType, filingDate));
    LOGGER.debug(String.format("DEBUG: Document is null? %s", doc == null));
    if (doc != null && doc.getDocumentElement() != null) {
      LOGGER.debug(String.format("DEBUG: Document root element: %s", doc.getDocumentElement().getNodeName()));
      LOGGER.debug(String.format("DEBUG: Document has %d child nodes", doc.getChildNodes().getLength()));
    }

    // Create schema for relationships
    Schema schema = SchemaBuilder.record("XbrlRelationship")
        .fields()
        .name("filing_date").doc("Date of the SEC filing (ISO 8601 format)").type().stringType().noDefault()
        .name("linkbase_type").doc("Type of linkbase relationship (presentation, calculation, definition)").type().stringType().noDefault()
        .name("arc_role").doc("Arc role defining relationship type (e.g., parent-child, summation-item)").type().stringType().noDefault()
        .name("from_concept").doc("Source concept in the relationship").type().stringType().noDefault()
        .name("to_concept").doc("Target concept in the relationship").type().stringType().noDefault()
        .name("weight").doc("Calculation weight (+1 for addition, -1 for subtraction)").type().nullable().doubleType().noDefault()
        .name("order").doc("Presentation order for display sequencing").type().nullable().intType().noDefault()
        .name("preferred_label").doc("Preferred label role for presentation").type().nullable().stringType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();

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

      GenericRecord record = new GenericData.Record(schema);
      record.put("filing_date", filingDate);

      // Determine linkbase type from namespace or arc role
      String arcRole = arc.getAttribute("arcrole");
      String linkbaseType = determineLinkbaseType(arcRole);
      record.put("linkbase_type", linkbaseType);
      record.put("arc_role", arcRole);

      // Get from and to concepts
      String from = arc.getAttribute("from");
      String to = arc.getAttribute("to");
      record.put("from_concept", cleanConceptName(from));
      record.put("to_concept", cleanConceptName(to));

      // Get weight for calculation linkbase
      String weight = arc.getAttribute("weight");
      if (weight != null && !weight.isEmpty()) {
        try {
          record.put("weight", Double.parseDouble(weight));
        } catch (NumberFormatException e) {
          record.put("weight", null);
        }
      }

      // Get order for presentation linkbase
      String order = arc.getAttribute("order");
      if (order != null && !order.isEmpty()) {
        try {
          record.put("order", Integer.parseInt(order));
        } catch (NumberFormatException e) {
          record.put("order", null);
        }
      }

      // Get preferred label
      record.put("preferred_label", arc.getAttribute("preferredLabel"));

      records.add(record);
      LOGGER.debug(String.format("DEBUG: Added arc relationship from %s to %s", from, to));
    }
    LOGGER.debug(String.format("DEBUG: Total arc-based relationships extracted: %d", records.size()));
    } else {
      LOGGER.debug(" Document is null, skipping arc extraction");
    }

    // Also extract relationships from inline XBRL if present
    int beforeInlineCount = records.size();
    LOGGER.debug(String.format("DEBUG: Before inline extraction, have %d relationships", beforeInlineCount));
    extractInlineXBRLRelationships(doc, schema, records, filingDate, sourcePath);
    int inlineRelationships = records.size() - beforeInlineCount;
    LOGGER.debug(String.format("DEBUG: After inline extraction, extracted %d inline relationships, total now %d", inlineRelationships, records.size()));

    // Always write the parquet file, even if empty, to satisfy cache validation
    try {
      LOGGER.debug(" About to write " + records.size() + " relationship records to " + outputPath);

      if (!records.isEmpty()) {
        storageProvider.writeAvroParquet(outputPath, schema, records, "relationships");
        LOGGER.info(
            String.format("Wrote %d relationships (%d arc-based, %d inline) to %s",
            records.size(), beforeInlineCount, inlineRelationships, outputPath));
      } else {
        // Create an empty parquet file to indicate that relationship extraction was completed
        // This is important for cache validation - without this file, the system will think
        // the filing hasn't been fully processed and will attempt to reprocess it
        // NOTE: Inline XBRL filings will typically have empty relationship files since
        // relationships are in separate linkbase files that we don't currently download
        LOGGER.debug(String.format("No relationships found for CIK %s filing type %s - creating empty relationships file (expected for inline XBRL)", cik, filingType));
        storageProvider.writeAvroParquet(outputPath, schema, records, "relationships"); // Write empty records list
        LOGGER.info(String.format("Created empty relationships file for %s (inline XBRL relationships in external linkbase files not downloaded): %s", filingType, outputPath));
      }

    } catch (Exception e) {
      LOGGER.error("Failed to write relationships parquet file for " + filingDate + " (CIK: " + cik + "): " + e.getClass().getName() + " - " + e.getMessage());
      trackConversionError(sourcePath, "relationships_write_exception",
                         "Exception: " + e.getClass().getName() + " - " + e.getMessage());
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
  private void extractInlineXBRLRelationships(Document doc, Schema schema,
      List<GenericRecord> records, String filingDate, String sourcePath) {

    String fileName = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    LOGGER.debug(" extractInlineXBRLRelationships START");
    LOGGER.debug(String.format("DEBUG: Document is null? %s", doc == null));
    LOGGER.debug(String.format("DEBUG: Source file is: %s", fileName));

    // First, try to download and parse external linkbase files
    // These contain the actual relationship definitions for inline XBRL
    try {
      downloadAndParseLinkbases(sourcePath, schema, records, filingDate);
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
              GenericRecord record = new GenericData.Record(schema);
              record.put("filing_date", filingDate);
              record.put("linkbase_type", determineLinkbaseType(arcrole));
              record.put("arc_role", arcrole);
              record.put("from_concept", cleanConceptName(fromRef));
              record.put("to_concept", cleanConceptName(toRef));
              record.put("weight", weight != null && !weight.isEmpty() ? Double.parseDouble(weight) : null);
              record.put("order", order != null && !order.isEmpty() ? Integer.parseInt(order) : i);
              record.put("preferred_label", ixRel.getAttribute("preferredLabel"));
              records.add(record);
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
                GenericRecord record = new GenericData.Record(schema);
                record.put("filing_date", filingDate);
                record.put("linkbase_type", "reference");
                record.put("arc_role", "http://www.xbrl.org/2009/arcrole/fact-explanatoryFact");
                record.put("from_concept", cleanConceptName(concept));
                record.put("to_concept", "footnote_" + id);
                record.put("weight", null);
                record.put("order", i * 1000 + j);
                record.put("preferred_label", footnoteRole);
                records.add(record);
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
                  GenericRecord record = new GenericData.Record(schema);
                  record.put("filing_date", filingDate);
                  record.put("linkbase_type", "presentation");
                  record.put("arc_role", "table-structure");
                  record.put("from_concept", cleanConceptName(lastParentConcept));
                  record.put("to_concept", cleanConceptName(concept));
                  record.put("weight", null);
                  record.put("order", r * 100 + c);  // Row-column based ordering
                  record.put("preferred_label", cell.getAttribute("preferredLabel"));
                  records.add(record);
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
            GenericRecord record = new GenericData.Record(schema);
            record.put("filing_date", filingDate);
            record.put("linkbase_type", "calculation");
            record.put("arc_role", "summation-item");
            record.put("from_concept", cleanConceptName(parentConcept));
            record.put("to_concept", cleanConceptName(concept));
            record.put("weight", weight.isEmpty() ? 1.0 : Double.parseDouble(weight));
            record.put("order", i);
            record.put("preferred_label", element.getAttribute("preferredLabel"));
            records.add(record);
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
  private void downloadAndParseLinkbases(String htmlPath, Schema schema,
      List<GenericRecord> records, String filingDate) throws Exception {

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
        extractLinkbaseRelationships(linkbaseDoc, schema, records, filingDate, linkbaseType);
      } catch (Exception e) {
        LOGGER.warn("Failed to parse linkbase " + linkbaseHref + ": " + e.getMessage());
      }
    }
  }

  /**
   * Extract relationships from a linkbase document.
   */
  private void extractLinkbaseRelationships(Document linkbaseDoc, Schema schema,
      List<GenericRecord> records, String filingDate, String linkbaseType) {

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
          GenericRecord record = new GenericData.Record(schema);
          record.put("filing_date", filingDate);
          record.put("linkbase_type", linkbaseType);

          // Get arc role
          String arcRole = element.getAttribute("xlink:arcrole");
          if (arcRole == null || arcRole.isEmpty()) {
            arcRole = element.getAttribute("arcrole");
          }
          record.put("arc_role", arcRole);

          // Clean concept names
          record.put("from_concept", cleanConceptName(from));
          record.put("to_concept", cleanConceptName(to));

          // Get weight for calculation linkbase
          String weight = element.getAttribute("weight");
          if (weight != null && !weight.isEmpty()) {
            try {
              record.put("weight", Double.parseDouble(weight));
            } catch (NumberFormatException e) {
              record.put("weight", null);
            }
          } else {
            record.put("weight", null);
          }

          // Get order for presentation linkbase
          String order = element.getAttribute("order");
          if (order != null && !order.isEmpty()) {
            try {
              record.put("order", Integer.parseInt(order));
            } catch (NumberFormatException e) {
              record.put("order", null);
            }
          } else {
            record.put("order", null);
          }

          // Get preferred label
          String preferredLabel = element.getAttribute("preferredLabel");
          record.put("preferred_label", preferredLabel);

          records.add(record);
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
   * Download file from URL.
   */
  private String downloadFile(String urlString) {
    try {
      URI uri = URI.create(urlString);
      URL url = uri.toURL();
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(10000);
      conn.setReadTimeout(10000);
      conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36");
      conn.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
      conn.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
      conn.setRequestProperty("Accept-Encoding", "gzip, deflate");
      conn.setRequestProperty("DNT", "1");
      conn.setRequestProperty("Connection", "keep-alive");
      conn.setRequestProperty("Upgrade-Insecure-Requests", "1");

      int responseCode = conn.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        String responseMessage = conn.getResponseMessage();
        LOGGER.warn("HTTP " + responseCode + " (" + responseMessage + ") for URL: " + urlString);

        // Try to read error response body for more details
        try {
          InputStream errorStream = conn.getErrorStream();
          if (errorStream != null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(errorStream));
            String errorResponse = reader.lines().collect(java.util.stream.Collectors.joining("\n"));
            LOGGER.warn("Error response body: " + errorResponse.substring(0, Math.min(500, errorResponse.length())));
          }
        } catch (Exception e) {
          LOGGER.debug("Could not read error response: " + e.getMessage());
        }
        return null;
      }

      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
        StringBuilder content = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
          content.append(line).append("\n");
        }
        return content.toString();
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to download " + urlString + ": " + e.getMessage());
      return null;
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
    NodeList ownershipDocs = doc.getElementsByTagName("ownershipDocument");
    return ownershipDocs.getLength() > 0;
  }

  /**
   * Convert Form 3/4/5 insider trading forms to Parquet.
   */
  private List<File> convertInsiderForm(Document doc, String sourcePath, String targetDirectoryPath,
      String cik, String filingType, String filingDate, String accession) throws IOException {
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    LOGGER.debug(" convertInsiderForm() START for " + filename + " - CIK: " + cik + ", Filing Type: " + filingType + ", Date: " + filingDate);
    List<File> outputFiles = new ArrayList<>();

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

      // Normalize filing type: remove both slashes and hyphens
      String normalizedFilingType = filingType.replace("/", "").replace("-", "");
      String partitionYear = getPartitionYear(filingType, filingDate, doc);

      // Build RELATIVE partition path (relative to targetDirectoryPath which already includes source=sec)
      String relativePartitionPath =
          String.format("cik=%s/filing_type=%s/year=%s", cik, normalizedFilingType, partitionYear);

      // Extract insider transactions
      List<GenericRecord> transactions = extractInsiderTransactions(doc, cik, filingType, filingDate);
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

      Schema schema = createInsiderTransactionSchema();
      writeParquetFile(transactions, schema, outputPath);
      LOGGER.debug(" Successfully wrote insider transactions parquet file: " + outputPath);

      // CRITICAL: Add insider file to outputFiles so addToManifest() can detect it
      outputFiles.add(new File(outputPath));
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Added insider file to outputFiles: {}", outputPath);
      }

      LOGGER.info("Converted Form " + filingType + " to insider transactions: "
          + transactions.size() + " records");

      // Create vectorized blobs for insider forms if text similarity is enabled
      // Note: For now, we're creating a minimal vectorized file for insider forms
      // This could be enhanced to vectorize transaction narratives or remarks
      if (enableVectorization) {
        // Reuse uniqueId from above - build FULL path with StorageProvider
        String vectorizedPath =
            storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_vectorized.parquet", cik, uniqueId));

        try {
          writeInsiderVectorizedBlobsToParquet(doc, vectorizedPath, cik, filingType, filingDate, sourcePath, accession);
          // CRITICAL: Add vectorized file to outputFiles so addToManifest() can detect it
          outputFiles.add(new File(vectorizedPath));
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Added vectorized file to outputFiles: {}", vectorizedPath);
          }
        } catch (Exception ve) {
          LOGGER.warn("Failed to create vectorized blobs for insider form: " + ve.getMessage());
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
  private List<GenericRecord> extractInsiderTransactions(Document doc, String cik,
      String filingType, String filingDate) {
    List<GenericRecord> records = new ArrayList<>();
    Schema schema = createInsiderTransactionSchema();

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
        addNonDerivativeTransactions(doc, cik, filingType, filingDate, reportingPersonCik, reportingPersonName,
            isDirector, isOfficer, isTenPercentOwner, officerTitle, schema, records);
        addNonDerivativeHoldings(doc, cik, filingType, filingDate, reportingPersonCik, reportingPersonName,
            isDirector, isOfficer, isTenPercentOwner, officerTitle, schema, records);
        addDerivativeTransactions(doc, cik, filingType, filingDate, reportingPersonCik, reportingPersonName,
            isDirector, isOfficer, isTenPercentOwner, officerTitle, schema, records);
        addDerivativeHoldings(doc, cik, filingType, filingDate, reportingPersonCik, reportingPersonName,
            isDirector, isOfficer, isTenPercentOwner, officerTitle, schema, records);
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

      addNonDerivativeTransactions(doc, cik, filingType, filingDate, reportingPersonCik, reportingPersonName,
          isDirector, isOfficer, isTenPercentOwner, officerTitle, schema, records);
      addNonDerivativeHoldings(doc, cik, filingType, filingDate, reportingPersonCik, reportingPersonName,
          isDirector, isOfficer, isTenPercentOwner, officerTitle, schema, records);
      addDerivativeTransactions(doc, cik, filingType, filingDate, reportingPersonCik, reportingPersonName,
          isDirector, isOfficer, isTenPercentOwner, officerTitle, schema, records);
      addDerivativeHoldings(doc, cik, filingType, filingDate, reportingPersonCik, reportingPersonName,
          isDirector, isOfficer, isTenPercentOwner, officerTitle, schema, records);
    }

    return records;
  }

  /**
   * Add non-derivative transactions for a specific reporting owner.
   */
  private void addNonDerivativeTransactions(Document doc, String cik, String filingType, String filingDate,
      String reportingPersonCik, String reportingPersonName, boolean isDirector, boolean isOfficer,
      boolean isTenPercentOwner, String officerTitle, Schema schema, List<GenericRecord> records) {

    NodeList nonDerivTrans = doc.getElementsByTagName("nonDerivativeTransaction");
    for (int i = 0; i < nonDerivTrans.getLength(); i++) {
      Element trans = (Element) nonDerivTrans.item(i);

      GenericRecord record = new GenericData.Record(schema);
      record.put("cik", cik);
      record.put("filing_date", filingDate);
      record.put("filing_type", filingType);
      record.put("reporting_person_cik", reportingPersonCik);
      record.put("reporting_person_name", reportingPersonName);
      record.put("is_director", isDirector);
      record.put("is_officer", isOfficer);
      record.put("is_ten_percent_owner", isTenPercentOwner);
      record.put("officer_title", officerTitle);

      // Transaction details
      record.put("transaction_date", getElementText(trans, "transactionDate", "value"));
      record.put("transaction_code", getElementText(trans, "transactionCode"));
      record.put("security_title", getElementText(trans, "securityTitle", "value"));

      String shares = getElementText(trans, "transactionShares", "value");
      record.put("shares_transacted", shares != null ? Double.parseDouble(shares) : null);

      String price = getElementText(trans, "transactionPricePerShare", "value");
      record.put("price_per_share", price != null ? Double.parseDouble(price) : null);

      String sharesAfter = getElementText(trans, "sharesOwnedFollowingTransaction", "value");
      record.put("shares_owned_after", sharesAfter != null ? Double.parseDouble(sharesAfter) : null);

      String acquiredDisposed = getElementText(trans, "transactionAcquiredDisposedCode", "value");
      record.put("acquired_disposed_code", acquiredDisposed);

      String ownership = getElementText(trans, "directOrIndirectOwnership", "value");
      record.put("ownership_type", ownership);

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
      record.put("footnotes", footnotes.length() > 0 ? footnotes.toString() : null);

      records.add(record);
    }
  }

  /**
   * Add non-derivative holdings for a specific reporting owner.
   */
  private void addNonDerivativeHoldings(Document doc, String cik, String filingType, String filingDate,
      String reportingPersonCik, String reportingPersonName, boolean isDirector, boolean isOfficer,
      boolean isTenPercentOwner, String officerTitle, Schema schema, List<GenericRecord> records) {

    NodeList holdings = doc.getElementsByTagName("nonDerivativeHolding");
    for (int i = 0; i < holdings.getLength(); i++) {
      Element holding = (Element) holdings.item(i);

      GenericRecord record = new GenericData.Record(schema);
      record.put("cik", cik);
      record.put("filing_date", filingDate);
      record.put("filing_type", filingType);
      record.put("reporting_person_cik", reportingPersonCik);
      record.put("reporting_person_name", reportingPersonName);
      record.put("is_director", isDirector);
      record.put("is_officer", isOfficer);
      record.put("is_ten_percent_owner", isTenPercentOwner);
      record.put("officer_title", officerTitle);

      // Holding details (no transaction)
      record.put("transaction_date", null);
      record.put("transaction_code", "H"); // H for holding
      record.put("security_title", getElementText(holding, "securityTitle", "value"));
      record.put("shares_transacted", null);
      record.put("price_per_share", null);

      String shares = getElementText(holding, "sharesOwnedFollowingTransaction", "value");
      record.put("shares_owned_after", shares != null ? Double.parseDouble(shares) : null);

      record.put("acquired_disposed_code", null);

      String ownership = getElementText(holding, "directOrIndirectOwnership", "value");
      record.put("ownership_type", ownership);

      String natureOfOwnership = getElementText(holding, "natureOfOwnership", "value");
      record.put("footnotes", natureOfOwnership);

      records.add(record);
    }
  }

  /**
   * Add derivative transactions for a specific reporting owner.
   */
  private void addDerivativeTransactions(Document doc, String cik, String filingType, String filingDate,
      String reportingPersonCik, String reportingPersonName, boolean isDirector, boolean isOfficer,
      boolean isTenPercentOwner, String officerTitle, Schema schema, List<GenericRecord> records) {

    NodeList derivTrans = doc.getElementsByTagName("derivativeTransaction");
    for (int i = 0; i < derivTrans.getLength(); i++) {
      Element trans = (Element) derivTrans.item(i);

      GenericRecord record = new GenericData.Record(schema);
      record.put("cik", cik);
      record.put("filing_date", filingDate);
      record.put("filing_type", filingType);
      record.put("reporting_person_cik", reportingPersonCik);
      record.put("reporting_person_name", reportingPersonName);
      record.put("is_director", isDirector);
      record.put("is_officer", isOfficer);
      record.put("is_ten_percent_owner", isTenPercentOwner);
      record.put("officer_title", officerTitle);

      // Transaction details for derivatives
      record.put("transaction_date", getElementText(trans, "transactionDate", "value"));
      record.put("transaction_code", getElementText(trans, "transactionCode"));

      // For derivatives, append " (Derivative)" to distinguish from non-derivative
      String secTitle = getElementText(trans, "securityTitle", "value");
      record.put("security_title", secTitle != null ? secTitle + " (Derivative)" : "Option/Warrant (Derivative)");

      String shares = getElementText(trans, "transactionShares", "value");
      record.put("shares_transacted", shares != null ? Double.parseDouble(shares) : null);

      // For derivatives, use conversion/exercise price if available
      String price = getElementText(trans, "transactionPricePerShare", "value");
      if (price == null || price.isEmpty()) {
        price = getElementText(trans, "conversionOrExercisePrice", "value");
      }
      record.put("price_per_share", price != null && !price.isEmpty() ? Double.parseDouble(price) : null);

      String sharesAfter = getElementText(trans, "sharesOwnedFollowingTransaction", "value");
      record.put("shares_owned_after", sharesAfter != null ? Double.parseDouble(sharesAfter) : null);

      String acquiredDisposed = getElementText(trans, "transactionAcquiredDisposedCode", "value");
      record.put("acquired_disposed_code", acquiredDisposed);

      String ownership = getElementText(trans, "directOrIndirectOwnership", "value");
      record.put("ownership_type", ownership);

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

      record.put("footnotes", footnotes.length() > 0 ? footnotes.toString() : null);

      records.add(record);
    }
  }

  /**
   * Add derivative holdings for a specific reporting owner.
   */
  private void addDerivativeHoldings(Document doc, String cik, String filingType, String filingDate,
      String reportingPersonCik, String reportingPersonName, boolean isDirector, boolean isOfficer,
      boolean isTenPercentOwner, String officerTitle, Schema schema, List<GenericRecord> records) {

    NodeList derivHoldings = doc.getElementsByTagName("derivativeHolding");
    for (int i = 0; i < derivHoldings.getLength(); i++) {
      Element holding = (Element) derivHoldings.item(i);

      GenericRecord record = new GenericData.Record(schema);
      record.put("cik", cik);
      record.put("filing_date", filingDate);
      record.put("filing_type", filingType);
      record.put("reporting_person_cik", reportingPersonCik);
      record.put("reporting_person_name", reportingPersonName);
      record.put("is_director", isDirector);
      record.put("is_officer", isOfficer);
      record.put("is_ten_percent_owner", isTenPercentOwner);
      record.put("officer_title", officerTitle);

      // Holding details for derivatives
      record.put("transaction_date", null);
      record.put("transaction_code", "H"); // H for holding

      String secTitle = getElementText(holding, "securityTitle", "value");
      record.put("security_title", secTitle != null ? secTitle + " (Derivative)" : "Option/Warrant Holding (Derivative)");

      record.put("shares_transacted", null);

      // For derivative holdings, get conversion/exercise price
      String price = getElementText(holding, "conversionOrExercisePrice", "value");
      record.put("price_per_share", price != null && !price.isEmpty() ? Double.parseDouble(price) : null);

      String shares = getElementText(holding, "sharesOwnedFollowingTransaction", "value");
      record.put("shares_owned_after", shares != null ? Double.parseDouble(shares) : null);

      record.put("acquired_disposed_code", null);

      String ownership = getElementText(holding, "directOrIndirectOwnership", "value");
      record.put("ownership_type", ownership);

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
      record.put("footnotes", footnotes.length() > 0 ? footnotes.toString() : null);

      records.add(record);
    }
  }

  /**
   * Create schema for insider transactions.
   */
  private Schema createInsiderTransactionSchema() {
    return SchemaBuilder.record("InsiderTransaction")
        .namespace("org.apache.calcite.adapter.sec")
        .fields()
        .name("cik").doc("Central Index Key of the issuer company").type().stringType().noDefault()
        .name("filing_date").doc("Date of the Form 3/4/5 filing (ISO 8601 format)").type().stringType().noDefault()
        .name("filing_type").doc("Type of insider filing (Form 3, Form 4, Form 5)").type().stringType().noDefault()
        .name("reporting_person_cik").doc("CIK of the reporting insider").type().nullable().stringType().noDefault()
        .name("reporting_person_name").doc("Name of the reporting insider").type().nullable().stringType().noDefault()
        .name("is_director").doc("Whether the insider is a director").type().booleanType().noDefault()
        .name("is_officer").doc("Whether the insider is an officer").type().booleanType().noDefault()
        .name("is_ten_percent_owner").doc("Whether the insider owns 10% or more of the company").type().booleanType().noDefault()
        .name("officer_title").doc("Title of the officer (if applicable)").type().nullable().stringType().noDefault()
        .name("transaction_date").doc("Date of the transaction (ISO 8601 format)").type().nullable().stringType().noDefault()
        .name("transaction_code").doc("Transaction code (P=purchase, S=sale, A=award, etc.)").type().nullable().stringType().noDefault()
        .name("security_title").doc("Title of the security transacted").type().nullable().stringType().noDefault()
        .name("shares_transacted").doc("Number of shares bought or sold").type().nullable().doubleType().noDefault()
        .name("price_per_share").doc("Price per share in the transaction").type().nullable().doubleType().noDefault()
        .name("shares_owned_after").doc("Shares beneficially owned after the transaction").type().nullable().doubleType().noDefault()
        .name("acquired_disposed_code").doc("Whether shares were acquired (A) or disposed (D)").type().nullable().stringType().noDefault()
        .name("ownership_type").doc("Type of ownership (direct or indirect)").type().nullable().stringType().noDefault()
        .name("footnotes").doc("Additional footnotes and explanations").type().nullable().stringType().noDefault()
        .endRecord();
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
   * Check if this is an 8-K filing.
   */
  private boolean is8KFiling(String filingType) {
    return filingType != null && (filingType.equals("8-K") || filingType.equals("8K")
        || filingType.startsWith("8-K/"));
  }

  /**
   * Extract 8-K exhibits (particularly 99.1 and 99.2 for earnings).
   */
  /**
   * Write vectorized blobs for insider forms (Form 3/4/5).
   * Creates minimal vectors for remarks and transaction descriptions.
   */
  private void writeInsiderVectorizedBlobsToParquet(Document doc, String outputPath,
      String cik, String filingType, String filingDate, String sourcePath, String accession) throws IOException {

    // Create schema for vectorized blobs
    Schema embeddingField = SchemaBuilder.array().items().floatType();
    Schema schema = SchemaBuilder.record("VectorizedBlob")
        .fields()
        .name("vector_id").doc("Unique identifier for this vector").type().stringType().noDefault()
        .name("original_blob_id").doc("Identifier of the original text blob").type().stringType().noDefault()
        .name("blob_type").doc("Type of text blob (insider_remark, insider_footnote)").type().stringType().noDefault()
        .name("blob_content").doc("Original text content of the blob").type().stringType().noDefault()
        .name("embedding").doc("Vector embedding of the text (float array)").type(embeddingField).noDefault()
        .name("cik").doc("Central Index Key of the company").type().stringType().noDefault()
        .name("filing_date").doc("Date of the filing (ISO 8601 format)").type().stringType().noDefault()
        .name("filing_type").doc("Type of filing (Form 3, Form 4, Form 5)").type().stringType().noDefault()
        .name("accession_number").doc("SEC accession number").type().nullable().stringType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();

    // Extract remarks and footnotes from insider forms
    NodeList remarks = doc.getElementsByTagName("remarks");
    NodeList footnotes = doc.getElementsByTagName("footnote");

    // Process remarks
    for (int i = 0; i < remarks.getLength(); i++) {
      String remarkText = remarks.item(i).getTextContent().trim();
      if (!remarkText.isEmpty() && remarkText.length() > 20) {
        GenericRecord record = new GenericData.Record(schema);
        String vectorId = UUID.randomUUID().toString();
        record.put("vector_id", vectorId);
        record.put("original_blob_id", "remark_" + i);
        record.put("blob_type", "insider_remark");
        record.put("blob_content", remarkText);

        // Generate simple embedding for insider forms
        // For now, use a simple hash-based approach as these are short texts
        List<Float> embedding = generateSimpleEmbedding(remarkText, 384);
        record.put("embedding", embedding);

        record.put("cik", cik);
        record.put("filing_date", filingDate);
        record.put("filing_type", filingType);
        record.put("accession_number", accession);

        records.add(record);
      }
    }

    // Process footnotes
    for (int i = 0; i < footnotes.getLength(); i++) {
      String footnoteText = footnotes.item(i).getTextContent().trim();
      if (!footnoteText.isEmpty() && footnoteText.length() > 20) {
        GenericRecord record = new GenericData.Record(schema);
        String vectorId = UUID.randomUUID().toString();
        record.put("vector_id", vectorId);
        record.put("original_blob_id", "footnote_" + i);
        record.put("blob_type", "insider_footnote");
        record.put("blob_content", footnoteText);

        // Generate simple embedding for insider forms
        List<Float> embedding = generateSimpleEmbedding(footnoteText, 384);
        record.put("embedding", embedding);

        record.put("cik", cik);
        record.put("filing_date", filingDate);
        record.put("filing_type", filingType);
        record.put("accession_number", accession);

        records.add(record);
      }
    }

    // Always write the file, even if empty, to satisfy cache validation
    writeRecordsToParquet(records, schema, outputPath);
    if (!records.isEmpty()) {
      LOGGER.info("Wrote " + records.size() + " vectorized insider blobs to " + outputPath);
    } else {
      LOGGER.info("Created empty vectorized file (no content > 20 chars) for " + outputPath);
    }
  }

  private List<File> extract8KExhibits(String sourcePath, String targetDirectoryPath,
      String cik, String filingType, String filingDate, String accession) {
    List<File> outputFiles = new ArrayList<>();

    try {
      // Parse the 8-K filing to find exhibits
      String fileContent;
      try (InputStream is = storageProvider.openInputStream(sourcePath)) {
        fileContent = new String(readAllBytes(is), java.nio.charset.StandardCharsets.UTF_8);
      }

      // Look for Exhibit 99.1 and 99.2 references
      List<GenericRecord> earningsRecords = new ArrayList<>();
      Schema earningsSchema = createEarningsTranscriptSchema();

      // Check if this file contains exhibit content directly
      if (fileContent.contains("EX-99.1") || fileContent.contains("EX-99.2")) {
        earningsRecords.addAll(extractEarningsFromExhibit(fileContent, cik, filingType, filingDate));
      }

      // Also check for earnings-related content patterns
      if (fileContent.toLowerCase().contains("financial results")
          || fileContent.toLowerCase().contains("earnings release")
          || fileContent.toLowerCase().contains("conference call")) {

        // Extract paragraphs from earnings content
        List<String> paragraphs = extractEarningsParagraphs(fileContent);

        for (int i = 0; i < paragraphs.size(); i++) {
          GenericRecord record = new GenericData.Record(earningsSchema);
          record.put("cik", cik);
          record.put("filing_date", filingDate);
          record.put("filing_type", filingType);
          record.put("exhibit_number", detectExhibitNumber(fileContent));
          record.put("section_type", detectSectionType(paragraphs.get(i)));
          record.put("paragraph_number", i + 1);
          record.put("paragraph_text", paragraphs.get(i));
          record.put("speaker_name", extractSpeaker(paragraphs.get(i)));
          record.put("speaker_role", extractSpeakerRole(paragraphs.get(i)));

          earningsRecords.add(record);
        }
      }

      // Write earnings transcripts to Parquet if we found any
      if (!earningsRecords.isEmpty()) {
        // Create partition directory
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

        String normalizedFilingType = filingType.replace("/", "").replace("-", "");
        String partitionYear = filingDate.substring(0, 4); // 8-K filings use filing year

        // Build RELATIVE partition path (relative to targetDirectoryPath which already includes source=sec)
        String relativePartitionPath =
            String.format("cik=%s/filing_type=%s/year=%s", cik, normalizedFilingType, partitionYear);
        String outputPath =
            storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_earnings.parquet", cik, (accession != null && !accession.isEmpty()) ? accession : filingDate));

        writeParquetFile(earningsRecords, earningsSchema, outputPath);

        LOGGER.info("Extracted " + earningsRecords.size() + " earnings paragraphs from 8-K");
      }

    } catch (Exception e) {
      LOGGER.warn("Failed to extract 8-K exhibits: " + e.getMessage());
    }

    return outputFiles;
  }

  /**
   * Extract earnings content from exhibit text.
   */
  private List<GenericRecord> extractEarningsFromExhibit(String exhibitContent,
      String cik, String filingType, String filingDate) {
    List<GenericRecord> records = new ArrayList<>();
    Schema schema = createEarningsTranscriptSchema();

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

      GenericRecord record = new GenericData.Record(schema);
      record.put("cik", cik);
      record.put("filing_date", filingDate);
      record.put("filing_type", filingType);
      record.put("exhibit_number", detectExhibitNumber(exhibitContent));
      record.put("section_type", detectSectionType(text));
      record.put("paragraph_number", paragraphNum);
      record.put("paragraph_text", text);
      record.put("speaker_name", extractSpeaker(text));
      record.put("speaker_role", extractSpeakerRole(text));

      records.add(record);
    }

    return records;
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
  private Schema createEarningsTranscriptSchema() {
    return SchemaBuilder.record("EarningsTranscript")
        .namespace("org.apache.calcite.adapter.sec")
        .fields()
        .name("cik").doc("Central Index Key of the company").type().stringType().noDefault()
        .name("filing_date").doc("Date of the filing (ISO 8601 format)").type().stringType().noDefault()
        .name("filing_type").doc("Type of filing (typically 8-K for earnings)").type().stringType().noDefault()
        .name("exhibit_number").doc("Exhibit number within the filing").type().nullable().stringType().noDefault()
        .name("section_type").doc("Section type (prepared_remarks, qa_session)").type().nullable().stringType().noDefault()
        .name("paragraph_number").doc("Sequential paragraph number").type().intType().noDefault()
        .name("paragraph_text").doc("Full text of the paragraph").type().stringType().noDefault()
        .name("speaker_name").doc("Name of the speaker (for Q&A sections)").type().nullable().stringType().noDefault()
        .name("speaker_role").doc("Role/title of the speaker").type().nullable().stringType().noDefault()
        .endRecord();
  }

  /**
   * Write vectorized blobs with contextual enrichment to Parquet.
   * Creates individual vectors for footnotes and MD&A paragraphs with relationships.
   */
  private void writeVectorizedBlobsToParquet(Document doc, String outputPath,
      String cik, String filingType, String filingDate, String sourcePath) throws IOException {

    // Create schema for vectorized blobs
    Schema embeddingField = SchemaBuilder.array().items().floatType();
    Schema schema = SchemaBuilder.record("VectorizedBlob")
        .fields()
        .name("vector_id").doc("Unique identifier for this vector").type().stringType().noDefault()
        .name("original_blob_id").doc("Identifier of the original text blob").type().stringType().noDefault()
        .name("blob_type").doc("Type of text blob (footnote, mda_paragraph, concept_group)").type().stringType().noDefault()
        .name("filing_date").doc("Date of the filing (ISO 8601 format)").type().stringType().noDefault()
        .name("original_text").doc("Original text before enrichment").type().stringType().noDefault()
        .name("enriched_text").doc("Text after adding financial context and relationships").type().stringType().noDefault()
        .name("embedding").doc("Vector embedding of the enriched text (256-dimensional float array)").type(embeddingField).noDefault()
        .name("parent_section").doc("Parent section identifier (e.g., 'Item 7', 'Note 1')").type().nullable().stringType().noDefault()
        .name("relationships").doc("JSON string of relationships to other blobs").type().nullable().stringType().noDefault()
        .name("financial_concepts").doc("Comma-separated list of referenced financial concepts").type().nullable().stringType().noDefault()
        .name("tokens_used").doc("Number of tokens used for this embedding").type().nullable().intType().noDefault()
        .name("token_budget").doc("Token budget allocated for this blob").type().nullable().intType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();

    // Extract footnotes as TextBlobs
    List<SecTextVectorizer.TextBlob> footnoteBlobs = extractFootnoteBlobs(doc);

    // Extract MD&A paragraphs as TextBlobs
    List<SecTextVectorizer.TextBlob> mdaBlobs = extractMDABlobs(doc, sourcePath);

    // Build relationship map (who references whom)
    Map<String, List<String>> references = buildReferenceMap(footnoteBlobs, mdaBlobs);

    // Extract financial facts for enrichment
    Map<String, SecTextVectorizer.FinancialFact> facts = extractFinancialFactsForVectorization(doc);

    // Create vectorizer instance
    SecTextVectorizer vectorizer = new SecTextVectorizer();

    // Generate individual enriched chunks
    List<SecTextVectorizer.ContextualChunk> individualChunks =
        vectorizer.createIndividualChunks(footnoteBlobs, mdaBlobs, references, facts);

    // Also generate concept group chunks (existing functionality)
    List<SecTextVectorizer.ContextualChunk> conceptChunks =
        vectorizer.createContextualChunks(doc, sourcePath);

    // Combine all chunks
    List<SecTextVectorizer.ContextualChunk> allChunks = new ArrayList<>();
    allChunks.addAll(individualChunks);
    allChunks.addAll(conceptChunks);

    // Convert chunks to Parquet records
    for (SecTextVectorizer.ContextualChunk chunk : allChunks) {
      GenericRecord record = new GenericData.Record(schema);

      record.put("vector_id", chunk.context);
      record.put("original_blob_id", chunk.originalBlobId != null ? chunk.originalBlobId : chunk.context);
      record.put("blob_type", chunk.blobType);
      record.put("filing_date", filingDate);

      // Truncate texts if they're too long for Parquet
      String originalText = chunk.metadata.containsKey("original_text") ?
          (String) chunk.metadata.get("original_text") : "";
      record.put("original_text", truncateText(originalText, 32000));
      record.put("enriched_text", truncateText(chunk.text, 32000));

      // Extract metadata
      record.put("parent_section", chunk.metadata.get("parent_section"));

      // Convert relationships to JSON string
      if (chunk.metadata.containsKey("referenced_by") || chunk.metadata.containsKey("references_footnotes")) {
        Map<String, Object> relationships = new HashMap<>();
        if (chunk.metadata.containsKey("referenced_by")) {
          relationships.put("referenced_by", chunk.metadata.get("referenced_by"));
        }
        if (chunk.metadata.containsKey("references_footnotes")) {
          relationships.put("references", chunk.metadata.get("references_footnotes"));
        }
        record.put("relationships", toJsonString(relationships));
      }

      // Financial concepts
      if (chunk.metadata.containsKey("financial_concepts")) {
        List<String> concepts = (List<String>) chunk.metadata.get("financial_concepts");
        record.put("financial_concepts", String.join(",", concepts));
      }

      // Token usage
      if (chunk.metadata.containsKey("tokens_used")) {
        record.put("tokens_used", chunk.metadata.get("tokens_used"));
      }
      if (chunk.metadata.containsKey("token_budget")) {
        record.put("token_budget", chunk.metadata.get("token_budget"));
      }

      // Add embedding vector (convert double[] to List<Float> for Avro)
      if (chunk.embedding != null && chunk.embedding.length > 0) {
        List<Float> embeddingList = new ArrayList<>();
        for (double value : chunk.embedding) {
          embeddingList.add((float) value);
        }
        record.put("embedding", embeddingList);
      } else {
        throw new IllegalStateException("Chunk missing embedding vector: " + chunk.context +
            " (type: " + chunk.blobType + ")");
      }

      records.add(record);
    }

    // Always write file, even if empty (zero rows) to ensure cache consistency
    storageProvider.writeAvroParquet(outputPath, schema, records, "vectorized_blobs");
    LOGGER.info("Successfully wrote " + records.size() + " vectorized blobs to " + outputPath);
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

      // Process extracted text if found
      if (text != null && text.length() > 200 && !text.startsWith("<")) {
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
   * Extract MD&A paragraphs as TextBlob objects.
   */
  private List<SecTextVectorizer.TextBlob> extractMDABlobs(Document xbrlDoc, String sourcePath) {
    List<SecTextVectorizer.TextBlob> blobs = new ArrayList<>();

    // Extract MD&A from both traditional XBRL and inline XBRL (iXBRL)
    NodeList mdaElements = xbrlDoc.getElementsByTagName("*");
    int mdaCounter = 0;

    for (int i = 0; i < mdaElements.getLength(); i++) {
      Element element = (Element) mdaElements.item(i);
      String localName = element.getLocalName();
      String namespaceURI = element.getNamespaceURI();

      String elementName = null;
      String text = null;

      // Check for traditional XBRL MD&A elements
      if (localName != null &&
          (localName.contains("ManagementDiscussionAndAnalysis") ||
           localName.contains("MDAndA") ||
           localName.contains("Item7"))) {
        text = element.getTextContent().trim();
        elementName = localName;
      }
      // Check for inline XBRL (iXBRL) MD&A elements
      else if ("nonNumeric".equals(localName) && namespaceURI != null
               && namespaceURI.contains("xbrl")) {
        String name = element.getAttribute("name");
        if (name != null &&
            (name.contains("ManagementDiscussionAndAnalysis") ||
             name.contains("MDAndA") ||
             name.contains("Item7"))) {
          text = element.getTextContent().trim();
          // Extract element name from the name attribute
          elementName = name.contains(":") ? name.substring(name.indexOf(":") + 1) : name;
        }
      }

      // Process extracted text if found
      if (text != null && text.length() > 200) {
        // Split into paragraphs for better vectorization
        String[] paragraphs = text.split("\\n\\n+|(?<=\\.)\\s+(?=[A-Z])");

        for (String paragraph : paragraphs) {
          paragraph = paragraph.trim();
          if (paragraph.length() > 100 && !paragraph.startsWith("<")) {
            String id = "mda_para_" + (++mdaCounter);
            String parentSection = "Management Discussion and Analysis";
            String subsection = detectMDASubsection(paragraph);

            Map<String, String> attributes = new HashMap<>();
            attributes.put("source", "xbrl");
            attributes.put("element", elementName != null ? elementName : "");

            SecTextVectorizer.TextBlob blob =
                new SecTextVectorizer.TextBlob(id, "mda_paragraph", paragraph, parentSection, subsection, attributes);
            blobs.add(blob);
          }
        }
      }
    }

    // If no MD&A in XBRL and source is HTML, extract from HTML
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    if (blobs.isEmpty() && (filename.endsWith(".htm") || filename.endsWith(".html"))) {
      blobs.addAll(extractMDAFromHtml(sourcePath));
    }

    // If still no MD&A and this is XBRL, look for associated HTML filing
    if (blobs.isEmpty() && filename.endsWith(".xml")) {
      String htmlPath = findAssociatedHtmlFiling(sourcePath);
      if (htmlPath != null) {
        blobs.addAll(extractMDAFromHtml(htmlPath));
      }
    }

    LOGGER.info("Extracted " + blobs.size() + " MD&A paragraphs");
    return blobs;
  }

  /**
   * Find the associated HTML filing for an XBRL file.
   */
  private String findAssociatedHtmlFiling(String xbrlPath) {
    // Look for 10-K or 10-Q HTML in same directory
    // This method is limited to local filesystem operations
    // For cloud storage, associated filings would need to be tracked differently
    String parentPath = xbrlPath.substring(0, xbrlPath.lastIndexOf('/'));

    // Only attempt directory listing for local filesystem paths
    if (parentPath.contains("://")) {
      // Cloud storage - cannot easily list directory contents
      // Would need to use StorageProvider.listFiles() if that method exists
      return null;
    }

    java.io.File parentDir = new java.io.File(parentPath);
    if (parentDir.exists() && parentDir.isDirectory()) {
      java.io.File[] htmlFiles = parentDir.listFiles((dir, name) ->
        (name.endsWith(".htm") || name.endsWith(".html")) &&
        !name.contains("_cal") && !name.contains("_def") && !name.contains("_lab") &&
        !name.contains("_pre") && !name.contains("_ref"));

      if (htmlFiles != null && htmlFiles.length > 0) {
        // Prefer files with 10-K or 10-Q in name
        for (java.io.File file : htmlFiles) {
          String name = file.getName().toLowerCase();
          if (name.contains("10-k") || name.contains("10k") ||
              name.contains("10-q") || name.contains("10q")) {
            return file.getAbsolutePath();
          }
        }
        return htmlFiles[0].getAbsolutePath();  // Return first HTML file if no specific match
      }
    }
    return null;
  }

  /**
   * Extract MD&A from HTML filing.
   */
  private List<SecTextVectorizer.TextBlob> extractMDAFromHtml(String htmlPath) {
    List<SecTextVectorizer.TextBlob> blobs = new ArrayList<>();

    try {
      org.jsoup.nodes.Document htmlDoc;
      try (InputStream is = storageProvider.openInputStream(htmlPath)) {
        htmlDoc = Jsoup.parse(is, "UTF-8", "");
      }

      // Find Item 7 or MD&A sections
      Elements mdaSections =
        htmlDoc.select("*:matchesOwn((?i)(item\\s*7[^0-9]|management.*discussion.*analysis))");

      int paraCounter = 0;
      for (org.jsoup.nodes.Element section : mdaSections) {
        // Skip if this is table of contents
        if (section.parents().select("table").size() > 0) continue;

        // Extract following content until next major item
        org.jsoup.nodes.Element current = section;
        int localCounter = 0;
        boolean inMDA = true;

        while (current != null && localCounter < 200 && inMDA) {
          current = current.nextElementSibling();
          if (current == null) break;

          String text = current.text().trim();

          // Stop at next item
          if (isNextItem(text)) {
            inMDA = false;
            break;
          }

          // Extract meaningful paragraphs
          if (text.length() > 100 && !isBoilerplate(text)) {
            String id = "mda_para_" + (++paraCounter);
            String parentSection = "Management Discussion and Analysis";
            String subsection = detectMDASubsection(text);

            Map<String, String> attributes = new HashMap<>();
            attributes.put("source", "html");
            String fileName = htmlPath.substring(htmlPath.lastIndexOf('/') + 1);
            attributes.put("file", fileName);

            SecTextVectorizer.TextBlob blob =
                new SecTextVectorizer.TextBlob(id, "mda_paragraph", text, parentSection, subsection, attributes);
            blobs.add(blob);
          }

          localCounter++;
        }
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to parse HTML for MD&A: " + e.getMessage());
    }

    return blobs;
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
   * Extract financial facts for vectorization enrichment.
   */
  private Map<String, SecTextVectorizer.FinancialFact> extractFinancialFactsForVectorization(Document doc) {
    // This method would need to be made accessible from SecTextVectorizer
    // For now, return empty map - the vectorizer will handle this internally
    return new HashMap<>();
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

  private String extractFilingDateFromHtml(File htmlFile) {
    try {
      String content = Files.readString(htmlFile.toPath());

      // Try various patterns to find the period end date in iXBRL
      // Pattern 1: ix:nonnumeric with name containing PeriodEndDate
      Pattern pattern1 = Pattern.compile("name=\"[^\"]*PeriodEndDate[^\"]*\"[^>]*>([^<]+)</");
      Matcher matcher1 = pattern1.matcher(content);
      if (matcher1.find()) {
        String dateStr = matcher1.group(1).trim();
        return normalizeDate(dateStr);
      }

      // Pattern 2: contextRef with period end date
      Pattern pattern2 = Pattern.compile("contextref=\"[^\"]*\"[^>]*>\\s*(\\d{4}-\\d{2}-\\d{2}|\\d{1,2}/\\d{1,2}/\\d{4})");
      Matcher matcher2 = pattern2.matcher(content);
      if (matcher2.find()) {
        String dateStr = matcher2.group(1).trim();
        return normalizeDate(dateStr);
      }

      // Pattern 3: Look for dei:DocumentPeriodEndDate
      Pattern pattern3 = Pattern.compile("dei:DocumentPeriodEndDate[^>]*>([^<]+)</");
      Matcher matcher3 = pattern3.matcher(content);
      if (matcher3.find()) {
        String dateStr = matcher3.group(1).trim();
        return normalizeDate(dateStr);
      }

      // Pattern 4: Look for period end in meta tags
      Pattern pattern4 = Pattern.compile("<meta[^>]*name=\"[^\"]*period[^\"]*\"[^>]*content=\"([^\"]+)\"");
      Matcher matcher4 = pattern4.matcher(content);
      if (matcher4.find()) {
        String dateStr = matcher4.group(1).trim();
        return normalizeDate(dateStr);
      }

    } catch (IOException e) {
      LOGGER.warn("Failed to read HTML file for date extraction: " + e.getMessage());
    }
    return null;
  }

  private Map<String, String> extractCompanyInfoFromHtml(File htmlFile) {
    Map<String, String> info = new HashMap<>();
    try {
      String content = Files.readString(htmlFile.toPath());

      // Extract company name
      Pattern namePattern = Pattern.compile("dei:EntityRegistrantName[^>]*>([^<]+)</");
      Matcher nameMatcher = namePattern.matcher(content);
      if (nameMatcher.find()) {
        info.put("company_name", nameMatcher.group(1).trim());
      }

      // Extract state of incorporation
      Pattern statePattern = Pattern.compile("dei:EntityIncorporationStateCountryCode[^>]*>([^<]+)</");
      Matcher stateMatcher = statePattern.matcher(content);
      if (stateMatcher.find()) {
        info.put("state_of_incorporation", stateMatcher.group(1).trim());
      }

      // Extract fiscal year end
      Pattern fyPattern = Pattern.compile("dei:CurrentFiscalYearEndDate[^>]*>([^<]+)</");
      Matcher fyMatcher = fyPattern.matcher(content);
      if (fyMatcher.find()) {
        info.put("fiscal_year_end", fyMatcher.group(1).trim());
      }

      // Extract SIC code
      Pattern sicPattern = Pattern.compile("dei:EntityStandardIndustrialClassificationCode[^>]*>([^<]+)</");
      Matcher sicMatcher = sicPattern.matcher(content);
      if (sicMatcher.find()) {
        info.put("sic_code", sicMatcher.group(1).trim());
      }

    } catch (IOException e) {
      LOGGER.warn("Failed to read HTML file for company info extraction: " + e.getMessage());
    }
    return info;
  }

  private String normalizeDate(String dateStr) {
    if (dateStr == null || dateStr.isEmpty()) {
      return null;
    }

    // Already in YYYY-MM-DD format
    if (dateStr.matches("\\d{4}-\\d{2}-\\d{2}")) {
      return dateStr;
    }

    // MM/DD/YYYY format
    if (dateStr.matches("\\d{1,2}/\\d{1,2}/\\d{4}")) {
      String[] parts = dateStr.split("/");
      return String.format("%s-%02d-%02d", parts[2],
          Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
    }

    // YYYYMMDD format
    if (dateStr.matches("\\d{8}")) {
      return dateStr.substring(0, 4) + "-" + dateStr.substring(4, 6) + "-" + dateStr.substring(6, 8);
    }

    // Month DD, YYYY format (e.g., "September 30, 2024")
    Pattern monthDayYear = Pattern.compile("(\\w+)\\s+(\\d{1,2}),\\s+(\\d{4})");
    Matcher matcher = monthDayYear.matcher(dateStr);
    if (matcher.find()) {
      String month = matcher.group(1);
      String day = matcher.group(2);
      String year = matcher.group(3);
      int monthNum = getMonthNumber(month);
      if (monthNum > 0) {
        return String.format("%s-%02d-%02d", year, monthNum, Integer.parseInt(day));
      }
    }

    return null;
  }

  private int getMonthNumber(String month) {
    String[] months = {"january", "february", "march", "april", "may", "june",
                       "july", "august", "september", "october", "november", "december"};
    String monthLower = month.toLowerCase();
    for (int i = 0; i < months.length; i++) {
      if (months[i].startsWith(monthLower.substring(0, Math.min(3, monthLower.length())))) {
        return i + 1;
      }
    }
    return 0;
  }

  private void createEnhancedMetadata(String sourcePath, String targetDirectoryPath, String cik,
      String filingType, String filingDate, String accession, Map<String, String> companyInfo) throws IOException {
    // Build partition path
    String normalizedFilingType = filingType.replace("-", "").replace("/", "");
    String partitionYear = filingDate.substring(0, 4); // For metadata without doc context, use filing year
    String relativePartitionPath = String.format("cik=%s/filing_type=%s/year=%s", cik, normalizedFilingType, partitionYear);

    // Build metadata file path with FULL path from StorageProvider
    String metadataPath =
        storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_metadata.parquet", cik, (accession != null && !accession.isEmpty()) ? accession : filingDate));

    // Define schema for metadata
    Schema schema = SchemaBuilder.record("FilingMetadata")
        .fields()
        .name("accession_number").doc("SEC accession number (unique filing identifier)").type().stringType().noDefault()
        .name("filing_date").doc("Date of the SEC filing (ISO 8601 format)").type().stringType().noDefault()
        .name("company_name").doc("Legal name of the registrant company").type().nullable().stringType().noDefault()
        .name("state_of_incorporation").doc("State or jurisdiction of incorporation").type().nullable().stringType().noDefault()
        .name("fiscal_year_end").doc("Fiscal year end date (MMDD format)").type().nullable().stringType().noDefault()
        .name("sic_code").doc("Standard Industrial Classification code").type().nullable().stringType().noDefault()
        .name("irs_number").doc("IRS Employer Identification Number (EIN)").type().nullable().stringType().noDefault()
        .name("business_address").doc("Physical business address of the company").type().nullable().stringType().noDefault()
        .name("mailing_address").doc("Mailing address for correspondence").type().nullable().stringType().noDefault()
        .name("filing_url").doc("URL or filename of the source filing document").type().stringType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    GenericRecord record = new GenericData.Record(schema);
    record.put("accession_number", accession != null ? accession : cik + "-" + filingDate);
    record.put("filing_date", filingDate);
    record.put("company_name", companyInfo.get("company_name"));
    record.put("state_of_incorporation", companyInfo.get("state_of_incorporation"));
    record.put("fiscal_year_end", companyInfo.get("fiscal_year_end"));
    record.put("sic_code", companyInfo.get("sic_code"));
    record.put("irs_number", companyInfo.get("irs_number"));
    record.put("business_address", companyInfo.get("business_address"));
    record.put("mailing_address", companyInfo.get("mailing_address"));
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    record.put("filing_url", filename);
    records.add(record);

    // Use consolidated StorageProvider method for Parquet writing
    storageProvider.writeAvroParquet(metadataPath, schema, records, "metadata");
  }

  private void createMinimalMetadata(String sourcePath, String targetDirectoryPath, String cik,
      String filingType, String filingDate, String accession) throws IOException {
    // Build partition path
    String normalizedFilingType = filingType.replace("-", "").replace("/", "");
    String partitionYear = filingDate.substring(0, 4); // For metadata without doc context, use filing year
    String relativePartitionPath = String.format("cik=%s/filing_type=%s/year=%s", cik, normalizedFilingType, partitionYear);

    // Build metadata file path with FULL path from StorageProvider
    String metadataPath =
        storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_metadata.parquet", cik, (accession != null && !accession.isEmpty()) ? accession : filingDate));

    // Define schema for minimal metadata
    Schema schema = SchemaBuilder.record("FilingMetadata")
        .fields()
        .name("accession_number").doc("SEC accession number (unique filing identifier)").type().stringType().noDefault()
        .name("filing_date").doc("Date of the SEC filing (ISO 8601 format)").type().stringType().noDefault()
        .name("company_name").doc("Legal name of the registrant company").type().nullable().stringType().noDefault()
        .name("state_of_incorporation").doc("State or jurisdiction of incorporation").type().nullable().stringType().noDefault()
        .name("fiscal_year_end").doc("Fiscal year end date (MMDD format)").type().nullable().stringType().noDefault()
        .name("sic_code").doc("Standard Industrial Classification code").type().nullable().stringType().noDefault()
        .name("irs_number").doc("IRS Employer Identification Number (EIN)").type().nullable().stringType().noDefault()
        .name("business_address").doc("Physical business address of the company").type().nullable().stringType().noDefault()
        .name("mailing_address").doc("Mailing address for correspondence").type().nullable().stringType().noDefault()
        .name("filing_url").doc("URL or filename of the source filing document").type().stringType().noDefault()
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();
    GenericRecord record = new GenericData.Record(schema);
    record.put("accession_number", accession != null ? accession : cik + "-" + filingDate);
    record.put("filing_date", filingDate);
    // We don't have company info from inline XBRL that failed to parse
    record.put("company_name", null);
    record.put("state_of_incorporation", null);
    record.put("fiscal_year_end", null);
    record.put("sic_code", null);
    record.put("irs_number", null);
    record.put("business_address", null);
    record.put("mailing_address", null);
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    record.put("filing_url", filename);
    records.add(record);

    // Use consolidated StorageProvider method for Parquet writing
    storageProvider.writeAvroParquet(metadataPath, schema, records, "metadata");
  }

  /**
   * Determine the appropriate year for partitioning based on filing type.
   * For 10-Q and 10-K filings, use fiscal year (from period end date).
   * For all other filings, use filing year.
   */
  private String getPartitionYear(String filingType, String filingDate, Document doc) {
    String normalizedType = filingType.replace("-", "").replace("/", "");

    // For 10-Q and 10-K filings, use fiscal year from period end date
    if ("10Q".equals(normalizedType) || "10K".equals(normalizedType)) {
      String periodEndDate = extractPeriodEndDateFromDocument(doc);
      if (periodEndDate != null && periodEndDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
        return periodEndDate.substring(0, 4); // Extract year from period end date
      }
    }

    // For all other filing types, use filing year
    return filingDate.substring(0, 4);
  }

  /**
   * Extract period end date from XBRL document.
   * Looks for endDate elements in the document.
   */
  private String extractPeriodEndDateFromDocument(Document doc) {
    try {
      NodeList endDateNodes = doc.getElementsByTagName("endDate");
      if (endDateNodes.getLength() > 0) {
        return endDateNodes.item(0).getTextContent().trim();
      }

      // Try alternative tag names
      endDateNodes = doc.getElementsByTagName("xbrli:endDate");
      if (endDateNodes.getLength() > 0) {
        return endDateNodes.item(0).getTextContent().trim();
      }

      // Try period elements with endDate
      NodeList periodNodes = doc.getElementsByTagName("period");
      for (int i = 0; i < periodNodes.getLength(); i++) {
        Element periodElement = (Element) periodNodes.item(i);
        NodeList endDates = periodElement.getElementsByTagName("endDate");
        if (endDates.getLength() > 0) {
          return endDates.item(0).getTextContent().trim();
        }
      }

      return null;
    } catch (Exception e) {
      LOGGER.warn("Error extracting period end date: " + e.getMessage());
      return null;
    }
  }

  /**
   * Track parsing errors to a file for offline review.
   * This helps identify which documents are failing to parse and why.
   */
  private void trackParsingError(String sourcePath, String errorType, String errorMessage) {
    try {
      // Create error log file in /tmp for easy access
      File errorLogFile = new File("/tmp/xbrl_parsing_errors.log");

      // Format: timestamp | file_path | error_type | error_message
      String timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
      String logEntry =
          String.format("%s | %s | %s | %s%n", timestamp,
          sourcePath,
          errorType,
          errorMessage);

      // Append to file
      java.nio.file.Files.write(
          errorLogFile.toPath(),
          logEntry.getBytes(),
          java.nio.file.StandardOpenOption.CREATE,
          java.nio.file.StandardOpenOption.APPEND);

      LOGGER.debug("Tracked parsing error to: " + errorLogFile.getAbsolutePath());

    } catch (IOException e) {
      LOGGER.warn("Failed to track parsing error: " + e.getMessage());
    }
  }

  private void trackConversionError(String sourcePath, String errorType, String errorMessage) {
    try {
      // Create error log file in /tmp for easy access
      File errorLogFile = new File("/tmp/xbrl_conversion_errors.log");

      // Format: timestamp | file_path | error_type | error_message
      String timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
      String logEntry =
          String.format("%s | %s | %s | %s%n", timestamp,
          sourcePath,
          errorType,
          errorMessage);

      // Append to file
      java.nio.file.Files.write(
          errorLogFile.toPath(),
          logEntry.getBytes(),
          java.nio.file.StandardOpenOption.CREATE,
          java.nio.file.StandardOpenOption.APPEND);

      LOGGER.debug("Tracked conversion error to: " + errorLogFile.getAbsolutePath());

    } catch (IOException e) {
      LOGGER.warn("Failed to track conversion error: " + e.getMessage());
    }
  }
}
