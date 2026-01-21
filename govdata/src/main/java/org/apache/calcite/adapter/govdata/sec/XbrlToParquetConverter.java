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
    // Direct String-based method for S3 paths - no File wrapping
    return convertInternal(sourcePath, targetDirectory, metadata);
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
          LOGGER.debug("Stack trace for inline XBRL extraction exception", e);
        }

        // If still no document, don't create minimal parquet files
        // Let the parsing failure be properly reported and track for offline review
        if (doc == null) {
          LOGGER.warn("Failed to extract XBRL data from inline XBRL file - parsing error for: {}", fileName);
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
        List<String> extraFiles = extract8KExhibits(sourceFilePath, targetDirectoryPath, cik, filingType, filingDate, accession);
        outputFiles.addAll(extraFiles);
      }

      // Create partitioned output path - BUILD RELATIVE PATHS
      // Year-only partitioning for optimal 128MB file sizes
      String partitionYear = getPartitionYear(filingType, filingDate, doc);

      // Build RELATIVE partition path (relative to targetDirectoryPath which already includes source=sec)
      // Uses year-only partitioning - CIK/filing_type filtering done via Parquet/Iceberg statistics
      String relativePartitionPath = String.format("year=%s", partitionYear);

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
        writeFactsToParquet(doc, factsPath, cik, filingType, filingDate, accession, sourceFilePath);
        // Paths are already full paths from storageProvider.resolvePath()
        outputFiles.add(factsPath);
        LOGGER.debug(" Successfully created facts.parquet: " + factsPath);
      } catch (Exception e) {
        LOGGER.error("Exception during facts.parquet creation for {}: {}", fileName, e.getMessage());
        throw e;
      }

      // Write filing metadata
      writeMetadataToParquet(doc, metadataPath, cik, filingType, filingDate, sourceFilePath);
      outputFiles.add(metadataPath);

      // Convert contexts to Parquet
      writeContextsToParquet(doc, contextsPath, cik, filingType, filingDate, accession);
      outputFiles.add(contextsPath);

      // Extract and write MD&A
      writeMDAToParquet(doc, mdaPath, cik, filingType, filingDate, accession, sourceFilePath);
      outputFiles.add(mdaPath);

      // Extract and write XBRL relationships
      LOGGER.debug(" Starting relationships.parquet generation for: " + fileName + " -> " + relationshipsPath);
      try {
        writeRelationshipsToParquet(doc, relationshipsPath, cik, filingType, filingDate, sourceFilePath);
        outputFiles.add(relationshipsPath);
        LOGGER.debug(" Successfully created relationships.parquet: " + relationshipsPath);
      } catch (Exception e) {
        LOGGER.error("Exception during relationships.parquet creation for {}: {}", fileName, e.getMessage());
        throw e;
      }

      // Create vectorized blobs with contextual enrichment if enabled
      if (enableVectorization) {
        String vectorizedPath =
            storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_vectorized.parquet", cik, uniqueId));
        writeVectorizedBlobsToParquet(doc, vectorizedPath, cik, filingType, filingDate, sourceFilePath);
        outputFiles.add(vectorizedPath);
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
  }

  private void writeMetadataToParquet(Document doc, String outputPath,
      String cik, String filingType, String filingDate, String sourcePath) throws IOException {

    // Load column metadata from sec-schema.json
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractSecDataDownloader.loadTableColumns("filing_metadata");

    List<Map<String, Object>> dataList = new ArrayList<>();
    Map<String, Object> data = new HashMap<>();

    // Extract accession number from filename
    String filename = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    String accessionNumber = extractAccessionNumber(filename);
    data.put("cik", cik);
    data.put("accession_number", accessionNumber != null ? accessionNumber : "");
    data.put("filing_type", filingType);
    data.put("filing_date", filingDate);

    // Extract DEI (Document and Entity Information) elements
    // Try different namespaces and formats
    String companyName = extractDeiValue(doc, "EntityRegistrantName", "RegistrantName");
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

  private void writeContextsToParquet(Document doc, String outputPath,
      String cik, String filingType, String filingDate, String accession) throws IOException {

    // Load column metadata from sec-schema.json
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractSecDataDownloader.loadTableColumns("filing_contexts");

    // Extract context elements
    List<Map<String, Object>> dataList = new ArrayList<>();
    NodeList contexts = doc.getElementsByTagNameNS("*", "context");

    for (int i = 0; i < contexts.getLength(); i++) {
      Element context = (Element) contexts.item(i);
      Map<String, Object> data = new HashMap<>();

      // Required data columns (cik and accession_number are in schema)
      data.put("cik", cik);
      data.put("accession_number", accession != null ? accession : cik + "-" + filingDate);
      data.put("filing_date", filingDate);
      data.put("context_id", context.getAttribute("id"));

      // Extract entity information
      NodeList identifiers = context.getElementsByTagNameNS("*", "identifier");
      if (identifiers.getLength() > 0) {
        Element identifier = (Element) identifiers.item(0);
        data.put("entity_identifier", identifier.getTextContent());
        data.put("entity_scheme", identifier.getAttribute("scheme"));
      } else {
        data.put("entity_identifier", null);
        data.put("entity_scheme", null);
      }

      // Extract period information
      NodeList startDates = context.getElementsByTagNameNS("*", "startDate");
      NodeList endDates = context.getElementsByTagNameNS("*", "endDate");
      NodeList instants = context.getElementsByTagNameNS("*", "instant");

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
   * Uses semantic chunking to create optimal text chunks for downstream processing.
   */
  private void writeMDAToParquet(Document doc, String outputPath,
      String cik, String filingType, String filingDate, String accession, String sourcePath)
      throws IOException {

    // Load column metadata from sec-schema.json
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractSecDataDownloader.loadTableColumns("mda_sections");

    List<Map<String, Object>> dataList = new ArrayList<>();

    // Format accession_number consistently (10-digit-CIK-YY-NNNNNN format)
    String accessionNumber = accession != null ? accession : cik + "-" + filingDate;

    // Use semantic chunker for optimal text extraction
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

        // Check if this is an MD&A-related concept
        if (isMDAConcept(concept)) {
          String text = element.getTextContent().trim();
          if (!text.isEmpty()) {
            // Use chunker for XBRL TextBlock content
            List<SemanticTextChunker.Chunk> chunks = chunker.chunkPlainText(text);
            for (SemanticTextChunker.Chunk chunk : chunks) {
              Map<String, Object> data = new HashMap<>();
              data.put("cik", cik);
              data.put("accession_number", accessionNumber);
              data.put("filing_date", filingDate);
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

    // Only write file if there's data - empty parquet files cause DuckDB union_by_name issues
    if (!dataList.isEmpty()) {
      storageProvider.writeAvroParquet(outputPath, columns, dataList, "MDASection", "MDASection");
      LOGGER.info("Successfully wrote " + dataList.size() + " MD&A chunks to " + outputPath);
    } else {
      LOGGER.debug("Skipping empty MD&A file: " + outputPath);
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
      String cik, String filingType, String filingDate, String sourcePath) throws IOException {

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
      data.put("filing_date", filingDate);

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
          data.put("order", Integer.parseInt(order));
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
    extractInlineXBRLRelationships(doc, columns, dataList, filingDate, sourcePath);
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
      List<Map<String, Object>> dataList, String filingDate, String sourcePath) {

    String fileName = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
    LOGGER.debug(" extractInlineXBRLRelationships START");
    LOGGER.debug(String.format("DEBUG: Document is null? %s", doc == null));
    LOGGER.debug(String.format("DEBUG: Source file is: %s", fileName));

    // First, try to download and parse external linkbase files
    // These contain the actual relationship definitions for inline XBRL
    try {
      downloadAndParseLinkbases(sourcePath, columns, dataList, filingDate);
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
              data.put("filing_date", filingDate);
              data.put("linkbase_type", determineLinkbaseType(arcrole));
              data.put("arc_role", arcrole);
              data.put("from_concept", cleanConceptName(fromRef));
              data.put("to_concept", cleanConceptName(toRef));
              data.put("weight", weight != null && !weight.isEmpty() ? Double.parseDouble(weight) : null);
              data.put("order", order != null && !order.isEmpty() ? Integer.parseInt(order) : i);
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
                data.put("filing_date", filingDate);
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
                  data.put("filing_date", filingDate);
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
            data.put("filing_date", filingDate);
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
      List<Map<String, Object>> dataList, String filingDate) throws Exception {

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
        extractLinkbaseRelationships(linkbaseDoc, columns, dataList, filingDate, linkbaseType);
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
      List<Map<String, Object>> dataList, String filingDate, String linkbaseType) {

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
          data.put("filing_date", filingDate);
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
              data.put("order", Integer.parseInt(order));
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
      // SEC.gov requires proper User-Agent identifying automated tools with contact info
      // See: https://www.sec.gov/os/accessing-edgar-data
      String userAgent = "Apache Calcite GovData Adapter 1.0 (kenstott@github.com)";
      conn.setRequestProperty("User-Agent", userAgent);
      conn.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
      conn.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
      // Removed "Accept-Encoding: gzip, deflate" - was causing SEC to return gzip-compressed error responses
      // that couldn't be read as plain text. SEC.gov appears to return 403 for XSD downloads regardless.
      conn.setRequestProperty("DNT", "1");
      conn.setRequestProperty("Connection", "keep-alive");
      conn.setRequestProperty("Upgrade-Insecure-Requests", "1");

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Attempting XSD download with User-Agent: {}", userAgent);
      }

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
      String partitionYear = getPartitionYear(filingType, filingDate, doc);

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
          outputFiles.add(vectorizedPath);
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
        addNonDerivativeTransactions(doc, cik, filingType, filingDate, accession, reportingPersonCik, reportingPersonName,
            isDirector, isOfficer, isTenPercentOwner, officerTitle, columns, dataList);
        addNonDerivativeHoldings(doc, cik, filingType, filingDate, accession, reportingPersonCik, reportingPersonName,
            isDirector, isOfficer, isTenPercentOwner, officerTitle, columns, dataList);
        addDerivativeTransactions(doc, cik, filingType, filingDate, accession, reportingPersonCik, reportingPersonName,
            isDirector, isOfficer, isTenPercentOwner, officerTitle, columns, dataList);
        addDerivativeHoldings(doc, cik, filingType, filingDate, accession, reportingPersonCik, reportingPersonName,
            isDirector, isOfficer, isTenPercentOwner, officerTitle, columns, dataList);
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

    // Load schema from metadata
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractSecDataDownloader.loadTableColumns("vectorized_blobs");

    List<Map<String, Object>> dataList = new ArrayList<>();

    // Extract remarks and footnotes from insider forms
    NodeList remarks = doc.getElementsByTagName("remarks");
    NodeList footnotes = doc.getElementsByTagName("footnote");

    // Process remarks
    for (int i = 0; i < remarks.getLength(); i++) {
      String remarkText = remarks.item(i).getTextContent().trim();
      if (!remarkText.isEmpty() && remarkText.length() > 20) {
        Map<String, Object> data = new HashMap<>();
        String vectorId = UUID.randomUUID().toString();
        data.put("vector_id", vectorId);
        data.put("original_blob_id", "remark_" + i);
        data.put("blob_type", "insider_remark");
        data.put("blob_content", remarkText);

        // Generate simple embedding for insider forms
        // For now, use a simple hash-based approach as these are short texts
        List<Float> embedding = generateSimpleEmbedding(remarkText, 384);
        data.put("embedding", embedding);

        data.put("cik", cik);
        data.put("filing_date", filingDate);
        data.put("filing_type", filingType);
        data.put("accession_number", accession);

        dataList.add(data);
      }
    }

    // Process footnotes
    for (int i = 0; i < footnotes.getLength(); i++) {
      String footnoteText = footnotes.item(i).getTextContent().trim();
      if (!footnoteText.isEmpty() && footnoteText.length() > 20) {
        Map<String, Object> data = new HashMap<>();
        String vectorId = UUID.randomUUID().toString();
        data.put("vector_id", vectorId);
        data.put("original_blob_id", "footnote_" + i);
        data.put("blob_type", "insider_footnote");
        data.put("blob_content", footnoteText);

        // Generate simple embedding for insider forms
        List<Float> embedding = generateSimpleEmbedding(footnoteText, 384);
        data.put("embedding", embedding);

        data.put("cik", cik);
        data.put("filing_date", filingDate);
        data.put("filing_type", filingType);
        data.put("accession_number", accession);

        dataList.add(data);
      }
    }

    // Always write the file, even if empty, to satisfy cache validation
    storageProvider.writeAvroParquet(outputPath, columns, dataList, "VectorizedBlob", "vectorized_blobs");
    if (!dataList.isEmpty()) {
      LOGGER.info("Wrote " + dataList.size() + " vectorized insider blobs to " + outputPath);
    } else {
      LOGGER.info("Created empty vectorized file (no content > 20 chars) for " + outputPath);
    }
  }

  private List<String> extract8KExhibits(String sourcePath, String targetDirectoryPath,
      String cik, String filingType, String filingDate, String accession) {
    List<String> outputFiles = new ArrayList<>();

    try {
      // Parse the 8-K filing to find exhibits
      String fileContent;
      try (InputStream is = storageProvider.openInputStream(sourcePath)) {
        fileContent = new String(readAllBytes(is), java.nio.charset.StandardCharsets.UTF_8);
      }

      // Look for Exhibit 99.1 and 99.2 references
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

        // Extract paragraphs from earnings content
        List<String> paragraphs = extractEarningsParagraphs(fileContent);

        for (int i = 0; i < paragraphs.size(); i++) {
          Map<String, Object> data = new HashMap<>();
          data.put("accession_number", accession);
          data.put("cik", cik);
          data.put("filing_date", filingDate);
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

        String partitionYear = filingDate.substring(0, 4); // 8-K filings use filing year

        // Build RELATIVE partition path (relative to targetDirectoryPath which already includes source=sec)
        // Uses year-only partitioning - CIK/filing_type filtering done via Parquet/Iceberg statistics
        String relativePartitionPath = String.format("year=%s", partitionYear);
        String outputPath =
            storageProvider.resolvePath(targetDirectoryPath, relativePartitionPath + "/" + String.format("%s_%s_earnings.parquet", cik, (accession != null && !accession.isEmpty()) ? accession : filingDate));

        storageProvider.writeAvroParquet(outputPath, earningsColumns, earningsRecords, "EarningsTranscript", "earnings_transcripts");

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
   * Write vectorized blobs with contextual enrichment to Parquet.
   * Creates individual vectors for footnotes and MD&A paragraphs with relationships.
   */
  private void writeVectorizedBlobsToParquet(Document doc, String outputPath,
      String cik, String filingType, String filingDate, String sourcePath) throws IOException {

    // Load schema from metadata
    java.util.List<org.apache.calcite.adapter.file.partition.PartitionedTableConfig.TableColumn> columns =
        AbstractSecDataDownloader.loadTableColumns("vectorized_blobs");

    List<Map<String, Object>> dataList = new ArrayList<>();

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
      Map<String, Object> data = new HashMap<>();

      data.put("vector_id", chunk.context);
      data.put("original_blob_id", chunk.originalBlobId != null ? chunk.originalBlobId : chunk.context);
      data.put("blob_type", chunk.blobType);
      data.put("filing_date", filingDate);

      // Truncate texts if they're too long for Parquet
      String originalText = chunk.metadata.containsKey("original_text") ?
          (String) chunk.metadata.get("original_text") : "";
      data.put("original_text", truncateText(originalText, 32000));
      data.put("enriched_text", truncateText(chunk.text, 32000));

      // Extract metadata
      data.put("parent_section", chunk.metadata.get("parent_section"));

      // Convert relationships to JSON string
      if (chunk.metadata.containsKey("referenced_by") || chunk.metadata.containsKey("references_footnotes")) {
        Map<String, Object> relationships = new HashMap<>();
        if (chunk.metadata.containsKey("referenced_by")) {
          relationships.put("referenced_by", chunk.metadata.get("referenced_by"));
        }
        if (chunk.metadata.containsKey("references_footnotes")) {
          relationships.put("references", chunk.metadata.get("references_footnotes"));
        }
        data.put("relationships", toJsonString(relationships));
      }

      // Financial concepts
      if (chunk.metadata.containsKey("financial_concepts")) {
        List<String> concepts = (List<String>) chunk.metadata.get("financial_concepts");
        data.put("financial_concepts", String.join(",", concepts));
      }

      // Token usage
      if (chunk.metadata.containsKey("tokens_used")) {
        data.put("tokens_used", chunk.metadata.get("tokens_used"));
      }
      if (chunk.metadata.containsKey("token_budget")) {
        data.put("token_budget", chunk.metadata.get("token_budget"));
      }

      // Add embedding vector (convert double[] to List<Float> for Avro)
      if (chunk.embedding != null && chunk.embedding.length > 0) {
        List<Float> embeddingList = new ArrayList<>();
        for (double value : chunk.embedding) {
          embeddingList.add((float) value);
        }
        data.put("embedding", embeddingList);
      } else {
        throw new IllegalStateException("Chunk missing embedding vector: " + chunk.context +
            " (type: " + chunk.blobType + ")");
      }

      dataList.add(data);
    }

    // Always write file, even if empty (zero rows) to ensure cache consistency
    storageProvider.writeAvroParquet(outputPath, columns, dataList, "VectorizedBlob", "vectorized_blobs");
    LOGGER.info("Successfully wrote " + dataList.size() + " vectorized blobs to " + outputPath);
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
    String parentPath = xbrlPath.substring(0, xbrlPath.lastIndexOf('/'));

    try {
      // Use StorageProvider for both local and cloud storage
      if (!storageProvider.exists(parentPath) || !storageProvider.isDirectory(parentPath)) {
        return null;
      }

      List<StorageProvider.FileEntry> files = storageProvider.listFiles(parentPath, false);
      List<StorageProvider.FileEntry> htmlFiles = new ArrayList<>();

      for (StorageProvider.FileEntry file : files) {
        if (file.isDirectory()) {
          continue;
        }
        String name = file.getName();
        if ((name.endsWith(".htm") || name.endsWith(".html")) &&
            !name.contains("_cal") && !name.contains("_def") && !name.contains("_lab") &&
            !name.contains("_pre") && !name.contains("_ref")) {
          htmlFiles.add(file);
        }
      }

      if (!htmlFiles.isEmpty()) {
        // Prefer files with 10-K or 10-Q in name
        for (StorageProvider.FileEntry file : htmlFiles) {
          String name = file.getName().toLowerCase();
          if (name.contains("10-k") || name.contains("10k") ||
              name.contains("10-q") || name.contains("10q")) {
            return file.getPath();
          }
        }
        return htmlFiles.get(0).getPath();  // Return first HTML file if no specific match
      }
    } catch (IOException e) {
      LOGGER.debug("Could not list files in {}: {}", parentPath, e.getMessage());
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
}
