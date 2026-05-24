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
package org.apache.calcite.adapter.govdata.fedregister;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

/**
 * DataProvider for Federal Register documents sourced from the govinfo.gov bulk XML repository.
 *
 * <p>Downloads monthly ZIP archives from:
 * {@code https://www.govinfo.gov/bulkdata/FR/{year}/{month:02d}/FR-{year}-{month:02d}.zip}
 *
 * <p>Each ZIP contains one XML file per publication day (e.g. FR-2025-01-02.xml).
 * Each daily XML contains RULE, PRORULE, NOTICE, and PRESDOC elements for that day.
 * All four document types are extracted from a single monthly batch.
 *
 * <p>Fields present in bulk XML: document_number, title, doc_type, publication_date,
 * effective_on, action, agency_names, cfr_references, rin, docket_ids, signing_date.
 */
public class FedRegisterBulkXmlDataProvider implements DataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FedRegisterBulkXmlDataProvider.class);

  @SuppressWarnings("InlineFormatString")
  private static final String GOVINFO_URL_TEMPLATE =
      "https://www.govinfo.gov/bulkdata/FR/%d/%02d/FR-%d-%02d.zip";

  private static final Pattern DOC_NUMBER_PATTERN = Pattern.compile("\\d{4}-\\d+");
  private static final Pattern FILENAME_DATE_PATTERN =
      Pattern.compile("FR-(\\d{4}-\\d{2}-\\d{2})\\.xml$");

  // [containerTag, docTag, docType]
  private static final String[][] CONTAINER_TYPES = {
      {"RULES",    "RULE",    "RULE"},
      {"PRORULES", "PRORULE", "PRORULE"},
      {"NOTICES",  "NOTICE",  "NOTICE"},
      {"PRESDOCS", "PRESDOCU", "PRESDOC"}
  };

  @Override
  public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {

    String yearStr  = variables.get("year");
    String monthStr = variables.get("month");

    if (yearStr == null || monthStr == null) {
      LOGGER.warn("fr_documents: missing year or month dimension — skipping batch");
      return Collections.emptyIterator();
    }

    int year;
    int month;
    try {
      year  = Integer.parseInt(yearStr);
      month = Integer.parseInt(monthStr);
    } catch (NumberFormatException e) {
      LOGGER.warn("fr_documents: invalid year/month: year={}, month={}", yearStr, monthStr);
      return Collections.emptyIterator();
    }

    String url = String.format(Locale.US, GOVINFO_URL_TEMPLATE, year, month, year, month);
    LOGGER.info("Downloading FR bulk XML: {}", url);

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    downloadAndParse(url, rows);

    LOGGER.info("fr_documents: extracted {} rows for {}-{}", rows.size(), year,
        String.format("%02d", month));
    return rows.iterator();
  }

  private void downloadAndParse(String urlString, List<Map<String, Object>> rows)
      throws IOException {

    URL url = URI.create(urlString).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty("User-Agent", "Apache-Calcite-GovData-Adapter");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setInstanceFollowRedirects(true);

    int status = conn.getResponseCode();
    if (status == 404) {
      // Month not yet published (future or pre-Federal Register era)
      LOGGER.info("FR bulk ZIP not yet available (404): {}", urlString);
      return;
    }
    if (status != 200) {
      throw new IOException("HTTP " + status + " fetching " + urlString);
    }

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    try {
      // Disable external entity resolution (XXE protection)
      factory.setFeature(
          "http://xml.org/sax/features/external-general-entities", false);
      factory.setFeature(
          "http://xml.org/sax/features/external-parameter-entities", false);
      factory.setFeature(
          "http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
      factory.setXIncludeAware(false);
      factory.setExpandEntityReferences(false);
    } catch (ParserConfigurationException e) {
      throw new IOException("XML parser configuration error", e);
    }

    try (InputStream httpIn = new BufferedInputStream(conn.getInputStream());
         ZipInputStream zis = new ZipInputStream(httpIn)) {

      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName();
        if (!FILENAME_DATE_PATTERN.matcher(name).find()) {
          zis.closeEntry();
          continue;
        }

        String pubDate = extractDateFromFilename(name);
        try {
          DocumentBuilder builder = factory.newDocumentBuilder();
          Document xmlDoc = builder.parse(new NonClosingInputStream(zis));
          parseXmlDocument(xmlDoc.getDocumentElement(), pubDate, rows);
        } catch (Exception e) {
          LOGGER.warn("fr_documents: failed to parse {}: {}", name, e.getMessage());
        }

        zis.closeEntry();
      }
    }
  }

  private void parseXmlDocument(Element root, String pubDate,
      List<Map<String, Object>> rows) {

    for (String[] containerInfo : CONTAINER_TYPES) {
      String containerTag = containerInfo[0];
      String docTag       = containerInfo[1];
      String docType      = containerInfo[2];

      NodeList containers = root.getElementsByTagName(containerTag);
      for (int ci = 0; ci < containers.getLength(); ci++) {
        Element container = (Element) containers.item(ci);
        NodeList docs = container.getElementsByTagName(docTag);
        for (int di = 0; di < docs.getLength(); di++) {
          Element doc = (Element) docs.item(di);
          // Only direct children of the container (not nested)
          if (!doc.getParentNode().isSameNode(container)) {
            continue;
          }
          Map<String, Object> row = extractDocument(doc, docType, pubDate);
          if (row.get("document_number") != null) {
            rows.add(row);
          }
        }
      }
    }
  }

  private Map<String, Object> extractDocument(Element doc, String docType, String pubDate) {
    Map<String, Object> row = new HashMap<String, Object>();

    // document_number: regex on FRDOC text (may be deeply nested for PRESDOC)
    String frdocText = findFirstTextDeep(doc, "FRDOC");
    row.put("document_number", extractDocNumber(frdocText));

    row.put("doc_type", docType);
    row.put("publication_date", pubDate);

    if ("PRESDOC".equals(docType)) {
      extractPresDocFields(doc, row);
    } else {
      extractPreamblFields(doc, row);
    }

    return row;
  }

  private void extractPreamblFields(Element doc, Map<String, Object> row) {
    Element preamb = firstChildElement(doc, "PREAMB");

    if (preamb == null) {
      row.put("title",         null);
      row.put("effective_on",  null);
      row.put("action",        null);
      row.put("agency_names",  null);
      row.put("cfr_references", null);
      row.put("rin",           null);
      row.put("docket_ids",    null);

      row.put("signing_date",  null);
      return;
    }

    row.put("title", cleanText(firstChildText(preamb, "SUBJECT")));

    // effective_on: text inside EFFDATE (may be wrapped in <P>)
    row.put("effective_on",
        extractDateText(deepText(firstChildElement(preamb, "EFFDATE"))));

    // action: text inside ACT (may be wrapped in <P>)
    row.put("action", cleanText(deepText(firstChildElement(preamb, "ACT"))));

    // agency_names: all <AGENCY> direct children of PREAMB, title-cased
    List<String> agencyNames = new ArrayList<String>();
    NodeList agencyNodes = preamb.getChildNodes();
    for (int i = 0; i < agencyNodes.getLength(); i++) {
      if (!(agencyNodes.item(i) instanceof Element)) {
        continue;
      }
      Element el = (Element) agencyNodes.item(i);
      if ("AGENCY".equals(el.getTagName())) {
        String name = el.getTextContent();
        if (name != null && !name.trim().isEmpty()) {
          agencyNames.add(toTitleCase(name.trim()));
        }
      }
    }
    row.put("agency_names", agencyNames.isEmpty() ? null : join(agencyNames, ", "));

    // cfr_references: all <CFR> direct children of PREAMB as JSON array
    List<String> cfrRefs = new ArrayList<String>();
    for (int i = 0; i < agencyNodes.getLength(); i++) {
      if (!(agencyNodes.item(i) instanceof Element)) {
        continue;
      }
      Element el = (Element) agencyNodes.item(i);
      if ("CFR".equals(el.getTagName())) {
        String cfr = el.getTextContent();
        if (cfr != null && !cfr.trim().isEmpty()) {
          cfrRefs.add(cfr.trim());
        }
      }
    }
    row.put("cfr_references", cfrRefs.isEmpty() ? null : toJsonArray(cfrRefs));

    // rin: strip "RIN " prefix
    String rin = firstChildText(preamb, "RIN");
    if (rin != null) {
      rin = rin.replaceFirst("(?i)^RIN\\s+", "").trim();
      row.put("rin", rin.isEmpty() ? null : rin);
    } else {
      row.put("rin", null);
    }

    // docket_ids: DEPDOC as single-element JSON array
    String depdoc = firstChildText(preamb, "DEPDOC");
    row.put("docket_ids", (depdoc != null && !depdoc.trim().isEmpty())
        ? toJsonArray(Collections.singletonList(depdoc.trim())) : null);

    row.put("president",    null);
    row.put("signing_date", null);
  }

  private void extractPresDocFields(Element doc, Map<String, Object> row) {
    // PRESDOC nests: PRESDOCU > EXECORD|PROCL|MEMO|...
    // Title: first HD element (the document title, not the type header)
    String title = findFirstTextDeep(doc, "HD");
    row.put("title", cleanText(title));

    // signing_date: DATE element deep inside (e.g. "December 30, 2024.")
    String dateText = findFirstTextDeep(doc, "DATE");
    row.put("signing_date", parseLongDate(dateText));

    row.put("president",     null);
    row.put("effective_on",  null);
    row.put("action",        null);
    row.put("agency_names",  null);
    row.put("cfr_references", null);
    row.put("rin",           null);
    row.put("docket_ids",    null);
  }

  private String extractDocNumber(String frdocText) {
    if (frdocText == null) {
      return null;
    }
    Matcher m = DOC_NUMBER_PATTERN.matcher(frdocText);
    return m.find() ? m.group() : null;
  }

  private String extractDateText(String text) {
    if (text == null) {
      return null;
    }
    // Prefer ISO format if present
    Matcher iso = Pattern.compile("\\d{4}-\\d{2}-\\d{2}").matcher(text);
    if (iso.find()) {
      return iso.group();
    }
    return parseLongDate(text);
  }

  private String parseLongDate(String text) {
    if (text == null) {
      return null;
    }
    // Strip trailing punctuation and normalize whitespace
    String cleaned = text.trim().replaceAll("[.,;]$", "").replaceAll("\\s+", " ").trim();
    String[] patterns = {"MMMM d yyyy", "MMMM dd yyyy", "MMM d yyyy", "MMM dd yyyy"};
    for (String pattern : patterns) {
      try {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern, Locale.US);
        sdf.setLenient(false);
        Date d = sdf.parse(cleaned);
        return new SimpleDateFormat("yyyy-MM-dd", Locale.US).format(d);
      } catch (ParseException e) {
        // try next
      }
    }
    return null;
  }

  private String extractDateFromFilename(String filename) {
    Matcher m = FILENAME_DATE_PATTERN.matcher(filename);
    return m.find() ? m.group(1) : null;
  }

  private String findFirstTextDeep(Element root, String tagName) {
    NodeList nodes = root.getElementsByTagName(tagName);
    if (nodes.getLength() > 0) {
      String text = nodes.item(0).getTextContent();
      if (text != null && !text.trim().isEmpty()) {
        return text.trim();
      }
    }
    return null;
  }

  private Element firstChildElement(Element parent, String tagName) {
    if (parent == null) {
      return null;
    }
    NodeList children = parent.getChildNodes();
    for (int i = 0; i < children.getLength(); i++) {
      if (children.item(i) instanceof Element) {
        Element el = (Element) children.item(i);
        if (tagName.equals(el.getTagName())) {
          return el;
        }
      }
    }
    return null;
  }

  private String firstChildText(Element parent, String tagName) {
    Element child = firstChildElement(parent, tagName);
    if (child == null) {
      return null;
    }
    String text = child.getTextContent();
    return (text != null && !text.trim().isEmpty()) ? text.trim() : null;
  }

  private String deepText(Element el) {
    if (el == null) {
      return null;
    }
    String text = el.getTextContent();
    return (text != null && !text.trim().isEmpty()) ? text.trim() : null;
  }

  private String cleanText(String text) {
    if (text == null) {
      return null;
    }
    String cleaned = text.replaceAll("\\s+", " ").trim();
    return cleaned.isEmpty() ? null : cleaned;
  }

  private String toTitleCase(String input) {
    if (input == null || input.isEmpty()) {
      return input;
    }
    StringBuilder sb = new StringBuilder(input.length());
    boolean nextUpper = true;
    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);
      if (Character.isWhitespace(c)) {
        sb.append(c);
        nextUpper = true;
      } else if (nextUpper) {
        sb.append(Character.toUpperCase(c));
        nextUpper = false;
      } else {
        sb.append(Character.toLowerCase(c));
      }
    }
    return sb.toString();
  }

  private String toJsonArray(List<String> items) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < items.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("\"").append(items.get(i).replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
    }
    sb.append("]");
    return sb.toString();
  }

  private String join(List<String> items, String delimiter) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < items.size(); i++) {
      if (i > 0) {
        sb.append(delimiter);
      }
      sb.append(items.get(i));
    }
    return sb.toString();
  }

  /**
   * Wraps an InputStream to prevent DocumentBuilder.parse() from closing the
   * underlying ZipInputStream when it finishes parsing a single entry.
   */
  private static final class NonClosingInputStream extends InputStream {
    private final InputStream delegate;

    NonClosingInputStream(InputStream delegate) {
      this.delegate = delegate;
    }

    @Override public int read() throws IOException {
      return delegate.read();
    }

    @Override public int read(byte[] b, int off, int len) throws IOException {
      return delegate.read(b, off, len);
    }

    @Override public void close() {
      // intentionally do not close — caller owns the ZipInputStream lifecycle
    }
  }
}
