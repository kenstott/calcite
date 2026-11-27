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
package org.apache.calcite.adapter.govdata.patents;

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Rows of patent_grants_weekly from one USPTO weekly bibliographic file (ODP product PTBLXML,
 * "Patent Grant Bibliographic (Front Page) Text Data - XML"), published each Tuesday for that
 * day's issue. One row per patent document in the file, all patent types.
 *
 * <p>The file is a concatenation of XML documents, each opening with its own XML declaration, so
 * it is split on the declaration and each document parsed on its own. A correction is published
 * as a whole replacement file with a {@code _rN} suffix; {@link PatentWeeklyIssueResolver}
 * selects the highest revision of each issue date.
 */
public class PatentGrantsWeeklyTransformer extends AbstractPatentsTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PatentGrantsWeeklyTransformer.class);

  private static final String PRODUCT = "PTBLXML";
  private static final String ODP_PTBLXML_BASE =
      "https://api.uspto.gov/api/v1/datasets/products/files/PTBLXML/";

  /** Weekly XML file names; the product also lists DTDs and annual non-issue archives. */
  private static final Pattern ISSUE_FILE =
      Pattern.compile("ipgb(\\d{4})(\\d{2})(\\d{2})_wk\\d{2}(?:_r(\\d+))?\\.zip");

  /** USPTO pads design, plant and reissue numbers with zeros; PatentsView patent_id does not. */
  private static final Pattern DOC_NUMBER = Pattern.compile("([A-Z]*)0*([1-9]\\d*)");

  private static final String BIB = "us-patent-grant/us-bibliographic-data-grant";

  @Override
  public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    final String year = requiredDimension(context, "year");
    final String issueDate = requiredDimension(context, "issue_date");
    final String issueFile = requiredDimension(context, "issue_file");

    String zipName = issueFile + ".zip";
    String zipPath = downloadAndCacheZip(ODP_PTBLXML_BASE + year + "/" + zipName,
        storageProvider().resolvePath(cacheFile("ptblxml"), zipName));

    InputStream raw = storageProvider().openInputStream(zipPath);
    ZipInputStream zip = new ZipInputStream(raw);
    ZipEntry entry = zip.getNextEntry();
    while (entry != null && !entry.getName().toLowerCase(java.util.Locale.ROOT).endsWith(".xml")) {
      entry = zip.getNextEntry();
    }
    if (entry == null) {
      zip.close();
      throw new IOException("No XML entry in " + zipName);
    }
    return rows(new BufferedReader(new InputStreamReader(zip, StandardCharsets.UTF_8)),
        issueDate, issueFile);
  }

  private static String requiredDimension(RequestContext context, String name) {
    String value = context.getDimensionValues().get(name);
    if (value == null || value.isEmpty()) {
      throw new IllegalStateException("PatentGrantsWeekly: " + name + " dimension not set");
    }
    return value;
  }

  /** The issue files of one year: issue date (YYYY-MM-DD) to the file name stem to load. */
  SortedMap<String, String> issuesForYear(String year) {
    JsonNode catalog = productMetadata(PRODUCT);
    if (catalog == null) {
      throw new IllegalStateException("PatentGrantsWeekly: USPTO " + PRODUCT
          + " product catalog unavailable, cannot list the issue files of " + year);
    }
    return latestRevisions(catalog, year);
  }

  /** Picks, for each issue date of {@code year} in the catalog, the highest file revision. */
  static SortedMap<String, String> latestRevisions(JsonNode catalog, String year) {
    SortedMap<String, String> files = new TreeMap<String, String>();
    Map<String, Integer> revisions = new HashMap<String, Integer>();
    for (JsonNode name : catalog.findValues("fileName")) {
      Matcher m = ISSUE_FILE.matcher(name.asText());
      if (!m.matches() || !m.group(1).equals(year)) {
        continue;
      }
      String date = m.group(1) + "-" + m.group(2) + "-" + m.group(3);
      int revision = m.group(4) == null ? 0 : Integer.parseInt(m.group(4));
      Integer seen = revisions.get(date);
      if (seen == null || revision > seen) {
        revisions.put(date, revision);
        files.put(date, name.asText().substring(0, name.asText().length() - ".zip".length()));
      }
    }
    return files;
  }

  /** Streams one row per document of a concatenated bibliographic XML file. */
  static Iterator<Map<String, Object>> rows(final BufferedReader reader, final String issueDate,
      final String issueFile) {
    final XMLInputFactory factory = XMLInputFactory.newInstance();
    // Documents name an external DTD that is not part of the download.
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);

    return new Iterator<Map<String, Object>>() {
      private final StringBuilder document = new StringBuilder();
      private int count;
      private boolean exhausted;
      private Map<String, Object> pending;
      { advance(); }

      private void advance() {
        pending = null;
        if (exhausted) {
          return;
        }
        try {
          String line;
          while ((line = reader.readLine()) != null) {
            if (line.startsWith("<?xml ") && document.length() > 0) {
              emit();
              document.append(line).append('\n');
              return;
            }
            document.append(line).append('\n');
          }
          exhausted = true;
          reader.close();
          if (document.length() > 0) {
            emit();
          }
          if (count == 0) {
            throw new IOException("No patent documents in " + issueFile);
          }
          LOGGER.info("PatentGrantsWeekly: {} patents issued {} ({})",
              count, issueDate, issueFile);
        } catch (IOException e) {
          throw new UncheckedIOException("PatentGrantsWeekly read failed for " + issueFile, e);
        }
      }

      private void emit() throws IOException {
        try {
          pending = parseDocument(factory, document.toString());
        } catch (XMLStreamException e) {
          throw new IOException("Unparseable document " + (count + 1) + " in " + issueFile, e);
        }
        document.setLength(0);
        if (!issueDate.equals(pending.get("patent_date"))) {
          throw new IOException("Patent " + pending.get("patent_id") + " in " + issueFile
              + " is dated " + pending.get("patent_date") + ", expected " + issueDate);
        }
        pending.put("source_file", issueFile + ".zip");
        count++;
      }

      @Override public boolean hasNext() {
        return pending != null;
      }

      @Override public Map<String, Object> next() {
        if (pending == null) {
          throw new NoSuchElementException();
        }
        Map<String, Object> row = pending;
        advance();
        return row;
      }
    };
  }

  /** Reads the front-page fields of one grant document. */
  private static Map<String, Object> parseDocument(XMLInputFactory factory, String xml)
      throws XMLStreamException, IOException {
    Map<String, String> fields = new HashMap<String, String>();
    XMLStreamReader in = factory.createXMLStreamReader(new StringReader(xml));
    StringBuilder path = new StringBuilder();
    StringBuilder text = new StringBuilder();
    while (in.hasNext()) {
      int event = in.next();
      if (event == XMLStreamConstants.START_ELEMENT) {
        if (path.length() > 0) {
          path.append('/');
        }
        path.append(in.getLocalName());
        // The title may carry inline markup (<i>, <sub>); keep its text across those children.
        if (path.indexOf(BIB + "/invention-title/") != 0) {
          text.setLength(0);
        }
        if (path.toString().equals(BIB + "/application-reference")) {
          fields.put("appl_type", in.getAttributeValue(null, "appl-type"));
        }
      } else if (event == XMLStreamConstants.CHARACTERS
          || event == XMLStreamConstants.CDATA || event == XMLStreamConstants.SPACE) {
        text.append(in.getText());
      } else if (event == XMLStreamConstants.END_ELEMENT) {
        String current = path.toString();
        if (current.equals(BIB)) {
          break;
        }
        if (current.startsWith(BIB + "/")) {
          String key = current.substring(BIB.length() + 1);
          if (!key.startsWith("invention-title/")) {
            fields.put(key, text.toString().trim());
          }
        }
        path.setLength(Math.max(0, path.lastIndexOf("/")));
      }
    }
    in.close();

    String docNumber = fields.get("publication-reference/document-id/doc-number");
    String grantDate = isoDate(fields.get("publication-reference/document-id/date"));
    if (docNumber == null || grantDate == null) {
      throw new IOException("Grant document without a publication number and date: "
          + xml.substring(0, Math.min(400, xml.length())));
    }
    Matcher number = DOC_NUMBER.matcher(docNumber);
    if (!number.matches()) {
      throw new IOException("Unrecognized patent number: " + docNumber);
    }
    String claims = fields.get("number-of-claims");

    Map<String, Object> row = new HashMap<String, Object>();
    row.put("patent_id", number.group(1) + number.group(2));
    row.put("grant_year", Integer.valueOf(grantDate.substring(0, 4)));
    row.put("patent_date", grantDate);
    row.put("patent_type", fields.get("appl_type"));
    row.put("patent_title", fields.get("invention-title"));
    row.put("num_claims", claims == null ? null : Integer.valueOf(claims));
    row.put("wipo_kind", fields.get("publication-reference/document-id/kind"));
    row.put("application_number",
        fields.get("application-reference/document-id/doc-number"));
    row.put("application_date",
        isoDate(fields.get("application-reference/document-id/date")));
    return row;
  }

  /** YYYYMMDD to YYYY-MM-DD; null when the element is absent. */
  private static String isoDate(String compact) throws IOException {
    if (compact == null) {
      return null;
    }
    if (!compact.matches("\\d{8}")) {
      throw new IOException("Unrecognized date: " + compact);
    }
    return compact.substring(0, 4) + "-" + compact.substring(4, 6) + "-" + compact.substring(6);
  }
}
