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
package org.apache.calcite.adapter.govdata.fiscal;

import org.apache.calcite.adapter.file.etl.CompleteBody;
import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.RawCache;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * DataProvider for {@code exempt_org_990} — IRS Form 990 e-file XML, one row per
 * filing (OBJECT_ID) for the IRS receipt year.
 *
 * <p>Reads the per-year index CSV
 * ({@code apps.irs.gov/pub/epostcard/990/xml/{YEAR}/index_{YEAR}.csv}) to
 * enumerate the distinct TEOS XML batch zips, then streams each zip and parses
 * every {@code *_public.xml} return with a StAX reader, matching header identity
 * and Part I summary financials by element LOCAL NAME (namespace- and
 * version-agnostic across 990 / 990-EZ / 990-PF). Zips are pulled one at a time
 * so the pipeline row-cap can stop early. NTEE is not in the XML — join to
 * {@code exempt_org_master} on {@code ein}.
 */
public class Irs990Provider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(Irs990Provider.class);

  private static final XMLInputFactory XML_FACTORY = newXmlFactory();

  /** candidate element local-name (lowercased) -> target output field. First hit wins per field. */
  private static final Map<String, String> ELEMENT_MAP = buildElementMap();
  /** output fields parsed as double. */
  private static final Set<String> DOUBLE_FIELDS = new java.util.HashSet<String>(
      java.util.Arrays.asList("total_revenue", "total_expenses", "total_assets",
          "salaries_comp_benefits", "fundraising_expenses_16b", "functional_expenses_total",
          "functional_expenses_program", "functional_expenses_mgmt_general",
          "functional_expenses_fundraising", "officer_comp_part_ix_line5"));
  /** Form 990 Part VII Section A person group; its amount children are summed per column. */
  private static final String PART_VII_GROUP = "Form990PartVIISectionAGrp";
  /** Part VII Section A element -> column index (D reportable comp, E related orgs, F other). */
  private static final Map<String, Integer> PART_VII_FIELDS = buildPartViiFields();
  /** lowercased "parent/local" -> output field, honoured only inside the IRS990 (Form 990) body. */
  private static final Map<String, String> PATH_MAP = buildPathMap();

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    if (year == null || year.isEmpty()) {
      LOGGER.warn("exempt_org_990: no year in dimension variables {}", variables);
      return Collections.emptyIterator();
    }
    final String yr = year.trim();
    final String base = "https://apps.irs.gov/pub/epostcard/990/xml/";
    final String indexUrl = base + yr + "/index_" + yr + ".csv";
    LOGGER.info("exempt_org_990: reading index {}", indexUrl);

    Index index = readIndex(indexUrl);
    LOGGER.info("exempt_org_990: index lists {} filings for year {} ({})", index.objectIds.size(),
        yr, index.batchIds.isEmpty() ? "no XML_BATCH_ID column"
            : index.batchIds.size() + " distinct XML_BATCH_IDs");
    ZipSet zips = discoverZips(base, yr, index, new HeadProbe());
    LOGGER.info("exempt_org_990: year {} is read from {} zips ({} layout): {}", yr,
        zips.paths.size(), zips.legacy ? "legacy" : "monthly", zips.paths);
    return rows(base, zips, index, rawCache, yr);
  }

  /** Builds the row iterator over the given zips. Package-private for tests. */
  static Iterator<Map<String, Object>> rows(String base, ZipSet zips, Index index,
      RawCache rawCache, String year) {
    return rows(base, zips, index, rawCache, year, KNOWN_ABSENT);
  }

  /** As {@link #rows(String, ZipSet, Index, RawCache, String)} with an explicit exclusion list. */
  static Iterator<Map<String, Object>> rows(String base, ZipSet zips, Index index,
      RawCache rawCache, String year, Map<String, String> knownAbsent) {
    return new BatchIterator(base, zips, index, rawCache, year, knownAbsent);
  }

  /**
   * Indexed OBJECT_IDs the IRS lists but publishes in no zip, each with the evidence. Checked
   * 2026-10-18 against every published XML zip for receipt years 2021 through 2026 (by
   * object id and by the 18-digit id) and against index_2024 and later, which do not list them.
   * An id here is excluded from the completeness check only when it is not found in a zip,
   * and each exclusion is logged and counted; any other indexed filing missing from the zips
   * still fails the load. Remove an entry once the IRS publishes the filing.
   */
  static final Map<String, String> KNOWN_ABSENT = buildKnownAbsent();

  private static Map<String, String> buildKnownAbsent() {
    Map<String, String> m = new java.util.LinkedHashMap<String, String>();
    m.put("202313409349100901", "Aurora Care Foundation Inc, Form 990-PF, receipt year 2023");
    m.put("202301929339300110", "Northwestern Memorial Hospital, Form 990-T, receipt year 2023");
    return Collections.unmodifiableMap(m);
  }

  /** Answers whether the IRS publishes a file at a URL. */
  interface Probe {
    boolean exists(String url) throws IOException;
  }

  /** HEAD request: 200 means published, the IRS answers a missing file with a 302 or 404. */
  private static final class HeadProbe implements Probe {
    @Override public boolean exists(String url) throws IOException {
      IOException last = null;
      for (int attempt = 1; attempt <= 3; attempt++) {
        HttpURLConnection conn = null;
        try {
          conn = (HttpURLConnection) new java.net.URL(url).openConnection();
          conn.setRequestMethod("HEAD");
          conn.setInstanceFollowRedirects(false);
          conn.setRequestProperty("User-Agent", FiscalHttp.USER_AGENT);
          conn.setConnectTimeout(30000);
          conn.setReadTimeout(30000);
          int code = conn.getResponseCode();
          if (code == 200) {
            return true;
          }
          if (code == 302 || code == 404) {
            return false;
          }
          last = new IOException("HTTP " + code + " probing " + url);
        } catch (IOException e) {
          last = e;
        } finally {
          if (conn != null) {
            conn.disconnect();
          }
        }
      }
      throw new IOException("exempt_org_990: cannot tell whether " + url + " exists", last);
    }
  }

  /** The zips holding a receipt year's filings: paths relative to the xml/ root, e.g. 2025/x. */
  static final class ZipSet {
    final List<String> paths = new ArrayList<String>();
    /**
     * True for the pre-2021 layout, where zips are grouped by the year an object id starts with,
     * not by receipt year: the year's zips and the previous year's both hold filings of other
     * receipt years, and CT1 repeats filings of the numbered series.
     */
    boolean legacy;
  }

  /**
   * Lists the zips the IRS publishes for a receipt year. The index does not identify them: the
   * 2019-2023 indexes have no XML_BATCH_ID column, and the 2025 index labels every filing of
   * 2025_TEOS_XML_05A although the IRS split that batch into 05A and 05B. The IRS naming
   * conventions are probed instead: {@code {Y}_TEOS_XML_{MM}{A,B,...}} (each month's suffixes
   * until the first absent one), {@code {Y}_TEOS_XML_CT1}, and the older
   * {@code download990xml_{Y}_{n}} series. A year with no monthly zip is the legacy layout (see
   * {@link ZipSet#legacy}) and also takes the previous year's CT1 and numbered series, because
   * the index of year Y lists filings whose object id starts with Y-1. Completeness is not
   * assumed from the probe: the iterator requires every indexed filing to be found in a zip, so
   * a zip this discovery missed fails the load. Every batch the index names must be among the
   * zips found.
   */
  static ZipSet discoverZips(String base, String year, Index index, Probe probe)
      throws IOException {
    ZipSet set = new ZipSet();
    for (int m = 1; m <= 12; m++) {
      for (char suffix = 'A'; suffix <= 'Z'; suffix++) {
        String path = year + "/" + year + "_TEOS_XML_" + (m < 10 ? "0" : "") + m + suffix;
        if (!probe.exists(base + path + ".zip")) {
          break;
        }
        set.paths.add(path);
      }
    }
    set.legacy = set.paths.isEmpty();
    if (set.legacy) {
      String prev = String.valueOf(Integer.parseInt(year) - 1);
      addSeries(base, prev, probe, set.paths);
    }
    addSeries(base, year, probe, set.paths);
    if (set.paths.isEmpty()) {
      throw new IOException("exempt_org_990: no zips are published under " + base + year + "/");
    }
    for (String batch : index.batchIds) {
      if (!set.paths.contains(year + "/" + batch)) {
        throw new IOException("exempt_org_990: the index names batch " + batch
            + " but the IRS publishes only " + set.paths);
      }
    }
    return set;
  }

  /** Adds a year's CT1 zip and its download990xml numbered series, whichever exist. */
  private static void addSeries(String base, String year, Probe probe, List<String> out)
      throws IOException {
    for (int n = 1; ; n++) {
      String path = year + "/download990xml_" + year + "_" + n;
      if (!probe.exists(base + path + ".zip")) {
        break;
      }
      out.add(path);
    }
    String ct1 = year + "/" + year + "_TEOS_XML_CT1";
    if (probe.exists(base + ct1 + ".zip")) {
      out.add(ct1);
    }
  }

  /** The filings an IRS index lists. */
  static final class Index {
    /** Every OBJECT_ID the index lists; each is one filing and must come out as one row. */
    final Set<String> objectIds = new HashSet<String>();
    /** Upper-cased XML_BATCH_IDs (zip file name stems); empty when the index has no such column. */
    final Set<String> batchIds = new LinkedHashSet<String>();

    void add(String objectId, String batchId, String source) throws IOException {
      if (!objectIds.add(objectId)) {
        throw new IOException("exempt_org_990: index " + source + " lists OBJECT_ID " + objectId
            + " more than once");
      }
      if (batchId != null) {
        batchIds.add(zipName(batchId));
      }
    }
  }

  /**
   * The zip file name stem for an index XML_BATCH_ID. The index lists some batches with a
   * lower-case suffix (2024_TEOS_XML_04a) while the file on the IRS server is upper-case
   * (2024_TEOS_XML_04A.zip); the lower-case URL answers 404.
   */
  static String zipName(String batchId) {
    return batchId.trim().toUpperCase(java.util.Locale.ROOT);
  }

  /** Downloads the index CSV. */
  private static Index readIndex(String indexUrl) throws IOException {
    HttpURLConnection conn = FiscalHttp.openGetWithRetry(indexUrl);
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(CompleteBody.of(conn), StandardCharsets.UTF_8));
    try {
      return parseIndex(reader, indexUrl);
    } finally {
      reader.close();
    }
  }

  /**
   * Reads an index CSV. XML_BATCH_ID exists only in the 2024+ indexes; when the column is present
   * a row without a value is an error, as is any row without an OBJECT_ID or a repeated one.
   */
  static Index parseIndex(BufferedReader reader, String source) throws IOException {
    String header = CsvRecordReader.readRecord(reader);
    if (header == null) {
      throw new IOException("exempt_org_990: empty index at " + source);
    }
    Map<String, Integer> idx = FiscalHttp.headerIndex(CsvRecordReader.splitFields(header, ','));
    Integer batchCol = FiscalHttp.col(idx, "XML_BATCH_ID");
    int objectCol = FiscalHttp.required(idx, "OBJECT_ID", source);
    Index index = new Index();
    String record;
    while ((record = CsvRecordReader.readRecord(reader)) != null) {
      List<String> cols = CsvRecordReader.splitFields(record, ',');
      String b = batchCol == null ? null : FiscalHttp.str(FiscalHttp.cell(cols, batchCol));
      String o = FiscalHttp.str(FiscalHttp.cell(cols, Integer.valueOf(objectCol)));
      if (o == null || (batchCol != null && b == null)) {
        throw new IOException("exempt_org_990: index " + source + " lists a filing without "
            + (batchCol == null ? "OBJECT_ID" : "XML_BATCH_ID/OBJECT_ID") + ": " + record);
      }
      index.add(o, b, source);
    }
    return index;
  }

  /**
   * Lazily walks each batch zip, yielding one row per XML return entry. Every listed filing must
   * come out as a row, exactly once: a zip that cannot be opened, a return that cannot be parsed,
   * a return the index does not list, a return found in two zips, or an indexed filing found in
   * no zip throws instead of being skipped. In the legacy layout ({@link ZipSet#legacy}) the
   * first two are expected and are skipped and counted; an indexed filing found in no zip still
   * throws.
   */
  private static final class BatchIterator implements Iterator<Map<String, Object>> {
    private static final int OPEN_ATTEMPTS = 3;

    private final RawCache rawCache;
    private final String base;
    private final String year;
    private final Index index;
    private final List<String> batchNames;
    private final boolean legacy;
    private final Map<String, String> knownAbsent;
    private final Set<String> written = new HashSet<String>();
    private int skippedUnindexed;
    private int skippedRepeated;
    private int batchPos = -1;
    private ZipArchiveInputStream zis;
    private int batchWritten;
    private Map<String, Object> nextRow;
    private boolean done;

    BatchIterator(String base, ZipSet zips, Index index, RawCache rawCache, String year,
        Map<String, String> knownAbsent) {
      this.base = base;
      this.knownAbsent = knownAbsent;
      this.index = index;
      this.rawCache = rawCache;
      this.year = year;
      this.batchNames = new ArrayList<String>(zips.paths);
      this.legacy = zips.legacy;
    }

    /** Opens the next batch zip, retrying a bounded number of times and then throwing. */
    private void openNextZip() throws IOException {
      batchPos++;
      batchWritten = 0;
      String zipUrl = base + batchNames.get(batchPos) + ".zip";
      IOException last = null;
      for (int attempt = 1; attempt <= OPEN_ATTEMPTS; attempt++) {
        try {
          LOGGER.info("exempt_org_990: fetching batch {} (attempt {}/{})", zipUrl, attempt,
              OPEN_ATTEMPTS);
          zis = new ZipArchiveInputStream(rawCache.openStream(zipUrl));
          return;
        } catch (IOException e) {
          last = e;
          LOGGER.warn("exempt_org_990: batch {} attempt {}/{} failed ({})", zipUrl, attempt,
              OPEN_ATTEMPTS, e.getMessage());
        }
      }
      throw new IOException("exempt_org_990: batch " + zipUrl + " could not be opened after "
          + OPEN_ATTEMPTS + " attempts; the load is incomplete", last);
    }

    private void closeZip() {
      if (zis != null) {
        try {
          zis.close();
        } catch (IOException ignore) {
          // best effort
        }
        zis = null;
      }
    }

    /** The zip is exhausted; the count check happens once every zip of the year is read. */
    private void finishBatch() {
      LOGGER.info("exempt_org_990: zip {} complete, {} filings written ({} of {} listed so far)",
          batchNames.get(batchPos), batchWritten, written.size(), index.objectIds.size());
    }

    /** Every indexed filing must have been found in exactly one zip. */
    private void finishYear() {
      int excluded = 0;
      List<String> missing = new ArrayList<String>();
      if (written.size() != index.objectIds.size()) {
        for (String id : index.objectIds) {
          if (written.contains(id)) {
            continue;
          }
          String reason = knownAbsent.get(id);
          if (reason != null) {
            excluded++;
            LOGGER.warn("exempt_org_990: year {} indexed filing {} is in no published zip"
                + " (known absent: {})", year, id, reason);
          } else if (missing.size() < 5) {
            missing.add(id);
          }
        }
      }
      if (!missing.isEmpty() || written.size() + excluded != index.objectIds.size()) {
        throw new IllegalStateException("exempt_org_990: year " + year + " wrote "
            + written.size() + " filings (" + excluded + " more known absent) but the index lists "
            + index.objectIds.size() + "; e.g. not found in any of " + batchNames + ": "
            + missing);
      }
      LOGGER.info("exempt_org_990: year {} complete, {} of {} listed filings written in {} zips"
          + " ({} known absent from every zip, skipped {} entries listed by another receipt"
          + " year's index, {} repeated entries)",
          year, written.size(), index.objectIds.size(), batchNames.size(), excluded,
          skippedUnindexed, skippedRepeated);
    }

    private void advance() {
      if (nextRow != null || done) {
        return;
      }
      try {
        while (true) {
          if (zis == null) {
            if (batchPos + 1 >= batchNames.size()) {
              done = true;
              finishYear();
              return;
            }
            openNextZip();
          }
          ZipArchiveEntry entry = (ZipArchiveEntry) zis.getNextEntry();
          if (entry == null) {
            closeZip();
            finishBatch();
            continue;  // exhausted this zip; move on
          }
          String name = entry.getName();
          if (entry.isDirectory() || name == null || !name.toLowerCase().endsWith(".xml")) {
            continue;
          }
          String objectId = objectIdFromName(name);
          if (!index.objectIds.contains(objectId)) {
            if (legacy) {
              skippedUnindexed++;  // a filing of the neighbouring receipt year's index
              continue;
            }
            throw new IllegalStateException("exempt_org_990: zip " + batchNames.get(batchPos)
                + " of year " + year + " holds return " + objectId + " that the index does not list");
          }
          if (!written.add(objectId)) {
            if (legacy) {
              skippedRepeated++;  // CT1 repeats numbered-series filings, reformatted
              continue;
            }
            throw new IllegalStateException("exempt_org_990: return " + objectId + " of year "
                + year + " is in more than one zip (again in " + batchNames.get(batchPos) + ")");
          }
          byte[] xml = readEntry(zis);
          Map<String, Object> row = parseReturn(xml, objectId);
          batchWritten++;
          nextRow = row;
          return;
        }
      } catch (IOException e) {
        closeZip();
        throw new RuntimeException("exempt_org_990: streaming failed in batch "
            + batchNames.get(batchPos), e);
      }
    }

    @Override public boolean hasNext() {
      advance();
      return nextRow != null;
    }

    @Override public Map<String, Object> next() {
      advance();
      if (nextRow == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = nextRow;
      nextRow = null;
      return row;
    }
  }

  private static byte[] readEntry(InputStream zis) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buf = new byte[16384];
    int n;
    while ((n = zis.read(buf)) > 0) {
      baos.write(buf, 0, n);
    }
    return baos.toByteArray();
  }

  private static String objectIdFromName(String name) {
    String base = name;
    int slash = base.lastIndexOf('/');
    if (slash >= 0) {
      base = base.substring(slash + 1);
    }
    int us = base.indexOf('_');
    return us > 0 ? base.substring(0, us) : base;
  }

  /**
   * Parses one return via StAX. Leaf elements are matched by local name (flat fields) or by
   * {@code parent/local} for the Part IX functional-expense columns and the Part IX line 5
   * officer total; the Part VII Section A compensation columns are summed over the listed
   * persons. A return with no EIN, or a malformed entry, throws: every listed filing must
   * become a row. A part absent from the return leaves its fields
   * null; nothing is zero-filled.
   */
  private static Map<String, Object> parseReturn(byte[] xml, String objectId) {
    Map<String, Object> out = new LinkedHashMap<String, Object>();
    double[] vii = new double[3];
    int[] viiSeen = new int[3];
    int persons = 0;
    List<String> names = new ArrayList<String>();
    List<Boolean> hasChild = new ArrayList<Boolean>();
    StringBuilder text = new StringBuilder();
    XMLStreamReader r = null;
    try {
      r = XML_FACTORY.createXMLStreamReader(new ByteArrayInputStream(xml), "UTF-8");
      while (r.hasNext()) {
        int ev = r.next();
        if (ev == XMLStreamConstants.START_ELEMENT) {
          if (!hasChild.isEmpty()) {
            hasChild.set(hasChild.size() - 1, Boolean.TRUE);
          }
          names.add(r.getLocalName());
          hasChild.add(Boolean.FALSE);
          text.setLength(0);
        } else if (ev == XMLStreamConstants.CHARACTERS || ev == XMLStreamConstants.CDATA) {
          text.append(r.getText());
        } else if (ev == XMLStreamConstants.END_ELEMENT) {
          int top = names.size() - 1;
          String local = names.get(top);
          String parent = top > 0 ? names.get(top - 1) : null;
          boolean leaf = !hasChild.get(top).booleanValue();
          if (leaf) {
            String txt = FiscalHttp.str(text.toString());
            if (txt != null) {
              if (PART_VII_GROUP.equals(parent) && PART_VII_FIELDS.containsKey(local)) {
                int k = PART_VII_FIELDS.get(local).intValue();
                Double v = FiscalHttp.toDouble(txt);
                if (v != null) {
                  vii[k] += v.doubleValue();
                  viiSeen[k]++;
                }
              } else {
                String field = null;
                if (parent != null && names.contains("IRS990")) {
                  field = PATH_MAP.get(parent.toLowerCase() + "/" + local.toLowerCase());
                }
                if (field == null) {
                  field = ELEMENT_MAP.get(local.toLowerCase());
                }
                if (field != null && !out.containsKey(field)) {
                  if (DOUBLE_FIELDS.contains(field)) {
                    out.put(field, FiscalHttp.toDouble(txt));
                  } else if ("tax_year".equals(field)) {
                    out.put(field, FiscalHttp.toInt(txt));
                  } else if ("ein".equals(field)) {
                    out.put(field, FiscalHttp.pad(txt, 9));
                  } else {
                    out.put(field, txt);
                  }
                }
              }
            }
          } else if (PART_VII_GROUP.equals(local)) {
            persons++;
          }
          names.remove(top);
          hasChild.remove(top);
          text.setLength(0);
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException("exempt_org_990: return " + objectId
          + " could not be parsed: " + e.getMessage(), e);
    } finally {
      if (r != null) {
        try {
          r.close();
        } catch (Exception ignore) {
          // best effort
        }
      }
    }
    if (out.get("ein") == null) {
      throw new IllegalStateException("exempt_org_990: return " + objectId + " carries no filer EIN");
    }
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("ein", out.get("ein"));
    row.put("org_name", out.get("org_name"));
    row.put("return_type", out.get("return_type"));
    row.put("tax_period_end", out.get("tax_period_end"));
    row.put("tax_year", out.get("tax_year"));
    row.put("total_revenue", out.get("total_revenue"));
    row.put("total_expenses", out.get("total_expenses"));
    row.put("total_assets", out.get("total_assets"));
    row.put("salaries_comp_benefits", out.get("salaries_comp_benefits"));
    row.put("fundraising_expenses_16b", out.get("fundraising_expenses_16b"));
    row.put("functional_expenses_total", out.get("functional_expenses_total"));
    row.put("functional_expenses_program", out.get("functional_expenses_program"));
    row.put("functional_expenses_mgmt_general", out.get("functional_expenses_mgmt_general"));
    row.put("functional_expenses_fundraising", out.get("functional_expenses_fundraising"));
    row.put("officer_comp_part_ix_line5", out.get("officer_comp_part_ix_line5"));
    // Part VII Section A totals are emitted only when every listed person carried the amount.
    row.put("part_vii_persons_listed", persons > 0 ? Integer.valueOf(persons) : null);
    row.put("part_vii_comp_from_org", viiTotal(vii, viiSeen, persons, 0));
    row.put("part_vii_comp_from_related_orgs", viiTotal(vii, viiSeen, persons, 1));
    row.put("part_vii_other_comp", viiTotal(vii, viiSeen, persons, 2));
    row.put("object_id", objectId);
    return row;
  }

  private static Double viiTotal(double[] sums, int[] seen, int persons, int k) {
    return persons > 0 && seen[k] == persons ? Double.valueOf(sums[k]) : null;
  }

  private static Map<String, String> buildElementMap() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("ein", "ein");
    m.put("businessnameline1txt", "org_name");
    m.put("businessnameline1", "org_name");
    m.put("returntypecd", "return_type");
    m.put("taxperiodenddt", "tax_period_end");
    m.put("taxperiodenddate", "tax_period_end");
    m.put("taxyr", "tax_year");
    m.put("taxyear", "tax_year");
    m.put("cytotalrevenueamt", "total_revenue");
    m.put("totalrevenueamt", "total_revenue");
    m.put("totalrevenuecurrentyear", "total_revenue");
    m.put("totalrevenue", "total_revenue");
    m.put("cytotalexpensesamt", "total_expenses");
    m.put("totalexpensesamt", "total_expenses");
    m.put("totalexpensescurrentyear", "total_expenses");
    m.put("totalexpenses", "total_expenses");
    m.put("totalassetseoyamt", "total_assets");
    m.put("totalassetseoy", "total_assets");
    // Form 990 Part I line 15 and line 16b
    m.put("cysalariescompempbnftpaidamt", "salaries_comp_benefits");
    m.put("cytotalfundraisingexpenseamt", "fundraising_expenses_16b");
    return Collections.unmodifiableMap(m);
  }

  private static Map<String, String> buildPathMap() {
    Map<String, String> m = new HashMap<String, String>();
    // Part IX line 25, columns A-D
    m.put("totalfunctionalexpensesgrp/totalamt", "functional_expenses_total");
    m.put("totalfunctionalexpensesgrp/programservicesamt", "functional_expenses_program");
    m.put("totalfunctionalexpensesgrp/managementandgeneralamt", "functional_expenses_mgmt_general");
    m.put("totalfunctionalexpensesgrp/fundraisingamt", "functional_expenses_fundraising");
    // Part IX line 5, column A
    m.put("compcurrentofcrdirectorsgrp/totalamt", "officer_comp_part_ix_line5");
    return Collections.unmodifiableMap(m);
  }

  private static Map<String, Integer> buildPartViiFields() {
    Map<String, Integer> m = new HashMap<String, Integer>();
    m.put("ReportableCompFromOrgAmt", Integer.valueOf(0));
    m.put("ReportableCompFromRltdOrgAmt", Integer.valueOf(1));
    m.put("OtherCompensationAmt", Integer.valueOf(2));
    return Collections.unmodifiableMap(m);
  }

  private static XMLInputFactory newXmlFactory() {
    XMLInputFactory f = XMLInputFactory.newInstance();
    // Harden against XXE and coalesce text so getElementText sees full values.
    f.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
    f.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);
    f.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);
    return f;
  }
}
