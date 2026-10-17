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

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.RawCache;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

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
    final String base = "https://apps.irs.gov/pub/epostcard/990/xml/" + yr + "/";
    final String indexUrl = base + "index_" + yr + ".csv";
    LOGGER.info("exempt_org_990: reading index {}", indexUrl);

    List<String> batchIds = readBatchIds(indexUrl);
    LOGGER.info("exempt_org_990: {} distinct XML batch zips for year {}", batchIds.size(), yr);
    return new BatchIterator(base, batchIds, rawCache);
  }

  /** Downloads the index CSV and returns the distinct XML_BATCH_ID values (in first-seen order). */
  private static List<String> readBatchIds(String indexUrl) throws IOException {
    HttpURLConnection conn = FiscalHttp.openGet(indexUrl);
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
    try {
      String header = CsvRecordReader.readRecord(reader);
      if (header == null) {
        throw new IOException("exempt_org_990: empty index at " + indexUrl);
      }
      Map<String, Integer> idx = FiscalHttp.headerIndex(CsvRecordReader.splitFields(header, ','));
      int batchCol = FiscalHttp.required(idx, "XML_BATCH_ID", indexUrl);
      Set<String> batches = new LinkedHashSet<String>();
      String record;
      while ((record = CsvRecordReader.readRecord(reader)) != null) {
        List<String> cols = CsvRecordReader.splitFields(record, ',');
        String b = FiscalHttp.cell(cols, Integer.valueOf(batchCol));
        b = FiscalHttp.str(b);
        if (b != null) {
          batches.add(b);
        }
      }
      return new ArrayList<String>(batches);
    } finally {
      reader.close();
    }
  }

  /** Lazily walks each batch zip, yielding one row per XML return entry. */
  private static final class BatchIterator implements Iterator<Map<String, Object>> {
    /**
     * Each discovered file is its own entry, keyed by its URL. The listing they came from
     * stays uncached: freezing that would pin the table to whatever was listed on the first
     * run, and the discovery step is the part that has to stay live.
     */
    private final RawCache rawCache;
    private final String base;
    private final List<String> batchIds;
    private int batchPos = -1;
    private ZipInputStream zis;
    private Map<String, Object> nextRow;
    private boolean done;

    BatchIterator(String base, List<String> batchIds, RawCache rawCache) {
      this.base = base;
      this.batchIds = batchIds;
      this.rawCache = rawCache;
    }

    private boolean openNextZip() {
      closeZip();
      batchPos++;
      if (batchPos >= batchIds.size()) {
        return false;
      }
      String zipUrl = base + batchIds.get(batchPos) + ".zip";
      try {
        LOGGER.info("exempt_org_990: fetching batch {}", zipUrl);
        zis = new ZipInputStream(rawCache.openStream(zipUrl));
        return true;
      // fallback-guard: allow comment below documents the design ("A single missing/failed batch should not abort the whole year"); narrowly scoped to one ZIP batch, iteration continues to the next
      } catch (IOException e) {
        // A single missing/failed batch should not abort the whole year.
        LOGGER.warn("exempt_org_990: skipping batch {} ({})", zipUrl, e.getMessage());
        zis = null;
        return true;  // advance() will try the next batch
      }
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

    private void advance() {
      if (nextRow != null || done) {
        return;
      }
      try {
        while (true) {
          if (zis == null) {
            if (batchPos + 1 >= batchIds.size()) {
              done = true;
              return;
            }
            if (!openNextZip()) {
              done = true;
              return;
            }
            if (zis == null) {
              continue;  // batch open failed; try the next one
            }
          }
          ZipEntry entry = zis.getNextEntry();
          if (entry == null) {
            closeZip();
            continue;  // exhausted this zip; move on
          }
          String name = entry.getName();
          if (name == null || !name.toLowerCase().endsWith(".xml")) {
            continue;
          }
          byte[] xml = readEntry(zis);
          String objectId = objectIdFromName(name);
          Map<String, Object> row = parseReturn(xml, objectId);
          if (row != null) {
            nextRow = row;
            return;
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("exempt_org_990: streaming failed", e);
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

  private static byte[] readEntry(ZipInputStream zis) throws IOException {
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
   * persons. A return with no EIN is skipped (returns null). A malformed entry logs a
   * warning and is skipped (returns null). A part absent from the return leaves its fields
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
    // fallback-guard: allow per-record catch that skips a single unparseable XBRL return (logged at debug) without failing the whole batch; the null result is filtered out downstream the same as a missing EIN
    } catch (Exception e) {
      LOGGER.debug("exempt_org_990: skipping unparseable return {} ({})", objectId, e.getMessage());
      return null;
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
      return null;  // no filer EIN — not a usable row
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
