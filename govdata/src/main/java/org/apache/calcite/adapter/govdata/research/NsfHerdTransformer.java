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
package org.apache.calcite.adapter.govdata.research;

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Streaming transformer for {@code research.nsf_herd_by_institution} — the NSF NCSES
 * Higher Education R&amp;D (HERD) survey institution-level microdata.
 *
 * <p>There is no NCSES JSON API; each survey year is published as a per-year ZIP of
 * institution microdata at {@code .../higher_education_r_and_d_{year}.zip}, containing
 * {@code herd{year}.csv}. The CSV is a tall EAV file (~264k rows/year for 2024): each row
 * is one cell of one questionnaire item, self-described by human-readable {@code question},
 * {@code row}, and {@code column} text fields (verified against herd2024.csv on 2026-07-16).
 * This class implements {@link StreamingResponseTransformer}: {@code HttpSource} calls
 * {@link #fetchAndTransform} directly, which opens the ZIP, positions on the CSV entry, and
 * streams one CSV record at a time via {@link CsvRecordReader}, keeping heap O(1) per row.
 *
 * <p>Only the two R&amp;D-expenditure question families are projected into the clean tall
 * table (all other questionnaire items are ignored). NCSES restructured the HERD
 * questionnaire starting with the FY2010 survey; the 2010+ and pre-2010 (FY2008/FY2009,
 * the tail of the predecessor Academic R&amp;D Expenditures Survey era) vintages use
 * different question text and a different column set, but describe the same underlying
 * facts, so both are mapped into the same output shape:
 * <ul>
 *   <li><b>2010+</b> — {@code "Federal expenditures by field and agency"}: {@code row}=S&amp;E
 *       field, {@code column}=federal agency (including a {@code "Total"} agency column)
 *       ⇒ {@code funding_source="Federal"}, {@code federal_agency}=column. {@code
 *       "Nonfederal expenditures by field and source"}: {@code row}=S&amp;E field, {@code
 *       column}=nonfederal source ⇒ {@code funding_source}=column, {@code federal_agency}=null.
 *       This question's own {@code column="Total"} is the sum of the five nonfederal sources
 *       only (it does not include Federal); relabeled to {@code funding_source="Total
 *       nonfederal"} so it can't be mistaken for a grand total — the true grand total is that
 *       value plus the row where {@code funding_source='Federal' AND federal_agency='Total'}.</li>
 *   <li><b>Pre-2010 (herd2008.csv/herd2009.csv)</b> — detected by the header carrying {@code
 *       fice} instead of {@code inst_id} (see {@link #isPreRestructure}). Three separate
 *       questions stand in for the two 2010+ families:
 *       <ul>
 *         <li>{@code "Federal expenditures by S&amp;E field and agency"} maps like 2010+'s
 *             federal-by-agency question, but this vintage has no {@code "Total"} agency
 *             column — the agencies do not necessarily sum to the field's true federal
 *             total (confirmed against Johns Hopkins FY2008: the 7 named agencies sum to
 *             $636.3M, versus the $1,454.4M true federal total below — the survey's federal
 *             total legitimately includes federal money not attributed to one of the 7
 *             listed agencies, the same reason 2010+ carries an explicit Total column
 *             instead of relying on a sum).</li>
 *         <li>{@code "Expenditures by S&amp;E field"} carries {@code column="Federal"} and
 *             {@code column="Total"} (federal+nonfederal) per field. {@code column="Federal"}
 *             supplies the missing {@code federal_agency="Total"} row. {@code column="Total"}
 *             minus the field's {@code Federal} value (buffered per field within one
 *             institution's contiguous block — Federal always precedes Total for the same
 *             field in source order) yields {@code funding_source="Total nonfederal"} for
 *             that field, mirroring the 2010+ nonfederal question's own Total column.</li>
 *         <li>{@code "Source"} breaks the institution-wide total (not by field — this
 *             vintage never crosses nonfederal source with S&amp;E field) down by nonfederal
 *             source; its {@code row} values are mapped to 2010+'s source labels ({@code
 *             "Industry"→"Business"}, {@code "Institution funds, total"→"Institution funds"})
 *             and emitted at {@code rd_field="All"} only. Its own {@code Federal} and {@code
 *             Total} rows are skipped (redundant with, and validated against, the
 *             {@code "Expenditures by S&amp;E field"} derivation above — for Johns Hopkins
 *             FY2008 the four nonfederal sources here sum to exactly the same $226.5M as
 *             {@code Total(1,680,927) - Federal(1,454,426)}).</li>
 *       </ul>
 *       Field labels and the institution identifier are carried through as this vintage
 *       publishes them (e.g. {@code fice} is a different id space than 2010+'s {@code
 *       inst_id}), not harmonized to the 2010+ taxonomy — the same as-published convention
 *       {@code nsf_rd_by_field} documents for its own pre/post-2021 field-taxonomy revision.
 *       {@code ncses_inst_id} and {@code ipeds_unitid} are not published for this vintage and
 *       are emitted null (legitimate absence, not a fabricated value).</li>
 * </ul>
 * {@code rd_field} is itself a two-level rollup ("All" ⊃ a broad field like "Life sciences,
 * all" ⊃ a detailed field like "Life sciences, health sciences") — summing
 * {@code rd_expenditure_usd_thousand} across rows for one institution/funding_source without
 * filtering to a single {@code rd_field} level multiplies the total.
 * Dollar values are in <b>thousands</b> (HERD PUF convention). {@code state_fips} is derived
 * from the USPS state code ({@code inst_state_code} 2010+, {@code inst_state} pre-2010); an
 * unmapped non-blank postal code fails loudly (project rule #6 — never default to a
 * placeholder FIPS). {@code ipeds_unitid} is passed through as published (null when NCSES
 * itself did not match the institution, or on the pre-2010 vintage that never published it) —
 * a blank id is a legitimate null, never a fabricated one. {@code county_fips} and {@code
 * control} are not present in this microdata and are emitted null.
 */
public class NsfHerdTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NsfHerdTransformer.class);

  private static final int CONNECT_TIMEOUT_MS = 60_000;
  private static final int READ_TIMEOUT_MS = 900_000;

  private static final String Q_FEDERAL = "Federal expenditures by field and agency";
  private static final String Q_NONFEDERAL = "Nonfederal expenditures by field and source";

  // Pre-2010 (herd2008.csv/herd2009.csv) vintage: NCSES's predecessor questionnaire, restructured
  // for the FY2010 survey. See the class javadoc for the full mapping and its live-data validation.
  private static final String Q_FEDERAL_OLD = "Federal expenditures by S&E field and agency";
  private static final String Q_EXPEND_BY_FIELD_OLD = "Expenditures by S&E field";
  private static final String Q_SOURCE_OLD = "Source";

  /** Pre-2010 "Source" row labels -> 2010+ nonfederal funding_source labels. */
  private static final Map<String, String> OLD_SOURCE_LABELS;

  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("Industry", "Business");
    m.put("Institution funds, total", "Institution funds");
    m.put("State and local government", "State and local government");
    m.put("All other sources", "All other sources");
    OLD_SOURCE_LABELS = Collections.unmodifiableMap(m);
  }

  /** USPS 2-letter code -> 2-digit state/territory FIPS (50 states + DC + territories). */
  private static final Map<String, String> STATE_FIPS;

  static {
    Map<String, String> m = new HashMap<String, String>();
    m.put("AL", "01"); m.put("AK", "02"); m.put("AZ", "04"); m.put("AR", "05");
    m.put("CA", "06"); m.put("CO", "08"); m.put("CT", "09"); m.put("DE", "10");
    m.put("DC", "11"); m.put("FL", "12"); m.put("GA", "13"); m.put("HI", "15");
    m.put("ID", "16"); m.put("IL", "17"); m.put("IN", "18"); m.put("IA", "19");
    m.put("KS", "20"); m.put("KY", "21"); m.put("LA", "22"); m.put("ME", "23");
    m.put("MD", "24"); m.put("MA", "25"); m.put("MI", "26"); m.put("MN", "27");
    m.put("MS", "28"); m.put("MO", "29"); m.put("MT", "30"); m.put("NE", "31");
    m.put("NV", "32"); m.put("NH", "33"); m.put("NJ", "34"); m.put("NM", "35");
    m.put("NY", "36"); m.put("NC", "37"); m.put("ND", "38"); m.put("OH", "39");
    m.put("OK", "40"); m.put("OR", "41"); m.put("PA", "42"); m.put("RI", "44");
    m.put("SC", "45"); m.put("SD", "46"); m.put("TN", "47"); m.put("TX", "48");
    m.put("UT", "49"); m.put("VT", "50"); m.put("VA", "51"); m.put("WA", "53");
    m.put("WV", "54"); m.put("WI", "55"); m.put("WY", "56"); m.put("PR", "72");
    m.put("VI", "78"); m.put("GU", "66"); m.put("AS", "60"); m.put("MP", "69");
    STATE_FIPS = Collections.unmodifiableMap(m);
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String url = context.getUrl();
    if (url == null || url.isEmpty()) {
      throw new IllegalStateException("NsfHerdTransformer: no URL in context");
    }
    LOGGER.info("herd: streaming {}", url);

    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
    conn.setReadTimeout(READ_TIMEOUT_MS);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status == HttpURLConnection.HTTP_NOT_FOUND) {
      // No microdata ZIP for this year yet — honest absence, not a fabricated row.
      conn.disconnect();
      LOGGER.warn("herd: HTTP 404 (no microdata) for {}", url);
      return Collections.<Map<String, Object>>emptyList().iterator();
    }
    if (status != HttpURLConnection.HTTP_OK) {
      conn.disconnect();
      throw new IOException("HTTP " + status + " from " + url);
    }

    InputStream raw = conn.getInputStream();
    ZipInputStream zis = new ZipInputStream(raw);
    ZipEntry entry = zis.getNextEntry();
    while (entry != null && !entry.getName().toLowerCase().endsWith(".csv")) {
      entry = zis.getNextEntry();
    }
    if (entry == null) {
      zis.close();
      conn.disconnect();
      throw new IOException("herd: no .csv entry in ZIP " + url);
    }

    BufferedReader reader = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8));
    String header = CsvRecordReader.readRecord(reader);
    if (header == null) {
      reader.close();
      conn.disconnect();
      throw new IOException("herd: empty CSV in " + url);
    }
    Map<String, Integer> idx = headerIndex(CsvRecordReader.splitFields(header, ','));
    return new HerdRowIterator(reader, conn, idx, url);
  }

  private static Map<String, Integer> headerIndex(List<String> header) {
    Map<String, Integer> idx = new HashMap<String, Integer>();
    for (int i = 0; i < header.size(); i++) {
      idx.put(header.get(i).trim(), i);
    }
    return idx;
  }

  /** True for the pre-2010 (herd2008.csv/herd2009.csv) header shape: {@code fice} instead of
   * {@code inst_id}, {@code inst_state} instead of {@code inst_state_code}, no {@code
   * ncses_inst_id}/{@code ipeds_unitid}. See the class javadoc for the full mapping. */
  private static boolean isPreRestructure(Map<String, Integer> idx) {
    return idx.containsKey("fice") && !idx.containsKey("inst_id");
  }

  /** Lazy row iterator over the ZIP'd CSV; closes the stream on exhaustion. */
  private static final class HerdRowIterator implements Iterator<Map<String, Object>> {
    private final BufferedReader reader;
    private final HttpURLConnection conn;
    private final Map<String, Integer> idx;
    private final String url;
    private final boolean preRestructure;
    // Pre-2010 only: field -> pending federal total from "Expenditures by S&E field",
    // buffered until that field's paired Total row arrives (Federal always precedes Total for
    // the same field within one institution's contiguous block in source order). Cleared on
    // institution change; bounded by one institution's field count (~40).
    private final Map<String, Double> pendingFieldFederal;
    private String currentInstKey;
    private Map<String, Object> mapped;
    private boolean done;

    HerdRowIterator(BufferedReader reader, HttpURLConnection conn, Map<String, Integer> idx,
        String url) {
      this.reader = reader;
      this.conn = conn;
      this.idx = idx;
      this.url = url;
      this.preRestructure = isPreRestructure(idx);
      this.pendingFieldFederal = preRestructure ? new HashMap<String, Double>() : null;
      advance();
    }

    private void advance() {
      try {
        String record = CsvRecordReader.readRecord(reader);
        while (record != null) {
          List<String> cols = CsvRecordReader.splitFields(record, ',');
          Map<String, Object> row = mapRow(cols);
          if (row != null) {
            mapped = row;
            return;
          }
          record = CsvRecordReader.readRecord(reader);
        }
        mapped = null;
        close();
      } catch (IOException e) {
        close();
        throw new RuntimeException("herd: read failed for " + url, e);
      }
    }

    private Map<String, Object> mapRow(List<String> cols) {
      String question = pick(cols, "question");
      if (question == null) {
        return null;
      }

      if (preRestructure) {
        return mapRowPreRestructure(cols, question);
      }

      boolean federal = Q_FEDERAL.equals(question);
      boolean nonfederal = Q_NONFEDERAL.equals(question);
      if (!federal && !nonfederal) {
        return null; // only the two R&D-expenditure question families are projected
      }

      Integer year = intVal(pick(cols, "year"));
      if (year == null) {
        return null; // no year → cannot partition
      }
      Double amount = dbl(pick(cols, "data"));
      if (amount == null) {
        return null; // suppressed / blank cell → honest absence, not a zero
      }

      String field = pick(cols, "row");
      String colLabel = pick(cols, "column");

      Map<String, Object> out = newBaseRow(cols, year);
      out.put("rd_field", field);
      if (federal) {
        out.put("funding_source", "Federal");
        out.put("federal_agency", colLabel);
      } else {
        // NCSES's own "Total" column on this question is the sum of the five nonfederal
        // sources only -- it excludes Federal entirely. Passed through as bare "Total" it
        // reads as a grand total; disambiguate so a caller can't mistake it for one (the
        // true grand total is this value plus funding_source='Federal' AND federal_agency='Total').
        out.put("funding_source", "Total".equals(colLabel) ? "Total nonfederal" : colLabel);
        out.put("federal_agency", null);
      }
      out.put("rd_expenditure_usd_thousand", amount);
      return out;
    }

    /** Pre-2010 (herd2008.csv/herd2009.csv) mapping — see the class javadoc. */
    private Map<String, Object> mapRowPreRestructure(List<String> cols, String question) {
      String instKey = pick(cols, "fice");
      if (!Objects.equals(instKey, currentInstKey)) {
        currentInstKey = instKey;
        pendingFieldFederal.clear();
      }

      Integer year = intVal(pick(cols, "year"));
      if (year == null) {
        return null;
      }
      String field = pick(cols, "row");
      String colLabel = pick(cols, "column");
      Double amount = dbl(pick(cols, "data"));

      if (Q_FEDERAL_OLD.equals(question)) {
        // row=S&E field, column=agency. No "Total" agency column in this vintage (see javadoc
        // for why the 7 named agencies do not necessarily sum to the field's true federal total).
        if (amount == null) {
          return null;
        }
        Map<String, Object> out = newBaseRow(cols, year);
        out.put("rd_field", field);
        out.put("funding_source", "Federal");
        out.put("federal_agency", colLabel);
        out.put("rd_expenditure_usd_thousand", amount);
        return out;
      }

      if (Q_EXPEND_BY_FIELD_OLD.equals(question)) {
        if ("Federal".equals(colLabel)) {
          // Field-level federal total (this vintage's stand-in for 2010+'s
          // federal_agency="Total" row) — buffer it to derive nonfederal once Total arrives.
          if (field != null && amount != null) {
            pendingFieldFederal.put(field, amount);
          }
          if (amount == null) {
            return null;
          }
          Map<String, Object> out = newBaseRow(cols, year);
          out.put("rd_field", field);
          out.put("funding_source", "Federal");
          out.put("federal_agency", "Total");
          out.put("rd_expenditure_usd_thousand", amount);
          return out;
        }
        if ("Total".equals(colLabel)) {
          Double fed = field == null ? null : pendingFieldFederal.remove(field);
          if (fed == null || amount == null) {
            return null; // no paired Federal value to subtract from — honest absence
          }
          Map<String, Object> out = newBaseRow(cols, year);
          out.put("rd_field", field);
          out.put("funding_source", "Total nonfederal");
          out.put("federal_agency", null);
          out.put("rd_expenditure_usd_thousand", amount - fed);
          return out;
        }
        return null;
      }

      if (Q_SOURCE_OLD.equals(question)) {
        // Institution-wide only (this vintage never crosses nonfederal source with S&E
        // field) — "row" here holds the source label itself. "Federal" and "Total" rows are
        // skipped: redundant with, and cross-validated against, the derivation above.
        String source = OLD_SOURCE_LABELS.get(field);
        if (source == null || amount == null) {
          return null;
        }
        Map<String, Object> out = newBaseRow(cols, year);
        out.put("rd_field", "All");
        out.put("funding_source", source);
        out.put("federal_agency", null);
        out.put("rd_expenditure_usd_thousand", amount);
        return out;
      }

      return null; // only the projected question families above are mapped
    }

    /** Common row scaffold shared by both vintages; era-specific fields (institution id,
     * ncses_inst_id, ipeds_unitid, state code column name) resolved per-column-name here so
     * callers only fill in rd_field/funding_source/federal_agency/rd_expenditure_usd_thousand. */
    private Map<String, Object> newBaseRow(List<String> cols, Integer year) {
      Map<String, Object> out = new LinkedHashMap<String, Object>();
      out.put("year", year);
      // inst_id is the unique campus key (1:1 with the institution, survives name changes) —
      // the institution component of the PK. Pre-2010, NCSES published only "fice" (a
      // different id space than 2010+'s inst_id), used here as-is. ncses_inst_id is a coarser
      // parent/system id (one id can span several campuses); not published pre-2010.
      out.put("inst_id", preRestructure ? pick(cols, "fice") : pick(cols, "inst_id"));
      out.put("ncses_inst_id", preRestructure ? null : pick(cols, "ncses_inst_id"));
      out.put("institution", pick(cols, "inst_name_long"));
      // IPEDS UnitID is always numeric; blank means NCSES did not match the institution
      // (legitimate null, never fabricated). Not published at all pre-2010. Emitted as
      // INTEGER to join edu.ipeds_institutions.unitid.
      out.put("ipeds_unitid", preRestructure ? null : intVal(pick(cols, "ipeds_unitid")));
      out.put("state_fips",
          stateFips(preRestructure ? pick(cols, "inst_state") : pick(cols, "inst_state_code")));
      out.put("county_fips", null);
      out.put("control", null);
      return out;
    }

    private String stateFips(String usps) {
      if (usps == null) {
        return null; // missing state — legitimately null, not an error
      }
      String code = usps.trim().toUpperCase();
      if (code.isEmpty()) {
        return null;
      }
      String fips = STATE_FIPS.get(code);
      if (fips == null) {
        // A real, non-blank postal code we do not recognize is a data-quality signal —
        // fail loudly rather than default to a placeholder FIPS (project rule #6).
        throw new IllegalStateException("herd: unmapped institution state code '" + code
            + "' in " + url);
      }
      return fips;
    }

    private String pick(List<String> cols, String name) {
      Integer i = idx.get(name);
      if (i != null && i < cols.size()) {
        String v = cols.get(i).trim();
        if (!v.isEmpty()) {
          return v;
        }
      }
      return null;
    }

    private static Integer intVal(String v) {
      if (v == null) {
        return null;
      }
      try {
        return Integer.parseInt(v.trim());
      // fallback-guard: allow nullable-field helper; null correctly signals unparseable, standard safe-parse idiom
      } catch (NumberFormatException e) {
        return null;
      }
    }

    private static Double dbl(String v) {
      if (v == null) {
        return null;
      }
      try {
        return Double.parseDouble(v.replace(",", ""));
      // fallback-guard: allow dbl() mirroring intVal() in this class, same nullable-field idiom
      } catch (NumberFormatException e) {
        return null;
      }
    }

    private void close() {
      if (done) {
        return;
      }
      done = true;
      try {
        reader.close();
      } catch (IOException ignored) {
        // best-effort
      }
      conn.disconnect();
    }

    @Override public boolean hasNext() {
      return mapped != null;
    }

    @Override public Map<String, Object> next() {
      if (mapped == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> row = mapped;
      advance();
      return row;
    }
  }
}
