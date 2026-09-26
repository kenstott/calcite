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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.CrossProcessRateLimiter;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.SkippedBatchException;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;

import org.apache.poi.hssf.eventusermodel.HSSFEventFactory;
import org.apache.poi.hssf.eventusermodel.HSSFListener;
import org.apache.poi.hssf.eventusermodel.HSSFRequest;
import org.apache.poi.hssf.eventusermodel.MissingRecordAwareHSSFListener;
import org.apache.poi.hssf.eventusermodel.dummyrecord.LastCellOfRowDummyRecord;
import org.apache.poi.hssf.record.BOFRecord;
import org.apache.poi.hssf.record.LabelRecord;
import org.apache.poi.hssf.record.LabelSSTRecord;
import org.apache.poi.hssf.record.NumberRecord;
import org.apache.poi.hssf.record.Record;
import org.apache.poi.hssf.record.SSTRecord;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.util.XMLHelper;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFComment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Reads one reference year of the BLS OEWS state workbook
 * ({@code https://www.bls.gov/oes/special-requests/oesm{YY}st.zip}) and emits the
 * {@code econ.state_occupation_employment} rows for it.
 *
 * <p>Each zip holds one state workbook: {@code .xls} for reference years 2010-2013 and
 * {@code .xlsx} from 2014 on. Both are read with POI's event APIs, so only the matching rows
 * (a few hundred per year) are ever held; the ~35k-row sheet is never materialised.
 * The data sheet is the first sheet (its name varies by vintage: {@code state_dl},
 * {@code State_M2019_dl}, {@code All May 2021 data}). Columns are located by lower-cased
 * header name because the header set and case change between vintages ({@code area_type},
 * {@code naics} and {@code own_code} exist only from 2019 on; earlier files are
 * cross-industry, all-ownership by construction).
 *
 * <p>Occupations are published under the SOC code the workbook carries for that year, with
 * no cross-vintage remapping: 17-0000 and 19-0000 are stable; software developers are
 * 15-1132 (Applications) plus 15-1133 (Systems Software) for 2010-2018, 15-1256 (Software
 * Developers and Software Quality Assurance Analysts and Testers) for 2019-2020, and 15-1252
 * (Software Developers) from 2021. Those are different occupation definitions, so the codes
 * stay distinct.
 *
 * <p>Areas are the 50 states, DC, Puerto Rico, Guam and the Virgin Islands (54), keyed by
 * 2-digit FIPS. A cell BLS marks {@code **} (suppressed) yields no row. The emitted
 * {@code series} is the BLS OE series id ({@code OEUS} + 7-digit area + 000000 industry +
 * 6-digit occupation + 01 datatype), the same key the flat-file feed produces.
 */
public class OewsStateWorkbookTransformer implements StreamingResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(OewsStateWorkbookTransformer.class);

  /** SOC code (no dash) to the occupation name published for it. */
  static final Map<String, String> OCCUPATIONS;

  static {
    Map<String, String> m = new LinkedHashMap<String, String>();
    m.put("170000", "Architecture and Engineering Occupations");
    m.put("190000", "Life, Physical, and Social Science Occupations");
    m.put("151132", "Software Developers, Applications");
    m.put("151133", "Software Developers, Systems Software");
    m.put("151256",
        "Software Developers and Software Quality Assurance Analysts and Testers");
    m.put("151252", "Software Developers");
    OCCUPATIONS = Collections.unmodifiableMap(m);
  }

  /** area_type 2 = state/DC, 3 = Guam/Puerto Rico/Virgin Islands. */
  private static final Set<String> AREA_TYPES =
      new HashSet<String>(Arrays.asList("2", "3"));

  private static final String CROSS_INDUSTRY_NAICS = "000000";
  private static final String ALL_OWNERSHIP = "1235";
  private static final String SUPPRESSED = "**";

  private static final Pattern STATE_ENTRY =
      Pattern.compile("(?i)(?:^|.*/)state_m(\\d{4})_dl\\.(xlsx|xls)$");

  private static final long BLS_MIN_REQUEST_INTERVAL_MS = 7_000L;
  private static final String BLS_RATE_LIMIT_KEY = "www.bls.gov";
  private static final int MAX_FETCH_RETRIES = 3;
  private static final long RETRY_BACKOFF_MS = 20_000L;

  private static final HttpClient CLIENT = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(30))
      .build();

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context)
      throws IOException {
    String yearText = context.getDimensionValues().get("effective_year");
    if (yearText == null) {
      throw new IllegalStateException(
          "OEWS state workbook: effective_year dimension is required to label the reference year");
    }
    int year = Integer.parseInt(yearText);
    String url = context.getUrl();

    Path zip = Files.createTempFile("oews-state-", ".zip");
    Path workbook = null;
    try {
      download(url, context.getHeaders(), zip);
      try (ZipFile zf = new ZipFile(zip.toFile())) {
        ZipEntry entry = findStateEntry(zf, url);
        String name = entry.getName().toLowerCase(Locale.ROOT);
        String ext = name.endsWith(".xlsx") ? ".xlsx" : ".xls";
        workbook = Files.createTempFile("oews-state-", ext);
        try (InputStream in = zf.getInputStream(entry)) {
          Files.copy(in, workbook, StandardCopyOption.REPLACE_EXISTING);
        }
        Matcher m = STATE_ENTRY.matcher(entry.getName());
        if (!m.matches() || Integer.parseInt(m.group(1)) != year) {
          throw new IOException("OEWS state workbook " + entry.getName() + " in " + url
              + " is not reference year " + year);
        }
      }
      Collector collector = new Collector(year);
      if (workbook.toString().endsWith(".xlsx")) {
        readXlsx(workbook.toFile(), collector);
      } else {
        readXls(workbook.toFile(), collector);
      }
      List<Map<String, Object>> rows = collector.finish(url);
      LOGGER.debug("OEWS state workbook {}: {} rows for reference year {}", url, rows.size(),
          year);
      return rows.iterator();
    } finally {
      Files.deleteIfExists(zip);
      if (workbook != null) {
        Files.deleteIfExists(workbook);
      }
    }
  }

  private static ZipEntry findStateEntry(ZipFile zf, String url) throws IOException {
    ZipEntry found = null;
    Enumeration<? extends ZipEntry> entries = zf.entries();
    while (entries.hasMoreElements()) {
      ZipEntry e = entries.nextElement();
      if (STATE_ENTRY.matcher(e.getName()).matches()) {
        if (found != null) {
          throw new IOException("OEWS: more than one state workbook in " + url);
        }
        found = e;
      }
    }
    if (found == null) {
      throw new IOException("OEWS: no state_M<year>_dl workbook in " + url);
    }
    return found;
  }

  private static void download(String url, Map<String, String> headers, Path target)
      throws IOException {
    IOException last = null;
    for (int attempt = 0; attempt <= MAX_FETCH_RETRIES; attempt++) {
      if (attempt > 0) {
        long backoff = RETRY_BACKOFF_MS * attempt;
        LOGGER.warn("OEWS: retrying {} (attempt {}/{}, backoff {}ms): {}", url, attempt + 1,
            MAX_FETCH_RETRIES + 1, backoff, last.getMessage());
        try {
          Thread.sleep(backoff);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while retrying OEWS download", ie);
        }
      }
      try {
        fetchOnce(url, headers, target);
        return;
      } catch (SkippedBatchException e) {
        throw e;
      } catch (IOException e) {
        last = e;
      }
    }
    throw last;
  }

  private static void fetchOnce(String url, Map<String, String> headers, Path target)
      throws IOException {
    CrossProcessRateLimiter.acquire(BLS_RATE_LIMIT_KEY, BLS_MIN_REQUEST_INTERVAL_MS);
    HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofMinutes(5))
        .GET();
    if (headers != null) {
      for (Map.Entry<String, String> h : headers.entrySet()) {
        builder.header(h.getKey(), h.getValue());
      }
    }
    HttpResponse<InputStream> response;
    try {
      response = CLIENT.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted fetching OEWS workbook: " + url, e);
    }
    int code = response.statusCode();
    try (InputStream body = response.body()) {
      if (code == 404) {
        throw new SkippedBatchException("OEWS state workbook not yet published (HTTP 404): "
            + url);
      }
      if (code != 200) {
        throw new IOException("OEWS state workbook download HTTP " + code + ": " + url);
      }
      Files.copy(body, target, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  // ---------------------------------------------------------------------------------------
  // Row filtering (shared by the xlsx and xls readers)
  // ---------------------------------------------------------------------------------------

  /** Header-aware filter that turns matching sheet rows into output rows. */
  static final class Collector {
    private final int year;
    private final List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    private final Set<String> seen = new HashSet<String>();
    private Map<String, Integer> columns;
    private int area = -1;
    private int areaType = -1;
    private int naics = -1;
    private int ownCode = -1;
    private int occCode = -1;
    private int totEmp = -1;

    Collector(int year) {
      this.year = year;
    }

    /** Receives one sheet row; cells are indexed by column, null where empty. */
    void accept(List<String> cells) {
      if (columns == null) {
        readHeader(cells);
        return;
      }
      String occ = get(cells, occCode);
      if (occ == null) {
        return;
      }
      String occKey = occ.replace("-", "").trim();
      String occName = OCCUPATIONS.get(occKey);
      if (occName == null) {
        return;
      }
      if (areaType >= 0 && !AREA_TYPES.contains(normalizeInt(get(cells, areaType)))) {
        return;
      }
      if (naics >= 0 && !CROSS_INDUSTRY_NAICS.equals(normalizeInt(get(cells, naics)))) {
        return;
      }
      if (ownCode >= 0 && !ALL_OWNERSHIP.equals(normalizeInt(get(cells, ownCode)))) {
        return;
      }
      String fips = fips(get(cells, area));
      String emp = get(cells, totEmp);
      if (emp == null) {
        throw new IllegalStateException("OEWS " + year + ": empty tot_emp for area " + fips
            + " occupation " + occ);
      }
      String trimmed = emp.trim();
      if (SUPPRESSED.equals(trimmed)) {
        return;
      }
      double employment;
      try {
        employment = Double.parseDouble(trimmed.replace(",", ""));
      } catch (NumberFormatException e) {
        throw new IllegalStateException("OEWS " + year + ": unrecognised tot_emp '" + emp
            + "' for area " + fips + " occupation " + occ, e);
      }
      if (!seen.add(fips + "|" + occKey)) {
        throw new IllegalStateException("OEWS " + year + ": duplicate row for area " + fips
            + " occupation " + occ);
      }
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("series", "OEUS" + fips + "00000" + "000000" + occKey + "01");
      row.put("year", Integer.valueOf(year));
      row.put("state_fips", fips);
      row.put("occupation_code", occKey);
      row.put("occupation_name", occName);
      row.put("employment", Double.valueOf(employment));
      row.put("footnotes", null);
      rows.add(row);
    }

    List<Map<String, Object>> finish(String url) {
      if (columns == null) {
        throw new IllegalStateException("OEWS " + year + ": no data sheet header found in "
            + url);
      }
      if (rows.isEmpty()) {
        throw new IllegalStateException("OEWS " + year + ": no matching rows in " + url);
      }
      return rows;
    }

    private void readHeader(List<String> cells) {
      Map<String, Integer> idx = new HashMap<String, Integer>();
      for (int i = 0; i < cells.size(); i++) {
        String h = cells.get(i);
        if (h != null && !h.trim().isEmpty()) {
          idx.put(h.trim().toLowerCase(Locale.ROOT), Integer.valueOf(i));
        }
      }
      columns = idx;
      area = required("area");
      occCode = required("occ_code");
      totEmp = required("tot_emp");
      areaType = optional("area_type");
      naics = optional("naics");
      ownCode = optional("own_code");
    }

    private int required(String name) {
      Integer i = columns.get(name);
      if (i == null) {
        throw new IllegalStateException("OEWS " + year + ": header has no '" + name
            + "' column: " + columns.keySet());
      }
      return i.intValue();
    }

    private int optional(String name) {
      Integer i = columns.get(name);
      return i == null ? -1 : i.intValue();
    }

    private static String get(List<String> cells, int col) {
      if (col < 0 || col >= cells.size()) {
        return null;
      }
      return cells.get(col);
    }

    private String fips(String raw) {
      if (raw == null) {
        throw new IllegalStateException("OEWS " + year + ": empty area code");
      }
      String n = normalizeInt(raw);
      if (n.length() == 1) {
        n = "0" + n;
      }
      if (n.length() != 2) {
        throw new IllegalStateException("OEWS " + year + ": area '" + raw
            + "' is not a 2-digit state FIPS");
      }
      return n;
    }

    /** Strips whitespace and a trailing ".0" so numeric and text cells compare equal. */
    private static String normalizeInt(String s) {
      if (s == null) {
        return "";
      }
      String t = s.trim();
      if (t.endsWith(".0")) {
        t = t.substring(0, t.length() - 2);
      }
      return t;
    }
  }

  // ---------------------------------------------------------------------------------------
  // xlsx (POI SAX)
  // ---------------------------------------------------------------------------------------

  static void readXlsx(File file, final Collector collector) throws IOException {
    // Sheet XML compresses far below POI's default zip-bomb inflate ratio; the source is a
    // trusted federal publication.
    ZipSecureFile.setMinInflateRatio(0.0);
    try (OPCPackage pkg = OPCPackage.open(file, PackageAccess.READ)) {
      XSSFReader reader = new XSSFReader(pkg);
      ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(pkg);
      StylesTable styles = reader.getStylesTable();
      XSSFReader.SheetIterator sheets = (XSSFReader.SheetIterator) reader.getSheetsData();
      if (!sheets.hasNext()) {
        throw new IOException("OEWS: workbook has no sheets: " + file);
      }
      try (InputStream sheet = sheets.next()) {
        XMLReader parser = XMLHelper.newXMLReader();
        parser.setContentHandler(new XSSFSheetXMLHandler(styles, null, strings,
            new RowHandler(collector), new DataFormatter(), false));
        parser.parse(new InputSource(sheet));
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("OEWS: failed to read xlsx " + file, e);
    }
  }

  private static final class RowHandler implements XSSFSheetXMLHandler.SheetContentsHandler {
    private final Collector collector;
    private List<String> current;

    RowHandler(Collector collector) {
      this.collector = collector;
    }

    @Override public void startRow(int rowNum) {
      current = new ArrayList<String>();
    }

    @Override public void endRow(int rowNum) {
      collector.accept(current);
    }

    @Override public void cell(String cellReference, String formattedValue,
        XSSFComment comment) {
      int col = new CellReference(cellReference).getCol();
      while (current.size() <= col) {
        current.add(null);
      }
      current.set(col, formattedValue);
    }
  }

  // ---------------------------------------------------------------------------------------
  // xls (POI HSSF events)
  // ---------------------------------------------------------------------------------------

  static void readXls(File file, Collector collector) throws IOException {
    try (POIFSFileSystem fs = new POIFSFileSystem(file, true)) {
      HSSFRequest request = new HSSFRequest();
      request.addListenerForAllRecords(
          new MissingRecordAwareHSSFListener(new XlsListener(collector)));
      new HSSFEventFactory().processWorkbookEvents(request, fs);
    }
  }

  private static final class XlsListener implements HSSFListener {
    private final Collector collector;
    private SSTRecord sst;
    private int sheetIndex = -1;
    private List<String> current = new ArrayList<String>();

    XlsListener(Collector collector) {
      this.collector = collector;
    }

    /** The data sheet is always the workbook's first sheet; later sheets are documentation. */
    private boolean inDataSheet() {
      return sheetIndex == 0;
    }

    private void put(int col, String value) {
      while (current.size() <= col) {
        current.add(null);
      }
      current.set(col, value);
    }

    @Override public void processRecord(Record record) {
      if (record instanceof BOFRecord) {
        if (((BOFRecord) record).getType() == BOFRecord.TYPE_WORKSHEET) {
          sheetIndex++;
        }
      } else if (record instanceof SSTRecord) {
        sst = (SSTRecord) record;
      } else if (!inDataSheet()) {
        return;
      } else if (record instanceof LabelSSTRecord) {
        LabelSSTRecord l = (LabelSSTRecord) record;
        put(l.getColumn(), sst.getString(l.getSSTIndex()).getString());
      } else if (record instanceof LabelRecord) {
        LabelRecord l = (LabelRecord) record;
        put(l.getColumn(), l.getValue());
      } else if (record instanceof NumberRecord) {
        NumberRecord n = (NumberRecord) record;
        put(n.getColumn(), numberText(n.getValue()));
      } else if (record instanceof LastCellOfRowDummyRecord) {
        collector.accept(current);
        current = new ArrayList<String>();
      }
    }
  }

  private static String numberText(double v) {
    if (v == Math.rint(v) && !Double.isInfinite(v)) {
      return String.valueOf((long) v);
    }
    return String.valueOf(v);
  }
}
