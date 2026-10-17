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
package org.apache.calcite.adapter.govdata.officials;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.util.XMLHelper;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFComment;

import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Strict reader of the Federal Election Commission's official "Federal Elections 2020" and
 * "Federal Elections 2022" workbooks, producing {@code house_election_results} rows for the
 * 50 states' general elections in the same shape as the MIT Election Lab rows.
 *
 * <p>The House sheet is a print-style listing: one row per candidate per party line carrying the
 * primary, primary-runoff, general and general-runoff results side by side, interleaved with
 * {@code Party Votes:}, {@code District Votes:} and {@code Total State Votes:} subtotal rows (and,
 * in 2022, {@code DISTRICT n} heading rows and {@code Combined Parties:} rows). Only the general
 * election and general runoff columns are read. Everything the reader does not recognise fails the
 * fetch: a changed header, an unknown state, an unknown party label, a subtotal that does not equal
 * the sum of its candidate rows, a winner flag that does not match the leader.
 *
 * <p>Rules, all documented in the table comment:
 * <ul>
 * <li>Delegate jurisdictions (AS, DC, GU, MP, PR, VI) are skipped, as the MIT series carries
 *     only the 50 states.</li>
 * <li>Each party line of a candidate is its own row (New York and Connecticut fusion); the
 *     {@code Combined Parties:} rows and the combined-total column are not read.</li>
 * <li>A general-runoff column value makes a second round with runoff=true (Louisiana 2020).</li>
 * <li>Instant-runoff (ranked-choice) contests: candidate_votes is the last-round count, as the Lab
 *     did for Maine 2018; the first-choice count is kept in first_choice_votes.</li>
 * <li>An {@code Unopposed} general result becomes the Lab's 1 vote out of 1 marker.</li>
 * </ul>
 */
final class FecHouseResultsParser {

  static final String TABLE = "house_election_results";
  static final String SOURCE = "FEC";

  private static final Set<String> DELEGATE_JURISDICTIONS =
      new HashSet<String>(Arrays.asList("AS", "DC", "GU", "MP", "PR", "VI"));

  /** Postal, FIPS, name. */
  private static final String[][] STATES = {
      {"AL", "01", "Alabama"}, {"AK", "02", "Alaska"}, {"AZ", "04", "Arizona"},
      {"AR", "05", "Arkansas"}, {"CA", "06", "California"}, {"CO", "08", "Colorado"},
      {"CT", "09", "Connecticut"}, {"DE", "10", "Delaware"}, {"FL", "12", "Florida"},
      {"GA", "13", "Georgia"}, {"HI", "15", "Hawaii"}, {"ID", "16", "Idaho"},
      {"IL", "17", "Illinois"}, {"IN", "18", "Indiana"}, {"IA", "19", "Iowa"},
      {"KS", "20", "Kansas"}, {"KY", "21", "Kentucky"}, {"LA", "22", "Louisiana"},
      {"ME", "23", "Maine"}, {"MD", "24", "Maryland"}, {"MA", "25", "Massachusetts"},
      {"MI", "26", "Michigan"}, {"MN", "27", "Minnesota"}, {"MS", "28", "Mississippi"},
      {"MO", "29", "Missouri"}, {"MT", "30", "Montana"}, {"NE", "31", "Nebraska"},
      {"NV", "32", "Nevada"}, {"NH", "33", "New Hampshire"}, {"NJ", "34", "New Jersey"},
      {"NM", "35", "New Mexico"}, {"NY", "36", "New York"}, {"NC", "37", "North Carolina"},
      {"ND", "38", "North Dakota"}, {"OH", "39", "Ohio"}, {"OK", "40", "Oklahoma"},
      {"OR", "41", "Oregon"}, {"PA", "42", "Pennsylvania"}, {"RI", "44", "Rhode Island"},
      {"SC", "45", "South Carolina"}, {"SD", "46", "South Dakota"}, {"TN", "47", "Tennessee"},
      {"TX", "48", "Texas"}, {"UT", "49", "Utah"}, {"VT", "50", "Vermont"},
      {"VA", "51", "Virginia"}, {"WA", "53", "Washington"}, {"WV", "54", "West Virginia"},
      {"WI", "55", "Wisconsin"}, {"WY", "56", "Wyoming"}};

  private static final Map<String, String[]> BY_POSTAL = new HashMap<String, String[]>();
  private static final Map<String, String[]> BY_NAME = new HashMap<String, String[]>();

  static {
    for (String[] s : STATES) {
      BY_POSTAL.put(s[0], s);
      BY_NAME.put(s[2].toLowerCase(Locale.ROOT), s);
    }
  }

  /** Party codes that are the Democratic Party (Minnesota DFL, North Dakota Democratic-NPL). */
  private static final Set<String> DEMOCRATIC =
      new HashSet<String>(Arrays.asList("D", "DFL", "DNL"));
  private static final Set<String> REPUBLICAN = new HashSet<String>(Arrays.asList("R"));

  /** Labels, in lower case, of the candidate column that are vote buckets and not people. */
  private static final Set<String> AGGREGATE_LABELS = new HashSet<String>(
      Arrays.asList("scattered", "scattered #", "all others", "anyone else"));

  private static final Pattern DISTRICT = Pattern.compile("(\\d{2})(-Full Term|-Unexpired Term)?");
  private static final Pattern DISTRICT_HEADING =
      Pattern.compile("DISTRICT \\d+( \\(FULL TERM\\)| \\(UNEXPIRED TERM\\))?|AT LARGE DISTRICT"
          + "|DELEGATE|RESIDENT COMMISSIONER");
  private static final Pattern WRAPPED = Pattern.compile("([WN])\\(([A-Z0-9]+)\\)");

  /** One pinned release of the FEC workbook. */
  static final class Release {
    final int year;
    final String url;
    final String houseSheet;
    final String labelsSheet;
    final String publicationSheet;
    final String publishedText;
    final String[] header;
    final String description;
    /** Codes the sheet uses that its own label table spells differently (checked by name). */
    final Map<String, String> aliases;

    Release(int year, String url, String houseSheet, String labelsSheet, String publicationSheet,
        String publishedText, String[] header, Map<String, String> aliases) {
      this.aliases = aliases;
      this.year = year;
      this.url = url;
      this.houseSheet = houseSheet;
      this.labelsSheet = labelsSheet;
      this.publicationSheet = publicationSheet;
      this.publishedText = publishedText;
      this.header = header;
      this.description = "FEC Federal Elections " + year + " official results workbook, "
          + "published " + publishedText + ", " + url;
    }
  }

  private static final String[] HEADER_2020 = {"1", "STATE ABBREVIATION", "STATE", "DISTRICT",
      "FEC ID", "(I) INCUMBENT INDICATOR", "CANDIDATE NAME (FIRST)", "CANDIDATE NAME (LAST)",
      "CANDIDATE NAME", "TOTAL VOTES", "PARTY", "PRIMARY VOTES", "PRIMARY %", "RUNOFF VOTES",
      "RUNOFF %", "GENERAL VOTES", "GENERAL %", "GE RUNOFF ELECTION VOTES (GA, GU, LA)",
      "GE RUNOFF ELECTION % (GA, GU, LA)", "COMBINED GE PARTY TOTALS (CT, NY)",
      "COMBINED % (CT, NY)", "PE WINNER INDICATOR", "GE WINNER INDICATOR", "FOOTNOTES"};

  private static final String[] HEADER_2022 = {"1", "STATE ABBREVIATION", "STATE", "DISTRICT",
      "FEC ID", "(I) INCUMBENT INDICATOR", "CANDIDATE NAME (FIRST)", "CANDIDATE NAME (LAST)",
      "CANDIDATE NAME", "TOTAL VOTES", "PARTY", "PRIMARY VOTES", "PRIMARY %", "RUNOFF VOTES",
      "RUNOFF %", "GENERAL VOTES", "GENERAL %", "GE RUNOFF ELECTION VOTES (WHEN APPLICABLE)",
      "GE RUNOFF ELECTION % (WHEN APPLICABLE)", "COMBINED GE PARTY TOTALS (WHEN APPLICABLE)",
      "COMBINED GE PARTY % (WHEN APPLICABLE)", "PE WINNER INDICATOR", "GE WINNER INDICATOR",
      "FOOTNOTES", "1ST ROUND RCV VOTES", "1ST ROUND RCV %", "2ND ROUND RCV VOTES",
      "2ND ROUND RCV %", "3RD ROUND RCV VOTES", "3RD ROUND RCV %"};

  static final Release[] RELEASES = {
      new Release(2020,
          "https://www.fec.gov/documents/4228/federalelections2020.xlsx",
          "13. US House Results by State", "14. 2020 Party Labels",
          "1. 2020 Publication Information", "October 2022", HEADER_2020,
          // Jan Kok, Colorado 1: the label table has APV, Approval Voting Party; AVP is not in it.
          Collections.singletonMap("AVP", "APV")),
      new Release(2022,
          "https://www.fec.gov/documents/5676/federalelections2022.xlsx",
          "8. US House Results by State", "9. 2022 Party Labels",
          "1. 2022 Publication Information", "February 2025", HEADER_2022,
          // Gregory Sobocinski, New Jersey 3: ballot slogan "God Save America" (NJ Division of
          // Elections; New Jersey Globe); the label table has GSA, not GS.
          Collections.singletonMap("GS", "GSA"))};

  private static final int C_ABBR = 1;
  private static final int C_STATE = 2;
  private static final int C_DISTRICT = 3;
  private static final int C_FEC_ID = 4;
  private static final int C_FIRST = 6;
  private static final int C_LAST = 7;
  private static final int C_NAME = 8;
  private static final int C_TOTAL = 9;
  private static final int C_PARTY = 10;
  private static final int C_PRIMARY = 11;
  private static final int C_GENERAL = 15;
  private static final int C_RUNOFF = 17;
  private static final int C_GE_WINNER = 22;
  /** Vote columns of the 1st, 2nd and 3rd ranked-choice rounds (2022 only). */
  private static final int[] C_RCV = {24, 26, 28};

  private FecHouseResultsParser() { }

  // ---------------------------------------------------------------------------------------
  // workbook reading
  // ---------------------------------------------------------------------------------------

  /** Reads the three sheets of the release from the xlsx bytes and parses them. */
  static List<Map<String, Object>> parseWorkbook(byte[] xlsx, Release rel) throws IOException {
    ZipSecureFile.setMinInflateRatio(0.0);
    try (OPCPackage pkg = OPCPackage.open(new ByteArrayInputStream(xlsx))) {
      Map<String, List<String[]>> sheets = readSheets(pkg,
          new HashSet<String>(Arrays.asList(rel.houseSheet, rel.labelsSheet,
              rel.publicationSheet)));
      for (String name : new String[] {rel.houseSheet, rel.labelsSheet, rel.publicationSheet}) {
        if (!sheets.containsKey(name)) {
          throw new IOException(TABLE + " " + rel.year + ": workbook lacks sheet '" + name
              + "'; the FEC release changed and the parser must be reviewed");
        }
      }
      return parse(rel, sheets.get(rel.houseSheet), sheets.get(rel.labelsSheet),
          sheets.get(rel.publicationSheet));
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(TABLE + " " + rel.year + ": cannot read the FEC workbook", e);
    }
  }

  private static Map<String, List<String[]>> readSheets(OPCPackage pkg, Set<String> wanted)
      throws Exception {
    ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(pkg);
    XSSFReader reader = new XSSFReader(pkg);
    StylesTable styles = reader.getStylesTable();
    XSSFReader.SheetIterator it = (XSSFReader.SheetIterator) reader.getSheetsData();
    Map<String, List<String[]>> out = new HashMap<String, List<String[]>>();
    while (it.hasNext()) {
      try (InputStream in = it.next()) {
        String name = it.getSheetName();
        if (!wanted.contains(name)) {
          continue;
        }
        RowCollector collector = new RowCollector();
        XMLReader parser = XMLHelper.newXMLReader();
        parser.setContentHandler(new XSSFSheetXMLHandler(styles, null, strings, collector,
            new RawNumberFormatter(), false));
        parser.parse(new InputSource(in));
        out.put(name, collector.rows);
      }
    }
    return out;
  }

  /** Renders numeric cells raw (integers unadorned) whatever the cell's number format. */
  private static final class RawNumberFormatter extends DataFormatter {
    @Override public String formatRawCellContents(double value, int formatIndex,
        String formatString) {
      return raw(value);
    }

    @Override public String formatRawCellContents(double value, int formatIndex,
        String formatString, boolean use1904Windowing) {
      return raw(value);
    }

    private static String raw(double value) {
      if (value == Math.floor(value) && !Double.isInfinite(value)) {
        return String.valueOf((long) value);
      }
      return String.valueOf(value);
    }
  }

  private static final class RowCollector implements XSSFSheetXMLHandler.SheetContentsHandler {
    final List<String[]> rows = new ArrayList<String[]>();
    private List<String> current;

    @Override public void startRow(int rowNum) {
      current = new ArrayList<String>();
    }

    @Override public void endRow(int rowNum) {
      rows.add(current.toArray(new String[0]));
    }

    @Override public void cell(String cellReference, String formattedValue,
        XSSFComment comment) {
      int col = new CellReference(cellReference).getCol();
      while (current.size() <= col) {
        current.add("");
      }
      current.set(col, formattedValue == null ? "" : formattedValue);
    }
  }

  // ---------------------------------------------------------------------------------------
  // parsing
  // ---------------------------------------------------------------------------------------

  private static String clean(String v) {
    return v == null ? "" : v.replace(' ', ' ').trim();
  }

  private static String g(String[] r, int i) {
    return i < r.length ? clean(r[i]) : "";
  }

  private static String norm(String v) {
    return clean(v).replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
  }

  private static IOException fail(Release rel, int line, String msg) {
    return new IOException(TABLE + " FEC " + rel.year + " house sheet row " + line + ": " + msg);
  }

  /** One candidate-line row read from the sheet. */
  private static final class Cand {
    final int line;
    final String name;
    final boolean aggregate;
    final String partyCode;
    final boolean primaryShown;
    final Long general;
    final boolean unopposed;
    final Long runoff;
    final Long[] rcv;
    final boolean winnerFlag;

    Cand(int line, String name, boolean aggregate, String partyCode, boolean primaryShown,
        Long general, boolean unopposed, Long runoff, Long[] rcv, boolean winnerFlag) {
      this.line = line;
      this.name = name;
      this.aggregate = aggregate;
      this.partyCode = partyCode;
      this.primaryShown = primaryShown;
      this.general = general;
      this.unopposed = unopposed;
      this.runoff = runoff;
      this.rcv = rcv;
      this.winnerFlag = winnerFlag;
    }
  }

  private static final class Block {
    final String key;
    final String[] state;
    final String districtText;
    final int firstLine;
    final List<Cand> cands = new ArrayList<Cand>();
    boolean totalSeen;
    String[] totalRow;

    Block(String key, String[] state, String districtText, int firstLine) {
      this.key = key;
      this.state = state;
      this.districtText = districtText;
      this.firstLine = firstLine;
    }
  }

  /** Parses the three sheets (already read into rows) of one release. */
  static List<Map<String, Object>> parse(Release rel, List<String[]> house,
      List<String[]> labelRows, List<String[]> publication) throws IOException {
    boolean published = false;
    for (String[] r : publication) {
      for (String c : r) {
        if (clean(c).equals(rel.publishedText)) {
          published = true;
        }
      }
    }
    if (!published) {
      throw new IOException(TABLE + " " + rel.year + ": publication sheet does not carry '"
          + rel.publishedText + "'; the FEC release was replaced and must be reviewed");
    }
    Map<String, String> labels = readLabels(rel, labelRows);
    if (house.isEmpty()) {
      throw new IOException(TABLE + " " + rel.year + ": empty house sheet");
    }
    String[] head = house.get(0);
    if (head.length < rel.header.length) {
      throw new IOException(TABLE + " " + rel.year + ": house header has " + head.length
          + " columns, expected " + rel.header.length);
    }
    for (int i = 0; i < rel.header.length; i++) {
      if (!norm(head[i]).equals(rel.header[i])) {
        throw new IOException(TABLE + " " + rel.year + ": house header column " + i + " is '"
            + norm(head[i]) + "', expected '" + rel.header[i] + "'");
      }
    }
    for (int i = rel.header.length; i < head.length; i++) {
      if (!clean(head[i]).isEmpty()) {
        throw new IOException(TABLE + " " + rel.year + ": unexpected extra header column " + i
            + " '" + clean(head[i]) + "'");
      }
    }
    boolean hasRcv = rel.header.length > C_RCV[0];

    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    Map<String, Integer> lineNo = new HashMap<String, Integer>();
    Set<String> done = new HashSet<String>();
    Block block = null;
    for (int i = 1; i < house.size(); i++) {
      String[] r = house.get(i);
      int line = i + 1;
      boolean blank = true;
      for (int c = 3; c < r.length; c++) {
        // A spacer row carries at most the state name and an 'n/a' placeholder in the FEC id
        // column.
        if (!clean(r[c]).isEmpty() && !(c == C_FEC_ID && clean(r[c]).equals("n/a"))) {
          blank = false;
        }
      }
      if (blank) {
        continue;
      }
      String rowNo = g(r, 0);
      if (!rowNo.matches("\\d+")) {
        throw fail(rel, line, "first column '" + rowNo + "' is not a row number");
      }
      String abbr = g(r, C_ABBR);
      String stateName = g(r, C_STATE);
      String[] state = null;
      boolean delegate = false;
      if (!abbr.isEmpty()) {
        if (DELEGATE_JURISDICTIONS.contains(abbr)) {
          delegate = true;
        } else {
          state = BY_POSTAL.get(abbr);
          if (state == null) {
            throw fail(rel, line, "unknown state abbreviation '" + abbr + "'");
          }
          if (!stateName.isEmpty() && !state[2].equalsIgnoreCase(stateName)) {
            throw fail(rel, line, "state '" + stateName + "' does not match " + abbr);
          }
        }
      } else {
        state = BY_NAME.get(stateName.toLowerCase(Locale.ROOT));
        if (state == null) {
          throw fail(rel, line, "state abbreviation is blank and state '" + stateName
              + "' is unknown");
        }
      }
      if (delegate) {
        continue;
      }
      String district = g(r, C_DISTRICT);
      String fecId = g(r, C_FEC_ID);
      String total = g(r, C_TOTAL);

      if (district.isEmpty()) {
        // A 2022 heading row ("DISTRICT 3", "AT LARGE DISTRICT"): no data.
        if (total.equals("Total State Votes:")) {
          continue;
        }
        if ((fecId.equals("n/a") || fecId.isEmpty()) && DISTRICT_HEADING.matcher(norm(g(r, C_NAME))).matches()) {
          continue;
        }
        throw fail(rel, line, "sheet row " + rowNo + " has no district and is not a heading row: "
            + Arrays.toString(r));
      }
      if (district.equals("H")) {
        if (!total.equals("Total State Votes:")) {
          throw fail(rel, line, "state row '" + total + "' is not 'Total State Votes:'");
        }
        continue;
      }
      boolean candidateCellsBlank = true;
      for (int c = 4; c < r.length; c++) {
        if (!clean(r[c]).isEmpty()) {
          candidateCellsBlank = false;
        }
      }
      if (candidateCellsBlank) {
        continue;
      }
      String key = state[0] + "|" + district;
      if (!total.isEmpty()) {
        if (total.equals("Party Votes:") || total.equals("Party Caucus Votes:")) {
          continue;
        }
        if (total.equals("District Votes:") || total.equals("Full Term District Votes:")
            || total.equals("Unexpired Term District Votes:")) {
          block = switchBlock(rel, block, key, state, district, line, labels, out, lineNo, done);
          if (block.totalSeen) {
            throw fail(rel, line, "second district-votes row for " + key);
          }
          block.totalSeen = true;
          block.totalRow = r;
          continue;
        }
        throw fail(rel, line, "unknown subtotal label '" + total + "'");
      }
      String party = g(r, C_PARTY);
      if (party.equals("Combined Parties:")) {
        continue;
      }
      String generalCell = g(r, C_GENERAL);
      String runoffCell = g(r, C_RUNOFF);
      Long[] rcv = new Long[C_RCV.length];
      boolean anyRcv = false;
      if (hasRcv) {
        for (int k = 0; k < C_RCV.length; k++) {
          String v = g(r, C_RCV[k]);
          if (!v.isEmpty()) {
            rcv[k] = Long.valueOf(votes(rel, line, v, "ranked-choice round " + (k + 1)));
            anyRcv = true;
          }
        }
      }
      // '#' in the general column: the candidate was taken off the ballot (Montana 2020 Green).
      boolean generalNone = generalCell.isEmpty() || generalCell.equals("#");
      boolean unopposed = generalCell.equals("Unopposed");
      if (generalNone && runoffCell.isEmpty() && !anyRcv) {
        continue;
      }
      if (fecId.isEmpty()) {
        throw fail(rel, line, "candidate row without an FEC id or 'n/a'");
      }
      Long general = null;
      if (!generalNone && !unopposed) {
        general = Long.valueOf(votes(rel, line, generalCell, "general votes"));
      }
      Long runoff = runoffCell.isEmpty() ? null
          : Long.valueOf(votes(rel, line, runoffCell, "general runoff votes"));
      String first = g(r, C_FIRST);
      String last = g(r, C_LAST);
      if (last.isEmpty() && first.isEmpty()) {
        // South Dakota 2020 Libertarian: only the "Last, First" column is filled.
        String full = g(r, C_NAME);
        int comma = full.indexOf(", ");
        if (comma <= 0) {
          throw fail(rel, line, "candidate row without a first or last name");
        }
        last = full.substring(0, comma);
        first = full.substring(comma + 2);
      }
      if (last.isEmpty()) {
        throw fail(rel, line, "candidate row without a last name");
      }
      boolean aggregate = first.isEmpty()
          && AGGREGATE_LABELS.contains(last.toLowerCase(Locale.ROOT));
      String name = first.isEmpty() ? last : first + " " + last;
      // Primary-only rows are not used to find the district (the FEC sheet files a few under the
      // wrong district); rows with general results are.
      block = switchBlock(rel, block, key, state, district, line, labels, out, lineNo, done);
      if (block.totalSeen) {
        // Primary-only rows may follow the total (Texas 4 caucus); general results may not.
        throw fail(rel, line, "general result after the district total of " + key);
      }
      block.cands.add(new Cand(line, name, aggregate, party, !g(r, C_PRIMARY).isEmpty(), general,
          unopposed, runoff, rcv, g(r, C_GE_WINNER).startsWith("W")));
    }
    if (block != null) {
      finish(rel, block, labels, out, lineNo, done);
    }
    if (out.isEmpty()) {
      throw new IOException(TABLE + " " + rel.year + ": no rows");
    }
    return out;
  }

  private static Block switchBlock(Release rel, Block block, String key, String[] state,
      String district, int line, Map<String, String> labels, List<Map<String, Object>> out,
      Map<String, Integer> lineNo, Set<String> done) throws IOException {
    if (block != null && block.key.equals(key)) {
      return block;
    }
    if (block != null) {
      finish(rel, block, labels, out, lineNo, done);
    }
    return new Block(key, state, district, line);
  }

  private static long votes(Release rel, int line, String v, String what) throws IOException {
    if (!v.matches("\\d+")) {
      throw fail(rel, line, what + " '" + v + "' is not a whole number");
    }
    return Long.parseLong(v);
  }

  /** The workbook's own party-label table: code to name. */
  private static Map<String, String> readLabels(Release rel, List<String[]> rows)
      throws IOException {
    Map<String, String> labels = new HashMap<String, String>();
    for (String[] r : rows) {
      String code = clean(r.length > 0 ? r[0] : "");
      String name = clean(r.length > 2 ? r[2] : "");
      if (code.isEmpty() || name.isEmpty() || !code.matches("[A-Z0-9()]+")) {
        continue;
      }
      String prev = labels.put(code, name);
      if (prev != null && !prev.equals(name)) {
        throw new IOException(TABLE + " " + rel.year + ": party label " + code + " is both '"
            + prev + "' and '" + name + "'");
      }
    }
    if (!labels.containsKey("D") || !labels.containsKey("R") || !labels.containsKey("W")) {
      throw new IOException(TABLE + " " + rel.year + ": party label sheet lacks D, R or W");
    }
    return labels;
  }

  /** The party reading of one label: published text, D/R/OTHER (null for a write-in), write-in. */
  private static final class Party {
    String published;
    String major;
    boolean writeIn;
  }

  private static Party party(Release rel, Cand c, Map<String, String> labels)
      throws IOException {
    String code = c.partyCode;
    if (code.endsWith("*")) {
      code = code.substring(0, code.length() - 1).trim();
    }
    if (code.isEmpty()) {
      throw fail(rel, c.line, "candidate with votes but no party label");
    }
    List<String> plain = new ArrayList<String>();
    boolean writeInToken = false;
    for (String t : code.split("/")) {
      t = t.trim();
      if (rel.aliases.containsKey(t)) {
        t = rel.aliases.get(t);
      }
      if (t.equals("W")) {
        writeInToken = true;
        continue;
      }
      Matcher w = WRAPPED.matcher(t);
      if (w.matches() && w.group(1).equals("W")) {
        if (!labels.containsKey(w.group(2))) {
          throw fail(rel, c.line, "unknown party label in '" + t + "'");
        }
        writeInToken = true;
        continue;
      }
      if (w.matches() && w.group(1).equals("N") && !labels.containsKey(t)) {
        if (!labels.containsKey(w.group(2))) {
          throw fail(rel, c.line, "unknown party label in '" + t + "'");
        }
        continue;
      }
      if (!labels.containsKey(t)) {
        throw fail(rel, c.line, "party label '" + t + "' is not in the workbook's label table");
      }
      plain.add(t);
    }
    Party p = new Party();
    if (plain.isEmpty()) {
      if (!writeInToken) {
        throw fail(rel, c.line, "party label '" + c.partyCode + "' has no recognised component");
      }
      p.writeIn = true;
      return p;
    }
    StringBuilder sb = new StringBuilder();
    int d = 0;
    int rr = 0;
    for (String t : plain) {
      if (sb.length() > 0) {
        sb.append('/');
      }
      sb.append(labels.get(t).toLowerCase(Locale.ROOT));
      if (DEMOCRATIC.contains(t)) {
        d++;
      }
      if (REPUBLICAN.contains(t)) {
        rr++;
      }
    }
    if (d > 0 && rr > 0 || d > 1 || rr > 1) {
      throw fail(rel, c.line, "party label '" + c.partyCode + "' combines major parties");
    }
    p.published = sb.toString();
    if (d + rr == 0) {
      p.major = "OTHER";
    } else if (plain.size() == 1) {
      p.major = d == 1 ? "D" : "R";
    } else {
      // A combined label such as D/WF is the major party's nominee only when the candidate went
      // through that party's primary or convention; D/IP on a candidate with no primary entry
      // is an independent whom the source tags with the party, and stays OTHER.
      p.major = c.primaryShown ? (d == 1 ? "D" : "R") : "OTHER";
    }
    return p;
  }

  private static void finish(Release rel, Block b, Map<String, String> labels,
      List<Map<String, Object>> out, Map<String, Integer> lineNo, Set<String> done)
      throws IOException {
    if (!done.add(b.key + "|" + b.districtText)) {
      throw fail(rel, b.firstLine, "district " + b.key + " appears in two separate blocks");
    }
    if (b.cands.isEmpty()) {
      return;
    }
    Matcher dm = DISTRICT.matcher(b.districtText);
    if (!dm.matches()) {
      throw fail(rel, b.firstLine, "district '" + b.districtText + "' is not NN, NN-Full Term or "
          + "NN-Unexpired Term");
    }
    int district = Integer.parseInt(dm.group(1));
    boolean special = "-Unexpired Term".equals(dm.group(2));
    boolean unopposed = false;
    for (Cand c : b.cands) {
      unopposed |= c.unopposed;
    }
    // An unopposed race may have no district-votes row at all (Louisiana 4, 2022).
    if (!b.totalSeen && !unopposed) {
      throw fail(rel, b.firstLine, "district " + b.key + " has no district-votes row");
    }
    String[] totalRow = b.totalSeen ? b.totalRow : new String[0];
    String genTotalCell = g(totalRow, C_GENERAL);
    String runoffTotalCell = g(totalRow, C_RUNOFF);
    if (unopposed) {
      if (b.cands.size() != 1 || !genTotalCell.isEmpty() || b.cands.get(0).runoff != null) {
        throw fail(rel, b.firstLine, "district " + b.key + " mixes an Unopposed result with "
            + "other results");
      }
      Cand c = b.cands.get(0);
      if (!c.winnerFlag) {
        throw fail(rel, c.line, "unopposed candidate " + c.name + " is not flagged the winner");
      }
      emit(rel, b, district, special, false, c, 1L, 1L, true, false, null, labels, out, lineNo);
      return;
    }

    long genSum = 0;
    int genRows = 0;
    boolean anyRcv = false;
    for (Cand c : b.cands) {
      if (c.general != null) {
        genSum += c.general.longValue();
        genRows++;
      }
      for (Long v : c.rcv) {
        anyRcv |= v != null;
      }
    }
    if (genRows == 0) {
      throw fail(rel, b.firstLine, "district " + b.key + " has no general votes");
    }
    long genTotal = votes(rel, b.firstLine, genTotalCell, "district general total");
    if (genSum != genTotal) {
      throw fail(rel, b.firstLine, "district " + b.key + " general votes sum to " + genSum
          + " but the district total is " + genTotal);
    }

    // Candidate-level rows of the first (or only) round.
    List<Cand> decisive = new ArrayList<Cand>();
    Map<Cand, Long> shown = new LinkedHashMap<Cand, Long>();
    long roundTotal = genTotal;
    boolean ranked = false;
    if (anyRcv) {
      int fin = -1;
      Long finTotal = null;
      for (int k = 0; k < C_RCV.length; k++) {
        String t = g(totalRow, C_RCV[k]);
        if (!t.isEmpty()) {
          fin = k;
          finTotal = Long.valueOf(votes(rel, b.firstLine, t, "ranked-choice round total"));
        }
      }
      if (fin < 0) {
        throw fail(rel, b.firstLine, "district " + b.key + " has ranked-choice votes but no "
            + "round total");
      }
      long sum = 0;
      for (Cand c : b.cands) {
        boolean any = false;
        for (Long v : c.rcv) {
          any |= v != null;
        }
        if (!any) {
          // Write-ins and buckets the instant-runoff tabulation leaves out.
          if (c.general != null && !party(rel, c, labels).writeIn) {
            throw fail(rel, c.line, "candidate " + c.name + " has general votes but no "
                + "ranked-choice count");
          }
          continue;
        }
        if (c.rcv[fin] == null || c.general == null) {
          throw fail(rel, c.line, "candidate " + c.name + " lacks the final-round or first-choice "
              + "count of a ranked-choice contest");
        }
        sum += c.rcv[fin].longValue();
        shown.put(c, c.rcv[fin]);
      }
      if (sum != finTotal.longValue()) {
        throw fail(rel, b.firstLine, "district " + b.key + " final-round counts sum to " + sum
            + " but the round total is " + finTotal);
      }
      roundTotal = finTotal.longValue();
      ranked = true;
    } else {
      for (Cand c : b.cands) {
        if (c.general != null) {
          shown.put(c, c.general);
        }
      }
    }
    boolean hasRunoff = false;
    long runoffSum = 0;
    for (Cand c : b.cands) {
      if (c.runoff != null) {
        hasRunoff = true;
        runoffSum += c.runoff.longValue();
      }
    }
    if (hasRunoff) {
      long rt = votes(rel, b.firstLine, runoffTotalCell, "district runoff total");
      if (rt != runoffSum) {
        throw fail(rel, b.firstLine, "district " + b.key + " runoff votes sum to " + runoffSum
            + " but the runoff total is " + rt);
      }
      if (ranked) {
        throw fail(rel, b.firstLine, "district " + b.key + " has both ranked-choice and runoff");
      }
    } else if (!runoffTotalCell.isEmpty()) {
      throw fail(rel, b.firstLine, "district " + b.key + " has a runoff total but no runoff rows");
    }

    for (Map.Entry<Cand, Long> e : shown.entrySet()) {
      emit(rel, b, district, special, false, e.getKey(), e.getValue().longValue(), roundTotal,
          false, ranked, ranked ? e.getKey().general : null, labels, out, lineNo);
      decisive.add(e.getKey());
    }
    if (hasRunoff) {
      decisive.clear();
      for (Cand c : b.cands) {
        if (c.runoff != null) {
          emit(rel, b, district, special, true, c, c.runoff.longValue(), runoffSum, false, false,
              null, labels, out, lineNo);
          decisive.add(c);
        }
      }
    }
    checkWinner(rel, b, decisive, hasRunoff, ranked, shown);
  }

  /** The winner flags must name the leader of the deciding round, when the sheet flags one. */
  private static void checkWinner(Release rel, Block b, List<Cand> decisive, boolean runoff,
      boolean ranked, Map<Cand, Long> shown) throws IOException {
    Map<String, Long> byName = new LinkedHashMap<String, Long>();
    for (Cand c : decisive) {
      long v = runoff ? c.runoff.longValue() : shown.get(c).longValue();
      Long prev = byName.get(c.name);
      byName.put(c.name, Long.valueOf(v + (prev == null ? 0 : prev.longValue())));
    }
    String leader = null;
    long best = -1;
    boolean tie = false;
    for (Map.Entry<String, Long> e : byName.entrySet()) {
      if (e.getValue().longValue() > best) {
        best = e.getValue().longValue();
        leader = e.getKey();
        tie = false;
      } else if (e.getValue().longValue() == best) {
        tie = true;
      }
    }
    Set<String> flagged = new HashSet<String>();
    for (Cand c : b.cands) {
      if (c.winnerFlag) {
        flagged.add(c.name);
      }
    }
    if (flagged.isEmpty()) {
      // The 2022 Indiana 2 full-term block flags nobody (the nominee died before the election).
      return;
    }
    if (tie || flagged.size() != 1 || !flagged.contains(leader)) {
      throw fail(rel, b.firstLine, "district " + b.key + " winner flags " + flagged
          + " do not match the leader " + leader + (tie ? " (tied)" : ""));
    }
  }

  private static void emit(Release rel, Block b, int district, boolean special, boolean runoff,
      Cand c, long votes, long total, boolean uncontested, boolean ranked, Long firstChoice,
      Map<String, String> labels, List<Map<String, Object>> out, Map<String, Integer> lineNo)
      throws IOException {
    Party p = party(rel, c, labels);
    String candidate = c.aggregate ? null : c.name;
    if (c.aggregate && !p.writeIn) {
      throw fail(rel, c.line, "bucket '" + c.name + "' is not a write-in line");
    }
    String stateFips = b.state[1];
    String round = rel.year + "|" + stateFips + "|" + district + "|" + special + "|" + runoff;
    Integer prev = lineNo.get(round);
    int next = prev == null ? 1 : prev.intValue() + 1;
    lineNo.put(round, Integer.valueOf(next));
    Map<String, Object> m = new LinkedHashMap<String, Object>();
    m.put("year", Integer.valueOf(rel.year));
    m.put("state_fips", stateFips);
    m.put("state_name", b.state[2]);
    m.put("state_postal", b.state[0]);
    m.put("district", Integer.valueOf(district));
    m.put("at_large", Boolean.valueOf(district == 0));
    m.put("cd_fips", stateFips + String.format(Locale.ROOT, "%02d", Integer.valueOf(district)));
    m.put("special", Boolean.valueOf(special));
    m.put("runoff", Boolean.valueOf(runoff));
    m.put("stage", "gen");
    m.put("line_no", Integer.valueOf(next));
    m.put("candidate", candidate);
    m.put("candidate_kind",
        ElectionResultsSupport.candidateKind(candidate, p.writeIn, TABLE, c.line));
    m.put("party_published", p.published);
    m.put("party_major", p.major);
    m.put("writein", Boolean.valueOf(p.writeIn));
    m.put("candidate_votes", Long.valueOf(votes));
    m.put("total_votes", Long.valueOf(total));
    m.put("uncontested", Boolean.valueOf(uncontested));
    m.put("unofficial", Boolean.FALSE);
    m.put("version_date", null);
    m.put("source_release", rel.description);
    m.put("source", SOURCE);
    m.put("ranked_choice", ranked ? Boolean.TRUE : Boolean.FALSE);
    m.put("first_choice_votes", firstChoice);
    out.add(m);
  }
}
