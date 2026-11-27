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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.StreamingResponseTransformer;
import org.apache.calcite.adapter.govdata.HeadedPageSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * FHWA apportionments by state, fiscal year and program, read from the FHWA notices on
 * highways.dot.gov (which answers HTTP 403 to non-browser clients, so pages are fetched through a
 * headed browser session). One row per state x fiscal year x program.
 *
 * <p>Two notice kinds are read, discovered from the notice index by title:
 * <ul>
 *   <li>"Apportionment of Federal-Aid Highway Program Funds for Fiscal Year YYYY" — Table 1 with one
 *       column per program (NHPP, STBG, HSIP, RHCP, CMAQ, MPP, NHFP, CRP, PROTECT);</li>
 *   <li>"Apportionment of Fiscal Year YYYY Highway Infrastructure Program Funds for the Bridge
 *       Formula Program" — Bridge (Main) and Off-System Bridges columns.</li>
 * </ul>
 * When a fiscal year has several notices of one kind (a revision) the highest notice number wins.
 * Fiscal years before 2022 are not read: the Bridge Formula Program starts in FY2022 and the main
 * table's program set is the one introduced by the Infrastructure Investment and Jobs Act. Every
 * state row's programs must add up to the notice's own total or the run fails; so must a page that
 * yields no rows or a header that names an unknown program.
 */
public class FhwaApportionmentsTransformer implements StreamingResponseTransformer {

  static final String INDEX_URL = "/laws-regulations/directives/notices";
  static final String NOTICE_URL = "/laws-regulations/directives/notices/n-";
  static final int FIRST_FISCAL_YEAR = 2022;

  private static final Pattern INDEX_ROW = Pattern.compile(
      "<tr[^>]*>(.*?)</tr>", Pattern.DOTALL);
  private static final Pattern NOTICE_NO = Pattern.compile("N\\s?4510\\.(\\d{3})");
  private static final Pattern MAIN_TITLE = Pattern.compile(
      "^(?:Revised )?Apportionment of Federal-Aid Highway Program Funds for Fiscal Year"
          + " (?:\\(FY\\) )?(\\d{4})$", Pattern.CASE_INSENSITIVE);
  private static final Pattern BFP_TITLE = Pattern.compile(
      "^(?:Revised )?Apportionment of Fiscal Year (?:\\(FY\\) )?(\\d{4}) Highway Infrastructure Program"
          + " Funds for the Bridge Formula Program\\b.*", Pattern.CASE_INSENSITIVE);
  private static final Pattern TABLE = Pattern.compile("<table.*?</table>", Pattern.DOTALL);
  private static final Pattern TR = Pattern.compile("<tr.*?</tr>", Pattern.DOTALL);
  private static final Pattern CELL = Pattern.compile("<t[hd][^>]*>(.*?)</t[hd]>", Pattern.DOTALL);

  private static final Map<String, String> STATE_ABBR = buildStateAbbrMap();

  @Override public String transform(String response, RequestContext context) {
    throw new UnsupportedOperationException(
        "fhwa_program_apportionments is a StreamingResponseTransformer; call fetchAndTransform");
  }

  @Override public Iterator<Map<String, Object>> fetchAndTransform(RequestContext context) {
    HeadedPageSession session = HeadedPageSession.get(context);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    try {
      String index = session.fetchText(INDEX_URL);
      if (index == null) {
        throw new IllegalStateException("fhwa_program_apportionments: " + INDEX_URL
            + " answered 404");
      }
      Map<Integer, Integer> mainNotices = new TreeMap<Integer, Integer>();
      Map<Integer, Integer> bfpNotices = new TreeMap<Integer, Integer>();
      discoverNotices(index, mainNotices, bfpNotices);
      for (Map.Entry<Integer, Integer> e : mainNotices.entrySet()) {
        rows.addAll(parseMain(e.getKey(), e.getValue(), fetchNotice(session, e.getValue())));
      }
      for (Map.Entry<Integer, Integer> e : bfpNotices.entrySet()) {
        rows.addAll(parseBfp(e.getKey(), e.getValue(), fetchNotice(session, e.getValue())));
      }
    } catch (IOException e) {
      throw new RuntimeException("fhwa_program_apportionments: cannot read FHWA notices", e);
    }
    if (rows.isEmpty()) {
      throw new IllegalStateException("fhwa_program_apportionments: no rows found");
    }
    return rows.iterator();
  }

  private static String fetchNotice(HeadedPageSession session, int notice) throws IOException {
    String url = NOTICE_URL + "4510" + notice;
    String html = session.fetchText(url);
    if (html == null) {
      throw new IllegalStateException("fhwa_program_apportionments: " + url + " answered 404");
    }
    return html;
  }

  /** Fills fiscal year -> highest notice suffix (the three digits after 4510.) for both kinds. */
  static void discoverNotices(String indexHtml, Map<Integer, Integer> main,
      Map<Integer, Integer> bfp) {
    Matcher rows = INDEX_ROW.matcher(indexHtml);
    while (rows.find()) {
      String text = text(rows.group(1));
      Matcher no = NOTICE_NO.matcher(text);
      if (!no.find()) {
        continue;
      }
      int notice = Integer.parseInt(no.group(1));
      String title = text.substring(no.end()).replaceFirst("^\\s*\\d{1,2}/\\d{1,2}/\\d{2,4}\\s*", "")
          .trim();
      Matcher m = MAIN_TITLE.matcher(title);
      Map<Integer, Integer> target = main;
      if (!m.matches()) {
        m = BFP_TITLE.matcher(title);
        target = bfp;
        if (!m.matches()) {
          continue;
        }
      }
      int fy = Integer.parseInt(m.group(1));
      if (fy < FIRST_FISCAL_YEAR) {
        continue;
      }
      Integer prior = target.get(fy);
      if (prior == null || prior.intValue() < notice) {
        target.put(fy, notice);
      }
    }
    if (main.isEmpty() || bfp.isEmpty()) {
      throw new IllegalStateException("fhwa_program_apportionments: notice index lists "
          + main.size() + " main and " + bfp.size() + " Bridge Formula Program notices from FY"
          + FIRST_FISCAL_YEAR + "; the index layout changed");
    }
  }

  /** Rows of Table 1: state x program, from the first table headed State / National Highway... */
  static List<Map<String, Object>> parseMain(int fy, int notice, String html) {
    String label = "N4510." + notice;
    List<String> programs = null;
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    Map<String, Boolean> seen = new HashMap<String, Boolean>();
    for (String table : tables(html)) {
      List<List<String>> trs = rowsOf(table);
      if (trs.isEmpty() || !"State".equals(trs.get(0).get(0)) || trs.get(0).size() < 3
          || !trs.get(0).get(1).startsWith("National Highway Perfo")) {
        continue;
      }
      if (programs != null) {
        break;
      }
      programs = new ArrayList<String>();
      List<String> header = trs.get(0);
      for (int c = 1; c < header.size() - 1; c++) {
        programs.add(mainProgram(header.get(c), label));
      }
      if (!header.get(header.size() - 1).startsWith("Apportioned Total")) {
        throw new IllegalStateException("fhwa_program_apportionments: " + label
            + " Table 1 last column is '" + header.get(header.size() - 1)
            + "', expected 'Apportioned Total'");
      }
      for (List<String> tr : trs) {
        String abbr = STATE_ABBR.get(tr.get(0));
        if (abbr == null) {
          continue;
        }
        if (tr.size() != header.size()) {
          throw new IllegalStateException("fhwa_program_apportionments: " + label + " "
              + tr.get(0) + " has " + tr.size() + " cells, header has " + header.size());
        }
        if (seen.put(abbr, Boolean.TRUE) != null) {
          throw new IllegalStateException("fhwa_program_apportionments: " + label
              + " lists " + tr.get(0) + " twice");
        }
        long sum = 0;
        for (int c = 1; c < tr.size() - 1; c++) {
          long amount = dollars(tr.get(c), label, tr.get(0));
          sum += amount;
          out.add(row(fy, programs.get(c - 1), programName(programs.get(c - 1)), abbr, tr.get(0),
              amount, label));
        }
        long total = dollars(tr.get(tr.size() - 1), label, tr.get(0));
        if (sum != total) {
          throw new IllegalStateException("fhwa_program_apportionments: " + label + " "
              + tr.get(0) + " programs sum to " + sum + " but the notice total is " + total);
        }
      }
    }
    if (out.isEmpty()) {
      throw new IllegalStateException("fhwa_program_apportionments: no state rows in " + label
          + " Table 1; the page layout changed");
    }
    return out;
  }

  /** Rows of the Bridge Formula Program state table: Bridge (Main), Off-System, Total. */
  static List<Map<String, Object>> parseBfp(int fy, int notice, String html) {
    String label = "N4510." + notice;
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    Map<String, Boolean> seen = new HashMap<String, Boolean>();
    boolean found = false;
    for (String table : tables(html)) {
      List<List<String>> trs = rowsOf(table);
      if (!tableMentions(trs, "Bridge Formula Program") || trs.size() < 20) {
        continue;
      }
      found = true;
      for (List<String> tr : trs) {
        String abbr = STATE_ABBR.get(tr.get(0));
        if (abbr == null) {
          continue;
        }
        if (tr.size() != 4) {
          throw new IllegalStateException("fhwa_program_apportionments: " + label + " "
              + tr.get(0) + " has " + tr.size() + " cells, expected 4");
        }
        if (seen.put(abbr, Boolean.TRUE) != null) {
          throw new IllegalStateException("fhwa_program_apportionments: " + label
              + " lists " + tr.get(0) + " twice");
        }
        long main = dollars(tr.get(1), label, tr.get(0));
        long off = dollars(tr.get(2), label, tr.get(0));
        long total = dollars(tr.get(3), label, tr.get(0));
        if (main + off != total) {
          throw new IllegalStateException("fhwa_program_apportionments: " + label + " "
              + tr.get(0) + " Bridge (Main) + Off-System is " + (main + off)
              + " but the notice total is " + total);
        }
        out.add(row(fy, "BFP_MAIN", programName("BFP_MAIN"), abbr, tr.get(0), main, label));
        out.add(row(fy, "BFP_OFF_SYSTEM", programName("BFP_OFF_SYSTEM"), abbr, tr.get(0), off,
            label));
      }
      break;
    }
    if (!found || out.isEmpty()) {
      throw new IllegalStateException("fhwa_program_apportionments: no Bridge Formula Program"
          + " state rows in " + label + "; the page layout changed");
    }
    return out;
  }

  private static boolean tableMentions(List<List<String>> trs, String needle) {
    for (int r = 0; r < Math.min(2, trs.size()); r++) {
      for (String c : trs.get(r)) {
        if (c.contains(needle)) {
          return true;
        }
      }
    }
    return false;
  }

  static String mainProgram(String header, String label) {
    String h = header.toLowerCase(java.util.Locale.ROOT);
    if (h.startsWith("national highway perfo")) {
      return "NHPP";
    } else if (h.startsWith("surface transportation")) {
      return "STBG";
    } else if (h.startsWith("highway safety improve")) {
      return "HSIP";
    } else if (h.startsWith("railway")) {
      return "RHCP";
    } else if (h.contains("mitigation") || h.contains("cmaq")) {
      return "CMAQ";
    } else if (h.startsWith("metropolitan planning")) {
      return "MPP";
    } else if (h.startsWith("national highway freight")) {
      return "NHFP";
    } else if (h.startsWith("carbon reduction")) {
      return "CRP";
    } else if (h.startsWith("protect")) {
      return "PROTECT";
    }
    throw new IllegalStateException("fhwa_program_apportionments: " + label
        + " Table 1 has an unknown program column '" + header + "'");
  }

  static String programName(String code) {
    switch (code) {
    case "NHPP": return "National Highway Performance Program";
    case "STBG": return "Surface Transportation Block Grant Program";
    case "HSIP": return "Highway Safety Improvement Program";
    case "RHCP": return "Railway-Highway Crossings Program";
    case "CMAQ": return "Congestion Mitigation and Air Quality Improvement Program";
    case "MPP": return "Metropolitan Planning Program";
    case "NHFP": return "National Highway Freight Program";
    case "CRP": return "Carbon Reduction Program";
    case "PROTECT": return "PROTECT Formula Program";
    case "BFP_MAIN": return "Bridge Formula Program - Bridge (Main)";
    case "BFP_OFF_SYSTEM": return "Bridge Formula Program - Off-System Bridges set-aside";
    default: throw new IllegalStateException("fhwa_program_apportionments: no name for " + code);
    }
  }

  private static Map<String, Object> row(int fy, String program, String programName, String abbr,
      String stateName, long amount, String notice) {
    Map<String, Object> r = new LinkedHashMap<String, Object>();
    r.put("fiscal_year", fy);
    r.put("program", program);
    r.put("program_name", programName);
    r.put("state_abbr", abbr);
    r.put("state_name", stateName);
    r.put("amount", Double.valueOf(amount));
    r.put("notice_number", notice);
    return r;
  }

  private static long dollars(String cell, String label, String state) {
    String digits = cell.replace(",", "").replace("$", "").trim();
    try {
      return Long.parseLong(digits);
    } catch (NumberFormatException e) {
      throw new IllegalStateException("fhwa_program_apportionments: " + label + " " + state
          + " amount '" + cell + "' is not a whole-dollar number", e);
    }
  }

  private static List<String> tables(String html) {
    List<String> out = new ArrayList<String>();
    Matcher m = TABLE.matcher(html);
    while (m.find()) {
      out.add(m.group());
    }
    return out;
  }

  private static List<List<String>> rowsOf(String table) {
    List<List<String>> out = new ArrayList<List<String>>();
    Matcher tr = TR.matcher(table);
    while (tr.find()) {
      List<String> cells = new ArrayList<String>();
      Matcher c = CELL.matcher(tr.group());
      while (c.find()) {
        cells.add(text(c.group(1)));
      }
      if (!cells.isEmpty()) {
        out.add(cells);
      }
    }
    return out;
  }

  private static String text(String html) {
    return html.replaceAll("<[^>]+>", " ").replace("&nbsp;", " ").replace("&amp;", "&")
        .replace("&#39;", "'").replaceAll("\\s+", " ").trim();
  }

  /** The postal abbreviation for a state, DC or Puerto Rico name, or null when unknown. */
  static String stateAbbr(String name) {
    return STATE_ABBR.get(name);
  }

  private static Map<String, String> buildStateAbbrMap() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("Alabama", "AL");
    m.put("Alaska", "AK");
    m.put("Arizona", "AZ");
    m.put("Arkansas", "AR");
    m.put("California", "CA");
    m.put("Colorado", "CO");
    m.put("Connecticut", "CT");
    m.put("Delaware", "DE");
    m.put("District of Columbia", "DC");
    m.put("Dist. of Col.", "DC");
    m.put("Florida", "FL");
    m.put("Georgia", "GA");
    m.put("Hawaii", "HI");
    m.put("Idaho", "ID");
    m.put("Illinois", "IL");
    m.put("Indiana", "IN");
    m.put("Iowa", "IA");
    m.put("Kansas", "KS");
    m.put("Kentucky", "KY");
    m.put("Louisiana", "LA");
    m.put("Maine", "ME");
    m.put("Maryland", "MD");
    m.put("Massachusetts", "MA");
    m.put("Michigan", "MI");
    m.put("Minnesota", "MN");
    m.put("Mississippi", "MS");
    m.put("Missouri", "MO");
    m.put("Montana", "MT");
    m.put("Nebraska", "NE");
    m.put("Nevada", "NV");
    m.put("New Hampshire", "NH");
    m.put("New Jersey", "NJ");
    m.put("New Mexico", "NM");
    m.put("New York", "NY");
    m.put("North Carolina", "NC");
    m.put("North Dakota", "ND");
    m.put("Ohio", "OH");
    m.put("Oklahoma", "OK");
    m.put("Oregon", "OR");
    m.put("Pennsylvania", "PA");
    m.put("Rhode Island", "RI");
    m.put("South Carolina", "SC");
    m.put("South Dakota", "SD");
    m.put("Tennessee", "TN");
    m.put("Texas", "TX");
    m.put("Utah", "UT");
    m.put("Vermont", "VT");
    m.put("Virginia", "VA");
    m.put("Washington", "WA");
    m.put("West Virginia", "WV");
    m.put("Wisconsin", "WI");
    m.put("Wyoming", "WY");
    m.put("Puerto Rico", "PR");
    return m;
  }
}
