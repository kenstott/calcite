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
package org.apache.calcite.adapter.govdata.crime;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * EOIR asylum decisions by nationality, one row per completed fiscal year and nationality.
 *
 * <p>Two documents are read. The configured source URL is EOIR's compilation of superseded
 * Adjudication Statistics pages, which holds the "Asylum Decision Rates by Nationality" tables
 * of earlier fiscal years; {@link #FY2024_URL} is the "Asylum Decisions by Nationality" table of
 * FY2024, which uses a different column layout. A page is loaded only when its data was generated
 * after its fiscal year ended, so quarterly part-year editions are never used; where several
 * editions of one year qualify, the latest generated wins. The credible-fear-origin tables are a
 * subset of these decisions and are not read.
 */
public class EoirAsylumNationalityTransformer extends EoirPdfTransformer {

  static final String FY2024_URL = "https://www.justice.gov/eoir/media/1344846/dl";

  private static final Pattern TITLE = Pattern.compile(
      "ADJUDICATION STATISTICS\\s+Asylum (?:Decision Rates|Decisions) by Nationality(?!\\s+in\\s)");
  private static final Pattern FISCAL_YEAR =
      Pattern.compile("Asylum rates for Fiscal Year (\\d{4})");
  private static final Pattern GENERATED =
      Pattern.compile("Data generated on ([A-Z][a-z]+) (\\d{1,2}), (\\d{4})");
  private static final DateTimeFormatter GENERATED_FORMAT =
      DateTimeFormatter.ofPattern("MMMM d yyyy", Locale.ENGLISH);
  private static final String VALUE = "(?:\\*|\\d[\\d,]*(?:\\.\\d+)?%?)";
  private static final Pattern DATA_LINE =
      Pattern.compile("^(.*?)\\s*((?:(?<=^|\\s)" + VALUE + "(?=\\s|$)\\s*){6,})$");
  private static final Pattern FOOTNOTE_START = Pattern.compile("^1 Asylum rates");

  /** The three printed column layouts; each constant lists the column of each printed value. */
  private enum Layout {
    /** FY2018: counts and rates, with administrative closures. */
    RATES_WITH_CLOSURE(false, "grants", null, "denials", null, "other_outcomes", null,
        "admin_closure", null, "total_decisions"),
    /** FY2019 to FY2023: counts and rates. */
    RATES(true, "grants", null, "denials", null, "other_outcomes", null, "total_decisions"),
    /** FY2024: counts of each outcome. */
    OUTCOMES(true, "grants", "denials", "abandonment", "not_adjudicated", "other", "withdrawn");

    /** Whether a wrapped nationality is vertically centred on its row (name above and below). */
    private final boolean centredNames;
    private final String[] columns;

    Layout(boolean centredNames, String... columns) {
      this.centredNames = centredNames;
      this.columns = columns;
    }
  }

  private static final String[] COUNT_COLUMNS = {"grants", "denials", "other_outcomes",
      "abandonment", "not_adjudicated", "other", "withdrawn", "admin_closure", "total_decisions"};

  @Override protected String rows(String response, String sourceUrl) throws IOException {
    List<String> pages = new ArrayList<String>(pageTexts(download(sourceUrl)));
    pages.addAll(pageTexts(download(FY2024_URL)));
    return parse(pages).toString();
  }

  /** Parses the pages of the source documents into rows. Package-visible for tests. */
  static ArrayNode parse(List<String> pages) {
    // fiscal year -> data-generated date -> nationality -> row
    Map<Integer, TreeMap<LocalDate, Map<String, ObjectNode>>> editions =
        new TreeMap<Integer, TreeMap<LocalDate, Map<String, ObjectNode>>>();
    for (String page : pages) {
      if (!TITLE.matcher(page).find()) {
        continue;
      }
      String flat = page.replaceAll("\\s+", " ");
      Matcher year = FISCAL_YEAR.matcher(flat);
      Matcher generated = GENERATED.matcher(flat);
      if (!year.find() || !generated.find()) {
        // Every table page repeats the footnote that states both.
        throw new IllegalStateException("EOIR nationality page without fiscal year or "
            + "generation date: " + flat.substring(0, Math.min(200, flat.length())));
      }
      int fiscalYear = Integer.parseInt(year.group(1));
      LocalDate date = LocalDate.parse(
          generated.group(1) + " " + generated.group(2) + " " + generated.group(3),
          GENERATED_FORMAT);
      if (date.isBefore(LocalDate.of(fiscalYear, 10, 1))) {
        continue;
      }
      TreeMap<LocalDate, Map<String, ObjectNode>> byDate = editions.get(fiscalYear);
      if (byDate == null) {
        byDate = new TreeMap<LocalDate, Map<String, ObjectNode>>();
        editions.put(fiscalYear, byDate);
      }
      Map<String, ObjectNode> rows = byDate.get(date);
      if (rows == null) {
        rows = new TreeMap<String, ObjectNode>();
        byDate.put(date, rows);
      }
      parsePage(page, fiscalYear, date, rows);
    }
    if (editions.isEmpty()) {
      throw new IllegalStateException("EOIR: no completed-year nationality tables found");
    }
    ArrayNode out = MAPPER.createArrayNode();
    for (TreeMap<LocalDate, Map<String, ObjectNode>> byDate : editions.values()) {
      for (ObjectNode row : byDate.lastEntry().getValue().values()) {
        out.add(row);
      }
    }
    return out;
  }

  private static void parsePage(String page, int fiscalYear, LocalDate generated,
      Map<String, ObjectNode> rows) {
    Layout layout;
    if (page.contains("Abandonment")) {
      layout = Layout.OUTCOMES;
    } else if (page.contains("Closure")
        && page.substring(0, page.indexOf("Nationality Grants")).contains("Admin")) {
      layout = Layout.RATES_WITH_CLOSURE;
    } else {
      layout = Layout.RATES;
    }
    boolean inTable = false;
    StringBuilder prefix = new StringBuilder();
    // A centred wrapped name whose lower part is still to be read.
    String openName = null;
    String[] openValues = null;
    for (String raw : page.split("\n")) {
      String line = raw.trim();
      if (!inTable) {
        // The column header is the line that names both the row label and the first count.
        inTable = line.contains("Nationality") && line.contains("Grants")
            && !line.contains("by Nationality");
        continue;
      }
      if (FOOTNOTE_START.matcher(line).find()) {
        break;
      }
      if (line.isEmpty() || "Rate Closure4".equals(line) || "Rate".equals(line)
          || "Adjudicated3".equals(line)) {
        continue;
      }
      Matcher data = DATA_LINE.matcher(line);
      if (data.matches()) {
        String[] values = data.group(2).trim().split("\\s+");
        if (values.length != layout.columns.length) {
          throw new IllegalStateException("EOIR nationality FY" + fiscalYear + ": expected "
              + layout.columns.length + " values: " + line);
        }
        if (openName != null) {
          throw new IllegalStateException("EOIR nationality FY" + fiscalYear
              + ": wrapped name not closed before: " + line);
        }
        boolean wrapped = prefix.length() > 0;
        String name = join(prefix.toString(), data.group(1));
        prefix.setLength(0);
        if (wrapped && layout.centredNames) {
          openName = name;
          openValues = values;
        } else {
          add(rows, fiscalYear, generated, name, layout, values);
        }
      } else if (openName != null) {
        add(rows, fiscalYear, generated, join(openName, line), layout, openValues);
        openName = null;
      } else {
        String joined = join(prefix.toString(), line);
        prefix.setLength(0);
        prefix.append(joined);
      }
    }
    if (!inTable || openName != null || prefix.length() > 0) {
      throw new IllegalStateException("EOIR nationality FY" + fiscalYear + ": table not fully "
          + "parsed (header found=" + inTable + ", leftover='" + prefix + "', open=" + openName + ")");
    }
  }

  /** Joins two lines of a wrapped name; a trailing hyphen continues the word. */
  private static String join(String head, String tail) {
    if (head.isEmpty()) {
      return tail;
    }
    if (tail.isEmpty()) {
      return head;
    }
    return head.endsWith("-") && !head.endsWith(" -") ? head + tail : head + " " + tail;
  }

  private static void add(Map<String, ObjectNode> rows, int fiscalYear, LocalDate generated,
      String printedName, Layout layout, String[] values) {
    // Strip the footnote marker printed against some names ("FORMER COUNTRIES4").
    // Some editions print "Former Countries" in mixed case; every other name is upper case.
    String name = printedName.replaceAll("\\d+$", "").trim().toUpperCase(Locale.ROOT);
    if (name.isEmpty()) {
      throw new IllegalStateException("EOIR nationality FY" + fiscalYear + ": row with no name");
    }
    ObjectNode row = MAPPER.createObjectNode();
    row.put("fiscal_year", fiscalYear);
    row.put("nationality", name);
    for (String column : COUNT_COLUMNS) {
      row.putNull(column);
    }
    for (int i = 0; i < values.length; i++) {
      // "*" is EOIR's suppression marker: the count exists but is withheld, so it stays NULL.
      if (layout.columns[i] != null && !"*".equals(values[i])) {
        row.put(layout.columns[i], Long.parseLong(values[i].replace(",", "")));
      }
    }
    row.put("data_generated", generated.toString());
    if (rows.put(name, row) != null) {
      throw new IllegalStateException("EOIR nationality FY" + fiscalYear + ": '" + name
          + "' appears twice in the edition of " + generated);
    }
  }
}
