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

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads the single jurisdiction-by-column data table on a BLS Census of Fatal Occupational
 * Injuries (CFOI) state page: one {@code <table>} whose first header cell labels the row
 * ("State") and whose body rows are a jurisdiction label followed by one rate cell per column.
 *
 * <p>Both CFOI state pages this schema ingests share that shape. The published label set is the
 * 50 states, the District of Columbia, and "New York City", which BLS indents beneath New York
 * (its rate is a subset of the New York row, not an additional jurisdiction).
 */
final class CfoiHtmlTable {

  /** Cell text BLS prints where a rate did not meet publication criteria or had no data. */
  static final String NOT_PUBLISHED = "-";

  static final String TYPE_STATE = "STATE";
  static final String TYPE_DISTRICT = "DISTRICT";
  static final String TYPE_CITY = "CITY";

  private static final String NEW_YORK_CITY = "New York City";
  private static final String DISTRICT_OF_COLUMBIA = "District of Columbia";
  private static final Map<String, String> STATE_NAME_TO_FIPS = buildStateFipsMap();

  /** Column header texts, first entry being the row-label header. */
  final List<String> headers;

  /** Body rows in page order. */
  final List<Row> rows;

  private CfoiHtmlTable(List<String> headers, List<Row> rows) {
    this.headers = headers;
    this.rows = rows;
  }

  /** One jurisdiction row: its label and the text of each rate cell, in header order. */
  static final class Row {
    final String label;
    final List<String> cells;

    Row(String label, List<String> cells) {
      this.label = label;
      this.cells = cells;
    }
  }

  /**
   * Parses the page's only data table.
   *
   * @throws IllegalStateException when the page does not hold exactly one table with a header
   *     and body, or a body row's cell count differs from the header's
   */
  static CfoiHtmlTable parse(String html, String source) {
    Document doc = Jsoup.parse(html);
    Elements tables = doc.select("table");
    if (tables.size() != 1) {
      throw new IllegalStateException("CFOI " + source + ": expected exactly one table, found "
          + tables.size() + " (a moved page answers with a non-data page)");
    }
    Element table = tables.first();
    Element head = table.selectFirst("thead tr");
    Element body = table.selectFirst("tbody");
    if (head == null || body == null) {
      throw new IllegalStateException("CFOI " + source + ": table has no thead/tbody");
    }
    List<String> headers = new ArrayList<String>();
    for (Element th : head.select("th")) {
      headers.add(th.text().trim());
    }
    List<Row> rows = new ArrayList<Row>();
    for (Element tr : body.select("tr")) {
      Element label = tr.selectFirst("th");
      if (label == null) {
        throw new IllegalStateException("CFOI " + source + ": body row without a label cell");
      }
      List<String> cells = new ArrayList<String>();
      for (Element td : tr.select("td")) {
        cells.add(td.text().trim());
      }
      if (cells.size() != headers.size() - 1) {
        throw new IllegalStateException("CFOI " + source + ": row '" + label.text().trim()
            + "' has " + cells.size() + " cells for " + (headers.size() - 1) + " columns");
      }
      rows.add(new Row(label.text().trim(), cells));
    }
    if (rows.isEmpty()) {
      throw new IllegalStateException("CFOI " + source + ": table body has no rows");
    }
    return new CfoiHtmlTable(headers, rows);
  }

  /** STATE, DISTRICT or CITY for a row label; throws for a label BLS has not published before. */
  static String jurisdictionType(String label) {
    if (NEW_YORK_CITY.equals(label)) {
      return TYPE_CITY;
    }
    if (DISTRICT_OF_COLUMBIA.equals(label)) {
      return TYPE_DISTRICT;
    }
    if (STATE_NAME_TO_FIPS.containsKey(label)) {
      return TYPE_STATE;
    }
    throw new IllegalStateException("CFOI: unrecognized jurisdiction label '" + label + "'");
  }

  /** 2-digit state FIPS, or null for New York City, which is a subset of the New York row. */
  static String stateFips(String label) {
    return NEW_YORK_CITY.equals(label) ? null : STATE_NAME_TO_FIPS.get(label);
  }

  /** The numeric rate in a cell, or null where BLS prints {@link #NOT_PUBLISHED}. */
  static Double parseRate(String cell, String context) {
    if (NOT_PUBLISHED.equals(cell)) {
      return null;
    }
    try {
      return Double.valueOf(cell);
    } catch (NumberFormatException e) {
      throw new IllegalStateException("CFOI " + context + ": unparseable rate cell '" + cell + "'",
          e);
    }
  }

  private static Map<String, String> buildStateFipsMap() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("Alabama", "01"); m.put("Alaska", "02"); m.put("Arizona", "04");
    m.put("Arkansas", "05"); m.put("California", "06"); m.put("Colorado", "08");
    m.put("Connecticut", "09"); m.put("Delaware", "10"); m.put(DISTRICT_OF_COLUMBIA, "11");
    m.put("Florida", "12"); m.put("Georgia", "13"); m.put("Hawaii", "15");
    m.put("Idaho", "16"); m.put("Illinois", "17"); m.put("Indiana", "18");
    m.put("Iowa", "19"); m.put("Kansas", "20"); m.put("Kentucky", "21");
    m.put("Louisiana", "22"); m.put("Maine", "23"); m.put("Maryland", "24");
    m.put("Massachusetts", "25"); m.put("Michigan", "26"); m.put("Minnesota", "27");
    m.put("Mississippi", "28"); m.put("Missouri", "29"); m.put("Montana", "30");
    m.put("Nebraska", "31"); m.put("Nevada", "32"); m.put("New Hampshire", "33");
    m.put("New Jersey", "34"); m.put("New Mexico", "35"); m.put("New York", "36");
    m.put("North Carolina", "37"); m.put("North Dakota", "38"); m.put("Ohio", "39");
    m.put("Oklahoma", "40"); m.put("Oregon", "41"); m.put("Pennsylvania", "42");
    m.put("Rhode Island", "44"); m.put("South Carolina", "45"); m.put("South Dakota", "46");
    m.put("Tennessee", "47"); m.put("Texas", "48"); m.put("Utah", "49");
    m.put("Vermont", "50"); m.put("Virginia", "51"); m.put("Washington", "53");
    m.put("West Virginia", "54"); m.put("Wisconsin", "55"); m.put("Wyoming", "56");
    return m;
  }
}
