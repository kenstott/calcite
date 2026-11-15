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
package org.apache.calcite.adapter.govdata.health;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

/**
 * Unpivots CMS's NHE summary table (Table 1: aggregate and per-capita amounts, annual
 * percent change, percent distribution, and share of GDP, calendar years 1960 onward) into
 * one row per (measure, item, year).
 *
 * <p>The CSV stacks measure blocks; each block's first row names the measure in a
 * parenthetical, e.g. {@code "National Health Expenditures (Amount in Billions)"} or
 * {@code "(Per Capita Amount)"}, and the following indented rows are components of that
 * total. The final row is the single-line "as a Percent of Gross Domestic Product" series.
 * Footnote digits glued to labels ("U.S. Population1") are stripped.
 */
public class NheSummaryTransformer extends AbstractNheTransformer {

  @Override String zipKeyword() {
    return "nhe-summary-including-share-gdp";
  }

  @Override ArrayNode parse(List<List<String>> rows) {
    int headerIdx = -1;
    for (int i = 0; i < rows.size(); i++) {
      if (rows.get(i).get(0).trim().equals("Expenditure Amount")) {
        headerIdx = i;
        break;
      }
    }
    if (headerIdx < 0) {
      throw new IllegalStateException("NHE summary: 'Expenditure Amount' header row not found");
    }
    List<String> header = rows.get(headerIdx);
    int[] years = new int[header.size()];
    for (int c = 1; c < header.size(); c++) {
      years[c] = Integer.parseInt(header.get(c).trim());
    }

    ArrayNode out = MAPPER.createArrayNode();
    String measure = null;
    for (int i = headerIdx + 1; i < rows.size(); i++) {
      List<String> row = rows.get(i);
      String raw = row.get(0);
      String text = raw.trim();
      if (text.startsWith("NOTE:") || text.startsWith("SOURCE:") || isAllEmpty(row)) {
        continue;
      }
      if (indent(raw) == 0) {
        String[] parsed = splitMeasure(text);
        if (parsed != null) {
          measure = parsed[1];
          emit(out, years, row, measure, parsed[0], 0);
          continue;
        }
        if (text.startsWith("U.S. Population") || text.startsWith("Gross Domestic Product")) {
          if (measure == null) {
            throw new IllegalStateException("NHE summary: row before any measure: " + text);
          }
          emit(out, years, row, measure, stripFootnote(text), 0);
          continue;
        }
        throw new IllegalStateException("NHE summary: unrecognized unindented row: " + text);
      }
      if (measure == null) {
        throw new IllegalStateException("NHE summary: indented row before any measure: " + text);
      }
      emit(out, years, row, measure, stripFootnote(text), indent(raw) > 5 ? 2 : 1);
    }
    return out;
  }

  /**
   * Splits "Item (Measure label)" into {item, measure_code}; returns null when the text has
   * no recognized measure parenthetical.
   */
  private static String[] splitMeasure(String text) {
    String normalized = text.trim();
    int open = normalized.lastIndexOf('(');
    int close = normalized.lastIndexOf(')');
    if (normalized.startsWith("National Health Expenditures as a Percent of Gross")) {
      return new String[] {"National Health Expenditures", "pct_of_gdp"};
    }
    if (open < 0 || close < open) {
      return null;
    }
    String item = normalized.substring(0, open).trim();
    String label = normalized.substring(open + 1, close).trim();
    String code;
    switch (label) {
    case "Amount in Billions":
      code = "amount_billions";
      break;
    case "Annual Percent Change":
      code = "annual_pct_change";
      break;
    case "Percent Distribution":
      code = "pct_distribution";
      break;
    case "Per Capita Amount":
      code = "per_capita_amount";
      break;
    default:
      return null;
    }
    return new String[] {item, code};
  }

  /** Removes the trailing footnote digit CMS glues onto labels such as "U.S. Population1". */
  private static String stripFootnote(String label) {
    String t = label.trim();
    t = t.replaceAll("^(U\\.S\\. Population)\\d(.*)$", "$1$2");
    t = t.replaceAll("^(Gross Domestic Product)\\d(.*)$", "$1$2");
    return t.replaceAll("\\s+", " ").trim();
  }

  private static boolean isAllEmpty(List<String> row) {
    for (int c = 1; c < row.size(); c++) {
      if (!row.get(c).trim().isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private static void emit(ArrayNode out, int[] years, List<String> row, String measure,
      String item, int depth) {
    for (int c = 1; c < years.length && c < row.size(); c++) {
      Double v = number(row.get(c));
      ObjectNode o = MAPPER.createObjectNode();
      o.put("year", years[c]);
      o.put("measure", measure);
      o.put("item", item);
      o.put("depth", depth);
      if (v == null) {
        o.putNull("value");
      } else {
        o.put("value", v.doubleValue());
      }
      out.add(o);
    }
  }
}
