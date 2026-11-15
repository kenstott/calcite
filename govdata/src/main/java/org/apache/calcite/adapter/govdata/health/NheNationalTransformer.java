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

import java.util.ArrayList;
import java.util.List;

/**
 * Unpivots CMS's "National Health Expenditures by Type of Service and Source of Funds"
 * CSV (calendar years 1960 onward, $ millions) into one row per
 * (service category, payer path, year).
 *
 * <p>The CSV is a stack of blocks. Each block starts with an unindented row naming a
 * service category (Total National Health Expenditures, Health Consumption Expenditures,
 * Personal Health Care, Total Hospital Expenditures, ...), followed by indented rows for
 * the sources of funds; indentation encodes the hierarchy, and labels such as "Federal"
 * and "State and Local" repeat under several parents, so the full path is rebuilt from
 * indentation rather than keyed on the bare label. Footnote asterisks are stripped.
 */
public class NheNationalTransformer extends AbstractNheTransformer {

  /** payer_path/payer of a category's own total row (an empty string would be written as NULL). */
  static final String ALL_PAYERS = "All payers";
  private static final String SEPARATOR = " > ";

  @Override String zipKeyword() {
    return "type-service-source-funds";
  }

  @Override ArrayNode parse(List<List<String>> rows) {
    int headerIdx = -1;
    for (int i = 0; i < rows.size(); i++) {
      if (rows.get(i).get(0).trim().startsWith("Expenditure Amount")) {
        headerIdx = i;
        break;
      }
    }
    if (headerIdx < 0) {
      throw new IllegalStateException("NHE national: 'Expenditure Amount' header row not found");
    }
    List<String> header = rows.get(headerIdx);
    int[] years = new int[header.size()];
    for (int c = 1; c < header.size(); c++) {
      years[c] = Integer.parseInt(header.get(c).trim());
    }

    ArrayNode out = MAPPER.createArrayNode();
    String category = null;
    List<String> pathLabels = new ArrayList<>();
    List<Integer> pathIndents = new ArrayList<>();

    for (int i = headerIdx + 1; i < rows.size(); i++) {
      List<String> row = rows.get(i);
      String raw = row.get(0);
      String label = raw.trim().replace("*", "").trim();
      if (label.startsWith("NOTE:") || label.startsWith("SOURCE:") || isFootnote(raw, row)) {
        continue;
      }
      int indent = indent(raw);
      if (indent == 0) {
        if ("POPULATION".equals(label)) {
          // Population is in thousands, not $ millions; nhe_summary carries U.S. population.
          category = null;
          pathLabels.clear();
          pathIndents.clear();
          continue;
        }
        if (label.startsWith("Total CMS Programs")) {
          // Closing line of every block: a memo item for the block's category, not a new block.
          if (category == null) {
            throw new IllegalStateException("NHE national: CMS programs row before any category");
          }
          pathLabels.clear();
          pathIndents.clear();
          emit(out, years, row, category, label, label, 1);
          continue;
        }
        category = label;
        pathLabels.clear();
        pathIndents.clear();
        // The block's own total row carries the category's values, labelled {@link #ALL_PAYERS}.
        emit(out, years, row, category, ALL_PAYERS, ALL_PAYERS, 0);
        continue;
      }
      if (category == null) {
        throw new IllegalStateException("NHE national: indented row before any category: " + label);
      }
      while (!pathIndents.isEmpty() && pathIndents.get(pathIndents.size() - 1) >= indent) {
        pathIndents.remove(pathIndents.size() - 1);
        pathLabels.remove(pathLabels.size() - 1);
      }
      pathIndents.add(indent);
      pathLabels.add(label);
      emit(out, years, row, category, String.join(SEPARATOR, pathLabels), label, pathLabels.size());
    }
    return out;
  }

  /** Footnote lines are unindented text rows with no numeric cells. */
  private static boolean isFootnote(String raw, List<String> row) {
    if (indent(raw) != 0 || row.size() < 2) {
      return false;
    }
    for (int c = 1; c < row.size(); c++) {
      if (!row.get(c).trim().isEmpty()) {
        return false;
      }
    }
    // A category header always has values; an all-empty unindented row is a note.
    return true;
  }

  private static void emit(ArrayNode out, int[] years, List<String> row, String category,
      String payerPath, String payer, int depth) {
    for (int c = 1; c < years.length && c < row.size(); c++) {
      Double v = number(row.get(c));
      ObjectNode o = MAPPER.createObjectNode();
      o.put("year", years[c]);
      o.put("service_category", category);
      o.put("payer_path", payerPath);
      o.put("payer", payer);
      o.put("depth", depth);
      if (v == null) {
        o.putNull("amount_millions");
      } else {
        o.put("amount_millions", v.doubleValue());
      }
      out.add(o);
    }
  }
}
