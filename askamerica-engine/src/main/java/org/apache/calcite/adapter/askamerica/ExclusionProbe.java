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
package org.apache.calcite.adapter.askamerica;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Names the units a caller's hand-written exclusion predicates removed.
 *
 * <p>The {@code explicit_exclusion} warning is built from the SQL text alone, so it can say that
 * {@code x IS NOT NULL} was applied but not that Delaware and Rhode Island were what it removed.
 * The units a predicate removed exist only as the difference between the query as written and the
 * same query without its exclusion conjuncts, so this re-runs the statement with those conjuncts
 * taken out of each {@code WHERE} and reports the unit labels present only in the relaxed result.
 *
 * <p>The conjuncts are located with Calcite's parser and cut from the original text by source
 * position, so the relaxed statement keeps the caller's own quoting, casing and dialect. Only
 * top-level {@code AND} terms are removed: an exclusion inside an {@code OR} is not a standalone
 * filter and removing it would change the query's meaning rather than relax it.
 */
final class ExclusionProbe {
  /** Executes a statement through the same path the caller's own SQL took. */
  interface SqlRunner {
    ArrayNode run(String sql) throws Exception;
  }

  /** Most units listed on the warning; the full count is always reported. */
  static final int MAX_LISTED = 25;

  /** Row cap for the relaxed statement, matching the largest result a caller can request. */
  static final int RELAXED_ROW_LIMIT = 5000;

  private ExclusionProbe() {
  }

  /** Raised when the relaxed statement cannot be built or compared; carries the reason shown
   *  to the caller in {@code excluded_units_unavailable}. */
  static final class Unavailable extends Exception {
    Unavailable(String reason) {
      super(reason);
    }
  }

  private static final class Edit {
    final int start;
    final int end;
    final String replacement;

    Edit(int start, int end, String replacement) {
      this.start = start;
      this.end = end;
      this.replacement = replacement;
    }
  }

  /**
   * Adds {@code excluded_units} (or {@code excluded_units_unavailable} with the reason) to an
   * {@code explicit_exclusion} warning.
   *
   * @param originalRows rows the caller's SQL returned, or null to have the probe run it
   * @param diffAliases  lower-cased aliases of LAG/LEAD-differenced columns, whose null filter is
   *                     a consequence of differencing rather than an exclusion
   */
  static void annotate(ObjectNode warning, String sql, ArrayNode originalRows,
      Set<String> diffAliases, SqlRunner runner) {
    try {
      if (runner == null) {
        throw new Unavailable("no statement runner is available on this path");
      }
      String relaxed = relax(sql, diffAliases);
      ArrayNode original = originalRows != null ? originalRows : runner.run(sql);
      ArrayNode wider = runner.run(relaxed);
      String label = labelColumn(original, wider);
      List<String> units = missingUnits(label, original, wider);
      warning.put("excluded_unit_column", label);
      warning.put("excluded_unit_count", units.size());
      ArrayNode listed = warning.putArray("excluded_units");
      for (int i = 0; i < units.size() && i < MAX_LISTED; i++) {
        listed.add(units.get(i));
      }
      StringBuilder note = new StringBuilder();
      if (units.size() > MAX_LISTED) {
        note.append("first ").append(MAX_LISTED).append(" of ").append(units.size())
            .append(" excluded units. ");
      }
      if (wider.size() >= RELAXED_ROW_LIMIT) {
        note.append("The unfiltered query hit the ").append(RELAXED_ROW_LIMIT)
            .append("-row cap, so this list may be incomplete.");
      }
      if (note.length() > 0) {
        warning.put("excluded_units_note", note.toString().trim());
      }
    } catch (Unavailable e) {
      warning.put("excluded_units_unavailable", e.getMessage());
    } catch (Exception e) {
      warning.put("excluded_units_unavailable",
          "the unfiltered query could not be run: " + e.getMessage());
    }
  }

  /**
   * The statement with every exclusion conjunct removed from its {@code WHERE} clauses.
   *
   * @throws Unavailable when the SQL does not parse or contains no removable conjunct
   */
  static String relax(String sql, Set<String> diffAliases) throws Unavailable {
    if (sql.indexOf('\t') >= 0) {
      // Parser columns count a tab as several characters, which would misplace the cuts.
      sql = sql.replace('\t', ' ');
    }
    String text = sql.replaceAll(";\\s*$", "");
    SqlNode root;
    try {
      root = SqlParser.create(text,
          SqlParser.config().withConformance(SqlConformanceEnum.LENIENT)).parseQuery();
    } catch (SqlParseException e) {
      throw new Unavailable("the SQL could not be parsed to remove its exclusions: "
          + e.getMessage().replaceAll("\\s+", " "));
    }
    final List<SqlNode> wheres = new ArrayList<>();
    root.accept(new SqlBasicVisitor<Void>() {
      @Override public Void visit(SqlCall call) {
        if (call instanceof SqlSelect && ((SqlSelect) call).getWhere() != null) {
          wheres.add(((SqlSelect) call).getWhere());
        }
        return super.visit(call);
      }
    });
    int[] lineStarts = lineStarts(text);
    List<Edit> edits = new ArrayList<>();
    for (SqlNode where : wheres) {
      List<SqlNode> terms = new ArrayList<>();
      conjuncts(where, terms);
      List<SqlNode> kept = new ArrayList<>();
      int removed = 0;
      for (SqlNode term : terms) {
        if (isExclusion(term, diffAliases)) {
          removed++;
        } else {
          kept.add(term);
        }
      }
      if (removed == 0) {
        continue;
      }
      StringBuilder replacement = new StringBuilder();
      for (SqlNode term : kept) {
        if (replacement.length() > 0) {
          replacement.append(" AND ");
        }
        int[] span = span(term, lineStarts, text);
        replacement.append(text, span[0], span[1]);
      }
      if (kept.isEmpty()) {
        replacement.append("1 = 1");
      }
      int[] whole = span(where, lineStarts, text);
      edits.add(new Edit(whole[0], whole[1], replacement.toString()));
    }
    if (edits.isEmpty()) {
      throw new Unavailable("no top-level AND-ed exclusion predicate was found in the SQL "
          + "(a predicate inside OR cannot be removed without changing the query's meaning)");
    }
    edits.sort(Comparator.comparingInt((Edit e) -> e.start).thenComparingInt(e -> -e.end));
    StringBuilder out = new StringBuilder();
    int cursor = 0;
    for (Edit e : edits) {
      if (e.start < cursor) {
        continue; // nested inside an edit already applied
      }
      out.append(text, cursor, e.start).append(e.replacement);
      cursor = e.end;
    }
    out.append(text, cursor, text.length());
    return out.toString();
  }

  private static void conjuncts(SqlNode node, List<SqlNode> out) {
    if (node.getKind() == SqlKind.AND) {
      for (SqlNode operand : ((SqlCall) node).getOperandList()) {
        conjuncts(operand, out);
      }
    } else {
      out.add(node);
    }
  }

  /** The same three shapes {@code QuestionDiagnostics.explicitExclusion} reports. */
  private static boolean isExclusion(SqlNode term, Set<String> diffAliases) {
    if (!(term instanceof SqlBasicCall)) {
      return false;
    }
    SqlBasicCall call = (SqlBasicCall) term;
    switch (call.getKind()) {
    case NOT_EQUALS:
      return call.operand(0) instanceof SqlIdentifier && call.operand(1) instanceof SqlLiteral;
    case NOT_IN:
      return call.operand(0) instanceof SqlIdentifier;
    case IS_NOT_NULL:
      if (!(call.operand(0) instanceof SqlIdentifier)) {
        return false;
      }
      SqlIdentifier id = call.operand(0);
      return !diffAliases.contains(id.names.get(id.names.size() - 1).toLowerCase(Locale.ROOT));
    default:
      return false;
    }
  }

  private static int[] lineStarts(String text) {
    List<Integer> starts = new ArrayList<>();
    starts.add(0);
    for (int i = 0; i < text.length(); i++) {
      if (text.charAt(i) == '\n') {
        starts.add(i + 1);
      }
    }
    int[] out = new int[starts.size()];
    for (int i = 0; i < out.length; i++) {
      out[i] = starts.get(i);
    }
    return out;
  }

  /** Half-open character range of a node in the source text. */
  private static int[] span(SqlNode node, int[] lineStarts, String text) {
    SqlParserPos pos = node.getParserPosition();
    int start = lineStarts[pos.getLineNum() - 1] + pos.getColumnNum() - 1;
    int end = lineStarts[pos.getEndLineNum() - 1] + pos.getEndColumnNum();
    return new int[] {start, end};
  }

  /** The column that identifies a unit, found by value rather than by a fixed list, in the
   *  columns both results share. A {@code name} column is preferred over a code. */
  static String labelColumn(ArrayNode original, ArrayNode wider) throws Unavailable {
    if (wider == null || wider.size() == 0) {
      throw new Unavailable("the query without its exclusions returned no rows");
    }
    List<String> shared = new ArrayList<>();
    Iterator<String> it = wider.get(0).fieldNames();
    while (it.hasNext()) {
      String c = it.next();
      if (original == null || original.size() == 0 || original.get(0).has(c)) {
        shared.add(c);
      }
    }
    String fallback = null;
    for (String c : shared) {
      String lower = c.toLowerCase(Locale.ROOT);
      if (lower.contains("name")) {
        return c;
      }
      if (fallback == null && (lower.contains("state") || lower.contains("geo")
          || lower.contains("fips") || lower.contains("area") || lower.contains("county")
          || lower.contains("zip") || lower.contains("tract") || lower.equals("id")
          || lower.endsWith("_id") || lower.endsWith("_code"))) {
        fallback = c;
      }
    }
    if (fallback == null) {
      throw new Unavailable("the result has no unit-label column (state, county, name, code) "
          + "to compare, so the excluded units cannot be named");
    }
    return fallback;
  }

  private static List<String> missingUnits(String label, ArrayNode original, ArrayNode wider) {
    Set<String> present = new LinkedHashSet<>();
    if (original != null) {
      for (JsonNode row : original) {
        present.add(row.path(label).asText());
      }
    }
    Set<String> missing = new LinkedHashSet<>();
    for (JsonNode row : wider) {
      JsonNode v = row.get(label);
      if (v != null && !v.isNull() && !present.contains(v.asText())) {
        missing.add(v.asText());
      }
    }
    return new ArrayList<>(missing);
  }
}
