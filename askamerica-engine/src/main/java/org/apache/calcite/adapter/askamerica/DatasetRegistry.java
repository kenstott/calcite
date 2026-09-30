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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Named, session-scoped datasets: a caller registers a SELECT once and then references it by
 * name in the {@code FROM}/{@code JOIN} of any later statement, including the {@code sql}
 * argument of every statistics tool.
 *
 * <p>The MCP server is a stdio process serving one client, so a process-wide registry is a
 * per-session registry. A dataset is not a database object: a reference is expanded into a
 * {@code WITH name AS (...)} prefix just before the statement is executed, so every tool sees
 * exactly the same text for the same name and nothing is created in the warehouse.
 *
 * <p>Each stored definition is self-contained — references to earlier datasets are expanded at
 * registration time — so redefining a dataset never changes what another dataset already
 * captured, and no dependency ordering is needed at expansion time.
 */
final class DatasetRegistry {
  private static final int MAX_NAME_LENGTH = 64;
  private static final Pattern NAME = Pattern.compile("[a-z_][a-z0-9_]*");

  private final Map<String, String> definitions = new LinkedHashMap<>();

  /** Registers (or replaces) {@code name} as the given SELECT or WITH statement. */
  synchronized String define(String name, String sql) {
    String key = name == null ? "" : name.trim().toLowerCase(Locale.ROOT);
    if (key.length() > MAX_NAME_LENGTH || !NAME.matcher(key).matches()) {
      throw new IllegalArgumentException("Dataset name '" + name + "' must be 1-"
          + MAX_NAME_LENGTH + " characters of lowercase letters, digits and underscores, "
          + "not starting with a digit.");
    }
    String body = sql == null ? "" : sql.trim();
    while (body.endsWith(";")) {
      body = body.substring(0, body.length() - 1).trim();
    }
    if (!startsWithKeyword(body, "select") && !startsWithKeyword(body, "with")
        && !body.startsWith("(")) {
      throw new IllegalArgumentException(
          "A dataset must be a SELECT or WITH statement; got: " + abbreviate(body));
    }
    String resolved = expand(body);
    definitions.put(key, resolved);
    return resolved;
  }

  /** Returns the registered names in registration order. */
  synchronized List<String> names() {
    return new ArrayList<>(definitions.keySet());
  }

  /**
   * Rewrites {@code sql} so every registered dataset it names in a {@code FROM}/{@code JOIN}
   * position is supplied by a leading CTE. Returns {@code sql} unchanged when it references
   * none, or when the caller already defines a CTE of the same name.
   */
  synchronized String expand(String sql) {
    if (sql == null || definitions.isEmpty()) {
      return sql;
    }
    Map<String, String> needed = new LinkedHashMap<>();
    for (String name : referencedNames(sql)) {
      String def = definitions.get(name);
      if (def != null && !definesCte(sql, name)) {
        needed.put(name, def);
      }
    }
    if (needed.isEmpty()) {
      return sql;
    }
    StringBuilder ctes = new StringBuilder();
    for (Map.Entry<String, String> e : needed.entrySet()) {
      if (ctes.length() > 0) {
        ctes.append(", ");
      }
      ctes.append(e.getKey()).append(" AS (").append(e.getValue()).append(')');
    }
    String trimmed = sql.trim();
    Matcher with =
        Pattern.compile("^(?i:with)(\\s+(?i:recursive))?\\s+").matcher(trimmed);
    if (with.find()) {
      return trimmed.substring(0, with.end()) + ctes + ", " + trimmed.substring(with.end());
    }
    return "WITH " + ctes + " " + trimmed;
  }

  private static boolean definesCte(String sql, String name) {
    return Pattern.compile("(?i)(^|[\\s,])" + Pattern.quote(name) + "\\s+as\\s*\\(")
        .matcher(sql).find();
  }

  /**
   * Lowercased identifiers that appear as a table reference: directly after FROM or JOIN, or
   * after a comma inside a FROM list. String literals, quoted identifiers and comments are
   * skipped, and a name followed or preceded by a dot is a qualified column or table, never
   * a dataset.
   */
  private static Set<String> referencedNames(String sql) {
    Set<String> found = new LinkedHashSet<>();
    int n = sql.length();
    int i = 0;
    String prev = "";
    boolean inFrom = false;
    while (i < n) {
      char c = sql.charAt(i);
      if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
        int nl = sql.indexOf('\n', i);
        i = nl < 0 ? n : nl;
      } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
        int close = sql.indexOf("*/", i + 2);
        i = close < 0 ? n : close + 2;
      } else if (c == '\'' || c == '"') {
        i = skipQuoted(sql, i, c);
        prev = "?";
      } else if (Character.isLetter(c) || c == '_') {
        int start = i;
        while (i < n && (Character.isLetterOrDigit(sql.charAt(i)) || sql.charAt(i) == '_')) {
          i++;
        }
        String word = sql.substring(start, i).toLowerCase(Locale.ROOT);
        boolean dotBefore = prev.equals(".");
        boolean dotAfter = i < n && sql.charAt(i) == '.';
        if (!dotBefore && !dotAfter && (prev.equals("from") || prev.equals("join")
            || (prev.equals(",") && inFrom))) {
          found.add(word);
        }
        if (word.equals("from")) {
          inFrom = true;
        } else if (isClauseKeyword(word)) {
          inFrom = false;
        }
        prev = word;
      } else {
        if (!Character.isWhitespace(c)) {
          prev = String.valueOf(c);
        }
        i++;
      }
    }
    return found;
  }

  private static int skipQuoted(String sql, int start, char quote) {
    int n = sql.length();
    int i = start + 1;
    while (i < n) {
      if (sql.charAt(i) == quote) {
        if (i + 1 < n && sql.charAt(i + 1) == quote) {
          i += 2;
          continue;
        }
        return i + 1;
      }
      i++;
    }
    return n;
  }

  private static boolean isClauseKeyword(String w) {
    switch (w) {
    case "select": case "where": case "group": case "order": case "having": case "on":
    case "limit": case "union": case "intersect": case "except": case "using":
    case "window": case "qualify": case "offset": case "fetch":
      return true;
    default:
      return false;
    }
  }

  /** Case-insensitive prefix check that never throws, regardless of how {@code body}'s
   *  length compares to {@code keyword}'s -- unlike a fixed-length substring, this works for
   *  keywords of different lengths ("select" is 6 chars, "with" is 4) without truncating or
   *  over-reading either one. */
  private static boolean startsWithKeyword(String body, String keyword) {
    return body.regionMatches(true, 0, keyword, 0, keyword.length());
  }

  private static String abbreviate(String s) {
    return s.length() <= 80 ? s : s.substring(0, 80) + "...";
  }
}
