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
package org.apache.calcite.adapter.servicenow;

import org.apache.calcite.adapter.servicenow.PushdownCapabilities.Op;
import org.apache.calcite.adapter.servicenow.PushdownCapabilities.Position;
import org.apache.calcite.adapter.servicenow.PushdownCapabilities.TypeGroup;
import org.apache.calcite.adapter.servicenow.PushdownHarness.Case;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * The cases of the differential harness, built from the data actually in the table so that the
 * literals exist and the boundaries (smallest, median, largest value; empty rows; mixed case) are
 * real.
 *
 * <p>Matrix, per column type where it applies: equality; inequality in both encodings (plain
 * {@code !=}, and with the added {@code ISNOTEMPTY}), which decides whether ServiceNow's {@code !=}
 * includes empty rows; {@code < <= > >=}; ranges at the boundary values (inclusive and
 * exclusive); {@code IN} with several values and with one; {@code NOT IN} in both encodings;
 * {@code IS NULL}; {@code IS NOT NULL}; {@code NOT} over a comparison; booleans as bare columns and
 * comparisons. For text: the same comparisons on the case-swapped value (is it case sensitive?),
 * {@code LIKE} as prefix, suffix, contains and exact, {@code LIKE} patterns that must not be
 * pushed ({@code _}, interior {@code %}, an ESCAPE clause), values with {@code = , ' % _ ^}
 * and spaces, and an empty-string literal (left to Calcite). For timestamps and dates: the
 * comparisons at the exact boundary values (time zone shows up as rows missing or added).
 * Boolean structure: AND of terms, a single OR group, AND with an OR group, two OR groups, and
 * shapes that must not be pushed (OR over AND).
 */
final class PushdownCases {
  private PushdownCases() {}

  /** A column of the table under test and the values seen in it. */
  static final class Col {
    final String name;
    final TypeGroup group;
    /** Distinct non-empty values, as text, in ascending order of the type. */
    final List<String> values;
    final boolean hasNulls;

    Col(String name, TypeGroup group, List<String> values, boolean hasNulls) {
      this.name = name;
      this.group = group;
      this.values = values;
      this.hasNulls = hasNulls;
    }

    String literal(String raw) {
      switch (group) {
      case TEXT:
      case REFERENCE:
      case GUID:
        return "'" + raw.replace("'", "''") + "'";
      case TIMESTAMP:
        return "TIMESTAMP '" + raw + "'";
      case DATE:
        return "DATE '" + raw + "'";
      default:
        return raw;
      }
    }
  }

  /** The columns the harness uses, per type, in order of preference. */
  static final Map<TypeGroup, List<String>> PREFERRED = new EnumMap<>(TypeGroup.class);

  static {
    PREFERRED.put(TypeGroup.TEXT, Arrays.asList("description", "short_description", "number"));
    PREFERRED.put(TypeGroup.NUMERIC, Arrays.asList("impact", "urgency", "priority"));
    PREFERRED.put(TypeGroup.BOOLEAN, Arrays.asList("knowledge", "active", "made_sla"));
    PREFERRED.put(TypeGroup.TIMESTAMP, Arrays.asList("opened_at", "sys_created_on"));
    PREFERRED.put(TypeGroup.DATE, Arrays.asList("start_date"));
    PREFERRED.put(TypeGroup.REFERENCE, Arrays.asList("caller_id", "assigned_to", "opened_by"));
    PREFERRED.put(TypeGroup.GUID, Arrays.asList("sys_id"));
  }

  /** Reads the columns the harness can use and their values from the table. */
  static List<Col> profile(Connection connection, String table) throws SQLException {
    final Set<String> existing = new HashSet<>();
    final DatabaseMetaData md = connection.getMetaData();
    try (ResultSet rs = md.getColumns(null, null, table, null)) {
      while (rs.next()) {
        existing.add(rs.getString("COLUMN_NAME"));
      }
    }
    final List<Col> columns = new ArrayList<>();
    for (Map.Entry<TypeGroup, List<String>> preference : PREFERRED.entrySet()) {
      for (String name : preference.getValue()) {
        if (!existing.contains(name)) {
          continue;
        }
        final Col col = column(connection, table, name, preference.getKey());
        if (col.values.size() >= 2) {
          columns.add(col);
          break;
        }
      }
    }
    return columns;
  }

  private static Col column(Connection connection, String table, String name, TypeGroup group)
      throws SQLException {
    final Set<String> distinct = new LinkedHashSet<>();
    boolean nulls = false;
    try (Statement statement = connection.createStatement();
         ResultSet rs = statement.executeQuery("SELECT " + name + " FROM " + table)) {
      while (rs.next()) {
        final String value = rs.getString(1);
        if (value == null) {
          nulls = true;
        } else {
          distinct.add(group == TypeGroup.TIMESTAMP && value.endsWith(".0")
              ? value.substring(0, value.length() - 2) : value);
        }
      }
    }
    final List<String> sorted = new ArrayList<>(distinct);
    if (group == TypeGroup.NUMERIC) {
      sorted.sort((a, b) -> new BigDecimal(a).compareTo(new BigDecimal(b)));
    } else {
      Collections.sort(sorted);
    }
    return new Col(name, group, sorted, nulls);
  }

  // ---- building -------------------------------------------------------------------------

  private static String e(Op op, TypeGroup group, Position position) {
    return PushdownCapabilities.id(op, group, position);
  }

  private static Set<String> set(String... entries) {
    return new LinkedHashSet<>(Arrays.asList(entries));
  }

  private static Set<String> union(Set<String> a, Set<String> b) {
    final Set<String> result = new LinkedHashSet<>(a);
    result.addAll(b);
    return result;
  }

  private static Case leaf(String id, String where, Set<String> subjects, String note) {
    return new Case(id, where, union(subjects, set(PushdownCapabilities.SHAPE_AND)), subjects,
        set(PushdownCapabilities.SHAPE_AND), true, note);
  }

  private static Case declined(String id, String where, String note) {
    return new Case(id, where, new LinkedHashSet<>(PushdownCapabilities.candidates()),
        Collections.<String>emptySet(), Collections.<String>emptySet(), false, note);
  }

  private static String swapCase(String text) {
    final StringBuilder out = new StringBuilder();
    for (char c : text.toCharArray()) {
      out.append(Character.isUpperCase(c) ? Character.toLowerCase(c)
          : Character.isLowerCase(c) ? Character.toUpperCase(c) : c);
    }
    return out.toString();
  }

  /** Builds the cases for the columns found. */
  static List<Case> build(List<Col> columns) {
    final List<Case> cases = new ArrayList<>();
    for (Col col : columns) {
      cases.addAll(columnCases(col));
    }
    cases.addAll(shapeCases(columns));
    return cases;
  }

  private static List<Case> columnCases(Col col) {
    final List<Case> cases = new ArrayList<>();
    final TypeGroup g = col.group;
    final String c = col.name;
    final int n = col.values.size();
    final String lo = col.values.get(0);
    final String hi = col.values.get(n - 1);
    final String mid = col.values.get(n / 2);
    final String other = col.values.get((n / 2 + 1) % n);
    final String p = c + ":" + g + ":";
    final Position and = Position.AND;
    final boolean ordered = g == TypeGroup.NUMERIC || g == TypeGroup.TIMESTAMP
        || g == TypeGroup.DATE || g == TypeGroup.TEXT;

    cases.add(leaf(p + "eq", c + " = " + col.literal(mid), set(e(Op.EQ, g, and)), ""));
    cases.add(leaf(p + "eq-hi", c + " = " + col.literal(hi), set(e(Op.EQ, g, and)),
        "largest value"));
    if (g == TypeGroup.BOOLEAN) {
      // Calcite turns <> and NOT on a boolean into = false / = true
      cases.add(leaf(p + "ne-bool", c + " <> " + col.literal(mid), set(e(Op.EQ, g, and)),
          "<> true is NOT the column: an empty row is neither"));
      cases.add(leaf(p + "not-eq", "NOT (" + c + " = " + col.literal(mid) + ")",
          set(e(Op.EQ, g, and)), "NOT over equality"));
    } else {
      cases.add(leaf(p + "ne-plain", c + " <> " + col.literal(mid), set(e(Op.NE, g, and)),
          "does != include rows where the field is empty? (SQL <> excludes them)"));
      cases.add(leaf(p + "ne-notempty", c + " <> " + col.literal(mid),
          set(e(Op.NE_NOT_EMPTY, g, and)), "!= with an added ISNOTEMPTY term"));
      cases.add(leaf(p + "not-eq", "NOT (" + c + " = " + col.literal(mid) + ")",
          set(e(Op.NE_NOT_EMPTY, g, and)), "NOT over equality, inverted to !="));
    }
    if (g != TypeGroup.BOOLEAN) {
    cases.add(leaf(p + "in2", c + " IN (" + col.literal(mid) + ", " + col.literal(other) + ")",
        set(e(Op.IN, g, and)), ""));
    }
    cases.add(leaf(p + "in1", c + " IN (" + col.literal(mid) + ")", set(e(Op.EQ, g, and)),
        "single-element IN becomes equality"));
    if (g != TypeGroup.BOOLEAN) {
    cases.add(leaf(p + "notin-plain",
        c + " NOT IN (" + col.literal(mid) + ", " + col.literal(other) + ")",
        set(e(Op.NOT_IN, g, and)), "NOT IN without the added term"));
    cases.add(leaf(p + "notin-notempty",
        c + " NOT IN (" + col.literal(mid) + ", " + col.literal(other) + ")",
        set(e(Op.NOT_IN_NOT_EMPTY, g, and)), "NOT IN with the added ISNOTEMPTY term"));
    }
    cases.add(leaf(p + "is-null", c + " IS NULL", set(e(Op.IS_NULL, g, and)),
        col.hasNulls ? "" : "no empty value in the data: weak evidence"));
    cases.add(leaf(p + "is-not-null", c + " IS NOT NULL", set(e(Op.IS_NOT_NULL, g, and)), ""));
    if (ordered) {
      cases.add(leaf(p + "lt", c + " < " + col.literal(mid), set(e(Op.LT, g, and)),
          "empty rows: does < include them?"));
      cases.add(leaf(p + "le", c + " <= " + col.literal(mid), set(e(Op.LE, g, and)), "boundary"));
      cases.add(leaf(p + "gt", c + " > " + col.literal(mid), set(e(Op.GT, g, and)), ""));
      cases.add(leaf(p + "ge", c + " >= " + col.literal(mid), set(e(Op.GE, g, and)),
          "boundary"));
      cases.add(leaf(p + "between-edges",
          c + " BETWEEN " + col.literal(lo) + " AND " + col.literal(hi),
          set(e(Op.GE, g, and), e(Op.LE, g, and)), "both bounds are existing values"));
      cases.add(leaf(p + "range-open",
          c + " > " + col.literal(lo) + " AND " + c + " < " + col.literal(hi),
          set(e(Op.GT, g, and), e(Op.LT, g, and)), "exclusive bounds at existing values"));
      cases.add(leaf(p + "not-lt", "NOT (" + c + " < " + col.literal(mid) + ")",
          set(e(Op.GE, g, and)), "NOT over < inverted to >="));
    }
    if (g == TypeGroup.BOOLEAN) {
      cases.add(leaf(p + "bare", c, set(e(Op.EQ, g, and)), "bare boolean column"));
      cases.add(leaf(p + "not-bare", "NOT " + c, set(e(Op.EQ, g, and)), ""));
    }
    if (g == TypeGroup.TEXT) {
      cases.addAll(textCases(col));
    }
    if (g == TypeGroup.TIMESTAMP) {
      cases.add(leaf(p + "same-instant", c + " = " + col.literal(lo), set(e(Op.EQ, g, and)),
          "time zone: an exact existing instant must match exactly"));
    }
    if (g == TypeGroup.REFERENCE) {
      cases.add(declined(p + "display-literal-is-a-different-column",
          c + "__display = " + col.literal(mid),
          "the display column is never pushed"));
    }
    return cases;
  }

  private static List<Case> textCases(Col col) {
    final List<Case> cases = new ArrayList<>();
    final String c = col.name;
    final TypeGroup g = TypeGroup.TEXT;
    final Position and = Position.AND;
    final String p = c + ":TEXT:";
    String sample = col.values.get(col.values.size() / 2);
    for (String value : col.values) {
      if (value.length() >= 4 && Character.isLetter(value.charAt(0)) && !value.contains("'")) {
        sample = value;
        break;
      }
    }
    final String prefix = sample.substring(0, Math.min(3, sample.length()));
    final String suffix = sample.substring(Math.max(0, sample.length() - 2));
    final String inner = sample.length() > 3 ? sample.substring(1, sample.length() - 1) : sample;
    final String swapped = swapCase(sample);
    cases.add(leaf(p + "like-prefix", c + " LIKE '" + sql(prefix) + "%'",
        set(e(Op.LIKE_PREFIX, g, and)), ""));
    cases.add(leaf(p + "like-suffix", c + " LIKE '%" + sql(suffix) + "'",
        set(e(Op.LIKE_SUFFIX, g, and)), ""));
    cases.add(leaf(p + "like-contains", c + " LIKE '%" + sql(inner) + "%'",
        set(e(Op.LIKE_CONTAINS, g, and)), ""));
    cases.add(leaf(p + "like-exact", c + " LIKE '" + sql(sample) + "'",
        set(e(Op.LIKE_EXACT, g, and)), ""));
    if (!swapped.equals(sample)) {
      cases.add(leaf(p + "case-eq", c + " = '" + sql(swapped) + "'", set(e(Op.EQ, g, and)),
          "case sensitivity: SQL equality is case sensitive"));
      cases.add(leaf(p + "case-prefix",
          c + " LIKE '" + sql(swapCase(prefix)) + "%'", set(e(Op.LIKE_PREFIX, g, and)),
          "case sensitivity of STARTSWITH"));
      cases.add(leaf(p + "case-contains",
          c + " LIKE '%" + sql(swapCase(inner)) + "%'", set(e(Op.LIKE_CONTAINS, g, and)),
          "case sensitivity of LIKE (contains)"));
      cases.add(leaf(p + "case-in",
          c + " IN ('" + sql(swapped) + "', '" + sql(sample) + "')", set(e(Op.IN, g, and)),
          "case sensitivity of IN"));
    }
    cases.add(declined(p + "like-underscore", c + " LIKE 'a_c%'",
        "SQL _ is one character; no ServiceNow equivalent"));
    cases.add(declined(p + "like-interior-percent", c + " LIKE 'a%b'",
        "interior % cannot be written"));
    cases.add(declined(p + "like-two-percents", c + " LIKE '%a%b%'", ""));
    cases.add(declined(p + "like-escape", c + " LIKE '%100\\%%' ESCAPE '\\'",
        "literal wildcard characters need ESCAPE; not pushed"));
    cases.add(declined(p + "not-like", c + " NOT LIKE 'a%'",
        "NOT LIKE has no verified encoding"));
    cases.add(declined(p + "empty-literal-eq", c + " = ''",
        "empty value is NULL here: = '' matches nothing and stays in Calcite"));
    cases.add(declined(p + "empty-literal-ne", c + " <> ''", "same"));
    cases.add(declined(p + "empty-literal-in", c + " IN ('', 'x')", "same"));
    cases.add(declined(p + "caret", c + " = 'a^b'", "the query separator is never sent in a value"));
    cases.add(declined(p + "comma-in-in", c + " IN ('a,b', 'c')", "a comma would split the list"));
    cases.add(declined(p + "function", "UPPER(" + c + ") = 'A'", "expressions stay in Calcite"));

    // Values that only look like syntax; they need VALUE:SPECIAL, evidence for it, and EQ:TEXT
    final Set<String> prerequisites =
        set(PushdownCapabilities.SHAPE_AND, e(Op.EQ, g, and));
    for (String value : col.values) {
      if (value.contains("^") || value.isEmpty() || !special(value)) {
        continue;
      }
      cases.add(new Case(p + "special[" + value + "]", c + " = '" + sql(value) + "'",
          union(prerequisites, set(PushdownCapabilities.VALUE_SPECIAL)),
          set(PushdownCapabilities.VALUE_SPECIAL), prerequisites, true,
          "value with punctuation: " + value));
    }
    return cases;
  }

  private static boolean special(String value) {
    return !value.matches("[A-Za-z0-9_.:/-]+( [A-Za-z0-9_.:/-]+)*");
  }

  private static String sql(String raw) {
    return raw.replace("'", "''");
  }

  private static List<Case> shapeCases(List<Col> columns) {
    final List<Case> cases = new ArrayList<>();
    final List<Col> usable = new ArrayList<>();
    for (Col col : columns) {
      if (col.group != TypeGroup.TEXT || col.values.size() >= 2) {
        usable.add(col);
      }
    }
    if (usable.size() < 2) {
      return cases;
    }
    final Col a = usable.get(0);
    final Col b = usable.get(1);
    final Col c = usable.get(usable.size() > 2 ? 2 : 0);
    final Col d = usable.get(usable.size() > 3 ? 3 : 1);
    final String ta = term(a, 0);
    final String tb = term(b, 1);
    final String tc = term(c, a == c ? 1 : 0);
    final String td = term(d, b == d ? 0 : 1);
    final Position and = Position.AND;
    final Position or = Position.OR;

    cases.add(new Case("shape:and", ta + " AND " + tb,
        set(PushdownCapabilities.SHAPE_AND, e(Op.EQ, a.group, and), e(Op.EQ, b.group, and)),
        set(PushdownCapabilities.SHAPE_AND), Collections.<String>emptySet(), true,
        "two conjuncts joined with ^, followed by the paging term"));
    final Set<String> orShapes = set(PushdownCapabilities.SHAPE_AND,
        PushdownCapabilities.SHAPE_OR_GROUP, PushdownCapabilities.SHAPE_OR_WITH_OTHER_TERMS);
    final Set<String> orLeaves = set(e(Op.EQ, a.group, or), e(Op.EQ, b.group, or),
        e(Op.EQ, c.group, or), e(Op.EQ, d.group, or));
    cases.add(new Case("shape:or-group", ta + " OR " + tb, union(orShapes, orLeaves),
        union(set(PushdownCapabilities.SHAPE_OR_GROUP,
            PushdownCapabilities.SHAPE_OR_WITH_OTHER_TERMS), set(e(Op.EQ, a.group, or),
            e(Op.EQ, b.group, or))),
        set(PushdownCapabilities.SHAPE_AND), true,
        "a^ORb followed by the paging term; ^OR attaches to the term before it"));
    final Set<String> andLeaves = set(e(Op.EQ, a.group, and), e(Op.EQ, b.group, and),
        e(Op.EQ, c.group, and), e(Op.EQ, d.group, and));
    cases.add(new Case("shape:and-with-or", ta + " AND (" + tb + " OR " + tc + ")",
        union(union(orShapes, orLeaves), andLeaves),
        set(PushdownCapabilities.SHAPE_OR_WITH_OTHER_TERMS),
        set(PushdownCapabilities.SHAPE_AND, PushdownCapabilities.SHAPE_OR_GROUP), true,
        "a^b^ORc must mean a AND (b OR c)"));
    cases.add(new Case("shape:or-and", "(" + ta + " OR " + tb + ") AND " + tc,
        union(union(orShapes, orLeaves), andLeaves),
        set(PushdownCapabilities.SHAPE_OR_WITH_OTHER_TERMS),
        set(PushdownCapabilities.SHAPE_AND, PushdownCapabilities.SHAPE_OR_GROUP), true,
        "a^ORb^c must mean (a OR b) AND c: the next ^ ends the OR group"));
    cases.add(new Case("shape:two-or-groups",
        "(" + ta + " OR " + tb + ") AND (" + tc + " OR " + td + ")",
        union(orShapes, orLeaves), set(PushdownCapabilities.SHAPE_OR_WITH_OTHER_TERMS),
        set(PushdownCapabilities.SHAPE_AND, PushdownCapabilities.SHAPE_OR_GROUP), true,
        "a^ORb^c^ORd must mean (a OR b) AND (c OR d)"));
    cases.add(declined("shape:or-over-and", "(" + ta + " AND " + tb + ") OR " + tc,
        "would need ^NQ or parentheses: never pushed"));
    cases.add(declined("shape:or-with-and-member", ta + " OR (" + tb + " AND " + tc + ")",
        "never pushed"));

    // Operators inside an OR group, each against the group's anchor term
    for (Col col : columns) {
      final Col anchor = col == a ? b : a;
      final String anchorTerm = col == a ? tb : ta;
      final TypeGroup g = col.group;
      final String x = col.name;
      final String v = col.values.get(col.values.size() / 2);
      final Set<String> prerequisites = set(PushdownCapabilities.SHAPE_AND,
          PushdownCapabilities.SHAPE_OR_GROUP, PushdownCapabilities.SHAPE_OR_WITH_OTHER_TERMS,
          e(Op.EQ, anchor.group, or));
      cases.add(orLeaf("or:" + x + ":eq", x + " = " + col.literal(v) + " OR " + anchorTerm,
          set(e(Op.EQ, g, or)), prerequisites));
      if (g != TypeGroup.BOOLEAN) {
        cases.add(orLeaf("or:" + x + ":notin",
            x + " NOT IN (" + col.literal(v) + ", " + col.literal(col.values.get(0)) + ") OR "
                + anchorTerm, set(e(Op.NOT_IN, g, or)), prerequisites));
      }
      if (g != TypeGroup.BOOLEAN) {
        cases.add(orLeaf("or:" + x + ":ne", x + " <> " + col.literal(v) + " OR " + anchorTerm,
            set(e(Op.NE, g, or)), prerequisites));
      }
      cases.add(orLeaf("or:" + x + ":is-null", x + " IS NULL OR " + anchorTerm,
          set(e(Op.IS_NULL, g, or)), prerequisites));
      cases.add(orLeaf("or:" + x + ":is-not-null", x + " IS NOT NULL OR " + anchorTerm,
          set(e(Op.IS_NOT_NULL, g, or)), prerequisites));
      if (g != TypeGroup.BOOLEAN) {
        cases.add(orLeaf("or:" + x + ":in",
            x + " IN (" + col.literal(v) + ", " + col.literal(col.values.get(0)) + ") OR "
                + anchorTerm, set(e(Op.IN, g, or)), prerequisites));
      }
      if (g == TypeGroup.NUMERIC || g == TypeGroup.TIMESTAMP || g == TypeGroup.DATE
          || g == TypeGroup.TEXT) {
        cases.add(orLeaf("or:" + x + ":lt", x + " < " + col.literal(v) + " OR " + anchorTerm,
            set(e(Op.LT, g, or)), prerequisites));
        cases.add(orLeaf("or:" + x + ":ge", x + " >= " + col.literal(v) + " OR " + anchorTerm,
            set(e(Op.GE, g, or)), prerequisites));
        cases.add(orLeaf("or:" + x + ":gt", x + " > " + col.literal(v) + " OR " + anchorTerm,
            set(e(Op.GT, g, or)), prerequisites));
        cases.add(orLeaf("or:" + x + ":le", x + " <= " + col.literal(v) + " OR " + anchorTerm,
            set(e(Op.LE, g, or)), prerequisites));
      }
      if (g == TypeGroup.TEXT) {
        cases.add(orLeaf("or:" + x + ":prefix",
            x + " LIKE '" + sql(v.substring(0, Math.min(3, v.length()))) + "%' OR " + anchorTerm,
            set(e(Op.LIKE_PREFIX, g, or)), prerequisites));
        cases.add(orLeaf("or:" + x + ":suffix",
            x + " LIKE '%" + sql(v.substring(Math.max(0, v.length() - 2))) + "' OR " + anchorTerm,
            set(e(Op.LIKE_SUFFIX, g, or)), prerequisites));
        cases.add(orLeaf("or:" + x + ":contains",
            x + " LIKE '%" + sql(v.substring(1, Math.max(2, v.length() - 1))) + "%' OR "
                + anchorTerm, set(e(Op.LIKE_CONTAINS, g, or)), prerequisites));
        cases.add(orLeaf("or:" + x + ":exact", x + " LIKE '" + sql(v) + "' OR " + anchorTerm,
            set(e(Op.LIKE_EXACT, g, or)), prerequisites));
      }
    }
    return cases;
  }

  private static Case orLeaf(String id, String where, Set<String> subjects,
      Set<String> prerequisites) {
    return new Case(id, where, union(subjects, prerequisites), subjects, prerequisites, true,
        "operator inside an OR group");
  }

  private static String term(Col col, int which) {
    final String v = col.values.get(Math.min(which * (col.values.size() - 1), col.values.size() - 1));
    if (col.group == TypeGroup.BOOLEAN) {
      return col.name + " = " + v;
    }
    return col.name + " = " + col.literal(v);
  }

  /** Every entry that at least one case in the list is evidence for. */
  static Set<String> covered(List<Case> cases) {
    final Set<String> covered = new TreeSet<>();
    for (Case testCase : cases) {
      covered.addAll(testCase.subjects);
    }
    return covered;
  }
}
