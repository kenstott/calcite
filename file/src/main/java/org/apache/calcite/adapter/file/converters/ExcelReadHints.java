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
package org.apache.calcite.adapter.file.converters;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * An operator's hints for how the Excel reader finds the tables in a workbook, each overriding
 * one rule the reader otherwise applies (see {@link ExcelTableFinder}); a hint left unset leaves
 * its rule as it is.
 *
 * <ul>
 * <li>{@code headerRows}: the exact number of header rows a table has; its header rows are not
 * detected.
 * <li>{@code maxHeaderRows}: the most rows detection takes as a table's header rows (3).
 * <li>{@code tableGapRows}: how many blank rows in a row end a table (2).
 * <li>{@code columnGap}: how many blank columns in a row separate tables side by side (2).
 * <li>{@code minColumns}: the fewest header cells a table has (2).
 * <li>{@code minDataRows}: the fewest data rows a table has; a table with fewer is not read
 * (1).
 * <li>{@code range}: the cells a table is, {@code Sheet!A3:F200}, read as that table whatever
 * the reader would find there.
 * </ul>
 *
 * <p>Given as the schema operand {@code excelHints}: for every workbook of the schema, for one
 * workbook (by its file name) and for one table (by the name the reader gives it,
 * {@code <file>__<sheet>}, see {@link #forTable}); the narrowest that sets a hint wins. A
 * table's gaps are found before the table is, so {@code tableGapRows} and {@code columnGap} are
 * given for every workbook or for one; {@code range} is given for one table, which it names:
 *
 * <pre>{@code
 * "excelHints": {
 *   "maxHeaderRows": 1,
 *   "workbooks": {"watch_list.xlsx": {"columnGap": 1}},
 *   "tables": {
 *     "report__summary": {"headerRows": 2},
 *     "report__totals": {"range": "summary!H3:K20"}
 *   }
 * }
 * }</pre>
 */
public final class ExcelReadHints {
  /** The most header rows detection takes when no hint caps it. */
  public static final int DEFAULT_MAX_HEADER_ROWS = 3;
  /** The blank rows in a row that end a table when no hint says otherwise. */
  public static final int DEFAULT_TABLE_GAP_ROWS = 2;
  /** The blank columns in a row that separate tables when no hint says otherwise. */
  public static final int DEFAULT_COLUMN_GAP = 2;
  /** The fewest header cells a table has when no hint says otherwise. */
  public static final int DEFAULT_MIN_COLUMNS = 2;
  /** The fewest data rows a table has when no hint says otherwise. */
  public static final int DEFAULT_MIN_DATA_ROWS = 1;

  private static final List<String> TABLE_KEYS =
      Arrays.asList("headerRows", "maxHeaderRows", "minColumns", "minDataRows", "range");
  private static final List<String> WORKBOOK_KEYS =
      Arrays.asList("headerRows", "maxHeaderRows", "tableGapRows", "columnGap", "minColumns",
          "minDataRows");

  /** No hints: every rule as the reader applies it. */
  public static final ExcelReadHints AUTO =
      new ExcelReadHints(Level.NONE, Collections.emptyMap(), Collections.emptyMap());

  private final Level all;
  private final Map<String, Level> workbooks;
  private final Map<String, Level> tables;

  private ExcelReadHints(Level all, Map<String, Level> workbooks, Map<String, Level> tables) {
    this.all = all;
    this.workbooks = workbooks;
    this.tables = tables;
  }

  /** How the reader finds the tables of one workbook. */
  public static final class ForWorkbook {
    public final int tableGapRows;
    public final int columnGap;

    ForWorkbook(int tableGapRows, int columnGap) {
      this.tableGapRows = tableGapRows;
      this.columnGap = columnGap;
    }
  }

  /** How the reader reads one table. */
  public static final class ForTable {
    /** The exact number of header rows, or null to detect them. */
    public final @Nullable Integer headerRows;
    public final int maxHeaderRows;
    public final int minColumns;
    public final int minDataRows;

    ForTable(@Nullable Integer headerRows, int maxHeaderRows, int minColumns, int minDataRows) {
      this.headerRows = headerRows;
      this.maxHeaderRows = maxHeaderRows;
      this.minColumns = minColumns;
      this.minDataRows = minDataRows;
    }
  }

  /** The hints of one level: every workbook, one workbook or one table. */
  private static final class Level {
    static final Level NONE = new Level(Collections.emptyMap(), null);

    final Map<String, Integer> counts;
    final @Nullable String range;

    Level(Map<String, Integer> counts, @Nullable String range) {
      this.counts = counts;
      this.range = range;
    }

    @Nullable Integer get(String key) {
      return counts.get(key);
    }

    @Override public String toString() {
      return new TreeMap<>(counts) + (range == null ? "" : " range=" + range);
    }
  }

  /** The hints of the operand value {@code excelHints}; {@link #AUTO} when it is null. */
  public static ExcelReadHints fromOperand(@Nullable Object value) {
    if (value == null) {
      return AUTO;
    }
    Map<String, Object> map = asMap(value, "excelHints");
    Map<String, Object> own = new LinkedHashMap<>(map);
    Object workbooks = own.remove("workbooks");
    Object tables = own.remove("tables");
    return new ExcelReadHints(level(own, "excelHints", WORKBOOK_KEYS),
        levels(workbooks, "excelHints.workbooks", WORKBOOK_KEYS),
        levels(tables, "excelHints.tables", TABLE_KEYS));
  }

  private static Map<String, Level> levels(@Nullable Object value, String where,
      List<String> keys) {
    if (value == null) {
      return Collections.emptyMap();
    }
    Map<String, Level> levels = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : asMap(value, where).entrySet()) {
      String at = where + "." + entry.getKey();
      levels.put(entry.getKey(), level(asMap(entry.getValue(), at), at, keys));
    }
    return Collections.unmodifiableMap(levels);
  }

  private static Level level(Map<String, Object> map, String where, List<String> keys) {
    Map<String, Integer> counts = new LinkedHashMap<>();
    String range = null;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      String key = entry.getKey();
      if (!keys.contains(key)) {
        throw new IllegalArgumentException(where + ": unknown key '" + key + "'; the keys are "
            + String.join(", ", keys)
            + (where.equals("excelHints") ? ", workbooks and tables" : ""));
      }
      if (key.equals("range")) {
        range = ExcelRange.parse(String.valueOf(entry.getValue()), where + ".range").text;
      } else {
        counts.put(key, count(entry.getValue(), where + "." + key));
      }
    }
    return new Level(Collections.unmodifiableMap(counts), range);
  }

  private static Integer count(@Nullable Object value, String where) {
    if (!(value instanceof Number)
        || ((Number) value).doubleValue() != ((Number) value).intValue()
        || ((Number) value).intValue() < 1) {
      throw new IllegalArgumentException(where + " must be a whole number of 1 or more, not "
          + value);
    }
    return ((Number) value).intValue();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> asMap(@Nullable Object value, String where) {
    if (!(value instanceof Map)) {
      throw new IllegalArgumentException(where + " must be an object, not " + value);
    }
    return (Map<String, Object>) value;
  }

  /** How the reader finds the tables of the workbook {@code fileName}. */
  public ForWorkbook forWorkbook(String fileName) {
    Level workbook = workbooks.getOrDefault(fileName, Level.NONE);
    return new ForWorkbook(
        first(null, workbook, "tableGapRows", DEFAULT_TABLE_GAP_ROWS),
        first(null, workbook, "columnGap", DEFAULT_COLUMN_GAP));
  }

  /**
   * How the reader reads the table {@code table} it finds in the workbook {@code fileName} --
   * named {@code <file>__<sheet>}, and {@code _<title>} after that when a title row stands above
   * it; a name the reader suffixes {@code _t<n>} to tell apart tables that would share one
   * takes that name's hints.
   */
  public ForTable forTable(String fileName, String table) {
    Level workbook = workbooks.getOrDefault(fileName, Level.NONE);
    Level named = tables.getOrDefault(table, Level.NONE);
    Integer headerRows = named.get("headerRows") != null ? named.get("headerRows")
        : workbook.get("headerRows") != null ? workbook.get("headerRows")
        : all.get("headerRows");
    return new ForTable(headerRows,
        first(named, workbook, "maxHeaderRows", DEFAULT_MAX_HEADER_ROWS),
        first(named, workbook, "minColumns", DEFAULT_MIN_COLUMNS),
        first(named, workbook, "minDataRows", DEFAULT_MIN_DATA_ROWS));
  }

  private int first(@Nullable Level named, Level workbook, String key, int auto) {
    Integer value = named == null ? null : named.get(key);
    if (value == null) {
      value = workbook.get(key);
    }
    if (value == null) {
      value = all.get(key);
    }
    return value == null ? auto : value;
  }

  /**
   * The tables the operator gives a range, by name, of the workbook whose tables are named
   * {@code <baseName>__...}.
   */
  public Map<String, ExcelRange> ranges(String baseName) {
    Map<String, ExcelRange> ranges = new LinkedHashMap<>();
    for (Map.Entry<String, Level> entry : tables.entrySet()) {
      String range = entry.getValue().range;
      if (range != null && entry.getKey().startsWith(baseName + "__")) {
        ranges.put(entry.getKey(), ExcelRange.parse(range, "excelHints.tables." + entry.getKey()));
      }
    }
    return ranges;
  }

  /**
   * The hints as text, recorded with a conversion: a workbook converted under other hints is
   * converted again.
   */
  public String fingerprint() {
    return "all=" + all + " workbooks=" + new TreeMap<>(workbooks)
        + " tables=" + new TreeMap<>(tables);
  }

  /**
   * The conversion type a workbook converted under these hints is recorded with: a record of
   * another type (converted under other hints) is converted again.
   */
  public String conversionType() {
    return equals(AUTO) ? "EXCEL_TO_JSON" : "EXCEL_TO_JSON[" + fingerprint() + "]";
  }

  @Override public boolean equals(@Nullable Object o) {
    return o instanceof ExcelReadHints
        && ((ExcelReadHints) o).fingerprint().equals(fingerprint());
  }

  @Override public int hashCode() {
    return Objects.hash(fingerprint());
  }

  @Override public String toString() {
    return fingerprint();
  }
}
