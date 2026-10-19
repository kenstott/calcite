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

import org.apache.calcite.adapter.file.util.SmartCasing;
import org.apache.calcite.util.trace.CalciteLogger;

import org.apache.poi.ss.SpreadsheetVersion;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.Date1904Support;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Name;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.AreaReference;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFTable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Finds the tables of one sheet and reads their rows.
 *
 * <p>A table is a block of cells. The workbook's own tables come first, as it declares them:
 * a range the operator gives a table ({@link ExcelReadHints}), then each Excel table ("Format as
 * Table"), then each named range of a single block of this sheet; a block overlapping one
 * already taken is not taken again. The rest of the sheet is searched for blocks of cells
 * separated by {@code tableGapRows} blank rows and {@code columnGap} blank columns (2 each), and
 * each block is read for its tables:
 *
 * <ul>
 * <li>A row holding a single cell, above a row of more, is the table's title, and names it.
 * <li>Its header rows follow: a row of at least {@code minColumns} cells (2) that reads as
 * labels -- text, none of it longer than 50 characters or on several lines, none of it an id, a
 * hex string, an email address or a URL, and no number over 1000, the text outnumbering the
 * numbers more than two to one. A wider row after it is a further header row (a group header
 * spans the columns it groups, so it is sparse); a row no wider is data. At most {@code maxHeaderRows} (3) rows are header rows; {@code headerRows} gives
 * their number exactly instead.
 * <li>Its data rows follow, to the end of the block or to {@code tableGapRows} blank rows; a
 * single blank row inside a table is skipped. Its columns are those its header rows have a cell
 * in; a table with fewer than {@code minDataRows} (1) data rows is not read.
 * </ul>
 *
 * <p>A cell is blank when it holds nothing or an empty string; a formula's cell is its value.
 * A row that starts no table is passed over.
 */
final class ExcelTableFinder {
  private static final CalciteLogger LOGGER =
      new CalciteLogger(LoggerFactory.getLogger(ExcelTableFinder.class));

  /** A table found: its name when the workbook or the operator names it, else its title. */
  static final class Found {
    final @Nullable String name;
    final @Nullable String title;
    final ArrayNode rows;
    final int top;
    final int left;

    Found(@Nullable String name, @Nullable String title, ArrayNode rows, int top, int left) {
      this.name = name;
      this.title = title;
      this.rows = rows;
      this.top = top;
      this.left = left;
    }
  }

  /** What a cell holds, judged by its value. */
  private enum Kind { BLANK, TEXT, NUMBER, OTHER }

  /** A cell's value: a formula's is the value it evaluates to. */
  private static final class Value {
    static final Value BLANK = new Value(Kind.BLANK, "", 0, false, false);

    final Kind kind;
    final String text;
    final double number;
    final boolean bool;
    final boolean date;

    Value(Kind kind, String text, double number, boolean bool, boolean date) {
      this.kind = kind;
      this.text = text;
      this.number = number;
      this.bool = bool;
      this.date = date;
    }
  }

  private final Sheet sheet;
  private final FormulaEvaluator evaluator;
  private final ObjectMapper mapper;
  private final String columnNameCasing;
  private final String tableNameCasing;
  private final ExcelReadHints hints;
  private final String fileName;
  private final String sheetTable;
  private final ExcelReadHints.ForWorkbook gaps;
  private final boolean date1904;
  private final Map<Long, Value> values = new HashMap<>();
  private final List<int[]> taken = new ArrayList<>();
  private final int lastRow;
  private final int lastCol;

  ExcelTableFinder(Sheet sheet, FormulaEvaluator evaluator, ObjectMapper mapper,
      String tableNameCasing, String columnNameCasing, ExcelReadHints hints, String fileName,
      String sheetTable) {
    this.sheet = sheet;
    this.evaluator = evaluator;
    this.mapper = mapper;
    this.tableNameCasing = tableNameCasing;
    this.columnNameCasing = columnNameCasing;
    this.hints = hints;
    this.fileName = fileName;
    this.sheetTable = sheetTable;
    this.gaps = hints.forWorkbook(fileName);
    Workbook workbook = sheet.getWorkbook();
    this.date1904 = workbook instanceof Date1904Support
        && ((Date1904Support) workbook).isDate1904();
    this.lastRow = sheet.getLastRowNum();
    int last = -1;
    for (Row row : sheet) {
      last = Math.max(last, row.getLastCellNum() - 1);
    }
    this.lastCol = last;
  }

  /** The name the reader gives a table the workbook does not name, by its title. */
  String nameFor(@Nullable String title) {
    return title == null || title.trim().isEmpty()
        ? sheetTable
        : sheetTable + "_" + SmartCasing.applyCasing(
            ConverterUtils.sanitizeIdentifier(title), tableNameCasing);
  }

  /** The sheet's tables, in the order they stand in it (top to bottom, left to right). */
  List<Found> find(Map<String, ExcelRange> ranges) {
    List<Found> found = new ArrayList<>();
    for (Map.Entry<String, ExcelRange> entry : ranges.entrySet()) {
      ExcelRange range = entry.getValue();
      if (range.sheet.equals(sheet.getSheetName())) {
        readTaken(found, entry.getKey(), range.firstRow, range.firstCol, range.lastRow,
            range.lastCol, null, 0);
      }
    }
    if (sheet instanceof XSSFSheet) {
      for (XSSFTable table : ((XSSFSheet) sheet).getTables()) {
        AreaReference area = table.getArea();
        CellReference first = area.getFirstCell();
        CellReference last = area.getLastCell();
        int headerRows = table.getHeaderRowCount();
        readTaken(found, null, first.getRow(), first.getCol(), last.getRow(), last.getCol(),
            headerRows > 0 ? headerRows : null, table.getTotalsRowCount());
      }
    }
    int sheetIndex = sheet.getWorkbook().getSheetIndex(sheet);
    for (Name name : sheet.getWorkbook().getAllNames()) {
      int[] block = namedBlock(name, sheetIndex);
      if (block != null) {
        readTaken(found, null, block[0], block[1], block[2], block[3], null, 0);
      }
    }
    if (lastCol >= 0) {
      for (int[] block : blocks(0, 0, lastRow, lastCol)) {
        found.addAll(tablesIn(block));
      }
    }
    found.sort(Comparator.<Found>comparingInt(f -> f.top).thenComparingInt(f -> f.left));
    return found;
  }

  /**
   * The block of this sheet a named range is ({first row, first column, last row, last
   * column}); null for a name that is no single block of this sheet: a function, a hidden or
   * built-in name (a print area, a filter), a formula, a reference that is broken or spans
   * several blocks or another sheet.
   */
  private int @Nullable [] namedBlock(Name name, int sheetIndex) {
    if (name.isFunctionName() || name.isHidden() || name.getNameName().startsWith("_xlnm")) {
      return null;
    }
    if (name.getSheetIndex() != -1 && name.getSheetIndex() != sheetIndex) {
      return null;
    }
    String formula = name.getRefersToFormula();
    if (formula == null || !AreaReference.isContiguous(formula)) {
      return null;
    }
    AreaReference area;
    try {
      area = new AreaReference(formula, SpreadsheetVersion.EXCEL2007);
    } catch (IllegalArgumentException | IllegalStateException e) {
      // A name refers to a formula or a constant as well as to cells: it is no block.
      LOGGER.debug("name " + name.getNameName() + " is no block of cells: " + e.getMessage());
      return null;
    }
    CellReference first = area.getFirstCell();
    CellReference last = area.getLastCell();
    if (!sheet.getSheetName().equals(first.getSheetName())) {
      return null;
    }
    return new int[] {first.getRow(), first.getCol(), last.getRow(), last.getCol()};
  }

  /** Read a block the workbook or the operator declares as one table, and take it. */
  private void readTaken(List<Found> found, @Nullable String name, int r0, int c0, int r1,
      int c1, @Nullable Integer headerRows, int totalsRows) {
    for (int[] t : taken) {
      if (t[0] <= r1 && r0 <= t[2] && t[1] <= c1 && c0 <= t[3]) {
        return;
      }
    }
    ExcelReadHints.ForTable table =
        hints.forTable(fileName, name != null ? name : sheetTable);
    Integer exact = table.headerRows != null ? table.headerRows : headerRows;
    List<Integer> header = new ArrayList<>();
    int row = r0;
    if (exact != null) {
      for (; row <= r1 && header.size() < exact; row++) {
        header.add(row);
      }
    } else {
      row = headerRowsAt(r0, r1, c0, c1, table, header);
    }
    if (!header.isEmpty()) {
      ArrayNode rows = rows(header, row, r1 - totalsRows, c0, c1);
      if (rows.size() >= table.minDataRows) {
        found.add(new Found(name, null, rows, r0, c0));
      }
    }
    taken.add(new int[] {r0, c0, r1, c1});
  }

  /** The blocks of cells within these bounds, separated by blank rows and columns. */
  private List<int[]> blocks(int r0, int c0, int r1, int c1) {
    List<int[]> out = new ArrayList<>();
    for (int[] band : split(r0, c0, r1, c1, true)) {
      List<int[]> parts = split(band[0], band[1], band[2], band[3], false);
      if (parts.size() == 1 && java.util.Arrays.equals(parts.get(0), band)) {
        out.add(band);
      } else {
        for (int[] part : parts) {
          out.addAll(blocks(part[0], part[1], part[2], part[3]));
        }
      }
    }
    return out;
  }

  /**
   * The parts of these bounds between runs of blank rows (or columns) as long as the gap, each
   * trimmed to the rows and columns that hold a cell.
   */
  private List<int[]> split(int r0, int c0, int r1, int c1, boolean byRows) {
    int gap = byRows ? gaps.tableGapRows : gaps.columnGap;
    int from = byRows ? r0 : c0;
    int to = byRows ? r1 : c1;
    List<int[]> parts = new ArrayList<>();
    int start = -1;
    int lastFilled = -1;
    int blanks = 0;
    for (int i = from; i <= to; i++) {
      boolean blank = byRows ? rowBlank(i, c0, c1) : columnBlank(i, r0, r1);
      if (blank) {
        blanks++;
        if (start >= 0 && blanks >= gap) {
          parts.add(trimmed(byRows, start, lastFilled, r0, c0, r1, c1));
          start = -1;
        }
      } else {
        if (start < 0) {
          start = i;
        }
        lastFilled = i;
        blanks = 0;
      }
    }
    if (start >= 0) {
      parts.add(trimmed(byRows, start, lastFilled, r0, c0, r1, c1));
    }
    return parts;
  }

  private int[] trimmed(boolean byRows, int start, int end, int r0, int c0, int r1, int c1) {
    int top = byRows ? start : r0;
    int bottom = byRows ? end : r1;
    int left = byRows ? c0 : start;
    int right = byRows ? c1 : end;
    while (top < bottom && rowBlank(top, left, right)) {
      top++;
    }
    while (bottom > top && rowBlank(bottom, left, right)) {
      bottom--;
    }
    while (left < right && columnBlank(left, top, bottom)) {
      left++;
    }
    while (right > left && columnBlank(right, top, bottom)) {
      right--;
    }
    return new int[] {top, left, bottom, right};
  }

  /** The tables of one block, top to bottom. */
  private List<Found> tablesIn(int[] block) {
    List<Found> found = new ArrayList<>();
    int row = block[0];
    while (row <= block[2]) {
      if (rowBlank(row, block[1], block[3])) {
        row++;
        continue;
      }
      int[] end = new int[1];
      Found table = tableAt(block, row, end);
      if (end[0] >= row) {
        if (table != null) {
          found.add(table);
        }
        row = end[0] + 1;
      } else {
        row++;
      }
    }
    return found;
  }

  /**
   * The table whose title or first header row is the row {@code start} of the block, or null
   * when it is too small to read; {@code end[0]} is set to its last row, or below
   * {@code start} when no table starts there.
   */
  private @Nullable Found tableAt(int[] block, int start, int[] end) {
    end[0] = start - 1;
    int c0 = block[1];
    int c1 = block[3];
    String title = null;
    int headerStart = start;
    if (filled(start, c0, c1) == 1) {
      int next = start + 1;
      while (next <= block[2] && rowBlank(next, c0, c1)) {
        next++;
      }
      if (next <= block[2] && next - start <= gaps.tableGapRows && filled(next, c0, c1) > 1) {
        title = text(firstFilled(start, c0, c1), start);
        headerStart = next;
      }
    }
    ExcelReadHints.ForTable table = hints.forTable(fileName, nameFor(title));
    List<Integer> header = new ArrayList<>();
    int row;
    if (table.headerRows != null) {
      row = headerStart;
      int blanks = 0;
      while (row <= block[2] && header.size() < table.headerRows) {
        if (rowBlank(row, c0, c1)) {
          if (++blanks >= gaps.tableGapRows) {
            break;
          }
        } else {
          blanks = 0;
          header.add(row);
        }
        row++;
      }
    } else {
      row = headerRowsAt(headerStart, block[2], c0, c1, table, header);
    }
    if (header.isEmpty()) {
      return null;
    }
    int last = header.get(header.size() - 1);
    int blanks = 0;
    for (int r = row; r <= block[2]; r++) {
      if (rowBlank(r, c0, c1)) {
        if (++blanks >= gaps.tableGapRows) {
          break;
        }
      } else {
        blanks = 0;
        last = r;
      }
    }
    end[0] = last;
    int left = Integer.MAX_VALUE;
    int right = -1;
    for (int r : header) {
      for (int c = c0; c <= c1; c++) {
        if (kind(r, c) != Kind.BLANK) {
          left = Math.min(left, c);
          right = Math.max(right, c);
        }
      }
    }
    ArrayNode rows = rows(header, row, last, left, right);
    if (rows.size() < table.minDataRows) {
      return null;
    }
    return new Found(null, title, rows, start, c0);
  }

  /**
   * Detect the header rows from the row {@code from}, adding them to {@code header}; the row
   * after the last of them.
   */
  private int headerRowsAt(int from, int to, int c0, int c1, ExcelReadHints.ForTable table,
      List<Integer> header) {
    int row = from;
    int blanks = 0;
    while (row <= to && header.size() < table.maxHeaderRows) {
      if (rowBlank(row, c0, c1)) {
        if (++blanks >= gaps.tableGapRows) {
          break;
        }
      } else {
        blanks = 0;
        boolean isHeader = header.isEmpty()
            ? looksLikeHeader(row, c0, c1, table.minColumns)
            : looksLikeHeaderAfter(row, header.get(header.size() - 1), c0, c1, table.minColumns);
        if (isHeader) {
          header.add(row);
        } else if (!header.isEmpty()) {
          break;
        } else {
          return row;
        }
      }
      row++;
    }
    return row;
  }

  /** Whether the row reads as a header row: labels, not data. */
  private boolean looksLikeHeader(int row, int c0, int c1, int minColumns) {
    if (filled(row, c0, c1) < minColumns) {
      return false;
    }
    int textCells = 0;
    int numericCells = 0;
    boolean largeNumber = false;
    for (int c = c0; c <= c1; c++) {
      Value value = value(row, c);
      if (value.kind == Kind.TEXT) {
        String text = value.text.trim();
        if (text.length() > 50 || text.contains("\n") || isDataPattern(text)) {
          return false;
        }
        textCells++;
      } else if (value.kind == Kind.NUMBER) {
        numericCells++;
        if (value.number > 1000) {
          largeNumber = true;
        }
      }
    }
    if (largeNumber) {
      return false;
    }
    if (numericCells == 0 && textCells >= 2) {
      return true;
    }
    if (numericCells > 0) {
      return textCells > numericCells * 2;
    }
    return textCells > 0;
  }

  /**
   * Whether the row is a further header row after the header row {@code previous}: wider than
   * it, as a detail header row is below the group header row whose cells span the columns they
   * group. A row no wider is data, whatever it holds.
   */
  private boolean looksLikeHeaderAfter(int row, int previous, int c0, int c1, int minColumns) {
    int cells = filled(row, c0, c1);
    return cells >= minColumns && cells > filled(previous, c0, c1);
  }

  /** Whether text is clearly data, not a label: an id, a hex string, an email address, a URL. */
  static boolean isDataPattern(String value) {
    return value.matches(
        "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
        || value.matches("^(0x|#)?[0-9a-fA-F]{6,}$")
        || (value.length() > 20 && value.matches("^[A-Za-z0-9+/]+=*$"))
        || value.matches("^[\\w._%+-]+@[\\w.-]+\\.[A-Za-z]{2,}$")
        || value.matches("^https?://.*") || value.matches("^www\\..*");
  }

  /** The data rows from {@code from} to {@code to}, under the header rows' column names. */
  private ArrayNode rows(List<Integer> header, int from, int to, int c0, int c1) {
    ArrayNode rows = mapper.createArrayNode();
    if (c1 < c0) {
      return rows;
    }
    Map<Integer, String> columns = columnNames(header, c0, c1);
    for (int r = from; r <= to; r++) {
      if (rowBlank(r, c0, c1)) {
        continue;
      }
      ObjectNode row = mapper.createObjectNode();
      for (int c = c0; c <= c1; c++) {
        String column = columns.get(c);
        if (column != null) {
          Object value = jsonValue(r, c);
          if (value == null) {
            row.putNull(column);
          } else {
            row.putPOJO(column, value);
          }
        }
      }
      if (row.size() > 0) {
        rows.add(row);
      }
    }
    return rows;
  }

  /**
   * Each column's name: its header cell, after the group headers above it that span it (a group
   * header names the columns from its own to the next group header's).
   */
  private Map<Integer, String> columnNames(List<Integer> header, int c0, int c1) {
    Map<Integer, String> names = new HashMap<>();
    int detail = header.get(header.size() - 1);
    Map<Integer, String> groups = new HashMap<>();
    for (int i = 0; i < header.size() - 1; i++) {
      int groupRow = header.get(i);
      String group = null;
      for (int c = c0; c <= c1; c++) {
        if (kind(groupRow, c) != Kind.BLANK) {
          group = text(c, groupRow);
        }
        if (group != null) {
          groups.merge(c, group, (above, below) -> above + "_" + below);
        }
      }
    }
    for (int c = c0; c <= c1; c++) {
      String name = groups.getOrDefault(c, "");
      if (kind(detail, c) != Kind.BLANK) {
        name = name.isEmpty() ? text(c, detail) : name + "_" + text(c, detail);
      }
      if (!name.isEmpty()) {
        names.put(c,
            ConverterUtils.sanitizeIdentifier(SmartCasing.applyCasing(name, columnNameCasing)));
      }
    }
    return names;
  }

  private boolean rowBlank(int row, int c0, int c1) {
    return filled(row, c0, c1) == 0;
  }

  private boolean columnBlank(int col, int r0, int r1) {
    for (int r = r0; r <= r1; r++) {
      if (kind(r, col) != Kind.BLANK) {
        return false;
      }
    }
    return true;
  }

  private int filled(int row, int c0, int c1) {
    int count = 0;
    for (int c = c0; c <= c1; c++) {
      if (kind(row, c) != Kind.BLANK) {
        count++;
      }
    }
    return count;
  }

  private int firstFilled(int row, int c0, int c1) {
    for (int c = c0; c <= c1; c++) {
      if (kind(row, c) != Kind.BLANK) {
        return c;
      }
    }
    return -1;
  }

  private Kind kind(int row, int col) {
    return value(row, col).kind;
  }

  /**
   * The cell's value; blank for a cell a declared table has taken (the search for the rest of
   * the sheet passes over it), for an empty cell and for one holding an empty string.
   */
  private Value value(int row, int col) {
    for (int[] t : taken) {
      if (t[0] <= row && row <= t[2] && t[1] <= col && col <= t[3]) {
        return Value.BLANK;
      }
    }
    return values.computeIfAbsent((long) row << 16 | col, k -> evaluate(row, col));
  }

  private Value evaluate(int row, int col) {
    Row r = sheet.getRow(row);
    Cell cell = r == null ? null : r.getCell(col);
    if (cell == null) {
      return Value.BLANK;
    }
    CellType type = cell.getCellType();
    String text = "";
    double number = 0;
    boolean bool = false;
    if (type == CellType.FORMULA) {
      CellValue evaluated = evaluator.evaluate(cell);
      if (evaluated == null) {
        return Value.BLANK;
      }
      type = evaluated.getCellType();
      if (type == CellType.STRING) {
        text = evaluated.getStringValue();
      } else if (type == CellType.NUMERIC) {
        number = evaluated.getNumberValue();
      } else if (type == CellType.BOOLEAN) {
        bool = evaluated.getBooleanValue();
      }
    } else if (type == CellType.STRING) {
      text = cell.getStringCellValue();
    } else if (type == CellType.NUMERIC) {
      number = cell.getNumericCellValue();
    } else if (type == CellType.BOOLEAN) {
      bool = cell.getBooleanCellValue();
    }
    switch (type) {
    case STRING:
      return text.isEmpty() ? Value.BLANK : new Value(Kind.TEXT, text, 0, false, false);
    case NUMERIC:
      boolean date = DateUtil.isADateFormat(cell.getCellStyle().getDataFormat(),
          cell.getCellStyle().getDataFormatString()) && DateUtil.isValidExcelDate(number);
      return new Value(Kind.NUMBER, "", number, false, date);
    case BOOLEAN:
      return new Value(Kind.OTHER, "", 0, bool, false);
    case ERROR:
      return new Value(Kind.OTHER, "", 0, false, false);
    default:
      return Value.BLANK;
    }
  }

  /** The cell's value as a label (a column name, a title). */
  @SuppressWarnings("JavaUtilDate")
  private String text(int col, int row) {
    Value value = value(row, col);
    switch (value.kind) {
    case TEXT:
      return value.text;
    case NUMBER:
      return value.date ? DateUtil.getJavaDate(value.number, date1904).toString()
          : String.valueOf(value.number);
    case OTHER:
      return String.valueOf(value.bool);
    default:
      return "";
    }
  }

  /** The cell's value as its table holds it: text, a whole number, a number, a date, a flag. */
  private @Nullable Object jsonValue(int row, int col) {
    Value value = value(row, col);
    switch (value.kind) {
    case TEXT:
      return value.text;
    case NUMBER:
      if (value.date) {
        Date date = DateUtil.getJavaDate(value.number, date1904);
        return date;
      }
      return value.number == (long) value.number ? (Object) (long) value.number : value.number;
    case OTHER:
      Row r = sheet.getRow(row);
      Cell cell = r == null ? null : r.getCell(col);
      boolean error = cell != null && (cell.getCellType() == CellType.ERROR
          || (cell.getCellType() == CellType.FORMULA
              && cell.getCachedFormulaResultType() == CellType.ERROR));
      return error ? null : value.bool;
    default:
      return null;
    }
  }
}
