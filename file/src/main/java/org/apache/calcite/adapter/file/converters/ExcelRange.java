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

import org.apache.poi.ss.SpreadsheetVersion;
import org.apache.poi.ss.util.AreaReference;
import org.apache.poi.ss.util.CellReference;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A block of cells of one sheet, as {@code Sheet!A3:F200} names it (a sheet name with spaces is
 * quoted, {@code 'Q1 results'!A1:D9}).
 */
public final class ExcelRange {
  /** The text the range was given as. */
  public final String text;
  public final String sheet;
  public final int firstRow;
  public final int firstCol;
  public final int lastRow;
  public final int lastCol;

  ExcelRange(String text, String sheet, int firstRow, int firstCol, int lastRow, int lastCol) {
    this.text = text;
    this.sheet = sheet;
    this.firstRow = firstRow;
    this.firstCol = firstCol;
    this.lastRow = lastRow;
    this.lastCol = lastCol;
  }

  /** The range {@code text} names; refused, naming {@code where}, when it names none. */
  public static ExcelRange parse(String text, String where) {
    AreaReference area;
    try {
      area = new AreaReference(text, SpreadsheetVersion.EXCEL2007);
    } catch (IllegalArgumentException | IllegalStateException e) {
      throw new IllegalArgumentException(where + ": '" + text
          + "' is not a range of one sheet such as Sheet!A3:F200 (" + e.getMessage() + ")", e);
    }
    CellReference first = area.getFirstCell();
    CellReference last = area.getLastCell();
    @Nullable String sheet = first.getSheetName();
    if (sheet == null) {
      throw new IllegalArgumentException(where + ": '" + text
          + "' names no sheet; give it as Sheet!A3:F200");
    }
    return new ExcelRange(text, sheet,
        Math.min(first.getRow(), last.getRow()), Math.min(first.getCol(), last.getCol()),
        Math.max(first.getRow(), last.getRow()), Math.max(first.getCol(), last.getCol()));
  }

  boolean overlaps(int r0, int c0, int r1, int c1) {
    return firstRow <= r1 && r0 <= lastRow && firstCol <= c1 && c0 <= lastCol;
  }

  @Override public String toString() {
    return text;
  }
}
