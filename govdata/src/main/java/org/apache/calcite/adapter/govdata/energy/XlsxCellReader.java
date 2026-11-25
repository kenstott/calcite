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
package org.apache.calcite.adapter.govdata.energy;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;

/** Typed reads of single POI cells, shared by the XLSX transformers and providers. */
public abstract class XlsxCellReader {

  protected String cellString(Cell cell) {
    if (cell == null) {
      return null;
    }
    CellType cellType = cell.getCellType();
    if (cellType == CellType.STRING) {
      return cell.getStringCellValue();
    }
    if (cellType == CellType.NUMERIC) {
      if (DateUtil.isCellDateFormatted(cell)) {
        return cell.getLocalDateTimeCellValue().toString();
      }
      double d = cell.getNumericCellValue();
      if (d == Math.floor(d) && !Double.isInfinite(d)) {
        return String.valueOf((long) d);
      }
      return String.valueOf(d);
    }
    if (cellType == CellType.BOOLEAN) {
      return String.valueOf(cell.getBooleanCellValue());
    }
    if (cellType == CellType.FORMULA) {
      try {
        return String.valueOf(cell.getStringCellValue());
      // fallback-guard: allow standard POI formula-cell fallback to the cached numeric value when cached string type doesn't match
      } catch (Exception e) {
        try {
          return String.valueOf(cell.getNumericCellValue());
        // fallback-guard: allow inner formula-cell fallback; null means neither cached string nor numeric was readable for one display string
        } catch (Exception ex) {
          return null;
        }
      }
    }
    return null;
  }

  protected Double cellDouble(Cell cell) {
    if (cell == null) {
      return null;
    }
    CellType cellType = cell.getCellType();
    if (cellType == CellType.NUMERIC) {
      return cell.getNumericCellValue();
    }
    if (cellType == CellType.STRING) {
      String s = cell.getStringCellValue().trim();
      if (s.isEmpty()) {
        return null;
      }
      try {
        return Double.parseDouble(s.replace(",", ""));
      // fallback-guard: allow per-cell numeric parser; null marks one cell unparseable
      } catch (NumberFormatException e) {
        return null;
      }
    }
    if (cellType == CellType.FORMULA) {
      try {
        return cell.getNumericCellValue();
      // fallback-guard: allow formula-cell fallback; a formula cell without a cached numeric value yields null for that one cell
      } catch (Exception e) {
        return null;
      }
    }
    return null;
  }

  protected Integer cellInt(Cell cell) {
    Double d = cellDouble(cell);
    if (d == null) {
      return null;
    }
    return d.intValue();
  }
}
