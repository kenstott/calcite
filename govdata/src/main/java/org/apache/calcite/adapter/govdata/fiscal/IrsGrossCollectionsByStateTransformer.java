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
package org.apache.calcite.adapter.govdata.fiscal;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.govdata.energy.EiaBulkXlsxTransformer;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.util.HashMap;
import java.util.Map;

/**
 * Transforms IRS SOI Table 1-5 ("Gross Collections, by Type of Tax and State") into
 * {@code fiscal.irs_gross_collections_by_state_year} rows — one row per state/area per fiscal
 * year, wide across the 11 tax-collection measures the table publishes.
 *
 * <p>The published sheet (named "Table 5" through FY2024, "Table 1-5" from FY2025) is a fixed
 * layout every year checked (FY2019, FY2024, FY2025): title/units rows, a two-row header (row 3
 * groups, row 4 sub-labels) plus a footnote-index row, then data starting at "United States,
 * total" — kept as its own row (state_abbr NULL, matching this table's non-state-bucket
 * convention) rather than dropped, mirroring soi_income_by_county's kept '000' state-total rows.
 * The 50 states + DC follow, then non-state buckets (overseas/territories, Puerto Rico,
 * International, Undistributed) that also get state_abbr NULL, then footnotes. Parsing stops at
 * the first row whose state-or-area cell is null or starts with '[' (a footnote), rather than a
 * hardcoded row count, since the exact row count has drifted by a couple of rows across the
 * years checked.
 */
public class IrsGrossCollectionsByStateTransformer extends EiaBulkXlsxTransformer {

  private static final int DATA_START_ROW = 5; // 0-indexed; row 6 in the spreadsheet ("United States, total")

  private static final Map<String, String> STATE_ABBR = buildStateAbbrMap();

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) throws Exception {
    // effective_year (year - dataLag), not the raw "year" dimension value: the URL's
    // {year_short} template resolves from effective_year, so the file actually fetched for
    // raw year N is FY(N - dataLag) — confirmed live (raw year 2024, dataLag 1 fetched
    // 23dbs01t05co.xlsx, FY2023's file). Stamping fiscal_year from raw "year" instead would
    // silently mislabel every row with the wrong fiscal year.
    String fiscalYear = context.getDimensionValues().get("effective_year");
    Sheet sheet = workbook.getSheetAt(0);
    ArrayNode result = MAPPER.createArrayNode();
    for (int r = DATA_START_ROW; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        break;
      }
      String stateOrArea = cellString(row.getCell(0));
      if (stateOrArea == null || stateOrArea.trim().isEmpty() || stateOrArea.trim().startsWith("[")) {
        break;
      }
      stateOrArea = stateOrArea.trim();
      ObjectNode out = MAPPER.createObjectNode();
      out.put("state_name", stateOrArea);
      String abbr = STATE_ABBR.get(stateOrArea);
      if (abbr != null) {
        out.put("state_abbr", abbr);
      } else {
        out.putNull("state_abbr");
      }
      out.put("fiscal_year", fiscalYear == null ? null : Integer.parseInt(fiscalYear.trim()));
      putNum(out, "total_collections", row.getCell(1));
      putNum(out, "business_income_taxes", row.getCell(2));
      putNum(out, "individual_employment_estate_total", row.getCell(3));
      putNum(out, "individual_withheld_and_fica_tax", row.getCell(4));
      putNum(out, "individual_payments_and_seca_tax", row.getCell(5));
      putNum(out, "unemployment_insurance_tax", row.getCell(6));
      putNum(out, "railroad_retirement_tax", row.getCell(7));
      putNum(out, "estate_and_trust_income_tax", row.getCell(8));
      putNum(out, "estate_tax", row.getCell(9));
      putNum(out, "gift_tax", row.getCell(10));
      putNum(out, "excise_taxes", row.getCell(11));
      result.add(out);
    }
    LOGGER.debug("IRS gross collections by state: emitted {} rows for FY{}", result.size(), fiscalYear);
    return result.toString();
  }

  private void putNum(ObjectNode out, String field, Cell cell) {
    Double v = cellDouble(cell);
    if (v != null) {
      out.put(field, v);
    } else {
      out.putNull(field);
    }
  }

  private static Map<String, String> buildStateAbbrMap() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("Alabama", "AL");
    m.put("Alaska", "AK");
    m.put("Arizona", "AZ");
    m.put("Arkansas", "AR");
    m.put("California", "CA");
    m.put("Colorado", "CO");
    m.put("Connecticut", "CT");
    m.put("Delaware", "DE");
    m.put("District of Columbia", "DC");
    m.put("Florida", "FL");
    m.put("Georgia", "GA");
    m.put("Hawaii", "HI");
    m.put("Idaho", "ID");
    m.put("Illinois", "IL");
    m.put("Indiana", "IN");
    m.put("Iowa", "IA");
    m.put("Kansas", "KS");
    m.put("Kentucky", "KY");
    m.put("Louisiana", "LA");
    m.put("Maine", "ME");
    m.put("Maryland", "MD");
    m.put("Massachusetts", "MA");
    m.put("Michigan", "MI");
    m.put("Minnesota", "MN");
    m.put("Mississippi", "MS");
    m.put("Missouri", "MO");
    m.put("Montana", "MT");
    m.put("Nebraska", "NE");
    m.put("Nevada", "NV");
    m.put("New Hampshire", "NH");
    m.put("New Jersey", "NJ");
    m.put("New Mexico", "NM");
    m.put("New York", "NY");
    m.put("North Carolina", "NC");
    m.put("North Dakota", "ND");
    m.put("Ohio", "OH");
    m.put("Oklahoma", "OK");
    m.put("Oregon", "OR");
    m.put("Pennsylvania", "PA");
    m.put("Rhode Island", "RI");
    m.put("South Carolina", "SC");
    m.put("South Dakota", "SD");
    m.put("Tennessee", "TN");
    m.put("Texas", "TX");
    m.put("Utah", "UT");
    m.put("Vermont", "VT");
    m.put("Virginia", "VA");
    m.put("Washington", "WA");
    m.put("West Virginia", "WV");
    m.put("Wisconsin", "WI");
    m.put("Wyoming", "WY");
    return m;
  }
}
