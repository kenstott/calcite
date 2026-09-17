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

import org.apache.calcite.adapter.file.etl.RequestContext;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * PJM Base Residual Auction (BRA) resource clearing prices — one row per LDA (pricing
 * zone) per delivery year, sourced from PJM's public, keyless per-delivery-year .xlsx
 * download (no PJM Data Miner API key required):
 * https://www.pjm.com/markets-and-operations/rpm.aspx
 *
 * <p>The dimension "year" is the delivery year's start (e.g. 2024 for the "2024/2025"
 * delivery year) and is used directly to build the download URL — there is no
 * publish/data lag here, since PJM posts each delivery year's BRA results as soon as
 * that auction clears, typically 1-3 years ahead of the delivery year itself.
 */
public class PjmCapacityAuctionTransformer extends EiaBulkXlsxTransformer {

  private static final String SHEET_NAME = "BRA Resource Clearing Results";

  // PJM renamed the results file at the 2026/2027 delivery year: delivery years through
  // 2025/2026 use "-base-residual-auction-results.xlsx"; 2026/2027 onward uses
  // "-bra-results.xlsx". Verified live 2026-09-17 against both naming conventions.
  private static final int FILENAME_CHANGE_YEAR = 2026;

  @Override
  public String transform(String response, RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    String yearStr = dims != null ? dims.get("effective_year") : null;
    String year = Objects.requireNonNull(yearStr,
        "effective_year dimension missing — contract violation");
    int deliveryYear = Integer.parseInt(year);
    int deliveryYearEnd = deliveryYear + 1;

    String suffix = deliveryYear >= FILENAME_CHANGE_YEAR
        ? "bra-results" : "base-residual-auction-results";
    String url = String.format(java.util.Locale.ROOT,
        "https://www.pjm.com/-/media/DotCom/markets-ops/rpm/rpm-auction-info/%d-%d/%d-%d-%s.xlsx",
        deliveryYear, deliveryYearEnd, deliveryYear, deliveryYearEnd, suffix);

    byte[] xlsxBytes;
    try {
      xlsxBytes = downloadBytes(url);
    // fallback-guard: allow narrow download-only catch; parse failures below rethrow as RuntimeException instead of being swallowed
    } catch (IOException e) {
      LOGGER.warn("PJM BRA: archive not found at {} ({}); treating as no data", url, e.getMessage());
      return "[]";
    }

    // PJM's CDN returns HTTP 200 with an HTML landing page (not a real 404) for a
    // delivery year whose auction has not cleared yet — downloadBytes() alone cannot
    // tell these apart from a real xlsx, so detect it by content signature instead
    // (OOXML/zip files start with the 'PK' local-file-header magic; the landing page is
    // plain "<!doctype html..." text). Confirmed live 2026-09-17: 2029/2030 (not yet
    // auctioned as of this writing) returns this HTML page at HTTP 200.
    if (!looksLikeZip(xlsxBytes)) {
      LOGGER.warn("PJM BRA: {} returned an HTML landing page, not xlsx — the auction for "
          + "delivery year {}/{} has likely not cleared yet; treating as no data",
          url, deliveryYear, deliveryYearEnd);
      return "[]";
    }

    try {
      return parseBraWorkbook(xlsxBytes, deliveryYear);
    } catch (Exception e) {
      throw new RuntimeException(
          "PJM BRA: archive present but unparseable from " + url + ": " + e.getMessage(), e);
    }
  }

  private boolean looksLikeZip(byte[] bytes) {
    return bytes.length >= 2 && bytes[0] == 'P' && bytes[1] == 'K';
  }

  private String parseBraWorkbook(byte[] xlsxBytes, int deliveryYear) throws Exception {
    ArrayNode result = MAPPER.createArrayNode();
    Workbook wb = WorkbookFactory.create(new ByteArrayInputStream(xlsxBytes));
    try {
      Sheet sheet = wb.getSheet(SHEET_NAME);
      if (sheet == null) {
        throw new IOException("Sheet '" + SHEET_NAME + "' not found for delivery year " + deliveryYear);
      }

      Row headerRow = findHeaderRow(sheet);
      if (headerRow == null) {
        throw new IOException(
            "Resource Clearing Prices header row (first column 'LDA') not found for delivery year "
                + deliveryYear);
      }

      int startRow = headerRow.getRowNum() + 1;
      for (int r = startRow; r <= sheet.getLastRowNum(); r++) {
        Row row = sheet.getRow(r);
        if (row == null) {
          continue;
        }
        String lda = cellString(row.getCell(0));
        if (lda == null || lda.trim().isEmpty()) {
          continue;
        }
        lda = lda.trim();
        // Footnote rows ("*System Marginal Price is...") and the sheet's second table
        // ("Cleared & Make-Whole MWs") both terminate the clearing-price table.
        if (lda.startsWith("*") || "Cleared & Make-Whole MWs".equals(lda)) {
          break;
        }

        Double smp = cellDouble(row.getCell(1));
        Double adder = cellDouble(row.getCell(2));
        Double clearingPrice = cellDouble(row.getCell(3));
        if (smp == null && adder == null && clearingPrice == null) {
          continue;
        }

        ObjectNode out = MAPPER.createObjectNode();
        out.put("delivery_year", deliveryYear);
        out.put("lda", lda);
        putDoubleOrNull(out, "system_marginal_price", smp);
        putDoubleOrNull(out, "locational_price_adder", adder);
        putDoubleOrNull(out, "resource_clearing_price", clearingPrice);
        out.put("auction_type", "Base Residual Auction");
        result.add(out);
      }
    } finally {
      wb.close();
    }
    LOGGER.debug("PJM BRA: parsed {} LDA clearing-price rows for delivery year {}",
        result.size(), deliveryYear);
    return result.toString();
  }

  private void putDoubleOrNull(ObjectNode out, String key, Double value) {
    if (value != null) {
      out.put(key, value);
    } else {
      out.putNull(key);
    }
  }

  private Row findHeaderRow(Sheet sheet) {
    for (int r = 0; r <= Math.min(sheet.getLastRowNum(), 10); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      if ("LDA".equals(cellString(row.getCell(0)))) {
        return row;
      }
    }
    return null;
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) throws Exception {
    // Not used — transform() is overridden above to handle the year-boundary filename
    // swap and HTML-landing-page detection before parsing.
    return "[]";
  }
}
