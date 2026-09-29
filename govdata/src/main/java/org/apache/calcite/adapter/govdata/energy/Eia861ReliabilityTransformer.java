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
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Parses the EIA-861 "Reliability" file (bundled in the same annual ZIP archive that
 * {@link Eia861UtilityTransformer} downloads for {@code eia_utility_annual}) into one row per
 * (report_year, utility_id, state_abbr): the utility's SAIDI / SAIFI / CAIDI interruption
 * indices from the "Reliability_States" sheet.
 *
 * <p>The sheet reports two independent measurement bases side by side (the IEEE 1366 standard
 * and "other" utility-defined standards), and a utility fills in one or the other. Both are kept
 * as separate column groups because the two bases are not comparable. Cells EIA marks "."
 * (not reported) become null. Column positions are identical in every archive from 2013 on,
 * while the number of header rows differs (2013-2016 have two, 2023 has three), so the data
 * rows are located by the "Data Year" header row and the layout is verified against it.
 */
public class Eia861ReliabilityTransformer extends EiaBulkXlsxTransformer {

  private static final String SHEET_NAME = "Reliability_States";

  private static final int COL_YEAR = 0;
  private static final int COL_UTILITY_ID = 1;
  private static final int COL_UTILITY_NAME = 2;
  private static final int COL_STATE = 3;
  private static final int COL_OWNERSHIP = 4;
  private static final int COL_SHORT_FORM = 5;
  private static final int COL_IEEE_SAIDI_MED = 5;
  private static final int COL_IEEE_SAIDI_NO_MED = 8;
  private static final int COL_IEEE_SAIDI_NO_LOS = 11;
  private static final int COL_IEEE_CUSTOMERS = 14;
  private static final int COL_OTHER_SAIDI_MED = 17;
  private static final int COL_OTHER_SAIDI_NO_MED = 20;
  private static final int COL_OTHER_CUSTOMERS = 23;

  private static final String[] IEEE_MED = {
      "ieee_saidi_with_med", "ieee_saifi_with_med", "ieee_caidi_with_med"};
  private static final String[] IEEE_NO_MED = {
      "ieee_saidi_without_med", "ieee_saifi_without_med", "ieee_caidi_without_med"};
  private static final String[] IEEE_NO_LOS = {
      "ieee_saidi_loss_of_supply_removed", "ieee_saifi_loss_of_supply_removed",
      "ieee_caidi_loss_of_supply_removed"};
  private static final String[] OTHER_MED = {
      "other_saidi_with_med", "other_saifi_with_med", "other_caidi_with_med"};
  private static final String[] OTHER_NO_MED = {
      "other_saidi_without_med", "other_saifi_without_med", "other_caidi_without_med"};

  @Override
  public String transform(String response, RequestContext context) {
    String url = context.getUrl();
    Map<String, String> dims = context.getDimensionValues();
    String yearStr = dims != null ? dims.get("effective_year") : null;
    if (yearStr == null || yearStr.isEmpty()) {
      throw new IllegalStateException("EIA-861 Reliability: effective_year dimension missing");
    }
    int year = Integer.parseInt(yearStr);

    // EIA moved 2024+ data out of /archive/zip/ to /zip/ (same move eia_utility_annual handles).
    if (year >= 2024) {
      url = url.replace("/archive/zip/", "/zip/");
    }
    try {
      return parseReliabilityZip(downloadBytes(url), year);
    } catch (Exception e) {
      throw new RuntimeException("EIA-861 Reliability: failed to parse ZIP from " + url, e);
    }
  }

  private String parseReliabilityZip(byte[] zipBytes, int year) throws Exception {
    byte[] xlsx = null;
    ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes));
    try {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String lower = entry.getName().toLowerCase();
        if (lower.contains("reliability") && lower.endsWith(".xlsx")) {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          byte[] buf = new byte[65536];
          int len;
          while ((len = zis.read(buf)) > 0) {
            baos.write(buf, 0, len);
          }
          xlsx = baos.toByteArray();
        }
        zis.closeEntry();
      }
    } finally {
      zis.close();
    }
    if (xlsx == null) {
      throw new IllegalStateException(
          "EIA-861 Reliability: no Reliability workbook in the " + year + " archive "
              + "(EIA publishes it for 2013 onward)");
    }
    XSSFWorkbook wb = new XSSFWorkbook(new ByteArrayInputStream(xlsx));
    try {
      return parseReliabilitySheet(wb, year);
    } finally {
      wb.close();
    }
  }

  private String parseReliabilitySheet(XSSFWorkbook wb, int year) {
    Sheet sheet = wb.getSheet(SHEET_NAME);
    if (sheet == null) {
      throw new IllegalStateException(
          "EIA-861 Reliability: sheet " + SHEET_NAME + " not found for " + year);
    }

    int headerRowNum = -1;
    for (int r = 0; r <= Math.min(sheet.getLastRowNum(), 10); r++) {
      Row row = sheet.getRow(r);
      String first = row != null ? cellString(row.getCell(COL_YEAR)) : null;
      if (first != null && "data year".equals(first.trim().toLowerCase())) {
        headerRowNum = r;
        break;
      }
    }
    if (headerRowNum < 0) {
      throw new IllegalStateException(
          "EIA-861 Reliability: 'Data Year' header row not found for " + year);
    }
    Row header = sheet.getRow(headerRowNum);
    String col5 = cellString(header.getCell(COL_SHORT_FORM));
    int shift = col5 != null && "short form".equals(col5.trim().toLowerCase()) ? 1 : 0;
    requireHeader(header, COL_UTILITY_ID, "utility number", year);
    requireHeader(header, COL_STATE, "state", year);
    requireHeader(header, COL_IEEE_SAIDI_MED + shift, "saidi", year);
    requireHeader(header, COL_IEEE_CUSTOMERS + shift, "number of customers", year);
    requireHeader(header, COL_OTHER_SAIDI_MED + shift, "saidi", year);
    requireHeader(header, COL_OTHER_CUSTOMERS + shift, "number of customers", year);

    ArrayNode result = MAPPER.createArrayNode();
    for (int r = headerRowNum + 1; r <= sheet.getLastRowNum(); r++) {
      Row row = sheet.getRow(r);
      if (row == null) {
        continue;
      }
      Integer utilId = cellInt(row.getCell(COL_UTILITY_ID));
      String state = cellString(row.getCell(COL_STATE));
      if (utilId == null || state == null || state.trim().isEmpty()) {
        // footnote rows below the data carry no utility number
        continue;
      }

      ObjectNode out = MAPPER.createObjectNode();
      out.put("report_year", year);
      out.put("utility_id", utilId);
      putText(out, "utility_name", cellString(row.getCell(COL_UTILITY_NAME)));
      out.put("state_abbr", state.trim());
      putText(out, "ownership_type", cellString(row.getCell(COL_OWNERSHIP)));
      putIndices(out, IEEE_MED, row, COL_IEEE_SAIDI_MED + shift);
      putIndices(out, IEEE_NO_MED, row, COL_IEEE_SAIDI_NO_MED + shift);
      putIndices(out, IEEE_NO_LOS, row, COL_IEEE_SAIDI_NO_LOS + shift);
      putNumber(out, "ieee_customers", cellDouble(row.getCell(COL_IEEE_CUSTOMERS + shift)));
      putIndices(out, OTHER_MED, row, COL_OTHER_SAIDI_MED + shift);
      putIndices(out, OTHER_NO_MED, row, COL_OTHER_SAIDI_NO_MED + shift);
      putNumber(out, "other_customers", cellDouble(row.getCell(COL_OTHER_CUSTOMERS + shift)));
      result.add(out);
    }

    if (result.size() == 0) {
      throw new IllegalStateException("EIA-861 Reliability: no data rows parsed for " + year);
    }
    return result.toString();
  }

  private void requireHeader(Row header, int col, String expectedPrefix, int year) {
    String hdr = cellString(header.getCell(col));
    if (hdr == null || !hdr.trim().toLowerCase().startsWith(expectedPrefix)) {
      throw new IllegalStateException("EIA-861 Reliability: column " + col + " header is '"
          + hdr + "', expected '" + expectedPrefix + "...' for " + year
          + " — workbook layout changed");
    }
  }

  private void putIndices(ObjectNode out, String[] names, Row row, int firstCol) {
    for (int i = 0; i < names.length; i++) {
      putNumber(out, names[i], cellDouble(row.getCell(firstCol + i)));
    }
  }

  private void putNumber(ObjectNode out, String name, Double value) {
    if (value == null) {
      out.putNull(name);
    } else {
      out.put(name, value);
    }
  }

  private void putText(ObjectNode out, String name, String value) {
    if (value == null || value.trim().isEmpty()) {
      out.putNull(name);
    } else {
      out.put(name, value.trim());
    }
  }

  @Override
  protected String parseWorkbook(XSSFWorkbook workbook, RequestContext context) throws Exception {
    // Never invoked: transform() above is fully overridden because the source is a ZIP archive.
    return parseReliabilitySheet(workbook, 0);
  }
}
