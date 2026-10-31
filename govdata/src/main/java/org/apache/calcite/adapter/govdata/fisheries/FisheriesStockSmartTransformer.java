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
package org.apache.calcite.adapter.govdata.fisheries;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Turns the NOAA Stock SMART stock list into one row per stock assessment, with the
 * assessment's fishing mortality and biomass reference points.
 *
 * <p>The table's source URL is the portal's {@code sis_servlet} {@code searchEntities} call,
 * which returns {@code {"data":[{"id":"10455","name":"..."}, ...]}}. The ids are the only thing
 * taken from that response: they go into one {@code data-export-servlet} "Assessment Summary
 * Data" request, which answers with an xlsx (binary, so it is downloaded here, not through the
 * text pipeline, as {@code EiaBulkXlsxTransformer} does). The workbook's first sheet is a
 * header row followed by one row per assessment.
 *
 * <p>Every output column is looked up by its workbook header, so a renamed or dropped column
 * fails the load instead of shifting values. A blank cell is emitted as null: the source
 * records no reference point for that assessment.
 */
public class FisheriesStockSmartTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String EXPORT_URL =
      "https://apps-st.fisheries.noaa.gov/stocksmart/data-export-servlet";

  private static final String T_STRING = "s";
  private static final String T_INT = "i";
  private static final String T_LONG = "l";
  private static final String T_DOUBLE = "d";
  private static final String T_FLAG = "f";

  /** Workbook header, output column, type — in output order. */
  private static final String[][] COLUMNS = {
      {"Stock ID", "stock_id", T_LONG},
      {"Stock Name", "stock_name", T_STRING},
      {"Jurisdiction", "jurisdiction", T_STRING},
      {"FMP", "fmp", T_STRING},
      {"Science Center", "science_center", T_STRING},
      {"Regional Ecosystem", "regional_ecosystem", T_STRING},
      {"FSSI Stock?", "is_fssi", T_FLAG},
      {"ITIS Taxon Serial Number", "tsn", T_LONG},
      {"Scientific Name", "scientific_name", T_STRING},
      {"Common Name", "common_name", T_STRING},
      {"Stock Area", "stock_area", T_STRING},
      {"Assessment ID", "assessment_id", T_LONG},
      {"Assessment Year", "assessment_year", T_INT},
      {"Assessment Month", "assessment_month", T_INT},
      {"Last Data Year", "last_data_year", T_INT},
      {"Update Type", "update_type", T_STRING},
      {"Review Result", "review_result", T_STRING},
      {"Assessment Model", "assessment_model", T_STRING},
      {"F Year", "f_year", T_INT},
      {"Estimated F", "estimated_f", T_DOUBLE},
      {"F Unit", "f_unit", T_STRING},
      {"Fmsy", "fmsy", T_DOUBLE},
      {"F/Fmsy", "f_fmsy_ratio", T_DOUBLE},
      {"B Year", "b_year", T_INT},
      {"Estimated B", "estimated_b", T_DOUBLE},
      {"B Unit", "b_unit", T_STRING},
      {"Bmsy", "bmsy", T_DOUBLE},
      {"B/Bmsy", "b_bmsy_ratio", T_DOUBLE},
      {"MSY", "msy", T_DOUBLE},
      {"MSY Unit", "msy_unit", T_STRING},
  };

  private static final String FIELDS =
      "as_year,as_month,as_last_data_year,as_update_type,as_review_type,as_model,"
          + "as_f_year,as_f_best,as_f_unit,as_fmsy,as_f_fmsy_ratio,"
          + "as_b_year,as_b_best,as_b_unit,as_bmsy,as_b_bmsy_ratio,as_msy,as_msy_unit";
  private static final String FIELD_LABELS =
      "Assessment Year,Assessment Month,Last Data Year,Update Type,Review Result,"
          + "Assessment Model,F Year,Estimated F,F Unit,Fmsy,F/Fmsy,B Year,Estimated B,B Unit,"
          + "Bmsy,B/Bmsy,MSY,MSY Unit";
  private static final String ENTITY_ATTRS =
      "ent_name,jur_name,fmp_name,sc_name,eco_name,ent_fssi_flag,tsn,sn,cn,sa";
  private static final String ENTITY_LABELS =
      "Stock Name,Jurisdiction,FMP,Science Center,Regional Ecosystem,FSSI Stock?,"
          + "ITIS Taxon Serial Number,Scientific Name,Common Name,Stock Area";

  @Override public String transform(String response, RequestContext context) {
    String ids = stockIds(response, context.getUrl());
    String url = EXPORT_URL + "?fileTypeList=&jsonParam="
        + encode(exportCriteria(ids));
    try {
      byte[] xlsx = download(url);
      XSSFWorkbook workbook = new XSSFWorkbook(new ByteArrayInputStream(xlsx));
      try {
        return MAPPER.writeValueAsString(parse(workbook.getSheetAt(0)));
      } finally {
        workbook.close();
      }
    } catch (IOException e) {
      throw new IllegalStateException("Stock SMART: cannot load assessment summary", e);
    }
  }

  private static String stockIds(String response, String url) {
    if (response == null || response.isEmpty()) {
      throw new IllegalStateException("Stock SMART: empty stock list from " + url);
    }
    JsonNode data;
    try {
      data = MAPPER.readTree(response).get("data");
    } catch (IOException e) {
      throw new IllegalStateException("Stock SMART: stock list is not JSON from " + url, e);
    }
    if (data == null || !data.isArray() || data.size() == 0) {
      throw new IllegalStateException("Stock SMART: stock list has no 'data' array from " + url);
    }
    StringBuilder ids = new StringBuilder();
    for (JsonNode stock : data) {
      if (ids.length() > 0) {
        ids.append(',');
      }
      ids.append(stock.get("id").asText());
    }
    return ids.toString();
  }

  private static String exportCriteria(String ids) {
    ObjectNode crit = MAPPER.createObjectNode();
    crit.put("dataType", "");
    crit.put("scName", "- Science Center -");
    crit.put("ecoName", "- Reg. Ecosystem -");
    crit.put("jurName", "- Jurisdiction -");
    crit.put("rgnName", "");
    crit.put("fmpName", "- Fish Mgmt Plan -");
    crit.put("monName", "");
    crit.put("entityIdList", ids);
    crit.put("outputFieldList", FIELDS);
    crit.put("outputLabelList", FIELD_LABELS);
    crit.put("entityAttrList", ENTITY_ATTRS);
    crit.put("entityAttrLabelList", ENTITY_LABELS);
    crit.put("fileTypeList", "");
    crit.put("DownloadTool_includeNoAsmt", "N");
    crit.put("segIndex", "0");
    crit.put("DownloadTool_carryForward", "N");
    crit.put("DownloadTool_ent_name", "");
    crit.put("DownloadTool_jur_select", "%");
    crit.put("DownloadTool_fmp_select", "%");
    crit.put("DownloadTool_sc_select", "%");
    crit.put("DownloadTool_eco_select", "%");
    crit.put("DownloadTool_fssi_select", "");
    crit.put("startYear", "1800");
    crit.put("endYear", "2032");
    crit.put("asmtYears", "");
    ObjectNode request = MAPPER.createObjectNode();
    request.set("crit", crit);
    request.put("dataType", "Assessment Summary Data");
    request.put("dataFormat", "excel");
    return request.toString();
  }

  private static String encode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (java.io.UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  private static ArrayNode parse(Sheet sheet) {
    Iterator<Row> rows = sheet.iterator();
    Map<String, Integer> col = null;
    while (rows.hasNext() && col == null) {
      Row r = rows.next();
      Cell first = r.getCell(0);
      if (first != null && first.getCellType() == CellType.STRING
          && "Stock ID".equals(first.getStringCellValue())) {
        col = new HashMap<String, Integer>();
        for (Cell c : r) {
          if (c.getCellType() == CellType.STRING) {
            col.put(c.getStringCellValue().trim(), c.getColumnIndex());
          }
        }
      }
    }
    if (col == null) {
      throw new IllegalStateException("Stock SMART: no 'Stock ID' header row in workbook");
    }
    for (String[] spec : COLUMNS) {
      if (!col.containsKey(spec[0])) {
        throw new IllegalStateException("Stock SMART: workbook has no '" + spec[0]
            + "' column; found " + col.keySet());
      }
    }

    ArrayNode out = MAPPER.createArrayNode();
    while (rows.hasNext()) {
      Row r = rows.next();
      Cell id = r.getCell(col.get("Stock ID"));
      if (id == null || id.getCellType() == CellType.BLANK) {
        continue;
      }
      ObjectNode row = MAPPER.createObjectNode();
      for (String[] spec : COLUMNS) {
        put(row, spec, r.getCell(col.get(spec[0])));
      }
      out.add(row);
    }
    if (out.size() == 0) {
      throw new IllegalStateException("Stock SMART: workbook has no assessment rows");
    }
    return out;
  }

  private static void put(ObjectNode row, String[] spec, Cell cell) {
    String name = spec[1];
    String type = spec[2];
    // The export writes an absent value as an empty string cell, not a blank one.
    if (cell == null || cell.getCellType() == CellType.BLANK
        || (cell.getCellType() == CellType.STRING && cell.getStringCellValue().trim().isEmpty())) {
      row.putNull(name);
      return;
    }
    if (T_STRING.equals(type)) {
      String s = cell.getCellType() == CellType.STRING
          ? cell.getStringCellValue().trim() : String.valueOf((long) cell.getNumericCellValue());
      if (s.isEmpty()) {
        row.putNull(name);
      } else {
        row.put(name, s);
      }
    } else if (T_FLAG.equals(type)) {
      String s = cell.getStringCellValue().trim();
      if ("Y".equals(s)) {
        row.put(name, true);
      } else if ("N".equals(s)) {
        row.put(name, false);
      } else {
        throw new IllegalStateException("Stock SMART: " + spec[0] + " is '" + s
            + "', expected Y or N");
      }
    } else if (cell.getCellType() != CellType.NUMERIC) {
      throw new IllegalStateException("Stock SMART: " + spec[0] + " at row "
          + (cell.getRowIndex() + 1) + " is not numeric: " + cell);
    } else if (T_DOUBLE.equals(type)) {
      row.put(name, cell.getNumericCellValue());
    } else if (T_LONG.equals(type)) {
      row.put(name, (long) cell.getNumericCellValue());
    } else {
      row.put(name, (int) cell.getNumericCellValue());
    }
  }

  private static byte[] download(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from Stock SMART export");
    }
    InputStream is = conn.getInputStream();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
    } finally {
      is.close();
    }
    return baos.toByteArray();
  }
}
