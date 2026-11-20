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
package org.apache.calcite.adapter.govdata.transport;

import org.apache.calcite.adapter.file.etl.NetworkOutageWait;
import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.util.XMLHelper;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFComment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Parses the USACE Waterborne Commerce Statistics Center <b>Vessel Inventory</b> workbook (the
 * "T.S. Vessel File" of Waterborne Transportation Lines of the United States, Volume 3) for one
 * calendar year into {@code usace_vessel_inventory} rows, one per US-flag vessel.
 *
 * <p>The workbooks are published on the USACE IWR Project Assistance Library under the series
 * "WTLUS Vessel Inventory". The library's document id is not predictable, so the file is found
 * by asking the library's public search endpoint for the document whose release year equals the
 * {@code year} dimension, then downloaded through its public download endpoint. A year with no
 * document raises an "HTTP 404" error so the pipeline marks the period unavailable. The sheet is
 * streamed (the 2013 sheet XML alone is 55 MB) and columns are located by header name, because
 * a few headers were renamed between releases (HRP/HP, EQUIP1/EQUP1, SERIES/REGION).
 */
public class UsaceVesselInventoryTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UsaceVesselInventoryTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String LIBRARY = "https://publibrary.sec.usace.army.mil/api/";
  private static final String SEARCH_BODY =
      "{\"data\":{\"archived\":false,\"audience\":\"[]\",\"businessarea\":\"[]\","
      + "\"cctopics\":\"[]\",\"certificationstatus\":\"[]\",\"collection\":\"\","
      + "\"creator\":\"\",\"creatornonusace\":\"[]\",\"docid\":\"\",\"doclist\":\"[]\","
      + "\"ecomodels\":false,\"ecosystemmethodology\":\"[]\",\"geotype\":\"\","
      + "\"incs\":0,\"msc\":\"\",\"numrecs\":1000,\"project\":\"\",\"projectid\":-1,"
      + "\"restorationtype\":\"[]\",\"series\":\"[\\\"WTLUS Vessel Inventory\\\"]\","
      + "\"since\":\"\",\"term\":\"\",\"title\":\"\",\"yearfrom\":1800,\"yearto\":2100}}";

  /** Source header (trimmed, upper case) to output column; numeric output columns listed below. */
  private static final Map<String, String> HEADERS = new HashMap<String, String>();
  private static final java.util.Set<String> NUMERIC = new java.util.HashSet<String>();

  static {
    HEADERS.put("VESSEL", "vessel_id");
    HEADERS.put("VS_NAME", "vessel_name");
    HEADERS.put("CG_NUMBER", "cg_number");
    HEADERS.put("VTCC", "vtcc");
    HEADERS.put("ICST", "icst");
    HEADERS.put("NRT", "nrt");
    HEADERS.put("HP", "horsepower");
    HEADERS.put("HRP", "horsepower");
    HEADERS.put("REG_LNGTH", "reg_length_ft");
    HEADERS.put("REG_BRDTH", "reg_breadth_ft");
    HEADERS.put("CAP_TONS", "cap_tons");
    HEADERS.put("YEAR", "year_built");
    HEADERS.put("YEAR_REBUILT", "year_rebuilt");
    HEADERS.put("YEAR_VESSEL", "year_vessel");
    HEADERS.put("LOAD_DRAFT", "load_draft_ft");
    HEADERS.put("LIGHT_DRAFT", "light_draft_ft");
    HEADERS.put("STATE", "state");
    HEADERS.put("BASE1", "base1");
    HEADERS.put("BASE2", "base2");
    HEADERS.put("REGION", "operating_area");
    HEADERS.put("SERIES", "operating_area");
    HEADERS.put("TS_OPER", "operator_code");
    for (String n : new String[] {"nrt", "horsepower", "reg_length_ft", "reg_breadth_ft",
        "cap_tons", "year_built", "year_rebuilt", "year_vessel", "load_draft_ft",
        "light_draft_ft"}) {
      NUMERIC.add(n);
    }
  }

  @Override public String transform(String response, RequestContext context) {
    String year = context.getDimensionValues().get("effective_year");
    String url = null;
    try {
      String[] doc = findDocument(year);
      url = LIBRARY + "download?id=" + doc[0] + "&filename="
          + URLEncoder.encode(doc[1], "UTF-8").replace("+", "%20");
      byte[] bytes = download(url, null);
      ArrayNode out = parse(bytes);
      LOGGER.debug("usace_vessel_inventory: {} vessels for {} from {}", out.size(), year, url);
      return MAPPER.writeValueAsString(out);
    } catch (Exception e) {
      throw new RuntimeException(
          "usace_vessel_inventory transform failed for year " + year + " (" + url + "): "
              + e.getMessage(), e);
    }
  }

  private static String[] findDocument(String year) throws IOException {
    byte[] resp = download(LIBRARY + "metadatasearchpublic/",
        SEARCH_BODY.getBytes(StandardCharsets.UTF_8));
    JsonNode root = MAPPER.readTree(resp);
    JsonNode docs = root.isArray() ? root : root.path("data");
    for (JsonNode d : docs) {
      if (year.equals(d.path("RELEASEYEAR").asText())
          && d.path("TITLE").asText().contains("Vessel Inventory")
          && "xlsx".equalsIgnoreCase(d.path("FILETYPE").asText())) {
        return new String[] {d.path("OBJECTID").asText(), d.path("FILENAME").asText()};
      }
    }
    throw new IOException("HTTP 404: no WTLUS Vessel Inventory document for " + year);
  }

  private static ArrayNode parse(byte[] bytes) throws Exception {
    final ArrayNode out = MAPPER.createArrayNode();
    OPCPackage pkg = OPCPackage.open(new ByteArrayInputStream(bytes));
    try {
      XSSFReader reader = new XSSFReader(pkg);
      ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(pkg);
      StylesTable styles = reader.getStylesTable();
      InputStream sheet = reader.getSheetsData().next();
      try {
        SheetHandler rows = new SheetHandler(out);
        XMLReader xml = XMLHelper.newXMLReader();
        xml.setContentHandler(
            new XSSFSheetXMLHandler(styles, null, strings, rows, new DataFormatter(), false));
        xml.parse(new InputSource(sheet));
        if (rows.columns.isEmpty()) {
          throw new IOException("header row with VESSEL column not found");
        }
      } finally {
        sheet.close();
      }
    } finally {
      pkg.revert();
    }
    return out;
  }

  /** Collects the header row, then one JSON object per vessel row. */
  private static final class SheetHandler implements XSSFSheetXMLHandler.SheetContentsHandler {
    private final ArrayNode out;
    final Map<Integer, String> columns = new HashMap<Integer, String>();
    private ObjectNode current;
    private boolean header;
    private int rowNum = -1;

    SheetHandler(ArrayNode out) {
      this.out = out;
    }

    @Override public void startRow(int rowNum) {
      this.rowNum = rowNum;
      header = rowNum == 0;
      current = header ? null : MAPPER.createObjectNode();
    }

    @Override public void endRow(int rowNum) {
      if (current != null && current.has("vessel_id")) {
        deriveType(current);
        out.add(current);
      }
      current = null;
    }

    @Override public void cell(String ref, String value, XSSFComment comment) {
      if (value == null) {
        return;
      }
      int col = columnIndex(ref);
      String v = value.trim();
      if (header) {
        String out2 = HEADERS.get(v.toUpperCase());
        if (out2 != null) {
          columns.put(col, out2);
        }
        return;
      }
      String name = columns.get(col);
      if (current == null || name == null || v.isEmpty()) {
        return;
      }
      if (NUMERIC.contains(name)) {
        try {
          double d = Double.parseDouble(v);
          if (d == Math.rint(d) && Math.abs(d) < 1e15) {
            current.put(name, (long) d);
          } else {
            current.put(name, d);
          }
        } catch (NumberFormatException e) {
          throw new IllegalStateException(
              "row " + (rowNum + 1) + " column " + name + " is not numeric: '" + v + "'", e);
        }
      } else {
        current.put(name, v);
      }
    }

    @Override public void headerFooter(String text, boolean isHeader, String tagName) {
    }
  }

  private static int columnIndex(String ref) {
    int idx = 0;
    for (int i = 0; i < ref.length() && Character.isLetter(ref.charAt(i)); i++) {
      idx = idx * 26 + (ref.charAt(i) - 'A' + 1);
    }
    return idx - 1;
  }

  /**
   * Splits the VTCC code (e.g. 4A48: type 4, steel, characteristic 48) using the WCSC data
   * dictionary appendix into a coarse vessel class and, for barges, a barge type.
   */
  private static void deriveType(ObjectNode row) {
    String vtcc = row.path("vtcc").asText("");
    if (vtcc.length() != 4) {
      return;
    }
    char type = vtcc.charAt(0);
    String ch = vtcc.substring(2);
    String cls;
    switch (type) {
    case '1':
      cls = "SELF_PROPELLED_DRY";
      break;
    case '2':
      cls = "SELF_PROPELLED_TANKER";
      break;
    case '3':
      cls = "TOWBOAT";
      break;
    case '4':
      cls = "DRY_BARGE";
      break;
    case '5':
      cls = "TANK_BARGE";
      break;
    case '6':
      cls = "OTHER";
      break;
    default:
      return;
    }
    row.put("vessel_class", cls);
    row.put("construction", constructionName(vtcc.charAt(1)));
    String barge = null;
    if (type == '4') {
      barge = dryBargeName(ch);
    } else if (type == '5') {
      barge = tankBargeName(ch);
    } else if (type == '3') {
      barge = "35".equals(ch) ? "Pushboat" : "36".equals(ch) ? "Tugboat" : null;
    }
    if (barge != null) {
      row.put("vessel_subtype", barge);
    }
  }

  private static String constructionName(char c) {
    switch (c) {
    case 'A':
      return "Steel";
    case 'B':
      return "Wood";
    case 'C':
      return "Aluminum";
    case 'D':
      return "Fiberglass";
    case 'E':
      return "Other";
    default:
      return "Unknown";
    }
  }

  private static String dryBargeName(String ch) {
    switch (ch) {
    case "40":
      return "Open Hopper Barge";
    case "41":
      return "Covered Hopper Barge";
    case "42":
      return "Carfloat";
    case "43":
      return "Flat / Deck Barge";
    case "44":
      return "Pontoon Barge";
    case "47":
      return "Open Dry Cargo Barge";
    case "48":
      return "Covered Dry Cargo Barge";
    case "49":
      return "RO-RO Barge";
    case "50":
      return "Container Barge";
    case "52":
      return "Lash / Seabee Barge";
    case "90":
      return "Convertible Barge";
    case "99":
      return "Other Dry Barge";
    default:
      return null;
    }
  }

  private static String tankBargeName(String ch) {
    switch (ch) {
    case "70":
      return "Liquid Cargo Barge (Single Hull)";
    case "71":
      return "Liquid Cargo Barge (Double Hull)";
    case "72":
      return "Liquid Cargo Barge (Double Sided Only)";
    case "73":
      return "Liquid Cargo Barge (Double Bottom Only)";
    case "74":
      return "Other Liquid Cargo Barge";
    default:
      return null;
    }
  }

  private static byte[] download(String url, byte[] postBody) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(300000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    if (postBody != null) {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setDoOutput(true);
      OutputStream os = conn.getOutputStream();
      try {
        os.write(postBody);
      } finally {
        os.close();
      }
    }
    NetworkOutageWait.connect(conn);
    int status = conn.getResponseCode();
    if (status != 200 && status != 201) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    InputStream is = conn.getInputStream();
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buf = new byte[65536];
      int len;
      while ((len = is.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
      return baos.toByteArray();
    } finally {
      is.close();
    }
  }
}
