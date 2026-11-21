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
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.openxml4j.util.ZipSecureFile;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.util.CellReference;
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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * EIA Form 923 plant-level monthly generation. Reads the "Page 1 Generation and Fuel Data" sheet
 * of the annual time-series workbook inside {@code f923_<year>.zip} and unpivots the twelve
 * {@code Netgen <Month>} columns (and the matching total-fuel-MMBtu columns) into one row per
 * plant, nuclear unit, prime mover, fuel type and month.
 *
 * <p>The workbook is ~20 MB; it is streamed with POI's event model so only one row is held.
 */
public class Eia923PlantGenerationTransformer extends EiaBulkXlsxTransformer
    implements ResponseTransformer {
  private static final Logger LOG = LoggerFactory.getLogger(Eia923PlantGenerationTransformer.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  private static final String SHEET_NAME = "Page 1 Generation and Fuel Data";
  private static final Pattern WORKBOOK_ENTRY =
      Pattern.compile("(?i).*schedules_2_3_4_5.*\\.xlsx");
  private static final String[] MONTHS = {
      "January", "February", "March", "April", "May", "June", "July", "August",
      "September", "October", "November", "December"};

  @Override public String transform(String response, RequestContext context) {
    Map<String, String> dims = context.getDimensionValues();
    String year = Objects.requireNonNull(dims != null ? dims.get("effective_year") : null,
        "effective_year dimension missing — contract violation");
    // EIA keeps the two newest archives under /xls/ and moves older ones to /archive/xls/
    // (verified 2026-11-21: 2025 and 2026 at /xls/, 2024 at /archive/xls/).
    String url = context.getUrl();
    if (Integer.parseInt(year) >= 2025) {
      url = url.replace("/archive/xls/", "/xls/");
    }
    byte[] zip;
    try {
      zip = downloadBytes(url);
    } catch (IOException e) {
      // Archive not yet published for this year: a legitimate "no data" signal.
      LOG.warn("EIA-923: archive not found at {} ({}); treating as no data", url, e.getMessage());
      return "[]";
    }
    try {
      return parseZip(zip, Integer.parseInt(year));
    } catch (Exception e) {
      throw new RuntimeException(
          "EIA-923 archive present but unparseable from " + url + ": " + e.getMessage(), e);
    }
  }

  private String parseZip(byte[] zip, int year) throws Exception {
    File tmp = File.createTempFile("eia923-", ".xlsx");
    try {
      boolean found = false;
      try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zip))) {
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
          String name = entry.getName().substring(entry.getName().lastIndexOf('/') + 1);
          if (WORKBOOK_ENTRY.matcher(name).matches()) {
            // justification: local scratch file for the streaming XLSX reader, deleted below
            Files.copy(zis, tmp.toPath(), StandardCopyOption.REPLACE_EXISTING);
            found = true;
            break;
          }
        }
      }
      if (!found) {
        throw new IOException("no Schedules_2_3_4_5 workbook in EIA-923 archive for " + year);
      }
      ArrayNode out = readSheet(tmp, year);
      if (out.size() == 0) {
        throw new IOException("EIA-923 workbook parsed to zero generation rows for " + year);
      }
      LOG.debug("EIA-923: {} plant-month rows for {}", out.size(), year);
      return out.toString();
    } finally {
      // justification: local scratch file
      Files.deleteIfExists(tmp.toPath());
    }
  }

  static ArrayNode readSheet(File xlsx, int year) throws Exception {
    ZipSecureFile.setMinInflateRatio(0.0);
    ArrayNode out = JSON.createArrayNode();
    try (OPCPackage pkg = OPCPackage.open(xlsx, PackageAccess.READ)) {
      XSSFReader reader = new XSSFReader(pkg);
      ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(pkg);
      StylesTable styles = reader.getStylesTable();
      XSSFReader.SheetIterator sheets = (XSSFReader.SheetIterator) reader.getSheetsData();
      boolean seen = false;
      while (sheets.hasNext()) {
        try (InputStream sheet = sheets.next()) {
          if (!SHEET_NAME.equals(sheets.getSheetName())) {
            continue;
          }
          seen = true;
          XMLReader parser = XMLHelper.newXMLReader();
          parser.setContentHandler(new XSSFSheetXMLHandler(styles, null, strings,
              new RowHandler(year, out), new DataFormatter(), false));
          parser.parse(new InputSource(sheet));
        }
      }
      if (!seen) {
        throw new IOException("sheet '" + SHEET_NAME + "' not found");
      }
    }
    return out;
  }

  /** Normalizes a header like "Netgen\r\nJanuary" to "Netgen January". */
  static String normalizeHeader(String h) {
    return h == null ? null : h.replaceAll("\\s+", " ").trim();
  }

  private static final class RowHandler implements XSSFSheetXMLHandler.SheetContentsHandler {
    private final int year;
    private final ArrayNode out;
    private List<String> current;
    private Map<String, Integer> col;

    RowHandler(int year, ArrayNode out) {
      this.year = year;
      this.out = out;
    }

    @Override public void startRow(int rowNum) {
      current = new ArrayList<String>();
    }

    @Override public void cell(String ref, String value, XSSFComment comment) {
      int c = new CellReference(ref).getCol();
      while (current.size() <= c) {
        current.add(null);
      }
      current.set(c, value);
    }

    @Override public void endRow(int rowNum) {
      if (current.isEmpty()) {
        return;
      }
      if ("Plant Id".equals(normalizeHeader(current.get(0)))) {
        bind();
        return;
      }
      if (col == null) {
        return; // title / banner rows above the header
      }
      String plantId = get("Plant Id");
      if (plantId == null) {
        return;
      }
      String fileYear = get("YEAR");
      if (fileYear != null && Integer.parseInt(fileYear.replace(".0", "")) != year) {
        throw new IllegalStateException("EIA-923 row " + rowNum + " carries YEAR " + fileYear
            + " in the archive for " + year);
      }
      for (int m = 0; m < 12; m++) {
        String net = get("Netgen " + MONTHS[m]);
        if (net == null) {
          continue;
        }
        ObjectNode row = JSON.createObjectNode();
        row.put("plant_id", Integer.parseInt(plantId.replace(".0", "")));
        putStr(row, "nuclear_unit_id", get("Nuclear Unit Id"));
        row.put("generation_year", year);
        row.put("generation_month", m + 1);
        putStr(row, "plant_name", get("Plant Name"));
        putInt(row, "operator_id", get("Operator Id"));
        putStr(row, "operator_name", get("Operator Name"));
        putStr(row, "state_abbr", get("Plant State"));
        putStr(row, "nerc_region", get("NERC Region"));
        putStr(row, "balancing_authority_code", get("Balancing Authority Code"));
        putInt(row, "sector_code", get("EIA Sector Number"));
        putStr(row, "sector", get("Sector Name"));
        putStr(row, "combined_heat_and_power", get("Combined Heat And Power Plant"));
        putStr(row, "prime_mover", get("Reported Prime Mover"));
        putStr(row, "fuel_type_code", get("Reported Fuel Type Code"));
        putStr(row, "mer_fuel_type_code", get("MER Fuel Type Code"));
        row.put("net_generation_mwh", Double.parseDouble(net.replace(",", "")));
        String fuel = get("Tot_MMBtu " + MONTHS[m]);
        if (fuel != null) {
          row.put("fuel_consumed_mmbtu", Double.parseDouble(fuel.replace(",", "")));
        } else {
          row.putNull("fuel_consumed_mmbtu");
        }
        out.add(row);
      }
    }

    private void bind() {
      col = new HashMap<String, Integer>();
      for (int i = 0; i < current.size(); i++) {
        String h = normalizeHeader(current.get(i));
        if (h != null && !h.isEmpty()) {
          col.put(h, i);
        }
      }
      List<String> required = new ArrayList<String>();
      required.add("Plant Id");
      required.add("Plant Name");
      required.add("Plant State");
      required.add("Reported Prime Mover");
      required.add("Reported Fuel Type Code");
      for (String m : MONTHS) {
        required.add("Netgen " + m);
      }
      for (String r : required) {
        if (!col.containsKey(r)) {
          throw new IllegalStateException("EIA-923 Page 1 header missing column '" + r + "'");
        }
      }
    }

    /** Cell text, or null when the column is absent from this vintage or the cell is blank. */
    private String get(String name) {
      Integer i = col.get(name);
      if (i == null || i >= current.size()) {
        return null;
      }
      String v = current.get(i);
      if (v == null) {
        return null;
      }
      v = v.trim();
      return v.isEmpty() || ".".equals(v) ? null : v;
    }

    private static void putStr(ObjectNode row, String key, String v) {
      if (v == null) {
        row.putNull(key);
      } else {
        row.put(key, v);
      }
    }

    private static void putInt(ObjectNode row, String key, String v) {
      if (v == null) {
        row.putNull(key);
      } else {
        row.put(key, (int) Double.parseDouble(v));
      }
    }
  }

  @Override protected String parseWorkbook(org.apache.poi.xssf.usermodel.XSSFWorkbook workbook,
      RequestContext context) {
    throw new UnsupportedOperationException("transform() is overridden to stream the ZIP");
  }
}
