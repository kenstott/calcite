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
package org.apache.calcite.adapter.govdata.housing;

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * FHFA annual all-transactions House Price Index at five-digit ZIP grain
 * ({@code hpi_at_zip5.xlsx}, about 690,000 ZIP-year rows). The sheet XML is ~240 MB, so it is
 * read with POI's event model (never the DOM) and each row is kept as a compact array until the
 * iterator hands it out as a map. Column order after the banner is
 * {@code Five-Digit ZIP Code | Year | Annual Change (%) | HPI | HPI with 1990 base |
 * HPI with 2000 base}; the header row is located by its first cell, not a fixed row number.
 */
public class FhfaZip5HpiDataProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(FhfaZip5HpiDataProvider.class);

  private static final String URL = "https://www.fhfa.gov/hpi/download/annual/hpi_at_zip5.xlsx";
  private static final String HEADER_ZIP = "Five-Digit ZIP Code";

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    LOGGER.info("FhfaZip5HpiDataProvider: fetching {} from {}", config.getName(), URL);
    File tmp = FhfaDownload.toTempFile(URL, ".xlsx");
    final List<Object[]> rows = new ArrayList<Object[]>(700_000);
    try {
      readSheet(tmp, rows);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("fhfa zip5 hpi: " + e.getMessage(), e);
    } finally {
      Files.deleteIfExists(tmp.toPath());
    }
    if (rows.isEmpty()) {
      throw new IOException("fhfa zip5 hpi: workbook parsed to zero rows");
    }
    LOGGER.info("FhfaZip5HpiDataProvider: {} rows", rows.size());
    final Iterator<Object[]> it = rows.iterator();
    return new Iterator<Map<String, Object>>() {
      @Override public boolean hasNext() {
        return it.hasNext();
      }

      @Override public Map<String, Object> next() {
        Object[] r = it.next();
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("type", "zip_house_price_index");
        row.put("zip5", r[0]);
        row.put("data_year", r[1]);
        row.put("annual_change_pct", r[2]);
        row.put("hpi", r[3]);
        row.put("hpi_1990_base", r[4]);
        row.put("hpi_2000_base", r[5]);
        return row;
      }
    };
  }

  private static void readSheet(File xlsx, List<Object[]> out) throws Exception {
    ZipSecureFile.setMinInflateRatio(0.0);
    try (OPCPackage pkg = OPCPackage.open(xlsx, PackageAccess.READ)) {
      XSSFReader reader = new XSSFReader(pkg);
      ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(pkg);
      StylesTable styles = reader.getStylesTable();
      XSSFReader.SheetIterator sheets = (XSSFReader.SheetIterator) reader.getSheetsData();
      if (!sheets.hasNext()) {
        throw new IOException("workbook has no sheets");
      }
      try (InputStream sheet = sheets.next()) {
        XMLReader parser = XMLHelper.newXMLReader();
        // formatter + formulasNotResults=false: numeric cells arrive as their raw text
        parser.setContentHandler(new XSSFSheetXMLHandler(styles, null, strings,
            new RowHandler(out), new DataFormatter(), false));
        parser.parse(new InputSource(sheet));
      }
    }
  }

  private static final class RowHandler implements XSSFSheetXMLHandler.SheetContentsHandler {
    private final List<Object[]> out;
    private final String[] cells = new String[6];
    private boolean headerSeen;
    private int rowNum;

    RowHandler(List<Object[]> out) {
      this.out = out;
    }

    @Override public void startRow(int rowNum) {
      this.rowNum = rowNum + 1;
      java.util.Arrays.fill(cells, null);
    }

    @Override public void cell(String ref, String value, XSSFComment comment) {
      int c = new CellReference(ref).getCol();
      if (c < cells.length) {
        cells[c] = value;
      }
    }

    @Override public void endRow(int rowNum) {
      if (!headerSeen) {
        if (cells[0] != null && HEADER_ZIP.equals(cells[0].trim())) {
          headerSeen = true;
        }
        return;
      }
      if (cells[0] == null || cells[0].trim().isEmpty()) {
        return;
      }
      try {
        String zip = cells[0].trim();
        if (zip.length() < 5 && zip.matches("\\d+")) {
          zip = "00000".substring(zip.length()) + zip;
        }
        if (!zip.matches("\\d{5}")) {
          throw new IOException("row " + this.rowNum + " ZIP '" + zip + "' is not 5 digits");
        }
        Double yr = FhfaDownload.number(cells[1], "year on row " + this.rowNum);
        if (yr == null) {
          throw new IOException("row " + this.rowNum + " has no year");
        }
        out.add(new Object[] {zip, Integer.valueOf((int) Math.round(yr)),
            FhfaDownload.number(cells[2], "annual change on row " + this.rowNum),
            FhfaDownload.number(cells[3], "hpi on row " + this.rowNum),
            FhfaDownload.number(cells[4], "hpi1990 on row " + this.rowNum),
            FhfaDownload.number(cells[5], "hpi2000 on row " + this.rowNum)});
      } catch (IOException e) {
        throw new IllegalStateException("fhfa zip5 hpi: " + e.getMessage(), e);
      }
    }
  }
}
