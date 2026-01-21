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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * DataSource implementation for local and S3 files.
 *
 * <p>Supports reading:
 * <ul>
 *   <li>xlsx, xls - Excel via Apache POI</li>
 *   <li>csv, tsv - Delimited via DuckDB</li>
 *   <li>json - JSON via DuckDB</li>
 *   <li>parquet - Parquet via DuckDB</li>
 * </ul>
 *
 * <h3>YAML Configuration</h3>
 * <pre>{@code
 * source:
 *   type: file
 *   path: "data/${year}/report.xlsx"   # supports variables
 *   format: xlsx                        # optional, detected from extension
 *   sheet: Sheet1                       # optional, for xlsx
 * }</pre>
 */
public class FileSource implements DataSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSource.class);

  private final FileSourceConfig config;

  public FileSource(FileSourceConfig config) {
    this.config = config;
  }

  @Override
  public Iterator<Map<String, Object>> fetch(Map<String, String> variables) throws IOException {
    String path = substituteVariables(config.getPath(), variables);
    String format = config.getFormat();

    // Auto-detect format from extension if not specified
    if (format == null || format.isEmpty()) {
      format = detectFormat(path);
    }

    // Select storage provider based on path scheme
    StorageProvider storageProvider = selectStorageProvider(path);
    LOGGER.info("Reading file: {} (format={}, provider={})", path, format, storageProvider.getStorageType());

    switch (format.toLowerCase()) {
      case "xlsx":
      case "xls":
        return readExcel(storageProvider, path, config.getSheet());
      case "csv":
        return readDelimited(path, ',');
      case "tsv":
        return readDelimited(path, '\t');
      case "json":
        return readJson(path);
      case "parquet":
        return readParquet(path);
      default:
        throw new IOException("Unsupported file format: " + format);
    }
  }

  @Override
  public String getType() {
    return "file";
  }

  /**
   * Selects appropriate storage provider based on path scheme.
   */
  private StorageProvider selectStorageProvider(String path) throws IOException {
    try {
      StorageProvider provider = StorageProviderFactory.createFromUrl(path);
      if (provider != null) {
        return provider;
      }
    } catch (Exception e) {
      throw new IOException("Failed to create storage provider for: " + path, e);
    }
    throw new IOException("No storage provider for path: " + path);
  }

  /**
   * Reads Excel file and returns rows as iterator.
   */
  private Iterator<Map<String, Object>> readExcel(StorageProvider storageProvider,
      String path, String sheetName) throws IOException {
    try (InputStream is = storageProvider.openInputStream(path);
         Workbook workbook = WorkbookFactory.create(is)) {

      Sheet sheet = sheetName != null && !sheetName.isEmpty()
          ? workbook.getSheet(sheetName)
          : workbook.getSheetAt(0);

      if (sheet == null) {
        throw new IOException("Sheet not found: " + sheetName);
      }

      // Read header row
      Row headerRow = sheet.getRow(0);
      if (headerRow == null) {
        return new ArrayList<Map<String, Object>>().iterator();
      }

      List<String> headers = new ArrayList<String>();
      for (Cell cell : headerRow) {
        headers.add(getCellStringValue(cell));
      }

      // Read data rows
      List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
      for (int i = 1; i <= sheet.getLastRowNum(); i++) {
        Row row = sheet.getRow(i);
        if (row == null) {
          continue;
        }

        Map<String, Object> record = new LinkedHashMap<String, Object>();
        for (int j = 0; j < headers.size(); j++) {
          Cell cell = row.getCell(j);
          record.put(headers.get(j), getCellValue(cell));
        }
        rows.add(record);
      }

      LOGGER.info("Read {} rows from Excel sheet '{}'", rows.size(), sheet.getSheetName());
      return rows.iterator();
    }
  }

  /**
   * Reads delimited file (CSV/TSV) via DuckDB.
   */
  private Iterator<Map<String, Object>> readDelimited(String path, char delimiter)
      throws IOException {
    String sql = String.format(
        "SELECT * FROM read_csv('%s', delim='%c', header=true, auto_detect=true)",
        escapePath(path), delimiter);
    return executeDuckDbQuery(sql);
  }

  /**
   * Reads JSON file via DuckDB.
   */
  private Iterator<Map<String, Object>> readJson(String path) throws IOException {
    String sql = String.format("SELECT * FROM read_json('%s', auto_detect=true)", escapePath(path));
    return executeDuckDbQuery(sql);
  }

  /**
   * Reads Parquet file via DuckDB.
   */
  private Iterator<Map<String, Object>> readParquet(String path) throws IOException {
    String sql = String.format("SELECT * FROM read_parquet('%s')", escapePath(path));
    return executeDuckDbQuery(sql);
  }

  /**
   * Executes DuckDB query and returns results as iterator.
   */
  private Iterator<Map<String, Object>> executeDuckDbQuery(String sql) throws IOException {
    try {
      Connection conn = DriverManager.getConnection("jdbc:duckdb:");
      Statement stmt = conn.createStatement();

      // Install and load httpfs for S3 support
      stmt.execute("INSTALL httpfs");
      stmt.execute("LOAD httpfs");

      // Configure S3 credentials if available
      configureS3Credentials(stmt);

      ResultSet rs = stmt.executeQuery(sql);
      return new ResultSetIterator(conn, stmt, rs);
    } catch (Exception e) {
      throw new IOException("Failed to read file via DuckDB: " + e.getMessage(), e);
    }
  }

  /**
   * Configures S3 credentials for DuckDB from environment.
   */
  private void configureS3Credentials(Statement stmt) {
    try {
      String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
      String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
      String endpoint = System.getenv("AWS_ENDPOINT_URL");
      String region = System.getenv("AWS_REGION");

      if (accessKey != null && secretKey != null) {
        stmt.execute("SET s3_access_key_id='" + accessKey + "'");
        stmt.execute("SET s3_secret_access_key='" + secretKey + "'");
      }
      if (endpoint != null) {
        stmt.execute("SET s3_endpoint='" + endpoint.replaceAll("https?://", "") + "'");
        stmt.execute("SET s3_url_style='path'");
      }
      if (region != null) {
        stmt.execute("SET s3_region='" + region + "'");
      }
    } catch (Exception e) {
      LOGGER.debug("Could not configure S3 credentials: {}", e.getMessage());
    }
  }

  private String getCellStringValue(Cell cell) {
    if (cell == null) {
      return "";
    }
    switch (cell.getCellType()) {
      case STRING:
        return cell.getStringCellValue();
      case NUMERIC:
        return String.valueOf((long) cell.getNumericCellValue());
      case BOOLEAN:
        return String.valueOf(cell.getBooleanCellValue());
      default:
        return "";
    }
  }

  private Object getCellValue(Cell cell) {
    if (cell == null) {
      return null;
    }
    switch (cell.getCellType()) {
      case STRING:
        return cell.getStringCellValue();
      case NUMERIC:
        if (DateUtil.isCellDateFormatted(cell)) {
          return cell.getLocalDateTimeCellValue();
        }
        double num = cell.getNumericCellValue();
        if (num == Math.floor(num) && !Double.isInfinite(num)) {
          return (long) num;
        }
        return num;
      case BOOLEAN:
        return cell.getBooleanCellValue();
      case FORMULA:
        try {
          return cell.getNumericCellValue();
        } catch (Exception e) {
          return cell.getStringCellValue();
        }
      default:
        return null;
    }
  }

  private String substituteVariables(String template, Map<String, String> variables) {
    String result = template;
    for (Map.Entry<String, String> e : variables.entrySet()) {
      result = result.replace("${" + e.getKey() + "}", e.getValue());
    }
    return result;
  }

  private String detectFormat(String path) {
    String lower = path.toLowerCase();
    if (lower.endsWith(".xlsx") || lower.endsWith(".xls")) {
      return "xlsx";
    } else if (lower.endsWith(".csv")) {
      return "csv";
    } else if (lower.endsWith(".tsv")) {
      return "tsv";
    } else if (lower.endsWith(".json")) {
      return "json";
    } else if (lower.endsWith(".parquet")) {
      return "parquet";
    }
    return "csv"; // default
  }

  private String escapePath(String path) {
    return path.replace("'", "''");
  }

  /**
   * Iterator that wraps a JDBC ResultSet and closes resources when exhausted.
   */
  private static class ResultSetIterator implements Iterator<Map<String, Object>> {
    private final Connection conn;
    private final Statement stmt;
    private final ResultSet rs;
    private final String[] columnNames;
    private Map<String, Object> next;
    private boolean closed;

    ResultSetIterator(Connection conn, Statement stmt, ResultSet rs) throws Exception {
      this.conn = conn;
      this.stmt = stmt;
      this.rs = rs;

      ResultSetMetaData meta = rs.getMetaData();
      columnNames = new String[meta.getColumnCount()];
      for (int i = 0; i < columnNames.length; i++) {
        columnNames[i] = meta.getColumnName(i + 1);
      }

      advance();
    }

    private void advance() {
      try {
        if (rs.next()) {
          next = new LinkedHashMap<String, Object>();
          for (int i = 0; i < columnNames.length; i++) {
            next.put(columnNames[i], rs.getObject(i + 1));
          }
        } else {
          next = null;
          close();
        }
      } catch (Exception e) {
        next = null;
        close();
      }
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public Map<String, Object> next() {
      if (next == null) {
        throw new NoSuchElementException();
      }
      Map<String, Object> current = next;
      advance();
      return current;
    }

    private void close() {
      if (!closed) {
        closed = true;
        try {
          rs.close();
        } catch (Exception e) { /* ignore */ }
        try {
          stmt.close();
        } catch (Exception e) { /* ignore */ }
        try {
          conn.close();
        } catch (Exception e) { /* ignore */ }
      }
    }
  }
}
