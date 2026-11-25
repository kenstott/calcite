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
package org.apache.calcite.adapter.govdata.census;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base for national tables built from the Census Bureau's CPS historical "adults" time-series
 * workbooks (Tables AD-1 and AD-3), which are legacy binary {@code .xls} files.
 *
 * <p>The configured source URL is the Census page that links the workbooks; each workbook link is
 * read from the page and downloaded here, because the response body is delivered as text.
 */
public abstract class CpsAdultsHistoryTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CpsAdultsHistoryTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** A year label: optional leading dots, the year, an optional footnote letter, "Census". */
  protected static final Pattern YEAR_LABEL =
      Pattern.compile("^\\.*(\\d{4})\\s*[a-z]?(\\s+Census)?$");

  /** Parses every workbook of this table into {@code out}. */
  protected abstract void parse(String pageHtml, String pageUrl, ArrayNode out)
      throws IOException;

  @Override public String transform(String response, RequestContext context) {
    String pageUrl = context.getUrl();
    try {
      ArrayNode out = MAPPER.createArrayNode();
      parse(response, pageUrl, out);
      return out.toString();
    } catch (IOException e) {
      throw new RuntimeException("CPS adults history: failed to read workbooks via " + pageUrl, e);
    }
  }

  /** Returns the URL of the workbook named {@code fileName} linked from the page. */
  static String workbookUrl(String pageHtml, String fileName) throws IOException {
    Matcher m = Pattern.compile(
        "href=\"((?:https:)?//www2\\.census\\.gov/[^\"]*/" + Pattern.quote(fileName) + ")\"")
        .matcher(pageHtml);
    if (!m.find()) {
      throw new IOException("no link to " + fileName + " on the page");
    }
    String url = m.group(1);
    return url.startsWith("//") ? "https:" + url : url;
  }

  /** Downloads the workbook named {@code fileName} linked from the page. */
  protected static HSSFWorkbook openWorkbook(String pageHtml, String fileName) throws IOException {
    String url = workbookUrl(pageHtml, fileName);
    LOGGER.info("CPS adults historical workbook: {}", url);
    return new HSSFWorkbook(
        new ByteArrayInputStream(CpsHistoricalXlsxTransformer.download(url)));
  }

  /** Text of a cell of the sheet, or null when empty. */
  protected static String text(Sheet sheet, int r, int c) {
    Row row = sheet.getRow(r);
    return row == null ? null : CpsHistoricalXlsxTransformer.text(row.getCell(c));
  }

  /** Whether the cell holds a number. */
  protected static boolean isNumber(Row row, int c) {
    Cell cell = row.getCell(c);
    return cell != null && cell.getCellType() == CellType.NUMERIC;
  }
}
