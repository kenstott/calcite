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
package org.apache.calcite.adapter.govdata.lands;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Transformer for DOI's Payments in Lieu of Taxes (PILT) per-county payment/acreage page
 * ({@code pilt.doi.gov/counties.cfm?fiscal_yr=YYYY&state_code=XX}) — no bulk CSV/JSON export
 * exists, only this per-(fiscal_yr, state_code) HTML table (confirmed live; see govdata Defect
 * Register D-044).
 *
 * <p>The results table has no distinguishing id/class, so it is located structurally: the
 * {@code <table>} whose header row's first cell reads "COUNTY" (site-wide constant text even for
 * states — Louisiana parishes, New England towns — where the row labels themselves are not
 * literally counties). Each data row has exactly two data columns: a payment amount (formatted
 * {@code $1,234,567} or {@code $0}) and a total-acres figure (formatted {@code 1,234,567}).
 * {@code area_type} is derived from the row label's own trailing word (COUNTY/PARISH/BOROUGH/
 * TOWN/CITY/...) rather than assumed from the state, since Vermont's table mixes town-level rows
 * with county-level subtotal rows in the same page.
 */
public class PiltCountyPaymentsTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern TRAILING_WORD = Pattern.compile("\\s(\\S+)$");

  @Override public String transform(String response, RequestContext context) {
    // "year" is the schema dimension name (bound to the URL's fiscal_yr query param via
    // {year} in the source.url template) -- not "fiscal_yr" itself.
    String fiscalYear = context.getDimensionValues().get("year");
    String stateCode = context.getDimensionValues().get("state_code");

    Document doc = Jsoup.parse(response);
    Element table = findDataTable(doc);
    ArrayNode result = MAPPER.createArrayNode();
    if (table == null) {
      return result.toString();
    }

    Elements rows = table.select("tr");
    for (int i = 1; i < rows.size(); i++) {
      Elements cells = rows.get(i).select("td");
      if (cells.size() < 2) {
        continue;
      }
      String areaName = cells.get(0).text().trim();
      if (areaName.isEmpty()) {
        continue;
      }
      String paymentRaw = cells.get(1).text().trim();
      String acresRaw = cells.get(cells.size() - 1).text().trim();

      ObjectNode row = MAPPER.createObjectNode();
      row.put("fiscal_year", fiscalYear);
      row.put("state_code", stateCode);
      row.put("area_name", areaName);
      row.put("area_type", trailingWord(areaName));
      Long payment = parseMoney(paymentRaw);
      if (payment != null) {
        row.put("payment_dollars", payment);
      } else {
        row.putNull("payment_dollars");
      }
      Long acres = parseNumber(acresRaw);
      if (acres != null) {
        row.put("total_acres", acres);
      } else {
        row.putNull("total_acres");
      }
      result.add(row);
    }
    return result.toString();
  }

  /** The results table is the one whose header row's first cell text is exactly "COUNTY". */
  private static Element findDataTable(Document doc) {
    for (Element table : doc.select("table")) {
      Elements headerCells = table.select("tr").first() != null
          ? table.select("tr").first().select("th")
          : new Elements();
      if (!headerCells.isEmpty() && "COUNTY".equalsIgnoreCase(headerCells.first().text().trim())) {
        return table;
      }
    }
    return null;
  }

  private static String trailingWord(String areaName) {
    Matcher m = TRAILING_WORD.matcher(areaName);
    return m.find() ? m.group(1) : null;
  }

  private static Long parseMoney(String raw) {
    if (raw == null || raw.isEmpty()) {
      return null;
    }
    String cleaned = raw.replace("$", "").replace(",", "").trim();
    try {
      return Long.valueOf(cleaned);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Long parseNumber(String raw) {
    if (raw == null || raw.isEmpty()) {
      return null;
    }
    String cleaned = raw.replace(",", "").trim();
    try {
      return Long.valueOf(cleaned);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
