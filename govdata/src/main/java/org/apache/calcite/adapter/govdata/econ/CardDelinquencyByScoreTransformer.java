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
// storage-provider-guard:ignore-file - audited: only local temp files holding the downloaded workbook.
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Emits {@code econ.card_delinquency_by_score} from Figure 5a of the accessible-data page of the
 * Federal Reserve Board FEDS Note "A Note on Recent Dynamics of Consumer Delinquency Rates"
 * (24 November 2025): one row per quarter and credit score band.
 *
 * <p>The page holds many tables, so the table is picked by its sub-heading ("Figure 5a. Credit
 * Card Delinquency Rates"); Figure 1 and Figure 5b look alike and must not be read instead. Any
 * departure from the published shape (missing table, different column order, a cell that is not a
 * number) raises an error.
 */
public class CardDelinquencyByScoreTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CardDelinquencyByScoreTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String TABLE_HEADING = "Figure 5a. Credit Card Delinquency Rates";
  private static final String SOURCE_RELEASE = "2025-11-24";

  // quarter-end dates, e.g. 3/31/2000
  private static final Pattern QUARTER_END = Pattern.compile("^(3|6|9|12)/(30|31)/(\\d{4})$");

  /** Score bands in the column order of the published table. */
  private enum Band {
    SUBPRIME("subprime", "Subprime", null, Integer.valueOf(619)),
    NEAR_PRIME("near_prime", "Near Prime", Integer.valueOf(620), Integer.valueOf(719)),
    PRIME("prime", "Prime", Integer.valueOf(720), null);

    private final String code;
    private final String headerPrefix;
    private final Integer scoreMin;
    private final Integer scoreMax;

    Band(String code, String headerPrefix, Integer scoreMin, Integer scoreMax) {
      this.code = code;
      this.headerPrefix = headerPrefix;
      this.scoreMin = scoreMin;
      this.scoreMax = scoreMax;
    }
  }

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      throw new IllegalStateException(
          "card_delinquency_by_score: empty response from " + context.getUrl());
    }
    Element table = findTable(Jsoup.parse(response));
    checkHeader(table.select("thead th"));

    ArrayNode out = MAPPER.createArrayNode();
    for (Element row : table.select("tbody tr")) {
      Elements cells = row.select("th, td");
      if (cells.size() != Band.values().length + 1) {
        throw new IllegalStateException(
            "card_delinquency_by_score: unexpected row: " + row.text());
      }
      Matcher date = QUARTER_END.matcher(cells.get(0).text().trim());
      if (!date.matches()) {
        throw new IllegalStateException(
            "card_delinquency_by_score: not a quarter-end date: " + cells.get(0).text());
      }
      int year = Integer.parseInt(date.group(3));
      int quarter = Integer.parseInt(date.group(1)) / 3;
      for (Band band : Band.values()) {
        ObjectNode record = out.addObject();
        record.put("score_band", band.code);
        record.put("score_min", band.scoreMin);
        record.put("score_max", band.scoreMax);
        record.put("year", year);
        record.put("quarter", quarter);
        record.put("period", year + "Q" + quarter);
        record.put("pct_balance_30plus_delinquent",
            Double.parseDouble(cells.get(band.ordinal() + 1).text().trim()));
        record.put("source_release", SOURCE_RELEASE);
      }
    }
    if (out.size() == 0) {
      throw new IllegalStateException("card_delinquency_by_score: table has no data rows");
    }
    LOGGER.debug("card_delinquency_by_score: {} rows", out.size());
    return out.toString();
  }

  private static Element findTable(Document doc) {
    for (Element heading : doc.select("span.tablesubhead")) {
      if (TABLE_HEADING.equals(heading.text().trim())) {
        for (Element parent : heading.parents()) {
          Element table = parent.selectFirst("table");
          if (table != null) {
            return table;
          }
        }
      }
    }
    throw new IllegalStateException(
        "card_delinquency_by_score: no table headed '" + TABLE_HEADING + "'");
  }

  private static void checkHeader(Elements header) {
    boolean ok = header.size() == Band.values().length + 1;
    for (Band band : Band.values()) {
      ok = ok && header.get(band.ordinal() + 1).text().startsWith(band.headerPrefix + " Credit");
    }
    if (!ok) {
      throw new IllegalStateException(
          "card_delinquency_by_score: unexpected table header: " + header.text());
    }
  }
}
