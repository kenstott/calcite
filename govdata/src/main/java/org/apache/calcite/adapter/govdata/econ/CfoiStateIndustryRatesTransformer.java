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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Turns one BLS "Fatal injury rates by state of incident and industry, all ownerships, YYYY"
 * page into {@code cfoi_state_industry_rates} rows: one per jurisdiction and industry sector
 * with a published rate.
 *
 * <p>The page is a jurisdiction-by-sector grid of fatal work injuries per 100,000 full-time
 * equivalent workers. The first data column is the jurisdiction's overall rate, headed
 * "YYYY Overall Rate"; it becomes the {@code All industries} sector and its year must equal the
 * requested reference year, which is what proves the page fetched is the one asked for. A cell
 * printed as "-" did not meet BLS publication criteria and produces no row rather than a zero.
 */
public class CfoiStateIndustryRatesTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CfoiStateIndustryRatesTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern OVERALL_HEADER = Pattern.compile("^(\\d{4}) Overall Rate$");
  private static final String ALL_INDUSTRIES = "All industries";

  @Override public String transform(String response, RequestContext context) {
    String yearText = context.getDimensionValues().get("effective_year");
    if (yearText == null) {
      throw new IllegalStateException(
          "CFOI state x industry: effective_year dimension is required to label the reference year");
    }
    int year = Integer.parseInt(yearText);
    String source = context.getUrl();

    CfoiHtmlTable table = CfoiHtmlTable.parse(response, source);
    if (table.headers.size() < 3) {
      throw new IllegalStateException("CFOI " + source + ": expected an overall column and at "
          + "least one industry column, found headers " + table.headers);
    }
    Matcher overall = OVERALL_HEADER.matcher(table.headers.get(1));
    if (!overall.matches() || Integer.parseInt(overall.group(1)) != year) {
      throw new IllegalStateException("CFOI " + source + ": first data column '"
          + table.headers.get(1) + "' is not the " + year + " overall rate");
    }

    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    for (CfoiHtmlTable.Row row : table.rows) {
      String type = CfoiHtmlTable.jurisdictionType(row.label);
      String fips = CfoiHtmlTable.stateFips(row.label);
      for (int i = 0; i < row.cells.size(); i++) {
        Double rate = CfoiHtmlTable.parseRate(row.cells.get(i), source + " " + row.label);
        if (rate == null) {
          continue;
        }
        Map<String, Object> rec = new LinkedHashMap<String, Object>();
        rec.put("year", Integer.valueOf(year));
        rec.put("jurisdiction_name", row.label);
        rec.put("jurisdiction_type", type);
        rec.put("state_fips", fips);
        rec.put("industry_sector", i == 0 ? ALL_INDUSTRIES : table.headers.get(i + 1));
        rec.put("fatal_injury_rate", rate);
        out.add(rec);
      }
    }
    LOGGER.info("CFOI state x industry {}: {} rows from {} jurisdictions", year, out.size(),
        table.rows.size());
    try {
      return MAPPER.writeValueAsString(out);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("CFOI " + source + ": cannot serialize rows", e);
    }
  }
}
