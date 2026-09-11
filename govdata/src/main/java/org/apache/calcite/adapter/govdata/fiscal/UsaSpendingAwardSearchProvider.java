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
package org.apache.calcite.adapter.govdata.fiscal;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.RawCache;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DataProvider for a single CFDA/assistance-listing number's award-level records
 * from USAspending {@code POST /api/v2/search/spending_by_award/}. Shared by
 * {@code broadband_reconnect_awards} (CFDA 10.752) and
 * {@code broadband_bead_state_allocations} (CFDA 11.035) — the {@code cfda_number}
 * dimension (a single-value {@code type: list}) selects which program a given
 * table pulls, so both tables reuse this one provider rather than duplicating the
 * pagination/mapping logic for what is otherwise an identical request shape.
 *
 * <p>Award-type codes 02/03/04/05 (block/formula/project/cooperative-agreement
 * grants) cover both target programs; neither issues contracts or loans. Pages
 * via the endpoint's own {@code page_metadata.hasNext}, not a total-count header
 * (the API does not expose one) — each page is cached individually so a partial
 * crawl resumes from the raw cache rather than re-fetching completed pages.
 */
public class UsaSpendingAwardSearchProvider implements CachingDataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UsaSpendingAwardSearchProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ENDPOINT =
      "https://api.usaspending.gov/api/v2/search/spending_by_award/";
  private static final int PAGE_SIZE = 100;
  private static final String FIELDS =
      "\"Recipient Name\",\"Award Amount\",\"Award ID\",\"Start Date\",\"End Date\","
      + "\"Recipient Location\",\"Awarding Agency\",\"Description\"";

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String cfda = variables.get("cfda_number");
    if (cfda == null || cfda.isEmpty()) {
      LOGGER.warn("usaspending award search: no cfda_number in dimension variables {}", variables);
      return Collections.emptyIterator();
    }

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    int page = 1;
    boolean hasNext = true;
    while (hasNext) {
      String body = "{\"filters\":{\"program_numbers\":[\"" + cfda + "\"],"
          + "\"award_type_codes\":[\"02\",\"03\",\"04\",\"05\"]},"
          + "\"fields\":[" + FIELDS + "],\"page\":" + page + ",\"limit\":" + PAGE_SIZE + ","
          + "\"subawards\":false,\"spending_level\":\"awards\","
          + "\"sort\":\"Award Amount\",\"order\":\"desc\"}";
      String cacheKey = ENDPOINT + "/cfda=" + cfda + "&page=" + page;
      LOGGER.info("usaspending award search: POST {} cfda={} page={}", ENDPOINT, cfda, page);

      JsonNode root;
      InputStream in = rawCache.openStream(cacheKey,
          () -> FiscalHttp.openPostJsonWithRetry(ENDPOINT, body).getInputStream());
      try {
        root = MAPPER.readTree(in);
      } finally {
        in.close();
      }

      JsonNode results = root.path("results");
      if (results.isArray()) {
        for (JsonNode r : results) {
          rows.add(mapAward(r, cfda));
        }
      }
      JsonNode pageMeta = root.path("page_metadata");
      hasNext = pageMeta.path("hasNext").asBoolean(false);
      page++;
    }
    LOGGER.info("usaspending award search: {} award rows for cfda {}", rows.size(), cfda);
    return rows.iterator();
  }

  private static Map<String, Object> mapAward(JsonNode r, String cfda) {
    JsonNode loc = r.path("Recipient Location");
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("cfda_number", cfda);
    row.put("award_id", text(r, "Award ID"));
    row.put("recipient_name", text(r, "Recipient Name"));
    row.put("award_amount", num(r, "Award Amount"));
    row.put("start_date", text(r, "Start Date"));
    row.put("end_date", text(r, "End Date"));
    row.put("awarding_agency", text(r, "Awarding Agency"));
    row.put("description", text(r, "Description"));
    String stateAbbr = text(loc, "state_code");
    String countyCode = text(loc, "county_code");
    row.put("state_abbr", stateAbbr);
    row.put("county_fips",
        (stateAbbr == null || countyCode == null) ? null
            : usFipsForState(stateAbbr) == null ? null
            : usFipsForState(stateAbbr) + countyCode);
    row.put("county_name", text(loc, "county_name"));
    row.put("city_name", text(loc, "city_name"));
    row.put("zip5", text(loc, "zip5"));
    return row;
  }

  // USAspending's Recipient Location carries a 2-letter state_code, not a state
  // FIPS code; county_fips needs the 2-digit state FIPS prefix. A small fixed
  // table rather than a schema join keeps this provider self-contained (every
  // value here is a permanent, non-changing USPS<->FIPS mapping).
  private static String usFipsForState(String abbr) {
    return STATE_FIPS.get(abbr);
  }

  private static final Map<String, String> STATE_FIPS = buildStateFips();

  private static Map<String, String> buildStateFips() {
    Map<String, String> m = new LinkedHashMap<String, String>();
    String[][] pairs = {
        {"AL", "01"}, {"AK", "02"}, {"AZ", "04"}, {"AR", "05"}, {"CA", "06"},
        {"CO", "08"}, {"CT", "09"}, {"DE", "10"}, {"DC", "11"}, {"FL", "12"},
        {"GA", "13"}, {"HI", "15"}, {"ID", "16"}, {"IL", "17"}, {"IN", "18"},
        {"IA", "19"}, {"KS", "20"}, {"KY", "21"}, {"LA", "22"}, {"ME", "23"},
        {"MD", "24"}, {"MA", "25"}, {"MI", "26"}, {"MN", "27"}, {"MS", "28"},
        {"MO", "29"}, {"MT", "30"}, {"NE", "31"}, {"NV", "32"}, {"NH", "33"},
        {"NJ", "34"}, {"NM", "35"}, {"NY", "36"}, {"NC", "37"}, {"ND", "38"},
        {"OH", "39"}, {"OK", "40"}, {"OR", "41"}, {"PA", "42"}, {"RI", "44"},
        {"SC", "45"}, {"SD", "46"}, {"TN", "47"}, {"TX", "48"}, {"UT", "49"},
        {"VT", "50"}, {"VA", "51"}, {"WA", "53"}, {"WV", "54"}, {"WI", "55"},
        {"WY", "56"}, {"PR", "72"}, {"AS", "60"}, {"GU", "66"}, {"MP", "69"},
        {"VI", "78"},
    };
    for (String[] p : pairs) {
      m.put(p[0], p[1]);
    }
    return Collections.unmodifiableMap(m);
  }

  private static String text(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    String s = v.asText();
    return (s == null || s.trim().isEmpty()) ? null : s;
  }

  private static Double num(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    return v.isNumber() ? v.asDouble() : FiscalHttp.toDouble(v.asText());
  }
}
