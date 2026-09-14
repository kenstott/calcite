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
package org.apache.calcite.adapter.govdata.crime;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;
import org.apache.calcite.adapter.file.etl.VariableResolver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DataProvider for {@code cde_police_employment_by_agency} — agency-year sworn-officer and
 * civilian-employee counts from FBI CDE, keyed on ORI so the (ori, year) grain rolls up cleanly
 * to county via {@code cde_agencies.county_name}.
 *
 * <p>FBI's CDE exposes agency-level Police Employee (PE) data at
 * {@code /LATEST/pe/agency/{ori}?from={year}&to={year}}, confirmed live 2026-09-14 (Nome PD
 * AK0010600 in FY2020: 9 male + 1 female officers, matching a real small-town department). The
 * corresponding state-total endpoint (used by {@link CdePoliceEmploymentTransformer}) is a
 * different path — {@code /LATEST/pe/{state_abbr}} — and returns aggregates, not per-agency
 * detail. So agency-year headcount needs its own ingest path, not a filter on the state pull.
 *
 * <p>For each dispatched (state_abbr, year) combo, this provider:
 * <ol>
 *   <li>Fetches the state's agency list from {@code api.usa.gov/crime/fbi/cde/agency/byStateAbbr/{state}}
 *       (the same feed {@link CdeAgencyTransformer} ingests) — one call per state, returns every
 *       ORI plus the county name FBI itself carries for that agency.</li>
 *   <li>For each ORI in the state, fetches {@code pe/agency/{ori}?from={year}&to={year}} — one
 *       call per agency per year, ~500 agencies/state × 51 states × 15 years = ~380k calls at
 *       the schema's 10 req/s ceiling for a full historical build. Per-year dispatch keeps
 *       partition writes bounded and lets a single re-run per year stay cheap once historical
 *       is complete.</li>
 * </ol>
 *
 * <p>The county name comes from the agency-list response, not the PE response. FBI's own bucket
 * strings ('NOT SPECIFIED' for state-police/university/airport agencies that don't sit in one
 * county) are preserved as-is; downstream views filter or coalesce as they need. The join back to
 * {@code geo.counties} lives in a view, not this table, because the FBI's county name uses
 * upper-case-with-hyphens ('KENAI PENINSULA') that geo.counties stores title-cased.
 */
public class CdeAgencyPoliceEmploymentProvider implements CachingDataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CdeAgencyPoliceEmploymentProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String AGENCIES_ENDPOINT =
      "https://api.usa.gov/crime/fbi/cde/agency/byStateAbbr/";
  private static final String PE_AGENCY_ENDPOINT =
      "https://cde.ucr.cjis.gov/LATEST/pe/agency/";

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String stateAbbr = variables.get("state_abbr");
    String year = variables.get("year");
    if (stateAbbr == null || stateAbbr.isEmpty() || year == null || year.isEmpty()) {
      LOGGER.warn("cde_police_employment_by_agency: missing state_abbr/year in {}", variables);
      return java.util.Collections.emptyIterator();
    }

    String apiKey = VariableResolver.resolveEnvVars("${API_DATA_GOV:}");
    if (apiKey == null || apiKey.trim().isEmpty()) {
      throw new IOException("cde_police_employment_by_agency: API_DATA_GOV env var is unset — "
          + "the agency-list endpoint (api.usa.gov/crime/fbi/cde) requires it. Same key already "
          + "used by cde_agencies.");
    }

    // Step 1: list agencies for this state (fresh fetch per (state, year) combo — cheap, one
    // call, and the cache key includes the year so a lookback re-fetch is a raw-cache hit).
    Map<String, AgencyInfo> agencies = fetchAgencies(stateAbbr, apiKey, year, rawCache);
    LOGGER.info("cde_police_employment_by_agency: {} agencies in {} for year {}",
        agencies.size(), stateAbbr, year);

    // Step 2: fetch PE for each ORI (one call per agency, single-year window).
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (Map.Entry<String, AgencyInfo> entry : agencies.entrySet()) {
      String ori = entry.getKey();
      AgencyInfo info = entry.getValue();
      Map<String, Object> row = fetchPeForAgency(ori, info, stateAbbr, year, rawCache);
      if (row != null) {
        rows.add(row);
      }
    }
    return rows.iterator();
  }

  /**
   * Walks the state's agency-list JSON and returns ORI → (county_name, agency_name,
   * agency_type_name). The endpoint's top-level object is keyed by county-bucket-name, and each
   * bucket carries a list of agencies whose fields include the same county string.
   */
  private Map<String, AgencyInfo> fetchAgencies(String state, String apiKey, String year,
      RawCache rawCache) throws IOException {
    String url = AGENCIES_ENDPOINT + state + "?API_KEY=" + apiKey;
    String cacheKey = url + "#year=" + year;
    JsonNode root;
    try (InputStream in = rawCache.openStream(cacheKey, () -> rawGet(url))) {
      root = MAPPER.readTree(in);
    }
    Map<String, AgencyInfo> out = new LinkedHashMap<String, AgencyInfo>();
    Iterator<Map.Entry<String, JsonNode>> counties = root.fields();
    while (counties.hasNext()) {
      Map.Entry<String, JsonNode> countyEntry = counties.next();
      JsonNode list = countyEntry.getValue();
      if (!list.isArray()) {
        continue;
      }
      for (JsonNode agency : list) {
        String ori = text(agency, "ori");
        if (ori == null) {
          continue;
        }
        AgencyInfo info = new AgencyInfo();
        info.countyName = text(agency, "counties");
        info.agencyName = text(agency, "agency_name");
        info.agencyType = text(agency, "agency_type_name");
        out.put(ori, info);
      }
    }
    return out;
  }

  /**
   * Fetches {@code pe/agency/{ori}} for one year. Returns null if the API returns no counts —
   * ORIs with no PE data for the year (agencies that didn't report to LEOKA that year) are
   * dropped rather than emitting NULL-only rows that fail T4_all_null_cols pointlessly.
   */
  private Map<String, Object> fetchPeForAgency(String ori, AgencyInfo info, String stateAbbr,
      String year, RawCache rawCache) throws IOException {
    String url = PE_AGENCY_ENDPOINT + ori + "?from=" + year + "&to=" + year;
    JsonNode root;
    try (InputStream in = rawCache.openStream(url, () -> rawGet(url))) {
      root = MAPPER.readTree(in);
    }
    JsonNode actuals = root.path("actuals");
    JsonNode rates = root.path("rates");
    JsonNode pops = root.path("populations");

    Long maleOfficers = getYearLong(actuals.path("Male Officers"), year);
    Long femaleOfficers = getYearLong(actuals.path("Female Officers"), year);
    Long maleCivilians = getYearLong(actuals.path("Male Civilians"), year);
    Long femaleCivilians = getYearLong(actuals.path("Female Civilians"), year);
    Long participated = getYearLong(pops.path("Participated Population"), year);
    Double officersPer1000 =
        getYearDouble(rates.path("Law Enforcement Employees per 1,000 People"), year);

    // Skip agencies with no counts at all — the CDE API returns a fully-null envelope for ORIs
    // that never reported PE data in the queried year, and storing that row adds nothing.
    if (maleOfficers == null && femaleOfficers == null && maleCivilians == null
        && femaleCivilians == null && participated == null) {
      return null;
    }
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("ori", ori);
    row.put("year", Integer.parseInt(year));
    row.put("state_abbr", stateAbbr);
    row.put("agency_name", info.agencyName);
    row.put("agency_type_name", info.agencyType);
    row.put("county_name", info.countyName);
    row.put("male_officers", maleOfficers);
    row.put("female_officers", femaleOfficers);
    row.put("male_civilians", maleCivilians);
    row.put("female_civilians", femaleCivilians);
    row.put("participated_population", participated);
    row.put("officers_per_1000", officersPer1000);
    return row;
  }

  private InputStream rawGet(String url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setRequestMethod("GET");
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(60000);
    int status = conn.getResponseCode();
    if (status < 200 || status >= 300) {
      throw new IOException("CDE HTTP " + status + " from " + url);
    }
    return conn.getInputStream();
  }

  private static Long getYearLong(JsonNode measureNode, String year) {
    JsonNode v = measureNode.get(year);
    return (v == null || v.isNull()) ? null : v.asLong();
  }

  private static Double getYearDouble(JsonNode measureNode, String year) {
    JsonNode v = measureNode.get(year);
    return (v == null || v.isNull()) ? null : v.asDouble();
  }

  private static String text(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    String s = v.asText();
    return (s == null || s.trim().isEmpty()) ? null : s;
  }

  private static final class AgencyInfo {
    String countyName;
    String agencyName;
    String agencyType;
  }
}
