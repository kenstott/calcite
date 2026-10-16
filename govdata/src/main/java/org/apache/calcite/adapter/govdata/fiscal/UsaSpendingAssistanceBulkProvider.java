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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * DataProvider for {@code usaspending_assistance} — every federal assistance transaction (grants,
 * cooperative agreements, direct payments, loans, insurance) of one federal fiscal year, from the
 * USAspending award data archive's all-agency {@code Full} file.
 *
 * <p>The archive file name carries the build date ({@code FY2022_All_Assistance_Full_20261006.zip})
 * and the previous date is deleted after 60 days, so the current URL is looked up through
 * {@code POST /api/v2/bulk_download/list_monthly_files/}. A Full file is the complete history of
 * its fiscal year with corrections and deletes already applied, so each run replaces the year.
 */
public class UsaSpendingAssistanceBulkProvider extends AbstractBulkCsvProvider {

  private static final String LIST_ENDPOINT =
      "https://api.usaspending.gov/api/v2/bulk_download/list_monthly_files/";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String[][] COLUMNS = {
      {"assistance_transaction_unique_key", "assistance_transaction_unique_key", S},
      {"assistance_award_unique_key", "assistance_award_unique_key", S},
      {"award_id_fain", "award_id_fain", S},
      {"modification_number", "modification_number", S},
      {"award_id_uri", "award_id_uri", S},
      {"sai_number", "sai_number", S},
      {"federal_action_obligation", "federal_action_obligation", D},
      {"total_obligated_amount", "total_obligated_amount", D},
      {"total_outlayed_amount_for_overall_award", "total_outlayed_amount_for_overall_award", D},
      {"non_federal_funding_amount", "non_federal_funding_amount", D},
      {"face_value_of_loan", "face_value_of_loan", D},
      {"original_loan_subsidy_cost", "original_loan_subsidy_cost", D},
      {"action_date", "action_date", S},
      {"period_of_performance_start_date", "period_of_performance_start_date", S},
      {"period_of_performance_current_end_date", "period_of_performance_current_end_date", S},
      {"awarding_agency_code", "awarding_agency_code", S},
      {"awarding_agency_name", "awarding_agency_name", S},
      {"awarding_sub_agency_code", "awarding_sub_agency_code", S},
      {"awarding_sub_agency_name", "awarding_sub_agency_name", S},
      {"awarding_office_name", "awarding_office_name", S},
      {"funding_agency_code", "funding_agency_code", S},
      {"funding_agency_name", "funding_agency_name", S},
      {"funding_sub_agency_name", "funding_sub_agency_name", S},
      {"recipient_uei", "recipient_uei", S},
      {"recipient_duns", "recipient_duns", S},
      {"recipient_name", "recipient_name", S},
      {"recipient_parent_uei", "recipient_parent_uei", S},
      {"recipient_parent_duns", "recipient_parent_duns", S},
      {"recipient_parent_name", "recipient_parent_name", S},
      {"recipient_country_code", "recipient_country_code", S},
      {"recipient_city_name", "recipient_city_name", S},
      {"recipient_county_fips", "prime_award_transaction_recipient_county_fips_code", S},
      {"recipient_state_code", "recipient_state_code", S},
      {"recipient_zip_code", "recipient_zip_code", S},
      {"recipient_congressional_district", "prime_award_transaction_recipient_cd_current", S},
      {"pop_state_fips", "prime_award_transaction_place_of_performance_state_fips_code", S},
      {"pop_state_name", "primary_place_of_performance_state_name", S},
      {"pop_county_fips", "prime_award_transaction_place_of_performance_county_fips_code", S},
      {"pop_county_name", "primary_place_of_performance_county_name", S},
      {"pop_city_name", "primary_place_of_performance_city_name", S},
      {"pop_zip4", "primary_place_of_performance_zip_4", S},
      {"pop_congressional_district", "prime_award_transaction_place_of_performance_cd_current", S},
      {"cfda_number", "cfda_number", S},
      {"cfda_title", "cfda_title", S},
      {"funding_opportunity_number", "funding_opportunity_number", S},
      {"assistance_type_code", "assistance_type_code", S},
      {"assistance_type_description", "assistance_type_description", S},
      {"business_types_code", "business_types_code", S},
      {"business_types_description", "business_types_description", S},
      {"action_type_code", "action_type_code", S},
      {"record_type_code", "record_type_code", S},
      {"transaction_description", "transaction_description", S},
      {"disaster_emergency_fund_codes", "disaster_emergency_fund_codes_for_overall_award", S},
      {"usaspending_permalink", "usaspending_permalink", S},
      {"last_modified_date", "last_modified_date", S},
  };

  @Override String tableName() {
    return "usaspending_assistance";
  }

  @Override String entrySuffix() {
    return ".csv";
  }

  @Override Charset charset() {
    return StandardCharsets.UTF_8;
  }

  @Override String[][] columns() {
    return COLUMNS;
  }

  @Override String resolveUrl(Map<String, String> variables) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      throw new IOException("usaspending_assistance: no effective_year in " + variables);
    }
    int fy = Integer.parseInt(year.trim());
    String body = "{\"agency\":\"all\",\"fiscal_year\":" + fy + ",\"type\":\"assistance\"}";
    HttpURLConnection conn = FiscalHttp.openPostJsonWithRetry(LIST_ENDPOINT, body);
    JsonNode root;
    InputStream in = conn.getInputStream();
    try {
      root = MAPPER.readTree(in);
    } finally {
      in.close();
    }
    JsonNode files = root.get("monthly_files");
    if (files == null || !files.isArray()) {
      throw new IOException("usaspending_assistance: no monthly_files in list response for FY" + fy);
    }
    for (JsonNode f : files) {
      String name = f.path("file_name").asText();
      if (name.startsWith("FY" + fy + "_All_Assistance_Full_")) {
        return f.path("url").asText();
      }
    }
    throw new IOException("usaspending_assistance: the archive lists no all-agency Assistance Full "
        + "file for FY" + fy);
  }
}
