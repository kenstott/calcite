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
 * DataProvider for {@code usaspending_contracts} - every federal prime contract transaction
 * (definitive contracts, purchase orders, delivery and task orders, and indefinite-delivery
 * vehicles) of one federal fiscal year, from USAspending's award data archive all-agency
 * {@code Contracts Full} file.
 *
 * <p>The archive file name carries the build date ({@code FY2024_All_Contracts_Full_20261106.zip})
 * and the previous date is deleted after 60 days, so the current URL is looked up through
 * {@code POST /api/v2/bulk_download/list_monthly_files/}. A Full file is the complete history of
 * its fiscal year with corrections and deletes already applied, so each run replaces the year.
 */
public class UsaSpendingContractsBulkProvider extends AbstractBulkCsvProvider {

  private static final String LIST_ENDPOINT =
      "https://api.usaspending.gov/api/v2/bulk_download/list_monthly_files/";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String[][] COLUMNS = {
      {"contract_transaction_unique_key", "contract_transaction_unique_key", S},
      {"contract_award_unique_key", "contract_award_unique_key", S},
      {"award_id_piid", "award_id_piid", S},
      {"modification_number", "modification_number", S},
      {"transaction_number", "transaction_number", S},
      {"parent_award_agency_id", "parent_award_agency_id", S},
      {"parent_award_agency_name", "parent_award_agency_name", S},
      {"parent_award_id_piid", "parent_award_id_piid", S},
      {"federal_action_obligation", "federal_action_obligation", D},
      {"total_dollars_obligated", "total_dollars_obligated", D},
      {"total_outlayed_amount_for_overall_award", "total_outlayed_amount_for_overall_award", D},
      {"base_and_exercised_options_value", "base_and_exercised_options_value", D},
      {"current_total_value_of_award", "current_total_value_of_award", D},
      {"base_and_all_options_value", "base_and_all_options_value", D},
      {"potential_total_value_of_award", "potential_total_value_of_award", D},
      {"disaster_emergency_fund_codes", "disaster_emergency_fund_codes_for_overall_award", S},
      {"action_date", "action_date", S},
      {"action_date_fiscal_year", "action_date_fiscal_year", S},
      {"period_of_performance_start_date", "period_of_performance_start_date", S},
      {"period_of_performance_current_end_date", "period_of_performance_current_end_date", S},
      {"period_of_performance_potential_end_date", "period_of_performance_potential_end_date", S},
      {"solicitation_date", "solicitation_date", S},
      {"awarding_agency_code", "awarding_agency_code", S},
      {"awarding_agency_name", "awarding_agency_name", S},
      {"awarding_sub_agency_code", "awarding_sub_agency_code", S},
      {"awarding_sub_agency_name", "awarding_sub_agency_name", S},
      {"awarding_office_code", "awarding_office_code", S},
      {"awarding_office_name", "awarding_office_name", S},
      {"funding_agency_code", "funding_agency_code", S},
      {"funding_agency_name", "funding_agency_name", S},
      {"funding_sub_agency_code", "funding_sub_agency_code", S},
      {"funding_sub_agency_name", "funding_sub_agency_name", S},
      {"funding_office_name", "funding_office_name", S},
      {"federal_accounts_funding_this_award", "federal_accounts_funding_this_award", S},
      {"program_activities_funding_this_award", "program_activities_funding_this_award", S},
      {"recipient_uei", "recipient_uei", S},
      {"recipient_duns", "recipient_duns", S},
      {"recipient_name", "recipient_name", S},
      {"recipient_doing_business_as_name", "recipient_doing_business_as_name", S},
      {"cage_code", "cage_code", S},
      {"recipient_parent_uei", "recipient_parent_uei", S},
      {"recipient_parent_duns", "recipient_parent_duns", S},
      {"recipient_parent_name", "recipient_parent_name", S},
      {"recipient_country_code", "recipient_country_code", S},
      {"recipient_address_line_1", "recipient_address_line_1", S},
      {"recipient_city_name", "recipient_city_name", S},
      {"recipient_county_fips", "prime_award_transaction_recipient_county_fips_code", S},
      {"recipient_state_code", "recipient_state_code", S},
      {"recipient_zip_code", "recipient_zip_4_code", S},
      {"recipient_congressional_district", "prime_award_transaction_recipient_cd_current", S},
      {"pop_country_code", "primary_place_of_performance_country_code", S},
      {"pop_city_name", "primary_place_of_performance_city_name", S},
      {"pop_county_fips", "prime_award_transaction_place_of_performance_county_fips_code", S},
      {"pop_county_name", "primary_place_of_performance_county_name", S},
      {"pop_state_fips", "prime_award_transaction_place_of_performance_state_fips_code", S},
      {"pop_state_code", "primary_place_of_performance_state_code", S},
      {"pop_state_name", "primary_place_of_performance_state_name", S},
      {"pop_zip4", "primary_place_of_performance_zip_4", S},
      {"pop_congressional_district", "prime_award_transaction_place_of_performance_cd_current", S},
      {"award_or_idv_flag", "award_or_idv_flag", S},
      {"award_type_code", "award_type_code", S},
      {"award_type", "award_type", S},
      {"idv_type", "idv_type", S},
      {"type_of_contract_pricing", "type_of_contract_pricing", S},
      {"transaction_description", "transaction_description", S},
      {"prime_award_base_transaction_description", "prime_award_base_transaction_description", S},
      {"action_type", "action_type", S},
      {"product_or_service_code", "product_or_service_code", S},
      {"product_or_service_code_description", "product_or_service_code_description", S},
      {"naics_code", "naics_code", S},
      {"naics_description", "naics_description", S},
      {"dod_acquisition_program_code", "dod_acquisition_program_code", S},
      {"dod_acquisition_program_description", "dod_acquisition_program_description", S},
      {"extent_competed", "extent_competed", S},
      {"solicitation_procedures", "solicitation_procedures", S},
      {"type_of_set_aside", "type_of_set_aside", S},
      {"number_of_offers_received", "number_of_offers_received", S},
      {"program_acronym", "program_acronym", S},
      {"domestic_or_foreign_entity", "domestic_or_foreign_entity", S},
      {"contracting_officers_determination_of_business_size", "contracting_officers_determination_of_business_size", S},
      {"organizational_type", "organizational_type", S},
      {"usaspending_permalink", "usaspending_permalink", S},
      {"last_modified_date", "last_modified_date", S},
  };

  @Override String tableName() {
    return "usaspending_contracts";
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
      throw new IOException("usaspending_contracts: no effective_year in " + variables);
    }
    int fy = Integer.parseInt(year.trim());
    String body = "{\"agency\":\"all\",\"fiscal_year\":" + fy + ",\"type\":\"contracts\"}";
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
      throw new IOException("usaspending_contracts: no monthly_files in list response for FY" + fy);
    }
    for (JsonNode f : files) {
      String name = f.path("file_name").asText();
      if (name.startsWith("FY" + fy + "_All_Contracts_Full_")) {
        return f.path("url").asText();
      }
    }
    throw new IOException("usaspending_contracts: the archive lists no all-agency Contracts Full "
        + "file for FY" + fy);
  }
}
