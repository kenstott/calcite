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
package org.apache.calcite.adapter.govdata.health;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Maps one CSV record of either CMS Provider of Services layout to the column set shared by
 * {@code cms_pos_facilities} and {@code cms_pos_termination_history}.
 *
 * <p>The QIES layout uses upper-case column names, coded values and {@code yyyyMMdd} dates. The
 * iQIES layout uses lower-case column names, numeric provider type ids, text labels,
 * {@code MM/dd/yyyy} dates and the literals {@code Not Available} / {@code Not Applicable} for
 * missing values. Both are mapped to QIES category codes. A column iQIES does not publish, or an
 * action-type label with no QIES code, is null.
 */
final class CmsPosProviderMapper {

  /** iQIES provider type id to the QIES category code of the provider types this table holds. */
  static final Map<String, String> IQIES_CATEGORIES = iqiesCategories();

  private static final Map<String, String> IQIES_ACTION_CODES = iqiesActionCodes();

  private static final String[][] BED_COLUMNS = {
      {"bed_count", "BED_CNT"},
      {"certified_bed_count", "CRTFD_BED_CNT"},
      {"icf_iid_bed_count", "ICFIID_BED_CNT"},
      {"medicaid_nf_bed_count", "MDCD_NF_BED_CNT"},
      {"medicare_snf_bed_count", "MDCR_SNF_BED_CNT"},
      {"medicare_medicaid_snf_bed_count", "MDCR_MDCD_SNF_BED_CNT"},
      {"hospice_bed_count", "HOSPC_BED_CNT"},
      {"rehab_bed_count", "REHAB_BED_CNT"},
      {"psych_unit_bed_count", "PSYCH_UNIT_BED_CNT"},
      {"rehab_unit_bed_count", "REHAB_UNIT_BED_CNT"}
  };

  /** QIES columns that iQIES also publishes, under the lower-case name. */
  private static final String[] SHARED_COLUMNS = {
      "PRVDR_NUM", "FAC_NAME", "CITY_NAME", "STATE_CD", "ZIP_CD", "FIPS_STATE_CD",
      "FIPS_CNTY_CD", "CRTFCTN_DT", "ORGNL_PRTCPTN_DT", "TRMNTN_EXPRTN_DT",
      "CRTFCTN_ACTN_TYPE_CD", "PGM_TRMNTN_CD", "BED_CNT", "CRTFD_BED_CNT"
  };

  private static final String[] QIES_ONLY_COLUMNS = {
      "PRVDR_CTGRY_CD", "PRVDR_CTGRY_SBTYP_CD", "GNRL_CNTL_TYPE_CD", "CROSS_REF_PROVIDER_NUMBER",
      "PSYCH_UNIT_TRMNTN_CD", "PSYCH_UNIT_TRMNTN_DT", "REHAB_UNIT_TRMNTN_CD",
      "REHAB_UNIT_TRMNTN_DT"
  };

  private final boolean iqies;
  private final Map<String, Integer> index;

  private CmsPosProviderMapper(boolean iqies, Map<String, Integer> index) {
    this.iqies = iqies;
    this.index = index;
  }

  static CmsPosProviderMapper qies(List<String> header) {
    CmsPosProviderMapper mapper = new CmsPosProviderMapper(false, index(header));
    mapper.require(SHARED_COLUMNS);
    mapper.require(QIES_ONLY_COLUMNS);
    for (String[] bed : BED_COLUMNS) {
      mapper.require(bed[1]);
    }
    return mapper;
  }

  static CmsPosProviderMapper iqies(List<String> header) {
    CmsPosProviderMapper mapper = new CmsPosProviderMapper(true, index(header));
    mapper.require(SHARED_COLUMNS);
    mapper.require("PRVDR_TYPE_ID", "CONTROL_TYPE");
    return mapper;
  }

  /** The record's provider number, which both layouts require. */
  String providerNumber(List<String> values) {
    String number = value(values, "PRVDR_NUM");
    if (number == null) {
      throw new IllegalStateException("CMS POS: record without a provider number: " + values);
    }
    return number;
  }

  /** QIES category code, or null for an iQIES provider type these tables do not hold. */
  String category(List<String> values) {
    return iqies
        ? IQIES_CATEGORIES.get(value(values, "PRVDR_TYPE_ID"))
        : value(values, "PRVDR_CTGRY_CD");
  }

  String terminationCode(List<String> values) {
    return value(values, "PGM_TRMNTN_CD");
  }

  String crossReference(List<String> values) {
    return value(values, "CROSS_REF_PROVIDER_NUMBER");
  }

  Map<String, Object> map(List<String> values) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("provider_number", providerNumber(values));
    row.put("provider_category_code", category(values));
    row.put("provider_subtype_code", qiesValue(values, "PRVDR_CTGRY_SBTYP_CD"));
    row.put("facility_name", value(values, "FAC_NAME"));
    row.put("city", value(values, "CITY_NAME"));
    row.put("state", value(values, "STATE_CD"));
    row.put("zip_code", value(values, "ZIP_CD"));
    String stateFips = value(values, "FIPS_STATE_CD");
    String countyCode = value(values, "FIPS_CNTY_CD");
    row.put("state_fips", stateFips);
    row.put("county_fips",
        stateFips != null && countyCode != null
            && stateFips.length() == 2 && countyCode.length() == 3
            ? stateFips + countyCode : null);
    row.put("ownership_code", qiesValue(values, "GNRL_CNTL_TYPE_CD"));
    row.put("ownership_description", iqies ? value(values, "CONTROL_TYPE") : null);
    String certified = isoDate(values, "CRTFCTN_DT");
    row.put("certification_date", certified == null ? null : certified.replace("-", ""));
    for (String[] bed : BED_COLUMNS) {
      row.put(bed[0], count(values, bed[1]));
    }
    row.put("cross_ref_provider_number", qiesValue(values, "CROSS_REF_PROVIDER_NUMBER"));
    row.put("original_participation_date", isoDate(values, "ORGNL_PRTCPTN_DT"));
    String action = value(values, "CRTFCTN_ACTN_TYPE_CD");
    row.put("certification_action_type_code",
        iqies && action != null ? IQIES_ACTION_CODES.get(action) : action);
    row.put("termination_code", terminationCode(values));
    String terminated = isoDate(values, "TRMNTN_EXPRTN_DT");
    row.put("termination_date", terminated);
    row.put("is_terminated", terminated != null);
    row.put("psych_unit_termination_code", qiesValue(values, "PSYCH_UNIT_TRMNTN_CD"));
    row.put("psych_unit_termination_date",
        iqies ? null : isoDate(values, "PSYCH_UNIT_TRMNTN_DT"));
    row.put("rehab_unit_termination_code", qiesValue(values, "REHAB_UNIT_TRMNTN_CD"));
    row.put("rehab_unit_termination_date",
        iqies ? null : isoDate(values, "REHAB_UNIT_TRMNTN_DT"));
    return row;
  }

  private String qiesValue(List<String> values, String column) {
    return iqies ? null : value(values, column);
  }

  private String value(List<String> values, String column) {
    Integer i = index.get(column);
    if (i == null) {
      // An optional bed column the iQIES layout does not carry.
      return null;
    }
    if (i >= values.size()) {
      throw new IllegalStateException("CMS POS: record has " + values.size()
          + " fields, too short for column " + column);
    }
    String v = values.get(i).trim();
    if (v.isEmpty() || iqies && ("Not Available".equals(v) || "Not Applicable".equals(v))) {
      return null;
    }
    return v;
  }

  private Long count(List<String> values, String column) {
    String v = value(values, column);
    return v == null ? null : Long.valueOf(v);
  }

  private String isoDate(List<String> values, String column) {
    String v = value(values, column);
    if (v == null) {
      return null;
    }
    if (iqies && v.length() == 10 && v.charAt(2) == '/' && v.charAt(5) == '/') {
      return v.substring(6) + "-" + v.substring(0, 2) + "-" + v.substring(3, 5);
    }
    if (!iqies && v.length() == 8) {
      return v.substring(0, 4) + "-" + v.substring(4, 6) + "-" + v.substring(6);
    }
    throw new IllegalStateException("CMS POS: unrecognised date '" + v + "' in column " + column);
  }

  private void require(String... columns) {
    for (String column : columns) {
      if (!index.containsKey(column)) {
        throw new IllegalStateException("CMS POS: " + (iqies ? "iQIES" : "QIES")
            + " file has no column " + column);
      }
    }
  }

  private static Map<String, Integer> index(List<String> header) {
    Map<String, Integer> index = new HashMap<String, Integer>();
    for (int i = 0; i < header.size(); i++) {
      index.put(header.get(i).trim().toUpperCase(Locale.ROOT), i);
    }
    return index;
  }

  private static Map<String, String> iqiesCategories() {
    Map<String, String> m = new LinkedHashMap<String, String>();
    m.put("4", "06"); // Psychiatric residential treatment facility
    m.put("9", "12"); // Rural health clinic
    m.put("14", "19"); // Community mental health center
    m.put("15", "21"); // Federally qualified health center
    return m;
  }

  private static Map<String, String> iqiesActionCodes() {
    Map<String, String> m = new HashMap<String, String>();
    m.put("Initial", "1");
    m.put("Recertification", "2");
    m.put("Termination", "3");
    m.put("CHOW", "4");
    return m;
  }
}
