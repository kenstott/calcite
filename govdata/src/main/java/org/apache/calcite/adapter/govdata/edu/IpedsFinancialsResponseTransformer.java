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
package org.apache.calcite.adapter.govdata.edu;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transforms NCES IPEDS Finance survey bulk CSV responses.
 *
 * <p>NCES distributes IPEDS Finance data as ZIP archives containing CSV files
 * with NCES-coded column names (e.g. F1B01, F1C011). This transformer:
 * <ol>
 *   <li>Parses the CSV text (handles quoted fields)</li>
 *   <li>Skips X-prefixed imputation flag columns</li>
 *   <li>Maps NCES coded names to canonical schema column names based on form_type</li>
 *   <li>Injects form_type and is_provisional from context dimension values</li>
 * </ol>
 *
 * <p>The {@code form_type} dimension drives which column mapping is applied:
 * <ul>
 *   <li>F1A — GASB public institutions</li>
 *   <li>F2  — FASB larger private institutions</li>
 *   <li>F3  — FASB smaller private institutions</li>
 * </ul>
 *
 * <p>Provisional data (FY 2022-23 and later) is flagged with {@code is_provisional=1}.
 */
public class IpedsFinancialsResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IpedsFinancialsResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // First fiscal year covered by provisional data
  private static final int PROVISIONAL_YEAR_THRESHOLD = 2023;

  // ──────────────────────────────────────────────────────────────────────────
  // NCES → canonical column mappings
  // Keys are uppercase NCES codes; values are canonical schema column names.
  // Confirmed: F1B01, F2B01, F3B01 (tuition net), F1H01/02, F2H01/02 (endowment),
  //            F3G01/02 (endowment F3), F1C011, F2C01, F3C01 (instruction).
  // ──────────────────────────────────────────────────────────────────────────

  private static final Map<String, String> F1A_MAP;
  private static final Map<String, String> F2_MAP;
  private static final Map<String, String> F3_MAP;

  static {
    Map<String, String> f1a = new HashMap<String, String>();
    f1a.put("UNITID",   "unitid");
    // Revenues (GASB F1B section)
    f1a.put("F1B02",    "rev_tuition_fees_gross");
    f1a.put("F1B01",    "rev_tuition_fees_net");
    f1a.put("F1B06",    "rev_appropriations_fed");
    f1a.put("F1B07",    "rev_appropriations_state");
    f1a.put("F1B04",    "rev_grants_contracts_federal");
    f1a.put("F1B05",    "rev_grants_contracts_state");
    f1a.put("F1B08",    "rev_gifts_grants_contracts");
    f1a.put("F1B09",    "rev_investment_return");
    f1a.put("F1B11",    "rev_auxiliary_enterprises_net");
    f1a.put("F1D01",    "rev_operating");
    f1a.put("F1D02",    "rev_nonoperating");
    f1a.put("F1D04",    "rev_total_current");
    // Scholarships / discounts (F1E section)
    f1a.put("F1E05",    "sch_pell_grant");
    f1a.put("F1E06",    "sch_grants_institutional");
    f1a.put("F1E01",    "sch_total_student_aid");
    f1a.put("F1E02",    "sch_allowances_tuition_fees");
    // Expenditures (F1C section — column 1 = SALARIES+WAGES+BENEFITS total)
    f1a.put("F1C011",   "exp_instruc_total");
    f1a.put("F1C021",   "exp_research_total");
    f1a.put("F1C031",   "exp_pub_serv_total");
    f1a.put("F1C041",   "exp_acad_supp_total");
    f1a.put("F1C051",   "exp_student_serv_total");
    f1a.put("F1C061",   "exp_inst_supp_total");
    f1a.put("F1C091",   "exp_aux_ent_total");
    f1a.put("F1C101",   "exp_net_grant_aid_total");
    f1a.put("F1C141",   "exp_total_current");
    f1a.put("F1C151",   "exp_total_salaries");
    // Endowment (F1H section — confirmed)
    f1a.put("F1H01",    "endowment_beg");
    f1a.put("F1H02",    "endowment_end");
    // Balance sheet (F1A section)
    f1a.put("F1A01",    "assets");
    f1a.put("F1A02",    "liabilities");
    f1a.put("F1A19",    "net_position_end");
    f1a.put("F1A06",    "longterm_debt");
    // FTE enrollment
    f1a.put("EFTEUG",   "est_fte");
    F1A_MAP = Collections.unmodifiableMap(f1a);

    Map<String, String> f2 = new HashMap<String, String>();
    f2.put("UNITID",    "unitid");
    // Revenues (FASB F2B section)
    f2.put("F2B02",     "rev_tuition_fees_gross");
    f2.put("F2B01",     "rev_tuition_fees_net");
    f2.put("F2B04",     "rev_fed_approps_grants");
    f2.put("F2B05",     "rev_state_local_approps_grants");
    f2.put("F2B03",     "rev_gifts_grants_contracts");
    f2.put("F2B06",     "rev_investment_return");
    f2.put("F2B07",     "rev_auxiliary_enterprises_net");
    f2.put("F2D01",     "rev_operating");
    f2.put("F2D02",     "rev_nonoperating");
    f2.put("F2D04",     "rev_total_current");
    // Scholarships (F2E section)
    f2.put("F2E05",     "sch_pell_grant");
    f2.put("F2E01",     "sch_total_student_aid");
    f2.put("F2E02",     "sch_allowances_tuition_fees");
    // Expenditures (F2C section — confirmed F2C01=instruction)
    f2.put("F2C01",     "exp_instruc_total");
    f2.put("F2C02",     "exp_research_total");
    f2.put("F2C03",     "exp_pub_serv_total");
    f2.put("F2C04",     "exp_acad_supp_total");
    f2.put("F2C05",     "exp_student_serv_total");
    f2.put("F2C06",     "exp_inst_supp_total");
    f2.put("F2C08",     "exp_aux_ent_total");
    f2.put("F2C09",     "exp_net_grant_aid_total");
    f2.put("F2C19",     "exp_total_current");
    f2.put("F2C20",     "exp_total_salaries");
    // Endowment (F2H section — confirmed)
    f2.put("F2H01",     "endowment_beg");
    f2.put("F2H02",     "endowment_end");
    // Balance sheet (F2A section)
    f2.put("F2A01",     "assets");
    f2.put("F2A02",     "liabilities");
    f2.put("F2A19",     "net_position_end");
    f2.put("F2A06",     "longterm_debt");
    f2.put("EFTEUG",    "est_fte");
    F2_MAP = Collections.unmodifiableMap(f2);

    Map<String, String> f3 = new HashMap<String, String>();
    f3.put("UNITID",    "unitid");
    // Revenues (FASB F3B section)
    f3.put("F3B01",     "rev_tuition_fees_net");
    f3.put("F3B04",     "rev_fed_approps_grants");
    f3.put("F3B05",     "rev_state_local_approps_grants");
    f3.put("F3B03",     "rev_gifts_grants_contracts");
    f3.put("F3B06",     "rev_investment_return");
    f3.put("F3B07",     "rev_auxiliary_enterprises_net");
    f3.put("F3D01",     "rev_total_current");
    // Scholarships (F3E section)
    f3.put("F3E01",     "sch_total_student_aid");
    // Expenditures (F3C section — confirmed F3C01=instruction)
    f3.put("F3C01",     "exp_instruc_total");
    f3.put("F3C02",     "exp_research_total");
    f3.put("F3C03",     "exp_pub_serv_total");
    f3.put("F3C04",     "exp_acad_supp_total");
    f3.put("F3C05",     "exp_student_serv_total");
    f3.put("F3C06",     "exp_inst_supp_total");
    f3.put("F3C08",     "exp_aux_ent_total");
    f3.put("F3C09",     "exp_net_grant_aid_total");
    f3.put("F3C19",     "exp_total_current");
    f3.put("F3C20",     "exp_total_salaries");
    // Endowment (F3G section — confirmed; F3 uses G not H)
    f3.put("F3G01",     "endowment_beg");
    f3.put("F3G02",     "endowment_end");
    // Balance sheet (F3A section)
    f3.put("F3A01",     "assets");
    f3.put("F3A02",     "liabilities");
    f3.put("F3A06",     "longterm_debt");
    f3.put("EFTEUG",    "est_fte");
    F3_MAP = Collections.unmodifiableMap(f3);
  }

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("IPEDS Financials: empty response for {}", context.getUrl());
      return "[]";
    }

    String formType = context.getDimensionValues().get("form_type");
    String yearStr = context.getDimensionValues().get("year");

    Map<String, String> columnMap = columnMapFor(formType);
    int isProvisional = isProvisional(yearStr) ? 1 : 0;

    try {
      List<String[]> rows = parseCsv(response);
      if (rows.size() < 2) {
        LOGGER.debug("IPEDS Financials: no data rows for form_type={}, year={}", formType, yearStr);
        return "[]";
      }

      String[] header = rows.get(0);
      // Uppercase header for map lookup
      for (int i = 0; i < header.length; i++) {
        header[i] = header[i].trim().toUpperCase();
      }

      ArrayNode out = MAPPER.createArrayNode();
      for (int r = 1; r < rows.size(); r++) {
        String[] values = rows.get(r);
        ObjectNode row = MAPPER.createObjectNode();

        if (formType != null) {
          row.put("form_type", formType);
        } else {
          LOGGER.warn("IPEDS Financials: form_type dimension missing for {}", context.getUrl());
          row.putNull("form_type");
        }
        row.put("is_provisional", isProvisional);
        if (yearStr != null) {
          try {
            row.put("year", Integer.parseInt(yearStr));
          } catch (NumberFormatException e) {
            LOGGER.warn("IPEDS Financials: non-integer year '{}' for {}", yearStr, context.getUrl());
          }
        }

        for (int c = 0; c < header.length && c < values.length; c++) {
          String col = header[c];

          // Skip imputation flag columns (X prefix)
          if (col.startsWith("X")) {
            continue;
          }

          String canonical = columnMap.get(col);
          if (canonical == null) {
            continue; // unmapped column — ignore
          }

          String rawValue = values[c].trim();
          if (rawValue.isEmpty()) {
            row.putNull(canonical);
            continue;
          }

          // unitid and est_fte are integers; everything else is double
          if ("unitid".equals(canonical) || "est_fte".equals(canonical)) {
            try {
              row.put(canonical, Integer.parseInt(rawValue));
            } catch (NumberFormatException e) {
              row.putNull(canonical);
            }
          } else {
            try {
              row.put(canonical, Double.parseDouble(rawValue));
            } catch (NumberFormatException e) {
              row.putNull(canonical);
            }
          }
        }

        // Only emit rows that have a unitid
        if (row.has("unitid") && !row.path("unitid").isNull()) {
          out.add(row);
        }
      }

      LOGGER.debug("IPEDS Financials: {} rows for form_type={}, year={}, provisional={}",
          out.size(), formType, yearStr, isProvisional);
      return out.toString();

    } catch (Exception e) {
      LOGGER.error("IPEDS Financials: transform failed for {}: {}", context.getUrl(), e.getMessage());
      return "[]";
    }
  }

  private static Map<String, String> columnMapFor(String formType) {
    if ("F1A".equalsIgnoreCase(formType)) {
      return F1A_MAP;
    } else if ("F2".equalsIgnoreCase(formType)) {
      return F2_MAP;
    } else if ("F3".equalsIgnoreCase(formType)) {
      return F3_MAP;
    }
    LOGGER.warn("IPEDS Financials: unknown form_type '{}', column mapping will be empty", formType);
    return Collections.emptyMap();
  }

  private static boolean isProvisional(String yearStr) {
    if (yearStr == null) {
      return false;
    }
    try {
      return Integer.parseInt(yearStr) >= PROVISIONAL_YEAR_THRESHOLD;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  /**
   * Minimal RFC-4180-compatible CSV parser.
   *
   * <p>Handles quoted fields with embedded commas and escaped quotes ({@code ""}).
   * Returns a list of string arrays, one per non-empty line.
   */
  static List<String[]> parseCsv(String text) throws IOException {
    List<String[]> result = new ArrayList<String[]>();
    BufferedReader reader = new BufferedReader(new StringReader(text));
    String line;
    while ((line = reader.readLine()) != null) {
      if (line.trim().isEmpty()) {
        continue;
      }
      result.add(parseCsvLine(line));
    }
    return result;
  }

  private static String[] parseCsvLine(String line) {
    List<String> fields = new ArrayList<String>();
    StringBuilder current = new StringBuilder();
    boolean inQuotes = false;

    for (int i = 0; i < line.length(); i++) {
      char c = line.charAt(i);
      if (inQuotes) {
        if (c == '"') {
          // Peek ahead for escaped quote ""
          if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
            current.append('"');
            i++; // skip second quote
          } else {
            inQuotes = false;
          }
        } else {
          current.append(c);
        }
      } else {
        if (c == '"') {
          inQuotes = true;
        } else if (c == ',') {
          fields.add(current.toString());
          current.setLength(0);
        } else {
          current.append(c);
        }
      }
    }
    fields.add(current.toString());
    return fields.toArray(new String[0]);
  }
}
