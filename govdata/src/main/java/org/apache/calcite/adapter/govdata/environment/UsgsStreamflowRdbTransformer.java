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
package org.apache.calcite.adapter.govdata.environment;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Maps a USGS NWIS {@code dv} (daily values) RDB response into {@code streamflow} rows —
 * one row per (site, date) with discharge and gage height.
 *
 * <p>A statewide {@code dv} pull concatenates one RDB block <b>per site</b>, each with its
 * own header whose value column is named {@code <tsid>_<param>_<stat>} (the {@code tsid}
 * varies by site, so column names differ block to block). This transformer detects each
 * header (first field {@code agency_cd}), resolves the value/qualifier column for parameter
 * {@code 00060} (discharge, cfs) and {@code 00065} (gage height, ft) by the 5-digit
 * parameter code embedded in the column name, and emits normalized rows. The {@code year}
 * partition comes from the {@code effective_year} dimension, so it is not emitted.
 */
public class UsgsStreamflowRdbTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(UsgsStreamflowRdbTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Pattern VALUE_COL = Pattern.compile("^\\d+_(\\d{5})_\\d+$");
  private static final Pattern QUAL_COL = Pattern.compile("^\\d+_(\\d{5})_\\d+_cd$");

  private static final String P_DISCHARGE = "00060";
  private static final String P_GAGE = "00065";

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      return "[]";
    }
    try {
      ArrayNode out = MAPPER.createArrayNode();
      int siteIdx = -1;
      int dateIdx = -1;
      int dischargeIdx = -1;
      int dischargeCdIdx = -1;
      int gageIdx = -1;
      int gageCdIdx = -1;
      boolean haveBlock = false;

      try (BufferedReader reader = new BufferedReader(new StringReader(response))) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.isEmpty() || line.charAt(0) == '#') {
            continue;
          }
          String[] f = UsgsRdbSupport.splitTabs(line);
          if (f.length > 0 && "agency_cd".equals(f[0].trim())) {
            // New per-site header block — resolve column positions.
            siteIdx = dateIdx = dischargeIdx = dischargeCdIdx = gageIdx = gageCdIdx = -1;
            for (int i = 0; i < f.length; i++) {
              String h = f[i].trim();
              if ("site_no".equals(h)) {
                siteIdx = i;
              } else if ("datetime".equals(h)) {
                dateIdx = i;
              } else {
                Matcher vm = VALUE_COL.matcher(h);
                Matcher qm = QUAL_COL.matcher(h);
                if (vm.matches()) {
                  if (P_DISCHARGE.equals(vm.group(1))) {
                    dischargeIdx = i;
                  } else if (P_GAGE.equals(vm.group(1))) {
                    gageIdx = i;
                  }
                } else if (qm.matches()) {
                  if (P_DISCHARGE.equals(qm.group(1))) {
                    dischargeCdIdx = i;
                  } else if (P_GAGE.equals(qm.group(1))) {
                    gageCdIdx = i;
                  }
                }
              }
            }
            haveBlock = siteIdx >= 0 && dateIdx >= 0;
            continue;
          }
          if (!haveBlock || UsgsRdbSupport.isCommentOrTypesRow(line)) {
            continue;
          }
          String site = at(f, siteIdx);
          String date = at(f, dateIdx);
          if (site == null || site.trim().isEmpty() || date == null || date.trim().isEmpty()) {
            continue;
          }
          ObjectNode row = MAPPER.createObjectNode();
          UsgsRdbSupport.putText(row, "site_no", site);
          UsgsRdbSupport.putText(row, "obs_date", date);
          UsgsRdbSupport.putDouble(row, "discharge_cfs", at(f, dischargeIdx));
          UsgsRdbSupport.putText(row, "discharge_qual", at(f, dischargeCdIdx));
          UsgsRdbSupport.putDouble(row, "gage_height_ft", at(f, gageIdx));
          UsgsRdbSupport.putText(row, "gage_height_qual", at(f, gageCdIdx));
          out.add(row);
        }
      }
      LOGGER.debug("streamflow: transformed {} daily observations", out.size());
      return MAPPER.writeValueAsString(out);
    } catch (IOException e) {
      throw new RuntimeException("streamflow transform failed: " + e.getMessage(), e);
    }
  }

  private static String at(String[] fields, int i) {
    return i >= 0 && i < fields.length ? fields[i] : null;
  }
}
