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

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * DataProvider for {@code snap_participants_by_age} — SNAP participants by age group, by state
 * x federal fiscal year, estimated from the USDA FNS SNAP Quality Control (QC) public-use files.
 *
 * <p>The QC files are household-level samples (wide: {@code AGE1..AGE16}, {@code FSAFIL1..16})
 * with a fiscal-year weight {@code FYWGT}. snapqcdata.net (hosted by Mathematica for FNS) links
 * one CSV zip per fiscal year ({@code qcfy<YYYY>_csv.zip}) under date-stamped folders, so the
 * current list is read from {@link #LANDING_URL}. A SNAP participant is a person whose
 * {@code FSAFIL<n>} is 1 (their count equals {@code FSUSIZE}); other household members are not
 * counted. Households with {@code STATUS} 1-3 (eligible, with or without payment error) are
 * included, matching FNS's "Characteristics of SNAP Households" universe. Ages are summed
 * weighted: persons = SUM(FYWGT) over participants.
 */
public class SnapParticipantsByAgeProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnapParticipantsByAgeProvider.class);

  /** Page listing every fiscal year's public-use file. */
  static final String LANDING_URL = "https://snapqcdata.net/datafiles";

  private static final Pattern ZIP_LINK = Pattern.compile(
      "href=\"((?:https://snapqcdata\\.net)?/sites/default/files/[^\"]*?qcfy(\\d{4})_csv\\.zip)\"");

  private static final int MAX_MEMBERS = 16;

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String page = FiscalHttp.readAll(FiscalHttp.openGetWithRetry(LANDING_URL).getInputStream());
    Map<Integer, String> links = new TreeMap<Integer, String>();
    Matcher m = ZIP_LINK.matcher(page);
    while (m.find()) {
      String url = m.group(1);
      links.put(Integer.valueOf(m.group(2)),
          url.startsWith("/") ? "https://snapqcdata.net" + url : url);
    }
    if (links.isEmpty()) {
      throw new IOException("snap_participants_by_age: no qcfy<YYYY>_csv.zip links found on "
          + LANDING_URL);
    }
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (Map.Entry<Integer, String> e : links.entrySet()) {
      LOGGER.info("snap_participants_by_age: FY{} from {}", e.getKey(), e.getValue());
      byte[] zipBytes = FiscalHttp.readBytes(FiscalHttp.openGetWithRetry(e.getValue()).getInputStream());
      parseZip(zipBytes, e.getKey().intValue(), rows);
    }
    LOGGER.info("snap_participants_by_age: {} rows", rows.size());
    return rows.iterator();
  }

  private void parseZip(byte[] zipBytes, int fiscalYear, List<Map<String, Object>> rows)
      throws IOException {
    ZipInputStream zin = new ZipInputStream(new java.io.ByteArrayInputStream(zipBytes));
    try {
      ZipEntry entry;
      while ((entry = zin.getNextEntry()) != null) {
        if (entry.getName().toLowerCase(Locale.ROOT).endsWith(".csv")) {
          parseCsv(new BufferedReader(new InputStreamReader(zin, StandardCharsets.UTF_8)),
              fiscalYear, rows);
          return;
        }
      }
    } finally {
      zin.close();
    }
    throw new IOException("snap_participants_by_age: no .csv entry in FY" + fiscalYear + " zip");
  }

  /** Per state x year accumulator. */
  private static final class Acc {
    String stateName;
    long sample;
    int firstMonth = Integer.MAX_VALUE;
    int lastMonth = Integer.MIN_VALUE;
    double households;
    double persons;
    double under5;
    double age5to17;
    double age18to59;
    double age60plus;
    double ageUnknown;
  }

  void parseCsv(BufferedReader in, int fiscalYear, List<Map<String, Object>> rows)
      throws IOException {
    String header = in.readLine();
    if (header == null) {
      throw new IOException("snap_participants_by_age: empty CSV for FY" + fiscalYear);
    }
    Map<String, Integer> idx = FiscalHttp.headerIndex(Arrays.asList(header.split(",", -1)));
    int iState = FiscalHttp.required(idx, "STATE", "FY" + fiscalYear);
    int iName = FiscalHttp.required(idx, "STATENAME", "FY" + fiscalYear);
    int iStatus = FiscalHttp.required(idx, "STATUS", "FY" + fiscalYear);
    int iWgt = FiscalHttp.required(idx, "FYWGT", "FY" + fiscalYear);
    int iMonth = FiscalHttp.required(idx, "YRMONTH", "FY" + fiscalYear);
    int[] iAge = new int[MAX_MEMBERS];
    int[] iAfil = new int[MAX_MEMBERS];
    for (int k = 0; k < MAX_MEMBERS; k++) {
      iAge[k] = FiscalHttp.required(idx, "AGE" + (k + 1), "FY" + fiscalYear);
      iAfil[k] = FiscalHttp.required(idx, "FSAFIL" + (k + 1), "FY" + fiscalYear);
    }
    Map<String, Acc> byState = new LinkedHashMap<String, Acc>();
    String line;
    while ((line = in.readLine()) != null) {
      if (line.isEmpty()) {
        continue;
      }
      List<String> c = Arrays.asList(line.split(",", -1));
      Integer status = FiscalHttp.toInt(FiscalHttp.cell(c, iStatus));
      if (status == null || status.intValue() < 1 || status.intValue() > 3) {
        continue; // ineligible cases are not SNAP households
      }
      Integer stateCode = FiscalHttp.toInt(FiscalHttp.cell(c, iState));
      Double wgt = FiscalHttp.toDouble(FiscalHttp.cell(c, iWgt));
      Integer ym = FiscalHttp.toInt(FiscalHttp.cell(c, iMonth));
      if (stateCode == null || wgt == null || ym == null) {
        throw new IOException("snap_participants_by_age: FY" + fiscalYear
            + " row missing STATE/FYWGT/YRMONTH: " + line.substring(0, Math.min(80, line.length())));
      }
      String key = FiscalHttp.pad(String.valueOf(stateCode), 2);
      Acc a = byState.get(key);
      if (a == null) {
        a = new Acc();
        a.stateName = FiscalHttp.str(FiscalHttp.cell(c, iName));
        byState.put(key, a);
      }
      a.sample++;
      a.firstMonth = Math.min(a.firstMonth, ym.intValue());
      a.lastMonth = Math.max(a.lastMonth, ym.intValue());
      a.households += wgt.doubleValue();
      for (int k = 0; k < MAX_MEMBERS; k++) {
        Integer afil = FiscalHttp.toInt(FiscalHttp.cell(c, iAfil[k]));
        if (afil == null || afil.intValue() != 1) {
          continue; // not a SNAP participant
        }
        a.persons += wgt.doubleValue();
        Integer age = FiscalHttp.toInt(FiscalHttp.cell(c, iAge[k]));
        if (age == null) {
          a.ageUnknown += wgt.doubleValue();
        } else if (age.intValue() < 5) {
          a.under5 += wgt.doubleValue();
        } else if (age.intValue() < 18) {
          a.age5to17 += wgt.doubleValue();
        } else if (age.intValue() < 60) {
          a.age18to59 += wgt.doubleValue();
        } else {
          a.age60plus += wgt.doubleValue();
        }
      }
    }
    for (Map.Entry<String, Acc> e : byState.entrySet()) {
      Acc a = e.getValue();
      String nameKey = a.stateName == null ? "" : a.stateName.toUpperCase(Locale.ROOT);
      String stFips = SnapBenefitsProvider.STATE_FIPS.get(nameKey);
      if (stFips == null || !stFips.equals(e.getKey())) {
        throw new IOException("snap_participants_by_age: FY" + fiscalYear + " STATE "
            + e.getKey() + " name '" + a.stateName + "' does not match a known state/FIPS");
      }
      Map<String, Object> r = new LinkedHashMap<String, Object>();
      r.put("state_fips", e.getKey());
      r.put("state_abbr", SnapBenefitsProvider.STATE_ABBR.get(nameKey));
      r.put("state_name", a.stateName);
      r.put("year", Integer.valueOf(fiscalYear));
      r.put("persons_participating", Long.valueOf(Math.round(a.persons)));
      r.put("households_participating", Long.valueOf(Math.round(a.households)));
      // Round each band once and derive the total from the rounded bands, so that
      // children_under_18 always equals children_under_5 + children_5_to_17.
      long under5 = Math.round(a.under5);
      long age5to17 = Math.round(a.age5to17);
      r.put("children_under_18", Long.valueOf(under5 + age5to17));
      r.put("children_under_5", Long.valueOf(under5));
      r.put("children_5_to_17", Long.valueOf(age5to17));
      r.put("adults_18_to_59", Long.valueOf(Math.round(a.age18to59)));
      r.put("adults_60_plus", Long.valueOf(Math.round(a.age60plus)));
      r.put("age_unknown", Long.valueOf(Math.round(a.ageUnknown)));
      r.put("qc_sample_households", Long.valueOf(a.sample));
      r.put("qc_first_month", Integer.valueOf(a.firstMonth));
      r.put("qc_last_month", Integer.valueOf(a.lastMonth));
      rows.add(r);
    }
  }
}
