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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Unpivots the AHRF county health-professions file ({@code *hp.csv}) into one row per
 * county, physician specialty and data year.
 *
 * <p>AHRF stores each specialty as year-suffixed column families, e.g. for general
 * obstetrics and gynecology: {@code md_nf_obgyn_gen_23} (non-federal MDs, all activities),
 * {@code md_nf_obgyn_gen_all_pc_23} (of those, in patient care), {@code do_nf_obgyn_gen_23},
 * {@code do_nf_obgyn_gen_all_pc_23} and {@code tot_md_do_obgyn_gen_23}. A specialty is
 * discovered from the header by the presence of an {@code md_nf_<specialty>_all_pc_YY}
 * column, so a specialty HRSA adds in a later release is picked up without a code change,
 * and every year suffix present for it is emitted (the release carries two data years per
 * variable). Specialty keys that begin with {@code all_} are HRSA's group totals (e.g.
 * {@code all_surg_spec}) and overlap the individual specialties.
 *
 * <p>The base class supplies release discovery, download and CSV parsing.
 */
public class AhrfPhysicianSpecialtyTransformer extends AhrfPhysicianSupplyTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(AhrfPhysicianSpecialtyTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern SPECIALTY_PATTERN = Pattern.compile("^md_nf_(.+)_all_pc_(\\d{2})$");

  @Override String parseZip(byte[] zipBytes) throws IOException {
    byte[] geoCsv = null;
    byte[] hpCsv = null;
    try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        String name = entry.getName().toLowerCase(Locale.ROOT);
        if (name.endsWith("geo.csv")) {
          geoCsv = readAll(zis);
        } else if (name.endsWith("hp.csv")) {
          hpCsv = readAll(zis);
        }
      }
    }
    if (geoCsv == null || hpCsv == null) {
      throw new IllegalStateException("AHRF: zip missing geo.csv or hp.csv (geo="
          + (geoCsv != null) + ", hp=" + (hpCsv != null) + ")");
    }

    CsvTable geo = readCsv(geoCsv);
    CsvTable hp = readCsv(hpCsv);

    int geoFipsIdx = requireIndex(geo.headers, "fips_st_cnty");
    int geoStateFipsIdx = requireIndex(geo.headers, "fips_st");
    int geoStateAbbrIdx = requireIndex(geo.headers, "st_name_abbrev");
    int geoCountyNameIdx = requireIndex(geo.headers, "cnty_name");
    int hpFipsIdx = requireIndex(hp.headers, "fips_st_cnty");

    // specialty -> (yy -> true), in sorted order
    Map<String, TreeSet<Integer>> specialties = new LinkedHashMap<>();
    for (String h : hp.headers) {
      Matcher m = SPECIALTY_PATTERN.matcher(h);
      if (m.matches()) {
        TreeSet<Integer> years = specialties.get(m.group(1));
        if (years == null) {
          years = new TreeSet<>();
          specialties.put(m.group(1), years);
        }
        years.add(Integer.parseInt(m.group(2)));
      }
    }
    if (specialties.isEmpty()) {
      throw new IllegalStateException("AHRF: no md_nf_<specialty>_all_pc_YY columns in the hp CSV"
          + " header — HRSA may have renamed the fields");
    }

    Map<String, List<String>> hpByFips = indexByColumn(hp, hpFipsIdx);
    ArrayNode out = MAPPER.createArrayNode();
    for (List<String> geoRow : geo.rows) {
      String fips = value(geoRow, geoFipsIdx);
      if (fips == null) {
        continue;
      }
      List<String> hpRow = hpByFips.get(fips);
      if (hpRow == null) {
        continue;
      }
      for (Map.Entry<String, TreeSet<Integer>> e : specialties.entrySet()) {
        String spec = e.getKey();
        for (int yy : e.getValue()) {
          String sfx = String.format(Locale.ROOT, "_%02d", yy);
          Long mdAll = cell(hp, hpRow, "md_nf_" + spec + sfx);
          Long mdPc = cell(hp, hpRow, "md_nf_" + spec + "_all_pc" + sfx);
          Long doAll = cell(hp, hpRow, "do_nf_" + spec + sfx);
          Long doPc = cell(hp, hpRow, "do_nf_" + spec + "_all_pc" + sfx);
          Long total = cell(hp, hpRow, "tot_md_do_" + spec + sfx);
          if (mdAll == null && mdPc == null && doAll == null && doPc == null && total == null) {
            continue;
          }
          ObjectNode row = MAPPER.createObjectNode();
          put(row, "county_fips", fips);
          put(row, "state_fips", value(geoRow, geoStateFipsIdx));
          put(row, "state_abbr", value(geoRow, geoStateAbbrIdx));
          put(row, "county_name", value(geoRow, geoCountyNameIdx));
          row.put("specialty", spec);
          row.put("data_year", 2000 + yy);
          putLong(row, "md_physicians", mdAll);
          putLong(row, "md_patient_care_physicians", mdPc);
          putLong(row, "do_physicians", doAll);
          putLong(row, "do_patient_care_physicians", doPc);
          putLong(row, "md_do_physicians", total);
          out.add(row);
        }
      }
    }
    LOGGER.debug("AHRF specialty: {} rows across {} specialties", out.size(), specialties.size());
    return out.toString();
  }

  /** Value of a named hp column for a row, or null when the column is absent or blank. */
  private Long cell(CsvTable table, List<String> row, String column) {
    int idx = table.headers.indexOf(column);
    return idx < 0 ? null : parseLong(value(row, idx));
  }
}
