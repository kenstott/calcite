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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link CmsPosTransformer}. */
@Tag("unit")
class CmsPosTransformerTest {

  private static final Charset CP1252 = Charset.forName("windows-1252");

  private static final String QIES_HEADER =
      "PRVDR_NUM,PRVDR_CTGRY_CD,PRVDR_CTGRY_SBTYP_CD,FAC_NAME,CITY_NAME,STATE_CD,ZIP_CD,"
          + "FIPS_STATE_CD,FIPS_CNTY_CD,CRTFCTN_DT,ORGNL_PRTCPTN_DT,TRMNTN_EXPRTN_DT,"
          + "CRTFCTN_ACTN_TYPE_CD,PGM_TRMNTN_CD,GNRL_CNTL_TYPE_CD,CROSS_REF_PROVIDER_NUMBER,"
          + "PSYCH_UNIT_TRMNTN_CD,PSYCH_UNIT_TRMNTN_DT,REHAB_UNIT_TRMNTN_CD,"
          + "REHAB_UNIT_TRMNTN_DT,BED_CNT,CRTFD_BED_CNT,ICFIID_BED_CNT,MDCD_NF_BED_CNT,"
          + "MDCR_SNF_BED_CNT,MDCR_MDCD_SNF_BED_CNT,HOSPC_BED_CNT,REHAB_BED_CNT,"
          + "PSYCH_UNIT_BED_CNT,REHAB_UNIT_BED_CNT";

  private static final String IQIES_HEADER =
      "prvdr_num,fac_name,prvdr_type_id,city_name,state_cd,zip_cd,fips_state_cd,fips_cnty_cd,"
          + "crtfctn_dt,orgnl_prtcptn_dt,trmntn_exprtn_dt,crtfctn_actn_type_cd,pgm_trmntn_cd,"
          + "control_type,bed_cnt,crtfd_bed_cnt";

  @TempDir Path tmp;

  private static String qies(String number, String category, String name, String terminated,
      String code, String crossRef) {
    return number + "," + category + ",01,\"" + name + "\",TOWN,AL,36093,01,051,20180201,"
        + "20000701," + terminated + ",2," + code + ",04," + crossRef + ",,,,,12,12,,,,,,,,";
  }

  private static String iqies(String number, String type, String terminated, String action) {
    return number + ",CLINIC " + number + "," + type + ",TOWN,AL,36093,01,051,02/01/2018,"
        + "07/01/2000," + terminated + "," + action + ","
        + ("Not Available".equals(terminated) ? "00" : "01 ")
        + ",Non Profit Corporation,Not Applicable,Not Applicable";
  }

  private CmsPosTransformer.Source source(Path dir, String name, Charset charset,
      String header, String... records) throws IOException {
    Path file = dir.resolve(name);
    Files.write(file, (header + "\n" + String.join("\n", records) + "\n").getBytes(charset));
    return new CmsPosTransformer.Source(file, charset);
  }

  private Map<String, Map<String, Object>> union(String[] hospitals, String[] others,
      String[] combined) throws IOException {
    Path dir = Files.createDirectory(tmp.resolve("pos"));
    Iterator<Map<String, Object>> rows = CmsPosTransformer.rows(dir,
        source(dir, "qies.csv", CP1252, QIES_HEADER, hospitals),
        source(dir, "iqies.csv", StandardCharsets.UTF_8, IQIES_HEADER, others),
        source(dir, "qies-combined.csv", CP1252, QIES_HEADER, combined));
    Map<String, Map<String, Object>> out = new LinkedHashMap<String, Map<String, Object>>();
    while (rows.hasNext()) {
      Map<String, Object> row = rows.next();
      out.put((String) row.get("provider_number"), row);
    }
    assertFalse(Files.exists(dir), "downloads are deleted once the rows are consumed");
    return out;
  }

  private static String[] allTypes() {
    return new String[] {
        iqies("010001P", "4", "Not Available", "Initial"),
        iqies("013434", "9", "03/02/2022", "Recertification"),
        iqies("010002C", "14", "Not Available", "Complaint"),
        iqies("011800", "15", "Not Available", "Not Available"),
        iqies("015000", "20", "Not Available", "Initial")
    };
  }

  @Test void unionsHospitalsIqiesTypesAndTerminatedLegacyProviders() throws IOException {
    Map<String, Map<String, Object>> rows = union(
        new String[] {qies("010001", "01", "CHILDREN’S HOSPITAL", "", "00", "")},
        allTypes(),
        new String[] {
            qies("010001", "01", "OLD NAME", "", "00", ""),
            qies("013434", "12", "CLINIC", "20220302", "01", "013867"),
            qies("019999", "19", "CLOSED CENTER", "19991231", "01", ""),
            qies("018888", "21", "ACTIVE BUT DROPPED", "", "00", ""),
            qies("017777", "01", "DROPPED HOSPITAL", "19991231", "01", "")
        });

    assertEquals(
        new ArrayList<String>(
            java.util.Arrays.asList(
                "010001", "010001P", "013434", "010002C", "011800", "019999")),
        new ArrayList<String>(rows.keySet()));

    Map<String, Object> hospital = rows.get("010001");
    assertEquals("CHILDREN’S HOSPITAL", hospital.get("facility_name"));
    assertEquals(Long.valueOf(12), hospital.get("bed_count"));
    assertEquals("04", hospital.get("ownership_code"));
    assertNull(hospital.get("ownership_description"));
    assertEquals(Boolean.FALSE, hospital.get("is_terminated"));

    Map<String, Object> clinic = rows.get("013434");
    assertEquals("12", clinic.get("provider_category_code"));
    assertEquals("2022-03-02", clinic.get("termination_date"));
    assertEquals(Boolean.TRUE, clinic.get("is_terminated"));
    assertEquals("01", clinic.get("termination_code"));
    assertEquals("20180201", clinic.get("certification_date"));
    assertEquals("2000-07-01", clinic.get("original_participation_date"));
    assertEquals("2", clinic.get("certification_action_type_code"));
    assertEquals("013867", clinic.get("cross_ref_provider_number"));
    assertEquals("01051", clinic.get("county_fips"));
    assertEquals("Non Profit Corporation", clinic.get("ownership_description"));
    assertNull(clinic.get("ownership_code"));
    assertNull(clinic.get("bed_count"));

    assertNull(rows.get("010002C").get("certification_action_type_code"));
    assertEquals("06", rows.get("010001P").get("provider_category_code"));
    assertEquals("19", rows.get("019999").get("provider_category_code"));
    assertEquals("1999-12-31", rows.get("019999").get("termination_date"));
  }

  @Test void failsWhenNewestQiesReleaseHoldsOtherCategories() {
    assertThrows(IOException.class, () -> union(
        new String[] {qies("013434", "12", "CLINIC", "", "00", "")}, allTypes(),
        new String[0]));
  }

  @Test void failsWhenIqiesReleaseLacksACategory() {
    IOException e = assertThrows(IOException.class, () -> union(
        new String[] {qies("010001", "01", "HOSPITAL", "", "00", "")},
        new String[] {iqies("013434", "9", "Not Available", "Initial")},
        new String[0]));
    assertTrue(e.getMessage().contains("06"), e.getMessage());
  }

  @Test void failsOnAProviderNumberInBothReleases() {
    assertThrows(IllegalStateException.class, () -> union(
        new String[] {qies("013434", "01", "HOSPITAL", "", "00", "")}, allTypes(),
        new String[0]));
  }

  private static InputStream catalog(String... datasets) {
    return new ByteArrayInputStream(
        ("{\"dataset\":[" + String.join(",", datasets) + "]}")
            .getBytes(StandardCharsets.UTF_8));
  }

  private static String dataset(String series, String start, String url) {
    return "{\"title\":\"" + series + " : " + start + "\",\"inSeries\":[{\"title\":\"" + series
        + "\"}],\"temporal\":[{\"startDate\":\"" + start + "\"}],\"distribution\":["
        + "{\"format\":\"CSV\",\"downloadURL\":\"" + url + "\"},{\"format\":\"API\"}]}";
  }

  @Test void resolvesReleasesOfASeriesByQuarter() throws IOException {
    CmsPosCatalog series = CmsPosCatalog.parse(
        catalog(dataset("QIES", "2026-07-01", "q3"), dataset("QIES", "2026-04-01", "q2"),
            dataset("Other", "2027-01-01", "x")),
        "QIES")[0];
    assertEquals("2026-07-01", series.latestQuarter());
    assertEquals("q2", series.csvUrl("2026-04-01"));
    assertThrows(IOException.class, () -> series.csvUrl("2025-01-01"));
  }

  @Test void failsWhenASeriesIsMissingFromTheCatalog() {
    assertThrows(IOException.class,
        () -> CmsPosCatalog.parse(catalog(dataset("QIES", "2026-07-01", "q3")), "iQIES"));
  }
}
