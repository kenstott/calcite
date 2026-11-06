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
package org.apache.calcite.adapter.govdata.census;

import org.apache.calcite.adapter.file.etl.RawCache;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link PepComponentsProvider}: column selection per data year, the header
 * difference between the 2010-2019 and later vintages, ISO-8859-1 names, and loud failure.
 * The rows are copied from the Census co-est2019-alldata.csv and co-est2025-alldata.csv files
 * (Alabama and Autauga County), cut to the columns the provider reads; the Dona Ana row is a
 * fixture for the encoding.
 */
@Tag("unit")
class PepComponentsProviderTest {

  private static final String OLD_2019 =
      "SUMLEV,STATE,COUNTY,STNAME,CTYNAME,POPESTIMATE2019,BIRTHS2019,DEATHS2019,NATURALINC2019,"
          + "INTERNATIONALMIG2019,DOMESTICMIG2019,NETMIG2019,RESIDUAL2019\n"
          + "040,01,000,Alabama,Alabama,4903185,57313,53879,3434,2772,9387,12159,-89\n"
          + "050,01,001,Alabama,Autauga County,55869,624,541,83,-16,270,254,-1\n";

  private static final String NEW_2021 =
      "SUMLEV,STATE,COUNTY,STNAME,CTYNAME,POPESTIMATE2021,BIRTHS2021,DEATHS2021,NATURALCHG2021,"
          + "INTERNATIONALMIG2021,DOMESTICMIG2021,NETMIG2021,RESIDUAL2021\n"
          + "040,01,000,Alabama,Alabama,5050058,57327,69137,-11810,1808,25795,27603,1303\n"
          + "050,01,001,Alabama,Autauga County,59207,702,700,2,15,218,233,58\n"
          + "050,35,013,New Mexico,Doña Ana County,1,2,3,-1,4,5,9,0\n";

  /** A RawCache that serves one fixture for any URL and remembers the URL asked for. */
  private static final class Fixture implements RawCache {
    private final byte[] bytes;
    private String url;

    Fixture(String csv) {
      this.bytes = csv.getBytes(StandardCharsets.ISO_8859_1);
    }

    @Override public InputStream openStream(String u) {
      this.url = u;
      return new ByteArrayInputStream(bytes);
    }

    @Override public InputStream openStream(String key, ContentSupplier onMiss) throws IOException {
      return onMiss.open();
    }

    @Override public boolean isEnabled() {
      return true;
    }
  }

  private static Map<String, String> vars(String year, String vintage) {
    Map<String, String> v = new HashMap<String, String>();
    v.put("effective_year", year);
    if (vintage != null) {
      v.put("pep_vintage", vintage);
    }
    return v;
  }

  private static List<Map<String, Object>> drain(Iterator<Map<String, Object>> it) {
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    while (it.hasNext()) {
      out.add(it.next());
    }
    return out;
  }

  @Test void oldVintageReadsNaturalIncAndSkipsStateRows() throws IOException {
    Fixture cache = new Fixture(OLD_2019);
    List<Map<String, Object>> rows = drain(
        new PepComponentsProvider().fetch(null, vars("2019", "2025"), cache));
    assertEquals(
        "https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/totals/"
            + "co-est2019-alldata.csv", cache.url);
    assertEquals(1, rows.size());
    Map<String, Object> r = rows.get(0);
    assertEquals("01001", r.get("county_fips"));
    assertEquals("01", r.get("state_fips"));
    assertEquals("Autauga County", r.get("county_name"));
    assertEquals(55869L, r.get("population_estimate"));
    assertEquals(624L, r.get("births"));
    assertEquals(541L, r.get("deaths"));
    assertEquals(83L, r.get("natural_change"));
    assertEquals(-16L, r.get("international_migration"));
    assertEquals(270L, r.get("domestic_migration"));
    assertEquals(254L, r.get("net_migration"));
    assertEquals(-1L, r.get("residual"));
    assertEquals("2019", r.get("vintage"));
  }

  @Test void newVintageReadsNaturalChgAndLatin1Names() throws IOException {
    Fixture cache = new Fixture(NEW_2021);
    List<Map<String, Object>> rows = drain(
        new PepComponentsProvider().fetch(null, vars("2021", "2025"), cache));
    assertEquals(
        "https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/counties/totals/"
            + "co-est2025-alldata.csv", cache.url);
    assertEquals(2, rows.size());
    assertEquals(2L, rows.get(0).get("natural_change"));
    assertEquals(218L, rows.get(0).get("domestic_migration"));
    assertEquals("2025", rows.get(0).get("vintage"));
    assertEquals("35013", rows.get(1).get("county_fips"));
    assertEquals("Doña Ana County", rows.get(1).get("county_name"));
  }

  @Test void yearMissingFromFileFailsLoudly() {
    Fixture cache = new Fixture(NEW_2021);
    assertThrows(IOException.class,
        () -> new PepComponentsProvider().fetch(null, vars("2026", "2025"), cache));
  }

  @Test void newYearWithoutVintageFailsLoudly() {
    Fixture cache = new Fixture(NEW_2021);
    assertThrows(IOException.class,
        () -> new PepComponentsProvider().fetch(null, vars("2021", null), cache));
  }
}
