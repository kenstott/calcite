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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link PepPopulationProvider}. Rows are copied from the Census
 * co-est2025-alldata.csv (Alabama and Autauga County), cut to the columns the provider reads;
 * the Dona Ana row is a fixture for the encoding.
 */
@Tag("unit")
class PepPopulationProviderTest {

  private static final String CSV =
      "SUMLEV,STATE,COUNTY,STNAME,CTYNAME,POPESTIMATE2024,POPESTIMATE2025\n"
          + "010,00,000,United States,United States,1,1\n"
          + "040,01,000,Alabama,Alabama,5163055,5193088\n"
          + "050,01,001,Alabama,Autauga County,61476,61920\n"
          + "050,35,013,New Mexico,Doña Ana County,7,8\n";

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

  @Test void emitsStateAndCountyRowsForTheYear() throws IOException {
    Fixture cache = new Fixture(CSV);
    List<Map<String, Object>> rows = drain(
        new PepPopulationProvider().fetch(null, vars("2025", "2025"), cache));
    assertEquals(
        "https://www2.census.gov/programs-surveys/popest/datasets/2020-2025/counties/totals/"
            + "co-est2025-alldata.csv", cache.url);
    assertEquals(3, rows.size());
    Map<String, Object> st = rows.get(0);
    assertEquals("state", st.get("geography"));
    assertEquals("01", st.get("state"));
    assertNull(st.get("county"));
    assertNull(st.get("county_fips"));
    assertEquals("Alabama", st.get("geo_name"));
    assertEquals(5193088L, st.get("population"));
    assertEquals("2025", st.get("vintage"));
    Map<String, Object> co = rows.get(1);
    assertEquals("county", co.get("geography"));
    assertEquals("01001", co.get("county_fips"));
    assertEquals("001", co.get("county"));
    assertEquals("Autauga County", co.get("geo_name"));
    assertEquals(61920L, co.get("population"));
    assertEquals("Doña Ana County", rows.get(2).get("geo_name"));
  }

  @Test void selectsTheRequestedYearColumn() throws IOException {
    List<Map<String, Object>> rows = drain(
        new PepPopulationProvider().fetch(null, vars("2024", "2025"), new Fixture(CSV)));
    assertEquals(5163055L, rows.get(0).get("population"));
    assertEquals(61476L, rows.get(1).get("population"));
  }

  @Test void yearMissingFromFileFailsLoudly() {
    assertThrows(IOException.class,
        () -> new PepPopulationProvider().fetch(null, vars("2023", "2025"), new Fixture(CSV)));
  }

  @Test void yearBeyondVintageFailsLoudly() {
    assertThrows(IOException.class,
        () -> new PepPopulationProvider().fetch(null, vars("2026", "2025"), new Fixture(CSV)));
  }

  @Test void missingVintageFailsLoudly() {
    assertThrows(IOException.class,
        () -> new PepPopulationProvider().fetch(null, vars("2025", null), new Fixture(CSV)));
  }
}
