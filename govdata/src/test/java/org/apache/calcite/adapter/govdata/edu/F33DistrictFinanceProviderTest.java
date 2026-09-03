/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.edu;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Covers the two ways the Census F-33 individual-unit file varies across fiscal years: the header
 * schema changed at FY2022, and the delimiter is not the same in every year despite the shared
 * {@code .txt} name.
 */
@Tag("unit")
class F33DistrictFinanceProviderTest {

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> parse(String content, int year) throws Exception {
    Method m = F33DistrictFinanceProvider.class.getDeclaredMethod(
        "parseRows", byte[].class, int.class);
    m.setAccessible(true);
    return (List<Map<String, Object>>) m.invoke(new F33DistrictFinanceProvider(),
        content.getBytes(StandardCharsets.ISO_8859_1), year);
  }

  /**
   * FY2022 onward: the header carries FIPST directly, alongside PID6/UNIT_TYPE/SCHLEV.
   */
  @Test void readsStateFipsFromFipstOnNewerLayout() throws Exception {
    String file = "\"STATE\",\"PID6\",\"UNIT_TYPE\",\"FIPST\",\"NAME\",\"CONUM\",\"NCESID\"\n"
        + "\"AL\",\"100191\",\"5\",\"01\",\"BALDWIN COUNTY\",\"01003\",\"0100270\"\n";
    List<Map<String, Object>> rows = parse(file, 2023);
    assertEquals(1, rows.size());
    assertEquals("01", rows.get(0).get("state_fips"));
    assertEquals("01003", rows.get(0).get("county_fips"));
    assertEquals("100191", rows.get(0).get("pid6"));
  }

  /**
   * FY2021 and earlier: no FIPST column at all, so the state FIPS comes from the leading two
   * digits of the county FIPS. The columns that vintage genuinely lacks stay null rather than
   * being invented.
   */
  @Test void derivesStateFipsFromCountyFipsOnOlderLayout() throws Exception {
    String file = "\"IDCENSUS\",\"NAME\",\"CONUM\",\"CSA\",\"CBSA\",\"NCESID\"\n"
        + "\"01500100100000\",\"AUTAUGA COUNTY\",\"01001\",\"N\",\"33860\",\"0100240\"\n";
    List<Map<String, Object>> rows = parse(file, 2016);
    assertEquals(1, rows.size());
    assertEquals("01", rows.get(0).get("state_fips"));
    assertEquals("01001", rows.get(0).get("county_fips"));
    assertNull(rows.get(0).get("pid6"), "PID6 does not exist in this vintage");
    assertNull(rows.get(0).get("school_level"), "SCHLEV does not exist in this vintage");
  }

  /**
   * A multi-county educational cooperative carries CONUM "M" (not applicable), which yields no
   * state — inventing one would be worse than leaving it null.
   */
  @Test void leavesStateFipsNullWhenCountyIsNotApplicable() throws Exception {
    String file = "\"IDCENSUS\",\"NAME\",\"CONUM\",\"NCESID\"\n"
        + "\"54999900000000\",\"EASTERN PANHANDLE COOP\",\"M\",\"5400001\"\n";
    List<Map<String, Object>> rows = parse(file, 2019);
    assertEquals(1, rows.size());
    assertNull(rows.get(0).get("state_fips"));
  }

  /**
   * FY2019's file is tab-separated where its neighbours are comma-separated. Parsing it with a
   * fixed comma collapses each line into one field, so every column past the first reads as
   * absent and the row is dropped for want of an NCESID.
   */
  @Test void parsesTabSeparatedVintage() throws Exception {
    String file = "IDCENSUS\tNAME\tCONUM\tCBSA\tNCESID\tTOTALREV\n"
        + "01500100100000\tAUTAUGA COUNTY\t01001\t33860\t0100240\t88218\n";
    List<Map<String, Object>> rows = parse(file, 2019);
    assertEquals(1, rows.size(), "tab-separated rows must parse, not collapse into one field");
    assertEquals("0100240", rows.get(0).get("leaid"));
    assertEquals("01", rows.get(0).get("state_fips"));
    assertEquals("AUTAUGA COUNTY", rows.get(0).get("district_name"));
    assertEquals(88218.0, rows.get(0).get("total_revenue_thousand"));
  }

  /** A comma-separated file whose values contain tabs must still be read as comma-separated. */
  @Test void prefersCommaWhenHeaderHasMoreCommasThanTabs() throws Exception {
    String file = "\"IDCENSUS\",\"NAME\",\"CONUM\",\"NCESID\"\n"
        + "\"01500100100000\",\"AUTAUGA\tCOUNTY\",\"01001\",\"0100240\"\n";
    List<Map<String, Object>> rows = parse(file, 2016);
    assertEquals(1, rows.size());
    assertEquals("0100240", rows.get(0).get("leaid"));
  }
}
