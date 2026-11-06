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

import org.apache.calcite.adapter.file.etl.CsvRecordReader;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SoiMigrationProvider}: header aliasing of the 2007-08 .. 2010-11 files,
 * row filtering, ISO-8859-1 names, and the handling of a county pair listed twice.
 */
@Tag("unit")
class SoiMigrationProviderTest {

  private static Map<String, Integer> index(String header) throws IOException {
    return SoiMigrationProvider.canonicalIndex(
        FiscalHttp.headerIndex(CsvRecordReader.splitFields(header, ',')));
  }

  @Test void legacyQuotedHeaderIsAliased() throws IOException {
    Map<String, Integer> idx = index("\"State_Code_Origin\",\"County_Code_Origin\","
        + "\"State_Code_Dest\",\"County_Code_Dest\",\"State_Abbrv\",\"County_Name\","
        + "\"Return_Num\",\"Exmpt_Num\",\"Aggr_AGI\"");
    assertEquals(0, FiscalHttp.required(idx, "y1_statefips", "t"));
    assertEquals(1, FiscalHttp.required(idx, "y1_countyfips", "t"));
    assertEquals(2, FiscalHttp.required(idx, "y2_statefips", "t"));
    assertEquals(3, FiscalHttp.required(idx, "y2_countyfips", "t"));
    assertEquals(5, FiscalHttp.required(idx, "y2_countyname", "t"));
    assertEquals(6, FiscalHttp.required(idx, "n1", "t"));
    assertEquals(7, FiscalHttp.required(idx, "n2", "t"));
    assertEquals(8, FiscalHttp.required(idx, "agi", "t"));
  }

  @Test void legacyUnquotedHeaderIsAliased() throws IOException {
    Map<String, Integer> idx = index("State_Code_Origin,County_Code_Origin,State_Code_Dest,"
        + "County_Code_Dest,State_Abbrv,County_Name,Return_Num,Exmpt_Num,Aggr_AGI");
    assertEquals(6, FiscalHttp.required(idx, "n1", "t"));
  }

  @Test void canonicalHeaderIsUnchanged() throws IOException {
    Map<String, Integer> idx = index(
        "y1_statefips,y1_countyfips,y2_statefips,y2_countyfips,y2_state,y2_countyname,n1,n2,agi");
    assertEquals(0, FiscalHttp.required(idx, "y1_statefips", "t"));
    assertEquals(8, FiscalHttp.required(idx, "agi", "t"));
  }

  @Test void incompleteLegacyHeaderStillFailsLoudly() throws IOException {
    // Return_Num missing: not the complete legacy set, so no aliasing and the required check throws.
    Map<String, Integer> idx = index("State_Code_Origin,County_Code_Origin,State_Code_Dest,"
        + "County_Code_Dest,State_Abbrv,County_Name,Exmpt_Num,Aggr_AGI");
    assertThrows(IOException.class, () -> FiscalHttp.required(idx, "y1_statefips", "t"));
  }

  @Test void unknownHeaderStillFailsLoudly() throws IOException {
    Map<String, Integer> idx = index("a,b,c,d,e,f,g,h,i");
    assertThrows(IOException.class, () -> FiscalHttp.required(idx, "n1", "t"));
  }

  private static final String LEGACY_HEADER = "\"State_Code_Origin\",\"County_Code_Origin\","
      + "\"State_Code_Dest\",\"County_Code_Dest\",\"State_Abbrv\",\"County_Name\","
      + "\"Return_Num\",\"Exmpt_Num\",\"Aggr_AGI\"\n";

  private static List<Map<String, Object>> fetch(String csv) throws IOException {
    final byte[] bytes = csv.getBytes(StandardCharsets.ISO_8859_1);
    RawCache cache = new RawCache() {
      @Override public InputStream openStream(String url) {
        return new ByteArrayInputStream(bytes);
      }

      @Override public InputStream openStream(String key, ContentSupplier onMiss)
          throws IOException {
        return onMiss.open();
      }

      @Override public boolean isEnabled() {
        return true;
      }
    };
    Map<String, String> vars = new HashMap<String, String>();
    vars.put("effective_year", "2008");
    Iterator<Map<String, Object>> it = new SoiMigrationProvider().fetch(null, vars, cache);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    while (it.hasNext()) {
      rows.add(it.next());
    }
    return rows;
  }

  @Test void legacyFileRowsAreFilteredAndMapped() throws IOException {
    // The header and the Alabama rows follow countyoutflow0809.csv: a county pair, a summary
    // bucket (state 96) and a same-county non-migrant row. The Dona Ana row is a fixture for
    // the ISO-8859-1 name (the real files carry that name as the single byte 0xF1).
    List<Map<String, Object>> rows = fetch(LEGACY_HEADER
        + "\"01\",\"001\",\"01\",\"003\",\"AL\",\"Baldwin County\",22,58,1267\n"
        + "\"01\",\"001\",\"96\",\"000\",\"AL\",\"Total Mig - US & For\",900,1900,40000\n"
        + "\"01\",\"001\",\"01\",\"001\",\"AL\",\"Autauga County\",9000,20000,500000\n"
        + "\"01\",\"001\",\"35\",\"013\",\"NM\",\"Do\u00f1a Ana County\",11,20,300\n");
    assertEquals(2, rows.size());
    assertEquals("01001", rows.get(0).get("origin_county_fips"));
    assertEquals("01003", rows.get(0).get("dest_county_fips"));
    assertEquals("Baldwin County", rows.get(0).get("dest_county_name"));
    assertEquals(22L, rows.get(0).get("num_returns"));
    assertEquals(58L, rows.get(0).get("num_individuals"));
    assertEquals(1267.0, (Double) rows.get(0).get("agi"), 0.0);
    assertEquals("Do\u00f1a Ana County", rows.get(1).get("dest_county_name"));
  }

  @Test void identicalRepeatedCountyPairIsEmittedOnce() throws IOException {
    // countyoutflow1314.csv lists 984 county pairs twice as identical rows; this is one (AL).
    List<Map<String, Object>> rows = fetch(LEGACY_HEADER
        + "\"01\",\"051\",\"01\",\"081\",\"AL\",\"Lee County\",59,124,2665\n"
        + "\"01\",\"051\",\"01\",\"081\",\"AL\",\"Lee County\",59,124,2665\n"
        + "\"01\",\"069\",\"12\",\"063\",\"FL\",\"Jackson County\",54,132,1443\n");
    assertEquals(2, rows.size());
  }

  @Test void conflictingRepeatedCountyPairFailsLoudly() {
    RuntimeException e = assertThrows(RuntimeException.class, () -> fetch(LEGACY_HEADER
        + "\"01\",\"051\",\"01\",\"081\",\"AL\",\"Lee County\",59,124,2665\n"
        + "\"01\",\"051\",\"01\",\"081\",\"AL\",\"Lee County\",60,124,2665\n"));
    assertTrue(e.getMessage().contains("twice with different values"), e.getMessage());
  }
}
