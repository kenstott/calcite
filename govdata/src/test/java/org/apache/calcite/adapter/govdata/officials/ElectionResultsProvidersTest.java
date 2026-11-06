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
package org.apache.calcite.adapter.govdata.officials;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the strict parsing of the MIT Election Lab House and Senate returns. */
@Tag("unit")
class ElectionResultsProvidersTest {

  private static final String SENATE_HEADER = "year,state,state_po,state_fips,state_cen,state_ic,"
      + "office,district,stage,special,candidate,party_detailed,writein,mode,candidatevotes,"
      + "totalvotes,unofficial,version,party_simplified\n";
  private static final String HOUSE_HEADER = "\"year\",\"state\",\"state_po\",\"state_fips\","
      + "\"state_cen\",\"state_ic\",\"office\",\"district\",\"stage\",\"runoff\",\"special\","
      + "\"candidate\",\"party\",\"writein\",\"mode\",\"candidatevotes\",\"totalvotes\","
      + "\"unofficial\",\"version\"\n";

  private static List<Map<String, Object>> senate(String body) throws IOException {
    return SenateElectionResultsProvider.parse(
        new BufferedReader(new StringReader(SENATE_HEADER + body)), "rel");
  }

  private static List<Map<String, Object>> house(String body) throws IOException {
    return HouseElectionResultsProvider.parse(
        new BufferedReader(new StringReader(HOUSE_HEADER + body)));
  }

  @Test void senateRowsParseWithFusionLinesAndResidualBuckets() throws Exception {
    List<Map<String, Object>> rows = senate(
        "2024,NEW YORK,NY,36,21.0,13,US SENATE,statewide,GEN,False,KIRSTEN E. GILLIBRAND,"
        + "DEMOCRATIC/WORKING FAMILIES,False,TOTAL,4711669.0,8381397.0,False,11/20/25,DEMOCRAT\n"
        + "2024,NEW YORK,NY,36,21.0,13,US SENATE,statewide,GEN,False,UNDERVOTES,,False,TOTAL,"
        + "365232.0,8381397.0,False,11/20/25,\n"
        + "1976,TENNESSEE,TN,47,62.0,54,US SENATE,statewide,gen,False,,,True,total,31.0,"
        + "1432046.0,False,20210114,OTHER\n");
    assertEquals(3, rows.size());
    Map<String, Object> g = rows.get(0);
    assertEquals(2024, g.get("year"));
    assertEquals("36", g.get("state_fips"));
    assertEquals("New York", g.get("state_name"));
    assertEquals("gen", g.get("stage"));
    assertEquals("D", g.get("party_major"));
    assertEquals("DEMOCRATIC/WORKING FAMILIES", g.get("party_published"));
    assertEquals(4711669L, g.get("candidate_votes"));
    assertEquals("2025-11-20", g.get("version_date"));
    assertEquals("candidate", g.get("candidate_kind"));
    assertEquals("residual", rows.get(1).get("candidate_kind"));
    assertNull(rows.get(1).get("party_major"));
    assertEquals("write_in_aggregate", rows.get(2).get("candidate_kind"));
    assertNull(rows.get(2).get("candidate"));
    assertEquals(1, g.get("line_no"));
    assertEquals(2, rows.get(1).get("line_no"));
  }

  @Test void senateRunoffStageAndSpecial() throws Exception {
    List<Map<String, Object>> rows = senate(
        "2022,GEORGIA,GA,13,58.0,44,US SENATE,statewide,GEN RUNOFF,False,RAPHAEL WARNOCK,"
        + "DEMOCRAT,False,TOTAL,1820633.0,3541877.0,False,20230920,DEMOCRAT\n"
        + "2021,GEORGIA,GA,13,58.0,44,US SENATE,statewide,runoff,True,KELLY LOEFFLER,"
        + "REPUBLICAN,False,total,2194848.0,4483294.0,True,20210114,REPUBLICAN\n");
    assertEquals("runoff", rows.get(0).get("stage"));
    assertEquals(Boolean.TRUE, rows.get(1).get("special"));
    assertEquals(Boolean.TRUE, rows.get(1).get("unofficial"));
  }

  @Test void senateRejectsUnknownStage() {
    IOException e = assertThrows(IOException.class, () -> senate(
        "2020,GEORGIA,GA,13,58.0,44,US SENATE,statewide,primary,False,X,DEMOCRAT,False,total,"
        + "1.0,2.0,False,20210114,DEMOCRAT\n"));
    assertTrue(e.getMessage().contains("stage 'primary'"), e.getMessage());
  }

  @Test void senateRejectsUnknownSimplifiedParty() {
    IOException e = assertThrows(IOException.class, () -> senate(
        "2020,GEORGIA,GA,13,58.0,44,US SENATE,statewide,gen,False,X,DEMOCRAT,False,total,"
        + "1.0,2.0,False,20210114,GREEN\n"));
    assertTrue(e.getMessage().contains("party_simplified 'GREEN'"), e.getMessage());
  }

  @Test void senatePartyLabelOverridesWrongSimplifiedAndConflictsFail() throws Exception {
    List<Map<String, Object>> rows = senate(
        "2022,ILLINOIS,IL,17,33.0,21,US SENATE,statewide,gen,False,TAMMY DUCKWORTH,DEMOCRATIC,"
        + "False,total,2329136.0,4000000.0,False,20230920,OTHER\n"
        + "2022,ALASKA,AK,2,94.0,81,US SENATE,statewide,gen,False,LISA MURKOWSKI,,False,total,"
        + "114118.0,250000.0,False,20230920,OTHER\n"
        + "2024,NEVADA,NV,32,88.0,65,US SENATE,statewide,gen,False,JACKY S. ROSEN,,False,total,"
        + "1.0,3.0,False,20241212,DEMOCRAT\n"
        + "2022,UTAH,UT,49,87.0,67,US SENATE,statewide,gen,False,EVAN MCMULLIN,INDEPENDENT,"
        + "False,total,1.0,3.0,False,20230920,OTHER\n");
    assertEquals("D", rows.get(0).get("party_major"));
    assertNull(rows.get(1).get("party_major"));
    assertEquals("D", rows.get(2).get("party_major"));
    assertEquals("OTHER", rows.get(3).get("party_major"));
    IOException conflict = assertThrows(IOException.class, () -> senate(
        "2022,UTAH,UT,49,87.0,67,US SENATE,statewide,gen,False,X,DEMOCRAT,False,total,1.0,3.0,"
        + "False,20230920,REPUBLICAN\n"));
    assertTrue(conflict.getMessage().contains("contradicts"), conflict.getMessage());
    IOException unlisted = assertThrows(IOException.class, () -> senate(
        "2022,UTAH,UT,49,87.0,67,US SENATE,statewide,gen,False,X,REFORM DEMOCRATS,False,total,"
        + "1.0,3.0,False,20230920,DEMOCRAT\n"));
    assertTrue(unlisted.getMessage().contains("not a listed"), unlisted.getMessage());
  }

  @Test void senateRejectsNonNumericVotesAndBadVersionAndBadFips() {
    assertThrows(IOException.class, () -> senate(
        "2020,GEORGIA,GA,13,58.0,44,US SENATE,statewide,gen,False,X,DEMOCRAT,False,total,"
        + "1.5,2.0,False,20210114,DEMOCRAT\n"));
    assertThrows(IOException.class, () -> senate(
        "2020,GEORGIA,GA,13,58.0,44,US SENATE,statewide,gen,False,X,DEMOCRAT,False,total,"
        + "1.0,2.0,False,2021-01-14,DEMOCRAT\n"));
    assertThrows(IOException.class, () -> senate(
        "2020,GEORGIA,GA,99,58.0,44,US SENATE,statewide,gen,False,X,DEMOCRAT,False,total,"
        + "1.0,2.0,False,20210114,DEMOCRAT\n"));
    assertThrows(IOException.class, () -> senate(
        "2020,GEORGIA,GA,13,58.0,44,US SENATE,statewide,gen,Maybe,X,DEMOCRAT,False,total,"
        + "1.0,2.0,False,20210114,DEMOCRAT\n"));
  }

  @Test void senateRejectsNonTotalModeAndMissingCandidate() {
    assertThrows(IOException.class, () -> senate(
        "2020,GEORGIA,GA,13,58.0,44,US SENATE,statewide,gen,False,X,DEMOCRAT,False,absentee,"
        + "1.0,2.0,False,20210114,DEMOCRAT\n"));
    assertThrows(IOException.class, () -> senate(
        "2020,GEORGIA,GA,13,58.0,44,US SENATE,statewide,gen,False,,DEMOCRAT,False,total,"
        + "1.0,2.0,False,20210114,DEMOCRAT\n"));
  }

  @Test void headerChangesFail() {
    IOException missing = assertThrows(IOException.class,
        () -> SenateElectionResultsProvider.parse(new BufferedReader(new StringReader(
            SENATE_HEADER.replace("candidatevotes", "votes") + "x\n")), "rel"));
    assertTrue(missing.getMessage().contains("lacks column 'candidatevotes'"));
    IOException extra = assertThrows(IOException.class,
        () -> SenateElectionResultsProvider.parse(new BufferedReader(new StringReader(
            SENATE_HEADER.trim() + ",extra\n")), "rel"));
    assertTrue(extra.getMessage().contains("unexpected columns [extra]"));
  }

  @Test void emptyFileFails() {
    assertThrows(IOException.class, () -> senate(""));
  }

  @Test void dataverseMetadataSelectsTheStateFileAndRejectsDrift() throws Exception {
    String json = "{\"status\":\"OK\",\"data\":{\"latestVersion\":{\"versionState\":\"RELEASED\","
        + "\"versionNumber\":8,\"releaseTime\":\"2026-05-11T16:13:52Z\",\"files\":["
        + "{\"label\":\"codebook-us-senate-1976-2024.md\",\"restricted\":false,"
        + "\"dataFile\":{\"id\":6708560}},"
        + "{\"label\":\"1976-2024-senate-state.tab\",\"restricted\":false,"
        + "\"dataFile\":{\"id\":13887039}}]}}}";
    JsonNode v = SenateElectionResultsProvider.latestVersion(json);
    assertEquals("13887039", SenateElectionResultsProvider.fileId(v));
    assertTrue(SenateElectionResultsProvider.release(v).endsWith("v8 (released 2026-05-11)"));
    assertThrows(IOException.class, () -> SenateElectionResultsProvider.latestVersion(
        json.replace("RELEASED", "DRAFT")));
    assertThrows(IOException.class, () -> SenateElectionResultsProvider.fileId(
        SenateElectionResultsProvider.latestVersion(
            json.replace("1976-2024-senate-state.tab", "other.tab"))));
    assertThrows(IOException.class, () -> SenateElectionResultsProvider.fileId(
        SenateElectionResultsProvider.latestVersion(
            json.replace("\"restricted\":false,\"dataFile\":{\"id\":13887039}",
                "\"restricted\":true,\"dataFile\":{\"id\":13887039}"))));
  }

  @Test void houseRowsParseNaMarkersAtLargeAndPartyMapping() throws Exception {
    List<Map<String, Object>> rows = house(
        "1976,\"Alabama\",\"AL\",1,63,41,\"US House\",1,\"gen\",FALSE,FALSE,\"Bill Davenport\","
        + "\"democrat\",FALSE,\"total\",58906,157170,FALSE,20171005\n"
        + "1976,\"Alabama\",\"AL\",1,63,41,\"US House\",1,\"gen\",FALSE,FALSE,NA,NA,TRUE,"
        + "\"total\",7,157170,FALSE,20171005\n"
        + "1980,\"Alaska\",\"AK\",2,94,81,\"US House\",0,\"gen\",FALSE,FALSE,\"Don Young\","
        + "\"republican\",FALSE,\"total\",1,2,FALSE,20171005\n"
        + "1982,\"Minnesota\",\"MN\",27,41,33,\"US House\",3,\"gen\",FALSE,FALSE,\"A B\","
        + "\"independent-republican\",FALSE,\"total\",1,2,FALSE,20171005\n"
        + "2006,\"Texas\",\"TX\",48,74,49,\"US House\",22,NA,NA,TRUE,\"Shelley Sekula Gibbs\","
        + "\"republican\",FALSE,\"total\",76924,123922,FALSE,20171005\n");
    assertEquals(5, rows.size());
    Map<String, Object> a = rows.get(0);
    assertEquals("01", a.get("state_fips"));
    assertEquals("0101", a.get("cd_fips"));
    assertEquals("D", a.get("party_major"));
    assertEquals(Boolean.FALSE, a.get("at_large"));
    Map<String, Object> wi = rows.get(1);
    assertNull(wi.get("candidate"));
    assertNull(wi.get("party_published"));
    assertNull(wi.get("party_major"));
    assertEquals("write_in_aggregate", wi.get("candidate_kind"));
    assertEquals(2, wi.get("line_no"));
    Map<String, Object> ak = rows.get(2);
    assertEquals(Boolean.TRUE, ak.get("at_large"));
    assertEquals("0200", ak.get("cd_fips"));
    assertEquals("R", rows.get(3).get("party_major"));
    Map<String, Object> tx = rows.get(4);
    assertNull(tx.get("stage"));
    assertNull(tx.get("runoff"));
    assertEquals(Boolean.TRUE, tx.get("special"));
    assertFalse(((String) tx.get("source_release")).isEmpty());
  }

  @Test void houseRejectsBadStageDistrictAndShortRecord() {
    assertThrows(IOException.class, () -> house(
        "1976,\"Alabama\",\"AL\",1,63,41,\"US House\",1,\"runoff\",FALSE,FALSE,\"X\",\"democrat\","
        + "FALSE,\"total\",1,2,FALSE,20171005\n"));
    assertThrows(IOException.class, () -> house(
        "1976,\"Alabama\",\"AL\",1,63,41,\"US House\",77,\"gen\",FALSE,FALSE,\"X\",\"democrat\","
        + "FALSE,\"total\",1,2,FALSE,20171005\n"));
    assertThrows(IOException.class, () -> house(
        "1976,\"Alabama\",\"AL\",1,63,41,\"US House\",1,\"gen\",FALSE\n"));
    assertThrows(IOException.class, () -> house(
        "1976,\"Alabama\",\"AL\",1,63,41,\"US Senate\",1,\"gen\",FALSE,FALSE,\"X\",\"democrat\","
        + "FALSE,\"total\",1,2,FALSE,20171005\n"));
  }

  @Test void stateNamesKeepLowercaseOf() {
    assertEquals("District of Columbia", ElectionResultsSupport.stateName("DISTRICT OF COLUMBIA"));
    assertEquals("New Hampshire", ElectionResultsSupport.stateName("New Hampshire"));
  }
}
