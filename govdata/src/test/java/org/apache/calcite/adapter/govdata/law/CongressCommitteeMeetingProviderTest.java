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
package org.apache.calcite.adapter.govdata.law;

import org.apache.calcite.adapter.govdata.GovDataException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link CongressCommitteeMeetingProvider#rowsOf} against the shape the Congress.gov API
 * returned on 2026-11-25 (event 338900 of the 119th Congress, with witnesses added in the shape
 * its House records carry them).
 */
@Tag("unit")
class CongressCommitteeMeetingProviderTest {

  private static final String MEETING = "{\"committeeMeeting\":{\"chamber\":\"Senate\","
      + "\"committees\":[{\"name\":\"Senate Banking, Housing, and Urban Affairs\","
      + "\"systemCode\":\"ssbk00\"}],\"congress\":119,\"date\":\"2026-12-04T02:00:00Z\","
      + "\"eventId\":\"338900\",\"location\":{\"building\":\"Dirksen Senate Office Building\","
      + "\"room\":\"106\"},\"meetingStatus\":\"Scheduled\","
      + "\"title\":\"Oversight hearings to examine United States export controls.\","
      + "\"type\":\"Open Hearing\",\"updateDate\":\"2026-11-24T05:08:41Z\","
      + "\"witnesses\":[{\"name\":\"Mr. A\",\"organization\":\"Org\",\"position\":\"CEO\"},"
      + "{\"name\":\"Ms. B\"}],"
      + "\"hearingTranscript\":[{\"jacketNumber\":35836},{\"jacketNumber\":35835}],"
      + "\"videos\":[{\"url\":\"https://www.congress.gov/event/1\"}]}}";

  private static List<Map<String, Object>> rows(String table, String json) throws Exception {
    JsonNode node = new ObjectMapper().readTree(json);
    return CongressCommitteeMeetingProvider.rowsOf(table, node);
  }

  @Test void meetingRowDatesInWashingtonTime() throws Exception {
    List<Map<String, Object>> rows = rows("committee_meetings", MEETING);
    assertEquals(1, rows.size());
    Map<String, Object> row = rows.get(0);
    assertEquals(119, row.get("congress"));
    assertEquals("338900", row.get("event_id"));
    // 02:00 UTC on the 4th is the evening of the 3rd in Washington.
    assertEquals("2026-12-03", row.get("meeting_date"));
    assertEquals("106", row.get("location_room"));
    assertNull(row.get("location_address"));
    assertEquals(2, row.get("witness_count"));
    assertEquals("35836,35835", row.get("transcript_jacket_numbers"));
  }

  @Test void committeeAndWitnessRows() throws Exception {
    assertEquals("ssbk00",
        rows("committee_meeting_committees", MEETING).get(0).get("committee_system_code"));
    List<Map<String, Object>> witnesses = rows("committee_meeting_witnesses", MEETING);
    assertEquals(2, witnesses.size());
    assertEquals(2, witnesses.get(1).get("witness_seq"));
    assertEquals("Ms. B", witnesses.get(1).get("witness_name"));
    assertNull(witnesses.get(1).get("organization"));
  }

  @Test void meetingWithoutWitnessesHasNoWitnessRows() throws Exception {
    assertTrue(rows("committee_meeting_witnesses",
        "{\"committeeMeeting\":{\"chamber\":\"Senate\",\"congress\":119,\"eventId\":\"1\"}}")
        .isEmpty());
  }

  @Test void unexpectedResponseFails() {
    assertThrows(GovDataException.class,
        () -> rows("committee_meetings", "{\"error\":\"x\"}"));
  }

  @Test void shortListingFails() {
    assertThrows(GovDataException.class,
        () -> AbstractCongressDocumentProvider.requireComplete("u", 9, 10));
  }
}
