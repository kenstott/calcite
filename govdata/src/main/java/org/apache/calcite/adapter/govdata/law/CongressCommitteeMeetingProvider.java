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

import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.govdata.GovDataException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Committee meetings, hearings and markups of both chambers, from the Congress.gov API.
 *
 * <p>A crawl for one Congress lists {@code /v3/committee-meeting/{congress}} (250 per page, each
 * item with an {@code eventId}, {@code chamber} and {@code updateDate}) and reads the detail of
 * each meeting. Three tables are cut from the same details: {@code committee_meetings} (one row
 * per meeting), {@code committee_meeting_committees} (one per committee holding it) and
 * {@code committee_meeting_witnesses} (one per listed witness).
 */
public class CongressCommitteeMeetingProvider extends AbstractCongressDocumentProvider {

  static final String MEETINGS = "committee_meetings";
  static final String COMMITTEES = "committee_meeting_committees";
  static final String WITNESSES = "committee_meeting_witnesses";

  /** Page size of the listing; 250 is the API maximum. */
  static final int PAGE_SIZE = 250;

  /** Committee schedules are set in Washington; the API states them in UTC. */
  private static final ZoneId WASHINGTON = ZoneId.of("America/New_York");

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override String cacheName() {
    return "congress_committee_meetings";
  }

  @Override String extension() {
    return "json";
  }

  @Override Map<String, String> documentHeaders(Map<String, String> sourceHeaders) {
    return sourceHeaders;
  }

  @Override Map<String, Item> list(String baseUrl, String congress, Map<String, String> headers,
      HttpSourceConfig.RateLimitConfig rateLimit) throws IOException {
    String first = baseUrl + "?format=json&limit=" + PAGE_SIZE;
    Map<String, Item> items = new LinkedHashMap<String, Item>();
    int total = -1;
    String next = first;
    while (next != null) {
      JsonNode page = MAPPER.readTree(get(next, headers, rateLimit));
      JsonNode list = page.get("committeeMeetings");
      JsonNode count = page.path("pagination").get("count");
      if (list == null || !list.isArray() || count == null) {
        throw new GovDataException("Listing " + next
            + " has no committeeMeetings array or pagination count");
      }
      if (total < 0) {
        total = count.asInt();
      }
      for (JsonNode node : list) {
        Item item = itemOf(congress, node);
        items.putIfAbsent(item.key, item);
      }
      JsonNode link = page.path("pagination").get("next");
      next = link == null || link.isNull() ? null : link.asText();
    }
    requireComplete(first, items.size(), total);
    return items;
  }

  static Item itemOf(String congress, JsonNode node) {
    String eventId = requiredText(node, "eventId", node);
    String chamber = requiredText(node, "chamber", node);
    return new Item("congress=" + congress + "/chamber=" + chamber + "/event=" + eventId,
        requiredText(node, "updateDate", node), requiredText(node, "url", node));
  }

  @Override List<Map<String, Object>> rows(String table, Item item, byte[] document)
      throws IOException {
    return rowsOf(table, MAPPER.readTree(document));
  }

  /**
   * The rows one meeting's detail response contributes to a table; for tests.
   *
   * @param table one of the three table names
   * @param response the parsed response body
   */
  static List<Map<String, Object>> rowsOf(String table, JsonNode response) {
    JsonNode m = response.get("committeeMeeting");
    if (m == null || !m.isObject()) {
      throw new GovDataException("Committee meeting response has no committeeMeeting object: "
          + response);
    }
    Map<String, Object> key = new LinkedHashMap<String, Object>();
    key.put("congress", Integer.valueOf(requiredText(m, "congress", m)));
    key.put("chamber", requiredText(m, "chamber", m));
    key.put("event_id", requiredText(m, "eventId", m));
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    if (COMMITTEES.equals(table)) {
      for (JsonNode committee : m.path("committees")) {
        Map<String, Object> row = new LinkedHashMap<String, Object>(key);
        row.put("committee_system_code", requiredText(committee, "systemCode", m));
        row.put("committee_name", text(committee, "name"));
        rows.add(row);
      }
    } else if (WITNESSES.equals(table)) {
      int seq = 0;
      for (JsonNode witness : m.path("witnesses")) {
        Map<String, Object> row = new LinkedHashMap<String, Object>(key);
        row.put("witness_seq", Integer.valueOf(++seq));
        row.put("witness_name", text(witness, "name"));
        row.put("position", text(witness, "position"));
        row.put("organization", text(witness, "organization"));
        rows.add(row);
      }
    } else if (MEETINGS.equals(table)) {
      rows.add(meetingRow(key, m));
    } else {
      throw new GovDataException("CongressCommitteeMeetingProvider does not build " + table);
    }
    return rows;
  }

  private static Map<String, Object> meetingRow(Map<String, Object> key, JsonNode m) {
    Map<String, Object> row = new LinkedHashMap<String, Object>(key);
    row.put("meeting_type", text(m, "type"));
    row.put("meeting_status", text(m, "meetingStatus"));
    row.put("title", text(m, "title"));
    String dateTime = text(m, "date");
    row.put("meeting_date", dateTime == null ? null
        : Instant.parse(dateTime).atZone(WASHINGTON).toLocalDate().toString());
    row.put("meeting_datetime_utc", dateTime);
    JsonNode location = m.path("location");
    row.put("location_building", text(location, "building"));
    row.put("location_room", text(location, "room"));
    row.put("location_address", location.get("address") == null ? null
        : location.get("address").isTextual() ? location.get("address").asText()
            : location.get("address").toString());
    row.put("witness_count", Integer.valueOf(m.path("witnesses").size()));
    List<String> jackets = new ArrayList<String>();
    for (JsonNode transcript : m.path("hearingTranscript")) {
      jackets.add(requiredText(transcript, "jacketNumber", m));
    }
    row.put("transcript_jacket_numbers", jackets.isEmpty() ? null : String.join(",", jackets));
    row.put("video_url", text(m.path("videos").path(0), "url"));
    row.put("update_date", text(m, "updateDate"));
    return row;
  }

  private static String requiredText(JsonNode node, String field, JsonNode context) {
    String v = text(node, field);
    if (v == null) {
      throw new GovDataException("Committee meeting record without " + field + ": " + context);
    }
    return v;
  }

  /** The field's text; null when the source omits it or leaves it blank. */
  private static String text(JsonNode node, String field) {
    JsonNode v = node.get(field);
    if (v == null || v.isNull()) {
      return null;
    }
    String s = v.asText().trim();
    return s.isEmpty() ? null : s;
  }
}
