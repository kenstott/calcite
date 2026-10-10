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

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Senate eFD report listing: pages through {@code /search/report/data/} for one filing year
 * and parses each row into a {@link Filing}. The listing caps a page at {@link #PAGE_SIZE} rows,
 * so a year is read page by page until {@code recordsTotal} rows have arrived.
 */
final class SenateEfdListing {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static final int PAGE_SIZE = 100;

  private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("MM/dd/yyyy");

  /** 7 Annual, 11 Periodic Transaction Report, 10 Due Date Extension, 14 Blind Trusts, 15 Other. */
  private static final String REPORT_TYPES = "[7,11,10,14,15]";

  private static final Pattern LINK =
      Pattern.compile("href=\"/search/view/([a-z\\-/]+?)/([0-9A-Fa-f\\-]{36})/\"[^>]*>([^<]*)<");
  private static final java.util.Set<String> FILER_TYPES =
      new java.util.HashSet<String>(java.util.Arrays.asList("Senator", "Former Senator",
          "Candidate"));
  private static final Pattern FILER_TYPE = Pattern.compile("\\(([^()]*)\\)\\s*$");

  /** One listing row. */
  static final class Filing {
    final String firstName;
    final String lastName;
    final String filerType;
    /** annual, ptr, paper, or extension-notice/regular — the {@code /search/view/} path kind. */
    final String kind;
    final String uuid;
    final String title;
    /** MM/DD/YYYY as the listing prints it. */
    final String filedDate;
    String state;

    Filing(String firstName, String lastName, String filerType, String kind, String uuid,
        String title, String filedDate) {
      this.firstName = firstName;
      this.lastName = lastName;
      this.filerType = filerType;
      this.kind = kind;
      this.uuid = uuid;
      this.title = title;
      this.filedDate = filedDate;
    }

    String viewPath() {
      return "/search/view/" + kind + "/" + uuid + "/";
    }
  }

  private SenateEfdListing() {
  }

  /**
   * Every filing submitted in {@code year}, with {@link Filing#state} set where the state-filtered
   * listing places it. The unfiltered listing is the authority on which filings exist; the per-state
   * passes only attach a state, so a filing no state pass returns keeps a null state.
   */
  static List<Filing> forYear(SenateEfdSession session, int year, List<String> states)
      throws IOException {
    List<Filing> all = page(session, year, "[1,4,5]", null, null);
    Map<String, Filing> byUuid = new LinkedHashMap<String, Filing>();
    for (Filing f : all) {
      if (byUuid.put(f.uuid, f) != null) {
        throw new GovDataException("eFD listing for " + year + " returned filing " + f.uuid
            + " twice");
      }
    }
    for (String state : states) {
      attachState(byUuid, page(session, year, "[1,5]", "senator_state", state), state);
      attachState(byUuid, page(session, year, "[4]", "candidate_state", state), state);
    }
    return all;
  }

  private static void attachState(Map<String, Filing> byUuid, List<Filing> inState,
      String state) {
    for (Filing f : inState) {
      Filing known = byUuid.get(f.uuid);
      if (known == null) {
        throw new GovDataException("eFD state listing (" + state + ") returned filing " + f.uuid
            + " absent from the unfiltered listing");
      }
      known.state = state;
    }
  }

  /**
   * Every row of one query, as a union of date windows each holding at most {@link #PAGE_SIZE}
   * rows. Paging a larger window with {@code start} is unreliable: the listing has no stable
   * order for rows that tie, so a row can arrive on two pages (and another on none). A window
   * that reports at most one page of rows needs no paging at all; a larger one is halved.
   */
  private static List<Filing> page(SenateEfdSession session, int year, String filerTypes,
      String stateParam, String state) throws IOException {
    List<Filing> out = new ArrayList<Filing>();
    window(session, year, LocalDate.of(year, 1, 1), LocalDate.of(year, 12, 31), filerTypes,
        stateParam, state, out);
    return out;
  }

  private static void window(SenateEfdSession session, int year, LocalDate from, LocalDate to,
      String filerTypes, String stateParam, String state, List<Filing> out) throws IOException {
    Map<String, String> params = new LinkedHashMap<String, String>();
    params.put("start", "0");
    params.put("length", Integer.toString(PAGE_SIZE));
    params.put("report_types", REPORT_TYPES);
    params.put("filer_types", filerTypes);
    params.put("submitted_start_date", DATE.format(from) + " 00:00:00");
    params.put("submitted_end_date", DATE.format(to) + " 23:59:59");
    params.put("draw", "1");
    if (stateParam != null) {
      params.put(stateParam, state);
    }
    JsonNode root = MAPPER.readTree(session.listing(params, year < LocalDate.now(ZoneOffset.UTC).getYear()));
    long total = root.path("recordsTotal").asLong(-1);
    JsonNode data = root.path("data");
    if (total < 0 || !data.isArray()) {
      throw new GovDataException("eFD listing " + from + ".." + to
          + ": response lacks recordsTotal/data");
    }
    if (total <= PAGE_SIZE) {
      if (data.size() != total) {
        throw new GovDataException("eFD listing " + from + ".." + to + ": recordsTotal " + total
            + " but " + data.size() + " rows returned");
      }
      for (JsonNode row : data) {
        out.add(parseRow(row));
      }
      return;
    }
    if (from.equals(to)) {
      throw new GovDataException("eFD listing: " + total + " rows filed on " + from
          + " exceed one page and cannot be split further");
    }
    LocalDate mid = from.plusDays(java.time.temporal.ChronoUnit.DAYS.between(from, to) / 2);
    window(session, year, from, mid, filerTypes, stateParam, state, out);
    window(session, year, mid.plusDays(1), to, filerTypes, stateParam, state, out);
  }

  static Filing parseRow(JsonNode row) {
    if (row.size() != 5) {
      throw new GovDataException("eFD listing row has " + row.size() + " cells, expected 5: "
          + row);
    }
    Matcher link = LINK.matcher(row.get(3).asText());
    if (!link.find()) {
      throw new GovDataException("eFD listing row has no report link: " + row);
    }
    String filer = row.get(2).asText().trim();
    String filerType;
    Matcher type = FILER_TYPE.matcher(filer);
    if (type.find()) {
      filerType = type.group(1);
    } else if (FILER_TYPES.contains(filer)) {
      // Scanned paper filings print the filer cell as the bare type, with no "(Name)" part.
      filerType = filer;
    } else {
      throw new GovDataException("eFD listing row has no filer type: " + row);
    }
    return new Filing(row.get(0).asText().trim(), row.get(1).asText().trim(), filerType,
        link.group(1), link.group(2).toLowerCase(java.util.Locale.ROOT),
        link.group(3).replaceAll("\\s+", " ").trim(), row.get(4).asText().trim());
  }
}
