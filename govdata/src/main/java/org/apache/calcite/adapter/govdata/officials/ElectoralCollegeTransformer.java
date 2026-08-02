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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses NARA's per-election Electoral College results page
 * ({@code archives.gov/electoral-college/{year}}) into flat rows.
 *
 * <p>Verified live across 1824, 1876, 1960, 1980, 2000, 2020, and 2024 (2026-08-02): every
 * page has (at least) a state-by-state {@code <table>} whose first header row has two
 * {@code <th>} for State/electoral-vote-count, then a {@code <th colspan=N>For President</th>}
 * and {@code <th colspan=M>For Vice-President</th>} — N and M are however many candidates
 * received at least one elector that year, so this parser reads the colspans rather than
 * assuming a fixed count. The second header row has N+M candidate-name cells (format varies:
 * "Name, of State" / "Name of State" / no state at all for very old pages); data rows are
 * State name, total electoral votes, then N+M numeric-or-"-" cells.
 *
 * <p>Party is derivable only for the page's designated winner/main-opponent, from a separate
 * summary block earlier on the page (e.g. "President: Donald J. Trump [R]") — every other
 * candidate's party is left null, since NARA's page does not state it.
 */
public class ElectoralCollegeTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElectoralCollegeTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // "Donald J. Trump [R]" -> name="Donald J. Trump", party="R"
  private static final Pattern SUMMARY_NAME_PARTY =
      Pattern.compile("^(.*?)\\s*\\[(\\w+)\\]\\s*$");
  // "Kamala D. Harris, of California" or "John F. Kennedy of Massachusetts" -> name + home state
  private static final Pattern NAME_OF_STATE =
      Pattern.compile("^(.*?),?\\s+of\\s+([A-Za-z .]+)$");

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Electoral College: Empty response for {}", context.getUrl());
      return "[]";
    }

    String yearStr = context.getDimensionValues().get("year");
    try {
      Document doc = Jsoup.parse(response);
      java.util.Map<String, String> partyByName = extractPartyFromSummary(doc);

      Elements tables = doc.select("table");
      Element stateTable = findStateByStateTable(tables);
      if (stateTable == null) {
        LOGGER.warn("Electoral College: No state-by-state table found for year={}", yearStr);
        return "[]";
      }

      return parseStateTable(stateTable, yearStr, partyByName);

    } catch (Exception e) {
      throw new RuntimeException("Electoral College: Failed to parse response for "
          + context.getUrl(), e);
    }
  }

  /**
   * The state-by-state table is identified by its header structure (a "For President" /
   * "For Vice-President" colspan header), not by table position — position varied across
   * the pages sampled (table index 1 for most years, but this is more robust than a
   * hardcoded index).
   */
  private Element findStateByStateTable(Elements tables) {
    for (Element table : tables) {
      Elements headerCells = table.select("tr").first() != null
          ? table.select("tr").first().select("th")
          : new Elements();
      boolean hasPresidentHeader = false;
      for (Element th : headerCells) {
        if (th.text().toLowerCase(java.util.Locale.ROOT).contains("for president")) {
          hasPresidentHeader = true;
          break;
        }
      }
      if (hasPresidentHeader) {
        return table;
      }
    }
    return null;
  }

  private String parseStateTable(Element table, String yearStr,
      java.util.Map<String, String> partyByName) {
    Elements rows = table.select("tr");
    if (rows.size() < 2) {
      return "[]";
    }

    Element groupHeaderRow = rows.get(0);
    Elements groupHeaders = groupHeaderRow.select("th");
    int presidentCount = 0;
    int vpCount = 0;
    boolean seenPresident = false;
    for (Element th : groupHeaders) {
      String text = th.text().toLowerCase(java.util.Locale.ROOT);
      int colspan = parseColspan(th);
      if (text.contains("for president")) {
        presidentCount = colspan;
        seenPresident = true;
      } else if (text.contains("for vice")) {
        vpCount = colspan;
      } else if (!seenPresident) {
        // "State" / electoral-vote-count columns before the group headers — not counted.
        continue;
      }
    }
    if (presidentCount == 0 && vpCount == 0) {
      return "[]";
    }

    Element nameHeaderRow = rows.get(1);
    Elements nameCells = nameHeaderRow.select("td");
    if (nameCells.isEmpty()) {
      nameCells = nameHeaderRow.select("th");
    }
    String[] candidateNames = new String[presidentCount + vpCount];
    String[] candidateHomeStates = new String[presidentCount + vpCount];
    for (int i = 0; i < candidateNames.length && i < nameCells.size(); i++) {
      String raw = stripFootnote(nameCells.get(i).text().trim());
      Matcher m = NAME_OF_STATE.matcher(raw);
      if (m.matches()) {
        candidateNames[i] = m.group(1).trim();
        candidateHomeStates[i] = m.group(2).trim();
      } else {
        candidateNames[i] = raw;
        candidateHomeStates[i] = null;
      }
    }

    ArrayNode result = MAPPER.createArrayNode();
    Integer year = parseIntOrNull(yearStr);

    for (int r = 2; r < rows.size(); r++) {
      Elements cells = rows.get(r).select("td");
      if (cells.size() < 2 + candidateNames.length) {
        continue;
      }
      String stateName = stripFootnote(cells.get(0).text().trim());
      if (stateName.isEmpty()) {
        continue;
      }
      Integer stateEv = parseIntOrNull(cells.get(1).text().trim());

      for (int i = 0; i < candidateNames.length; i++) {
        if (candidateNames[i] == null) {
          continue;
        }
        String office = i < presidentCount ? "PRESIDENT" : "VICE_PRESIDENT";
        Integer votes = parseIntOrNull(cells.get(2 + i).text().trim());
        if (votes == null || votes == 0) {
          continue;
        }
        ObjectNode row = MAPPER.createObjectNode();
        row.put("year", year);
        row.put("state_name", stateName);
        if (stateEv != null) {
          row.put("state_electoral_votes", stateEv);
        } else {
          row.putNull("state_electoral_votes");
        }
        row.put("office", office);
        row.put("candidate_name", candidateNames[i]);
        if (candidateHomeStates[i] != null) {
          row.put("candidate_home_state", candidateHomeStates[i]);
        } else {
          row.putNull("candidate_home_state");
        }
        String party = partyByName.get(candidateNames[i]);
        if (party != null) {
          row.put("candidate_party", party);
        } else {
          row.putNull("candidate_party");
        }
        row.put("electoral_votes_won", votes);
        result.add(row);
      }
    }

    LOGGER.debug("Electoral College: Parsed {} rows for year={}", result.size(), yearStr);
    return result.toString();
  }

  /**
   * Reads the page's summary block (e.g. "President: Donald J. Trump [R]") for the two
   * candidates NARA states a party for. Returns an empty map if the summary block isn't in
   * the expected layout — party is a nice-to-have enrichment, not a required field.
   */
  private java.util.Map<String, String> extractPartyFromSummary(Document doc) {
    java.util.Map<String, String> result = new java.util.HashMap<>();
    Elements rows = doc.select("table tr");
    for (Element row : rows) {
      Elements headers = row.select("th");
      Elements cells = row.select("td");
      if (headers.isEmpty() || cells.isEmpty()) {
        continue;
      }
      String label = headers.get(0).text().toLowerCase(java.util.Locale.ROOT);
      if (label.contains("president") || label.contains("opponent")) {
        Matcher m = SUMMARY_NAME_PARTY.matcher(cells.get(0).text().trim());
        if (m.matches()) {
          result.put(m.group(1).trim(), m.group(2).trim());
        }
      }
    }
    return result;
  }

  /**
   * Strips a trailing footnote marker (Jsoup's {@code .text()} concatenates a nested
   * {@code <sup>1</sup>} directly onto the preceding word, e.g. "Connecticut" + "3" ->
   * "Connecticut3", seen on the 1824 page's footnoted state/candidate cells). No real
   * state or candidate name ends in a bare digit, so this is unambiguous.
   */
  private String stripFootnote(String s) {
    return s.replaceAll("\\d+$", "").trim();
  }

  private int parseColspan(Element th) {
    String attr = th.attr("colspan");
    if (attr == null || attr.isEmpty()) {
      return 1;
    }
    try {
      return Integer.parseInt(attr.trim());
    } catch (NumberFormatException e) {
      return 1;
    }
  }

  private Integer parseIntOrNull(String s) {
    if (s == null || s.isEmpty() || "-".equals(s.trim())) {
      return null;
    }
    try {
      return Integer.parseInt(s.trim().replace(",", ""));
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
