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

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Aggregates CPS Voting and Registration Supplement person-level microdata into
 * state-level reported voting/registration rates for {@code cps_voting_supplement}.
 *
 * <p>The Census CPS API ({@code api.census.gov/data/{year}/cps/voting/nov}) returns
 * person-level survey microdata in the standard Census 2-D array format (a header row
 * followed by one row per respondent) — it does not return a pre-tabulated summary
 * table the way the ACS detail/subject-table endpoints do. Each row carries the
 * respondent's state (the {@code for=state:*} geography clause appends a lower-case
 * {@code state} FIPS field to every row), the self-reported answers to the vote
 * question ({@code PES1}) and the registration question ({@code PES2}), and the
 * person weight ({@code PWSSWGT}) needed to produce a population estimate rather than
 * a raw sample count.
 *
 * <p>{@code PWSSWGT} carries 4 implied decimal places — the same convention documented
 * for every CPS replicate weight (see the {@code cps_voting_repwgt_<mon><yy>.sas}
 * program Census ships alongside each supplement's raw files: {@code wt(i)=wt(i)/10000}
 * to "undo" the scaling) — so the raw integer is divided by {@link #WEIGHT_SCALE} to
 * recover the actual person weight before summing.
 *
 * <p>For each state this transformer sums weighted respondent counts for the vote
 * question (universe: citizens 18 and over; {@code PES1} is {@code -1} "Not in
 * Universe" for anyone outside that universe) and the registration question,
 * producing one row per state with the population-weighted "Yes" share among
 * respondents who gave a "Yes"/"No" answer — Census's own voting/registration rate
 * methodology — excluding "Not in Universe", "Refused", and "Don't Know" from both
 * the numerator and the denominator.
 *
 * @see ResponseTransformer
 */
public class CpsVotingSupplementTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CpsVotingSupplementTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** CPS weight variables carry 4 implied decimal places; divide to recover the true weight. */
  private static final double WEIGHT_SCALE = 10000.0;

  /** {@code PES1}/{@code PES2} response codes. */
  private static final int ANSWER_YES = 1;
  private static final int ANSWER_NO = 2;

  /** Sentinel returned by {@link #parseIntSafe(JsonNode)} for a missing/unparseable cell. */
  private static final int PARSE_FAILURE = Integer.MIN_VALUE;

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("cps_voting_supplement: empty response for {}", context.getUrl());
      return "[]";
    }

    JsonNode root;
    try {
      root = MAPPER.readTree(response);
    } catch (Exception e) {
      throw new RuntimeException(
          "cps_voting_supplement: failed to parse response: " + e.getMessage(), e);
    }

    if (root.isObject() && root.has("error")) {
      LOGGER.warn("cps_voting_supplement: API error for {}: {}",
          context.getUrl(), root.path("error").asText());
      return "[]";
    }

    if (!root.isArray() || root.size() < 2) {
      LOGGER.debug("cps_voting_supplement: no data rows for {}", context.getUrl());
      return "[]";
    }

    JsonNode headerRow = root.get(0);
    JsonNode firstCell = headerRow.size() > 0 ? headerRow.get(0) : null;
    if (firstCell != null && firstCell.isTextual()
        && firstCell.asText().toLowerCase(Locale.ROOT).startsWith("error")) {
      LOGGER.warn("cps_voting_supplement: API error for {}: {}",
          context.getUrl(), firstCell.asText());
      return "[]";
    }

    Map<String, Integer> colIndex = new LinkedHashMap<String, Integer>();
    for (int i = 0; i < headerRow.size(); i++) {
      colIndex.put(headerRow.get(i).asText(), Integer.valueOf(i));
    }

    Integer pes1Idx = colIndex.get("PES1");
    Integer pes2Idx = colIndex.get("PES2");
    Integer weightIdx = colIndex.get("PWSSWGT");
    Integer stateIdx = colIndex.get("state");
    if (pes1Idx == null || pes2Idx == null || weightIdx == null || stateIdx == null) {
      throw new RuntimeException("cps_voting_supplement: response missing a required column "
          + "(PES1/PES2/PWSSWGT/state) for " + context.getUrl());
    }

    Map<String, StateAccumulator> byState = new LinkedHashMap<String, StateAccumulator>();
    for (int r = 1; r < root.size(); r++) {
      JsonNode row = root.get(r);
      int weightRaw = parseIntSafe(row.get(weightIdx.intValue()));
      if (weightRaw == PARSE_FAILURE) {
        // Malformed weight cell - the row cannot contribute a population estimate.
        continue;
      }
      String state = row.get(stateIdx.intValue()).asText();
      double weight = weightRaw / WEIGHT_SCALE;
      int pes1 = parseIntSafe(row.get(pes1Idx.intValue()));
      int pes2 = parseIntSafe(row.get(pes2Idx.intValue()));

      StateAccumulator acc = byState.get(state);
      if (acc == null) {
        acc = new StateAccumulator();
        byState.put(state, acc);
      }

      if (pes1 == ANSWER_YES || pes1 == ANSWER_NO) {
        acc.votedSampleSize++;
        acc.votedWeightDenom += weight;
        if (pes1 == ANSWER_YES) {
          acc.votedWeightYes += weight;
        }
      }
      if (pes2 == ANSWER_YES || pes2 == ANSWER_NO) {
        acc.registeredSampleSize++;
        acc.registeredWeightDenom += weight;
        if (pes2 == ANSWER_YES) {
          acc.registeredWeightYes += weight;
        }
      }
    }

    ArrayNode out = MAPPER.createArrayNode();
    for (Map.Entry<String, StateAccumulator> entry : byState.entrySet()) {
      StateAccumulator acc = entry.getValue();
      ObjectNode obj = MAPPER.createObjectNode();
      obj.put("state", entry.getKey());
      obj.put("sample_size_voted", acc.votedSampleSize);
      obj.put("population_reported_voted", acc.votedWeightDenom);
      obj.put("population_voted", acc.votedWeightYes);
      obj.put("voted_rate_pct", ratePct(acc.votedWeightYes, acc.votedWeightDenom));
      obj.put("sample_size_registered", acc.registeredSampleSize);
      obj.put("population_reported_registered", acc.registeredWeightDenom);
      obj.put("population_registered", acc.registeredWeightYes);
      obj.put("registered_rate_pct", ratePct(acc.registeredWeightYes, acc.registeredWeightDenom));
      out.add(obj);
    }

    LOGGER.debug("cps_voting_supplement: aggregated {} states from {} raw rows for {}",
        Integer.valueOf(byState.size()), Integer.valueOf(root.size() - 1), context.getUrl());
    return out.toString();
  }

  /** Returns {@code null} (not zero) when the denominator is zero — an unmeasured rate. */
  private static Double ratePct(double weightedYes, double weightedDenom) {
    return weightedDenom > 0.0 ? Double.valueOf(100.0 * weightedYes / weightedDenom) : null;
  }

  /**
   * Parses a Census microdata cell as an integer. Census microdata cells are JSON strings
   * (numeric text) in the standard 2-D array response; {@link JsonNode#asText()} also
   * handles a cell that arrived as a JSON number.
   *
   * @return the parsed value, or {@link #PARSE_FAILURE} if the cell is missing, null, or
   *     not a valid integer
   */
  private static int parseIntSafe(JsonNode node) {
    if (node == null || node.isNull()) {
      return PARSE_FAILURE;
    }
    String text = node.asText();
    if (text == null || text.isEmpty()) {
      return PARSE_FAILURE;
    }
    try {
      return Integer.parseInt(text.trim());
    } catch (NumberFormatException e) {
      return PARSE_FAILURE;
    }
  }

  /** Per-state running totals for the vote and registration questions. */
  private static final class StateAccumulator {
    private long votedSampleSize;
    private double votedWeightYes;
    private double votedWeightDenom;
    private long registeredSampleSize;
    private double registeredWeightYes;
    private double registeredWeightDenom;
  }
}
