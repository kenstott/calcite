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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;

import java.util.List;
import java.util.Map;

/**
 * Transforms the {@code unitedstates/congress-legislators}
 * {@code committee-membership-current.yaml} body (a YAML map of committee id -> list of
 * member entries) into flat rows, one per (committee, member) assignment.
 *
 * <p>{@code committee_id} joins to {@link CongressCommitteesTransformer}'s
 * {@code committee_id} (full committees and subcommittees share the same id space).
 * {@code bioguide_id} joins to {@code officials.members} — this file is always a
 * current-Congress snapshot with no congress number of its own, so join against the
 * member's most recent congress rather than a specific one.
 */
public class CongressCommitteeMembershipTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CongressCommitteeMembershipTransformer.class);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Congress Committee Membership: Empty response for {}", context.getUrl());
      return "[]";
    }

    Map<String, Object> byCommittee = parseYamlMap(response);
    ArrayNode result = JSON_MAPPER.createArrayNode();
    for (Map.Entry<String, Object> entry : byCommittee.entrySet()) {
      String committeeId = entry.getKey();
      if (!(entry.getValue() instanceof List)) {
        continue;
      }
      for (Object memberObj : (List<?>) entry.getValue()) {
        if (!(memberObj instanceof Map)) {
          continue;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> member = (Map<String, Object>) memberObj;
        ObjectNode row = JSON_MAPPER.createObjectNode();
        row.put("committee_id", committeeId);
        row.put("bioguide_id", asString(member.get("bioguide")));
        row.put("member_name", asString(member.get("name")));
        row.put("party", asString(member.get("party")));
        putIntOrNull(row, "rank", member.get("rank"));
        row.put("title", asString(member.get("title")));
        result.add(row);
      }
    }

    LOGGER.debug("Congress Committee Membership: Transformed {} rows", result.size());
    return result.toString();
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> parseYamlMap(String yamlText) {
    LoaderOptions loaderOptions = new LoaderOptions();
    Yaml yaml = new Yaml(loaderOptions);
    Object parsed = yaml.load(yamlText);
    if (!(parsed instanceof Map)) {
      throw new IllegalStateException(
          "Congress Committee Membership: expected a YAML map at the document root");
    }
    return (Map<String, Object>) parsed;
  }

  private static String asString(Object value) {
    return value == null ? null : value.toString();
  }

  private static void putIntOrNull(ObjectNode row, String key, Object value) {
    if (value instanceof Number) {
      row.put(key, ((Number) value).intValue());
    } else {
      row.putNull(key);
    }
  }
}
