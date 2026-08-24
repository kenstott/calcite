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
 * Transforms the {@code unitedstates/congress-legislators} {@code committees-current.yaml}
 * body (a YAML list of committee entries, each optionally holding a nested
 * {@code subcommittees} list) into flat rows — one per full committee and one per
 * subcommittee, linked by {@code parent_committee_id}.
 *
 * <p>{@code committee_id} is the join key into {@code committee_membership} — for a
 * subcommittee it is synthesized as the parent's {@code thomas_id} plus the subcommittee's
 * own {@code thomas_id} (e.g. parent {@code HSAG} + subcommittee {@code 15} = {@code HSAG15}),
 * matching how {@code committee-membership-current.yaml} keys its map.
 */
public class CongressCommitteesTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CongressCommitteesTransformer.class);
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("Congress Committees: Empty response for {}", context.getUrl());
      return "[]";
    }

    List<Map<String, Object>> committees = parseYamlList(response);
    ArrayNode result = JSON_MAPPER.createArrayNode();
    for (Map<String, Object> committee : committees) {
      String chamber = asString(committee.get("type"));
      String thomasId = asString(committee.get("thomas_id"));
      result.add(toRow(committee, chamber, thomasId, null));

      Object subs = committee.get("subcommittees");
      if (subs instanceof List) {
        for (Object subObj : (List<?>) subs) {
          if (!(subObj instanceof Map)) {
            continue;
          }
          @SuppressWarnings("unchecked")
          Map<String, Object> sub = (Map<String, Object>) subObj;
          String subThomasId = asString(sub.get("thomas_id"));
          String compositeId = thomasId != null && subThomasId != null
              ? thomasId + subThomasId : null;
          result.add(toRow(sub, chamber, compositeId, thomasId));
        }
      }
    }

    LOGGER.debug("Congress Committees: Transformed {} rows", result.size());
    return result.toString();
  }

  private ObjectNode toRow(Map<String, Object> entry, String chamber, String committeeId,
      String parentCommitteeId) {
    ObjectNode row = JSON_MAPPER.createObjectNode();
    row.put("committee_id", committeeId);
    row.put("chamber", chamber);
    row.put("name", asString(entry.get("name")));
    row.put("parent_committee_id", parentCommitteeId);
    row.put("is_subcommittee", parentCommitteeId != null);
    row.put("url", asString(entry.get("url")));
    row.put("minority_url", asString(entry.get("minority_url")));
    row.put("jurisdiction", asString(entry.get("jurisdiction")));
    row.put("jurisdiction_source", asString(entry.get("jurisdiction_source")));
    row.put("address", asString(entry.get("address")));
    row.put("phone", asString(entry.get("phone")));
    row.put("rss_url", asString(entry.get("rss_url")));
    return row;
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> parseYamlList(String yamlText) {
    LoaderOptions loaderOptions = new LoaderOptions();
    Yaml yaml = new Yaml(loaderOptions);
    Object parsed = yaml.load(yamlText);
    if (!(parsed instanceof List)) {
      throw new IllegalStateException(
          "Congress Committees: expected a YAML list at the document root");
    }
    return (List<Map<String, Object>>) (List<?>) parsed;
  }

  private static String asString(Object value) {
    return value == null ? null : value.toString();
  }
}
