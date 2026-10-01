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

import org.apache.calcite.adapter.file.etl.CrossProcessRateLimiter;
import org.apache.calcite.adapter.file.etl.RowContext;
import org.apache.calcite.adapter.file.etl.RowTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Populates {@code members.current_member} with a per-member detail call to
 * {@code /v3/member/{bioguideId}} — the only Congress.gov endpoint that carries the
 * {@code currentMember} flag; the list endpoint this table otherwise fetches from does not
 * (see the {@code current_member} column comment in officials-schema.yaml).
 *
 * <p>The same detail call also resolves {@code party_name} to the party held in the row's
 * Congress from {@code partyHistory} (see {@link #partyForCongress}).
 *
 * <p>One detail call per row (N+1): a member serving several congresses is re-fetched once per
 * congress-partition row, since row transformers are re-instantiated per partition and cache
 * nothing across them. On any fetch/parse failure for a row, {@code current_member} is left
 * null (its accurate "unknown" state) rather than defaulted to true/false; {@code party_name}
 * is likewise nulled, never left as the list endpoint's latest-party value — the row itself is
 * kept and a warning is logged, so a failed detail call cannot drop otherwise-good member data.
 *
 * <p>Reads {@code api_key} from this table's already-resolved {@code source.parameters} (the
 * same value the primary list-endpoint fetch uses) rather than {@link
 * org.apache.calcite.adapter.file.etl.ModelOperand} — {@code ModelOperand.capture} runs only
 * after {@code FileSchemaFactory}'s ETL phase completes, so during the ETL run itself (when row
 * transformers execute) a schema-level operand would still read back null.
 */
public class CongressMemberCurrentEnricher implements RowTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CongressMemberCurrentEnricher.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String RATE_LIMIT_KEY = "api.congress.gov:member-detail";
  private static final long INTERVAL_MS = 200L;

  @Override public List<Map<String, Object>> transform(Map<String, Object> row, RowContext context) {
    Object bioguideId = row.get("bioguide_id");
    if (bioguideId == null) {
      row.put("current_member", null);
      row.put("party_name", null);
      return Collections.singletonList(row);
    }

    String apiKey = context.getTableConfig().getSource().getParameters().get("api_key");
    int congress = Integer.parseInt(context.getDimensionValues().get("congress"));

    try {
      CrossProcessRateLimiter.acquire(RATE_LIMIT_KEY, INTERVAL_MS);
      JsonNode member = fetchMember(bioguideId.toString(), apiKey);
      JsonNode currentMember = member.path("currentMember");
      row.put("current_member", currentMember.isBoolean() ? currentMember.asBoolean() : null);
      row.put("party_name", partyForCongress(member.path("partyHistory"), congress));
    } catch (Exception e) {
      LOGGER.warn("Congress Member: Failed to fetch detail for bioguideId {}: {}",
          bioguideId, e.getMessage());
      row.put("current_member", null);
      row.put("party_name", null);
    }
    return Collections.singletonList(row);
  }

  /**
   * Returns the party in effect at the end of the given Congress: the span with the latest
   * {@code startYear} that is not after the Congress's final calendar year. The list endpoint
   * carries only the member's latest party, which is wrong for every earlier Congress of a
   * party switcher; {@code partyHistory} spans are year-granular, so a mid-Congress switch
   * resolves to the party held when the Congress ended.
   */
  static String partyForCongress(JsonNode partyHistory, int congress) {
    int lastYear = 1789 + 2 * congress - 1;
    String party = null;
    int bestStart = Integer.MIN_VALUE;
    for (JsonNode span : partyHistory) {
      int start = span.path("startYear").asInt(Integer.MAX_VALUE);
      if (start <= lastYear && start >= bestStart) {
        bestStart = start;
        party = span.path("partyName").asText(null);
      }
    }
    return party;
  }

  private JsonNode fetchMember(String bioguideId, String apiKey) throws IOException {
    String url = "https://api.congress.gov/v3/member/" + bioguideId
        + "?api_key=" + apiKey + "&format=json";
    HttpURLConnection conn = (HttpURLConnection) URI.create(url).toURL().openConnection();
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);
    conn.setRequestProperty("User-Agent", "GovData/1.0");
    int status = conn.getResponseCode();
    if (status != 200) {
      throw new IOException("HTTP " + status + " from " + url);
    }
    try (InputStream is = conn.getInputStream()) {
      return MAPPER.readTree(is).path("member");
    }
  }
}
