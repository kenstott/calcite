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
 * <p>One detail call per row (N+1): a member serving several congresses is re-fetched once per
 * congress-partition row, since row transformers are re-instantiated per partition and cache
 * nothing across them. On any fetch/parse failure for a row, {@code current_member} is left
 * null (its accurate "unknown" state) rather than defaulted to true/false — the row itself is
 * kept and a warning is logged, so a flaky detail call cannot drop otherwise-good member data.
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
      return Collections.singletonList(row);
    }

    String apiKey = context.getTableConfig().getSource().getParameters().get("api_key");

    try {
      CrossProcessRateLimiter.acquire(RATE_LIMIT_KEY, INTERVAL_MS);
      Boolean currentMember = fetchCurrentMember(bioguideId.toString(), apiKey);
      row.put("current_member", currentMember);
    } catch (Exception e) {
      LOGGER.warn("Congress Member: Failed to fetch currentMember for bioguideId {}: {}",
          bioguideId, e.getMessage());
      row.put("current_member", null);
    }
    return Collections.singletonList(row);
  }

  private Boolean fetchCurrentMember(String bioguideId, String apiKey) throws IOException {
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
      JsonNode root = MAPPER.readTree(is);
      JsonNode member = root.path("member");
      JsonNode currentMember = member.path("currentMember");
      return currentMember.isBoolean() ? currentMember.asBoolean() : null;
    }
  }
}
