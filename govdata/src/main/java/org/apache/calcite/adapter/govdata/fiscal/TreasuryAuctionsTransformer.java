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
package org.apache.calcite.adapter.govdata.fiscal;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Normalises the Treasury Fiscal Data {@code auctions_query} response for
 * {@code fiscal.treasury_auction_results}.
 *
 * <p>Fiscal Data serialises every missing value as the text {@code "null"}, including in numeric
 * and date fields. This transformer rewrites each such value to a JSON null so the writer sees a
 * real null instead of an unparseable string. A response without a {@code data} array is an API
 * error and fails the fetch. Returns the {@code data} array itself: HttpSource does not apply
 * {@code dataPath} to a transformer's output.
 */
public class TreasuryAuctionsTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    try {
      JsonNode root = MAPPER.readTree(response);
      JsonNode data = root.get("data");
      if (data == null || !data.isArray()) {
        throw new IllegalStateException("Treasury auctions response has no data array: "
            + response.substring(0, Math.min(300, response.length())));
      }
      for (JsonNode row : data) {
        ObjectNode obj = (ObjectNode) row;
        List<String> names = new ArrayList<>();
        obj.fieldNames().forEachRemaining(names::add);
        for (String name : names) {
          JsonNode value = obj.get(name);
          if (value.isTextual() && "null".equals(value.asText())) {
            obj.putNull(name);
          }
        }
      }
      return MAPPER.writeValueAsString(data);
    } catch (IOException e) {
      throw new IllegalStateException("Treasury auctions response is not valid JSON", e);
    }
  }
}
