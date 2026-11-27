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
package org.apache.calcite.adapter.govdata.econ;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Extracts the data array from a World Bank indicator response, {@code [metadata, data]}.
 *
 * <p>The API answers HTTP 200 for a request it rejects, including one for a deleted or archived
 * indicator, with {@code [{"message": [...]}]}. Every catalog indicator is expected to deliver
 * rows, so any response other than the two-element envelope with a non-empty data array fails
 * the batch.
 */
public class WorldBankResponseTransformer implements ResponseTransformer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    JsonNode root;
    try {
      root = MAPPER.readTree(response);
    } catch (IOException e) {
      throw new IllegalStateException("World Bank response is not JSON for "
          + context.getDimensionValues(), e);
    }
    JsonNode data = root.path(1);
    if (!data.isArray() || data.size() == 0) {
      throw new IllegalStateException("World Bank returned no data for "
          + context.getDimensionValues() + ": " + abbreviate(response));
    }
    return data.toString();
  }

  private static String abbreviate(String response) {
    return response.length() <= 500 ? response : response.substring(0, 500) + "...";
  }
}
