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
package org.apache.calcite.adapter.govdata.ref;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * Transforms the SEC EDGAR company tickers JSON into a flat list of records.
 *
 * <p>SEC publishes company tickers at {@code https://www.sec.gov/files/company_tickers.json}
 * as a JSON object keyed by sequential integers:
 *
 * <pre>{@code
 * {
 *   "0": {"cik_str": 1000032, "ticker": "AAON", "title": "AAON, Inc."},
 *   "1": {"cik_str": 1000228, "ticker": "GNBC", "title": "GNBC Inc"},
 *   ...
 * }
 * }</pre>
 *
 * <p>This transformer converts that to an array of records compatible with
 * the {@code sec_company_tickers} table schema:
 *
 * <pre>{@code
 * [
 *   {"cik": "1000032", "ticker": "AAON", "title": "AAON, Inc."},
 *   ...
 * ]
 * }</pre>
 *
 * <p>CIK is stored as a plain numeric string (no zero-padding), matching
 * the format used in {@code gleif_cik_mapping}.
 */
public class SecCompanyTickersTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SecCompanyTickersTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("SEC company tickers: empty response from {}", context.getUrl());
      return "[]";
    }

    try {
      JsonNode root = MAPPER.readTree(response);

      if (!root.isObject()) {
        LOGGER.warn("SEC company tickers: unexpected response type '{}', expected object",
            root.getNodeType());
        return "[]";
      }

      ArrayNode result = MAPPER.createArrayNode();
      Iterator<Map.Entry<String, JsonNode>> fields = root.fields();

      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        JsonNode company = entry.getValue();

        if (!company.isObject()) {
          continue;
        }

        ObjectNode record = MAPPER.createObjectNode();

        JsonNode cikNode = company.get("cik_str");
        if (cikNode != null && !cikNode.isNull()) {
          record.put("cik", cikNode.asText());
        }

        JsonNode tickerNode = company.get("ticker");
        if (tickerNode != null && !tickerNode.isNull()) {
          record.put("ticker", tickerNode.asText());
        }

        JsonNode titleNode = company.get("title");
        if (titleNode != null && !titleNode.isNull()) {
          record.put("title", titleNode.asText());
        }

        result.add(record);
      }

      LOGGER.info("SEC company tickers: extracted {} records", result.size());
      return result.toString();

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.error("SEC company tickers: failed to parse response from {}: {}",
          context.getUrl(), e.getMessage());
      throw new RuntimeException("Failed to parse SEC company tickers: " + e.getMessage(), e);
    }
  }
}
