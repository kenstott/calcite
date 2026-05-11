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
package org.apache.calcite.adapter.govdata.lands;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;

/**
 * Transforms NPS IRMA visitation API responses into {@code nps_visitation} rows.
 *
 * <p>The IRMA API at {@code irmaservices.nps.gov/Stats/v1/visitation} returns XML.
 * The transformer also accepts legacy JSON responses as a fallback.
 *
 * <p>XML input format:
 * <pre>
 * &lt;ArrayOfVisitationData
 *     xmlns="http://schemas.datacontract.org/2004/07/NPS.Stats.Service.Rest.v3"&gt;
 *   &lt;VisitationData&gt;
 *     &lt;Month&gt;1&lt;/Month&gt;
 *     &lt;NonRecreationVisitors&gt;0&lt;/NonRecreationVisitors&gt;
 *     &lt;RecreationVisitors&gt;5833&lt;/RecreationVisitors&gt;
 *     &lt;UnitCode&gt;ABLI&lt;/UnitCode&gt;
 *     &lt;UnitName&gt;Abraham Lincoln Birthplace NHP&lt;/UnitName&gt;
 *     &lt;Year&gt;2022&lt;/Year&gt;
 *   &lt;/VisitationData&gt;
 * &lt;/ArrayOfVisitationData&gt;
 * </pre>
 *
 * <p>Output: JSON array string with columns matching the {@code nps_visitation} schema.
 */
public class NpsIrmaTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(NpsIrmaTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("nps_visitation: empty response from NPS IRMA API");
      return "[]";
    }

    String trimmed = response.trim();
    if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
      return transformJson(trimmed);
    }
    return transformXml(trimmed);
  }

  private String transformJson(String response) {
    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode result = MAPPER.createArrayNode();

      JsonNode records = root.isArray() ? root : root.path("data");
      if (!records.isArray()) {
        LOGGER.warn("nps_visitation: expected JSON array from IRMA API, got: {}",
            root.getNodeType());
        return "[]";
      }

      for (JsonNode record : records) {
        ObjectNode row = MAPPER.createObjectNode();
        row.put("unit_code", textOrNull(record, "UnitCode"));
        row.put("visit_year", intOrNull(record, "Year"));
        row.put("visit_month", intOrNull(record, "Month"));
        row.put("recreation_visits", intOrNull(record, "RecreationVisitors"));
        row.put("non_recreation_visits", intOrNull(record, "NonRecreationVisitors"));
        result.add(row);
      }

      LOGGER.debug("nps_visitation: transformed {} JSON records", result.size());
      return MAPPER.writeValueAsString(result);
    } catch (Exception e) {
      LOGGER.error("nps_visitation: failed to transform JSON response: {}", e.getMessage(), e);
      throw new RuntimeException("nps_visitation JSON transform failed", e);
    }
  }

  private String transformXml(String response) {
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setNamespaceAware(true);
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document document = builder.parse(new InputSource(new StringReader(response)));

      ArrayNode result = MAPPER.createArrayNode();
      NodeList visitationNodes = document.getElementsByTagName("VisitationData");

      for (int i = 0; i < visitationNodes.getLength(); i++) {
        Element visitationData = (Element) visitationNodes.item(i);

        ObjectNode row = MAPPER.createObjectNode();
        row.put("unit_code", childText(visitationData, "UnitCode"));
        row.put("visit_year", parseChildInt(visitationData, "Year"));
        row.put("visit_month", parseChildInt(visitationData, "Month"));
        row.put("recreation_visits", parseChildInt(visitationData, "RecreationVisitors"));
        row.put("non_recreation_visits", parseChildInt(visitationData, "NonRecreationVisitors"));
        result.add(row);
      }

      LOGGER.debug("nps_visitation: transformed {} XML records", result.size());
      return MAPPER.writeValueAsString(result);
    } catch (Exception e) {
      LOGGER.error("nps_visitation: failed to transform XML response: {}", e.getMessage(), e);
      throw new RuntimeException("nps_visitation XML transform failed", e);
    }
  }

  private String childText(Element parent, String tagName) {
    NodeList nodes = parent.getElementsByTagName(tagName);
    if (nodes.getLength() == 0) {
      return null;
    }
    String text = nodes.item(0).getTextContent();
    return (text == null || text.trim().isEmpty()) ? null : text.trim();
  }

  private Integer parseChildInt(Element parent, String tagName) {
    String text = childText(parent, tagName);
    if (text == null) {
      return null;
    }
    try {
      return Integer.parseInt(text);
    } catch (NumberFormatException e) {
      LOGGER.warn("nps_visitation: could not parse integer from element {}: {}", tagName, text);
      return null;
    }
  }

  private String textOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    return val.isNull() || val.isMissingNode() ? null : val.asText(null);
  }

  private Integer intOrNull(JsonNode node, String field) {
    JsonNode val = node.path(field);
    if (val.isNull() || val.isMissingNode()) {
      return null;
    }
    return val.asInt();
  }
}
