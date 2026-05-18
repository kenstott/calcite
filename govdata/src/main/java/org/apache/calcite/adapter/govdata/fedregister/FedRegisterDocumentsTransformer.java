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
package org.apache.calcite.adapter.govdata.fedregister;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Normalizes a single Federal Register API page response into flat rows.
 *
 * <p>Pagination is handled by the pipeline's PAGE-type PaginatedIterator using
 * {@code per_page} / {@code page} params and {@code count} for termination.
 * This transformer is called once per page and must not perform its own HTTP fetches.
 *
 * <p>Complex fields (agencies, cfr_references, docket_ids) are stored as JSON strings.
 * Agency names and slugs are also denormalized to comma-separated strings for simpler
 * SQL filtering without json_extract.
 */
public class FedRegisterDocumentsTransformer implements ResponseTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FedRegisterDocumentsTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("FedRegister Documents: empty response for {}", context.getUrl());
      return "[]";
    }

    String docType = context.getDimensionValues().get("doc_type");

    try {
      JsonNode root = MAPPER.readTree(response);
      ArrayNode results = MAPPER.createArrayNode();
      addResults(root, results, docType);
      return results.toString();

    } catch (Exception e) {
      LOGGER.error("FedRegister Documents: failed to parse response for doc_type={}: {}",
          docType, e.getMessage());
      return "[]";
    }
  }

  /**
   * Extracts and normalizes document records from a single page response.
   */
  private void addResults(JsonNode pageRoot, ArrayNode accumulator, String docType) {
    JsonNode results = pageRoot.path("results");
    if (!results.isArray()) {
      return;
    }

    for (JsonNode doc : results) {
      ObjectNode row = MAPPER.createObjectNode();

      row.put("document_number", getTextOrNull(doc, "document_number"));
      row.put("title", getTextOrNull(doc, "title"));
      row.put("doc_type", docType);
      row.put("abstract", getTextOrNull(doc, "abstract"));
      row.put("publication_date", getTextOrNull(doc, "publication_date"));
      row.put("effective_on", getTextOrNull(doc, "effective_on"));
      row.put("action", getTextOrNull(doc, "action"));

      // Agencies — denormalize to comma strings and preserve full JSON
      extractAgencies(doc, row);

      // CFR references — preserve as JSON string
      JsonNode cfrRefs = doc.path("cfr_references");
      row.put("cfr_references", cfrRefs.isArray() ? cfrRefs.toString() : null);

      // RIN — take first entry if multiple
      JsonNode rins = doc.path("regulation_id_numbers");
      if (rins.isArray() && rins.size() > 0) {
        row.put("rin", rins.get(0).asText());
      } else {
        row.putNull("rin");
      }

      // Significant flag (OIRA economically significant)
      JsonNode sig = doc.get("significant");
      if (sig != null && sig.isBoolean()) {
        row.put("significant", sig.booleanValue());
      } else {
        row.putNull("significant");
      }

      // Docket IDs — preserve as JSON string
      JsonNode docketIds = doc.path("docket_ids");
      row.put("docket_ids", docketIds.isArray() ? docketIds.toString() : null);

      // Presidential document fields
      row.put("president", getTextOrNull(doc, "president"));
      row.put("signing_date", getTextOrNull(doc, "signing_date"));

      // URLs
      row.put("full_text_xml_url", getTextOrNull(doc, "full_text_xml_url"));
      row.put("body_html_url", getTextOrNull(doc, "body_html_url"));

      accumulator.add(row);
    }
  }

  /**
   * Extracts agency names and slugs from the nested agencies array,
   * producing comma-separated strings and a full JSON representation.
   */
  private void extractAgencies(JsonNode doc, ObjectNode row) {
    JsonNode agencies = doc.path("agencies");
    if (!agencies.isArray() || agencies.size() == 0) {
      row.putNull("agency_names");
      row.putNull("agency_slugs");
      return;
    }

    List<String> names = new ArrayList<String>();
    List<String> slugs = new ArrayList<String>();

    for (JsonNode agency : agencies) {
      String name = getTextOrNull(agency, "name");
      String slug = getTextOrNull(agency, "slug");
      if (name != null) {
        names.add(name);
      }
      if (slug != null) {
        slugs.add(slug);
      }
    }

    row.put("agency_names", names.isEmpty() ? null : join(names, ", "));
    row.put("agency_slugs", slugs.isEmpty() ? null : join(slugs, ","));
  }

  private static String getTextOrNull(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull() || value.isMissingNode()) {
      return null;
    }
    String text = value.asText();
    return text.isEmpty() ? null : text;
  }

  private static String join(List<String> items, String delimiter) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < items.size(); i++) {
      if (i > 0) {
        sb.append(delimiter);
      }
      sb.append(items.get(i));
    }
    return sb.toString();
  }
}
