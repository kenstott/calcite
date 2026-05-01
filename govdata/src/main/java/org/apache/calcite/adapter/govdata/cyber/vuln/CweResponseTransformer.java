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
package org.apache.calcite.adapter.govdata.cyber.vuln;

import org.apache.calcite.adapter.file.etl.RequestContext;
import org.apache.calcite.adapter.file.etl.ResponseTransformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * Transforms the MITRE CWE XML catalog into flat row JSON.
 *
 * <p>Input: XML content from {@code cwec_latest.xml.zip} (the ZIP is decompressed
 * upstream; this transformer receives the raw XML string). The root element is
 * {@code <Weakness_Catalog>}; weaknesses live under
 * {@code //Weaknesses/Weakness}.
 *
 * <p>Output: JSON array where each element maps to one row in {@code cwe_catalog}:
 * <pre>
 * [
 *   {
 *     "cwe_id": "CWE-79",
 *     "name": "Improper Neutralization of Input During Web Page Generation",
 *     "abstraction": "Variant",
 *     "structure": "Simple",
 *     "status": "Stable",
 *     "description": "...",
 *     "extended_description": "...",
 *     "parent_ids": "CWE-74|CWE-116",
 *     "cwe_url": "https://cwe.mitre.org/data/definitions/79.html"
 *   },
 *   ...
 * ]
 * </pre>
 *
 * <p>ChildOf relationships are concatenated with {@code |} into {@code parent_ids}.
 */
public class CweResponseTransformer implements ResponseTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(CweResponseTransformer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String CWE_URL_PREFIX = "https://cwe.mitre.org/data/definitions/";

  @Override public String transform(String response, RequestContext context) {
    if (response == null || response.isEmpty()) {
      LOGGER.warn("CWE: empty response from {}", context.getUrl());
      return "[]";
    }

    try {
      Document doc = parseXml(response);
      ArrayNode rows = MAPPER.createArrayNode();

      NodeList weaknesses = doc.getElementsByTagNameNS("*", "Weakness");
      if (weaknesses.getLength() == 0) {
        // Try without namespace
        weaknesses = doc.getElementsByTagName("Weakness");
      }

      for (int i = 0; i < weaknesses.getLength(); i++) {
        Element w = (Element) weaknesses.item(i);
        ObjectNode row = buildRow(w);
        rows.add(row);
      }

      LOGGER.info("CWE: parsed {} weakness entries", rows.size());
      return rows.toString();

    } catch (Exception e) {
      LOGGER.error("CWE: failed to parse XML: {}", e.getMessage());
      throw new RuntimeException("Failed to parse CWE XML: " + e.getMessage(), e);
    }
  }

  private ObjectNode buildRow(Element w) {
    ObjectNode row = MAPPER.createObjectNode();

    String id = w.getAttribute("ID");
    String cweId = id.isEmpty() ? null : "CWE-" + id;
    row.put("cwe_id", cweId);
    row.put("name", nullIfEmpty(w.getAttribute("Name")));
    row.put("abstraction", nullIfEmpty(w.getAttribute("Abstraction")));
    row.put("structure", nullIfEmpty(w.getAttribute("Structure")));
    row.put("status", nullIfEmpty(w.getAttribute("Status")));

    row.put("description", firstChildText(w, "Description"));
    row.put("extended_description", firstChildText(w, "Extended_Description"));

    row.put("parent_ids", extractParentIds(w));
    row.put("cwe_url", id.isEmpty() ? null : CWE_URL_PREFIX + id + ".html");

    return row;
  }

  private String extractParentIds(Element weakness) {
    List<String> parents = new ArrayList<String>();

    NodeList relNodes = weakness.getElementsByTagNameNS("*", "Related_Weakness");
    if (relNodes.getLength() == 0) {
      relNodes = weakness.getElementsByTagName("Related_Weakness");
    }

    for (int i = 0; i < relNodes.getLength(); i++) {
      Element rel = (Element) relNodes.item(i);
      if ("ChildOf".equals(rel.getAttribute("Nature"))) {
        String cweid = rel.getAttribute("CWE_ID");
        if (!cweid.isEmpty()) {
          parents.add("CWE-" + cweid);
        }
      }
    }

    if (parents.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < parents.size(); i++) {
      if (i > 0) {
        sb.append("|");
      }
      sb.append(parents.get(i));
    }
    return sb.toString();
  }

  private String firstChildText(Element parent, String tagName) {
    NodeList nodes = parent.getElementsByTagNameNS("*", tagName);
    if (nodes.getLength() == 0) {
      nodes = parent.getElementsByTagName(tagName);
    }
    if (nodes.getLength() == 0) {
      return null;
    }
    String text = nodes.item(0).getTextContent();
    return text == null || text.trim().isEmpty() ? null : text.trim().replaceAll("\\s+", " ");
  }

  private Document parseXml(String xml) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    // Disable external entity processing (security hardening)
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", false);
    factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
    factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
    factory.setAttribute("http://javax.xml.XMLConstants/property/accessExternalDTD", "");
    factory.setAttribute("http://javax.xml.XMLConstants/property/accessExternalSchema", "");
    DocumentBuilder builder = factory.newDocumentBuilder();
    return builder.parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
  }

  private static String nullIfEmpty(String s) {
    return (s == null || s.isEmpty()) ? null : s;
  }
}
