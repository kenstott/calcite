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
package org.apache.calcite.adapter.govdata.law;

import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.govdata.GovDataException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

/**
 * Published congressional hearing transcripts and the witnesses each one records, from the
 * GovInfo CHRG collection.
 *
 * <p>A crawl for one Congress lists the collection's packages ({@code /collections/CHRG}, keyed
 * with api.data.gov) and reads each hearing package's MODS record from GovInfo's keyless
 * metadata path. {@code committee_hearings} has one row per package and
 * {@code committee_hearing_witnesses} one per witness the record lists, on the package itself or
 * on one of its parts. The collection also holds publications of bodies that are not committee
 * hearings under other package id prefixes; only {@code CHRG-} packages are read.
 */
public class GovInfoHearingProvider extends AbstractCongressDocumentProvider {

  static final String HEARINGS = "committee_hearings";
  static final String WITNESSES = "committee_hearing_witnesses";

  private static final String MODS_NS = "http://www.loc.gov/mods/v3";
  /** The earliest change stamp: lists every package of the Congress. */
  private static final String SINCE = "1990-01-01T00:00:00Z";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override String cacheName() {
    return "govinfo_committee_hearings";
  }

  @Override String extension() {
    return "xml";
  }

  /** The metadata path is keyless, so the listing's API key is not sent to it. */
  @Override Map<String, String> documentHeaders(Map<String, String> sourceHeaders) {
    return Collections.emptyMap();
  }

  @Override Map<String, Item> list(String baseUrl, String congress, Map<String, String> headers,
      HttpSourceConfig.RateLimitConfig rateLimit) throws IOException {
    String first = baseUrl + "/" + SINCE + "?pageSize=1000&offsetMark=*&congress=" + congress;
    Map<String, Item> items = new LinkedHashMap<String, Item>();
    Set<String> listed = new LinkedHashSet<String>();
    int total = -1;
    String next = first;
    while (next != null) {
      JsonNode page = MAPPER.readTree(get(next, headers, rateLimit));
      JsonNode packages = page.get("packages");
      JsonNode count = page.get("count");
      if (packages == null || !packages.isArray() || count == null) {
        throw new GovDataException("Listing " + next + " has no packages array or count");
      }
      if (total < 0) {
        total = count.asInt();
      }
      for (JsonNode p : packages) {
        JsonNode id = p.get("packageId");
        JsonNode modified = p.get("lastModified");
        if (id == null || modified == null) {
          throw new GovDataException("Listing item without packageId or lastModified: " + p);
        }
        listed.add(id.asText());
        if (id.asText().startsWith("CHRG-")) {
          items.put(id.asText(), new Item("congress=" + congress + "/package=" + id.asText(),
              modified.asText(), String.format(java.util.Locale.ROOT,
                  "https://www.govinfo.gov/metadata/pkg/%s/mods.xml", id.asText())));
        }
      }
      JsonNode link = page.get("nextPage");
      next = link == null || link.isNull() ? null : link.asText();
    }
    requireComplete(first, listed.size(), total);
    return items;
  }

  @Override List<Map<String, Object>> rows(String table, Item item, byte[] document)
      throws IOException {
    return rowsOf(table, item.stamp, document);
  }

  /**
   * The rows one package's MODS record contributes to a table; for tests.
   *
   * @param table {@code committee_hearings} or {@code committee_hearing_witnesses}
   * @param lastModified the package's change stamp from the listing
   * @param document the MODS record
   */
  static List<Map<String, Object>> rowsOf(String table, String lastModified, byte[] document)
      throws IOException {
    Element mods = parse(document);
    List<Element> own = children(mods, "extension");
    String packageId = required(first(own, "accessId"), "accessId", "(unknown)");
    int congress = Integer.parseInt(required(first(own, "congress"), "congress", packageId));
    Set<String> witnesses = new LinkedHashSet<String>(texts(own, "witness"));
    for (Element part : children(mods, "relatedItem")) {
      if ("constituent".equals(part.getAttribute("type"))) {
        witnesses.addAll(texts(children(part, "extension"), "witness"));
      }
    }
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    if (WITNESSES.equals(table)) {
      int seq = 0;
      for (String witness : witnesses) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("congress", congress);
        row.put("package_id", packageId);
        row.put("witness_seq", Integer.valueOf(++seq));
        row.put("witness", witness);
        rows.add(row);
      }
      return rows;
    }
    if (!HEARINGS.equals(table)) {
      throw new GovDataException("GovInfoHearingProvider does not build " + table);
    }
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("congress", congress);
    row.put("package_id", packageId);
    String chamber = required(first(own, "chamber"), "chamber", packageId);
    row.put("chamber", chamber.substring(0, 1).toUpperCase(java.util.Locale.ROOT)
        + chamber.substring(1).toLowerCase(java.util.Locale.ROOT));
    row.put("session_number", integer(first(own, "session")));
    row.put("hearing_number", first(own, "number"));
    String jacket = first(own, "jacketId");
    row.put("jacket_number",
        jacket == null ? null : integer(jacket.replaceAll("[^0-9]", "")));
    row.put("citation", first(own, "preferredCitation"));
    List<Element> titleInfo = children(mods, "titleInfo");
    row.put("title",
        titleInfo.isEmpty() ? null : first(Collections.singletonList(titleInfo.get(0)), "title"));
    List<String> held = new ArrayList<String>(texts(own, "heldDate"));
    Collections.sort(held);
    row.put("first_held_date", held.isEmpty() ? null : held.get(0));
    row.put("last_held_date", held.isEmpty() ? null : held.get(held.size() - 1));
    row.put("held_dates", held.isEmpty() ? null : String.join(",", held));
    Map<String, Element> committees = new LinkedHashMap<String, Element>();
    for (Element extension : own) {
      for (Element committee : children(extension, "congCommittee")) {
        String code = committee.getAttribute("authorityId");
        if (!code.isEmpty() && !committees.containsKey(code)) {
          committees.put(code, committee);
        }
      }
    }
    String code = null;
    String name = null;
    String subcommittee = null;
    if (!committees.isEmpty()) {
      Map.Entry<String, Element> lead = committees.entrySet().iterator().next();
      code = lead.getKey();
      for (Element n : children(lead.getValue(), "name")) {
        if ("authority-standard".equals(n.getAttribute("type"))) {
          name = trimmed(n);
        }
      }
      subcommittee = first(children(lead.getValue(), "subCommittee"), "name");
    }
    row.put("committee_system_code", code);
    row.put("committee_name", name);
    row.put("subcommittee_name", subcommittee);
    row.put("committee_system_codes",
        committees.isEmpty() ? null : String.join(",", committees.keySet()));
    row.put("witness_count", Integer.valueOf(witnesses.size()));
    row.put("details_url", "https://www.govinfo.gov/app/details/" + packageId);
    // A date, not the full stamp: the writer's type inference reformats timestamp strings.
    row.put("last_modified_date",
        Instant.parse(lastModified).atZone(ZoneOffset.UTC).toLocalDate().toString());
    rows.add(row);
    return rows;
  }

  private static Element parse(byte[] document) throws IOException {
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setNamespaceAware(true);
      factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
      Element root = factory.newDocumentBuilder()
          .parse(new ByteArrayInputStream(document)).getDocumentElement();
      if (!"mods".equals(root.getLocalName())) {
        throw new GovDataException("Expected a MODS record, got <" + root.getTagName() + ">");
      }
      return root;
    } catch (ParserConfigurationException | SAXException e) {
      throw new GovDataException("Unreadable MODS record", e);
    }
  }

  private static List<Element> children(Element parent, String name) {
    List<Element> out = new ArrayList<Element>();
    for (Node n = parent.getFirstChild(); n != null; n = n.getNextSibling()) {
      if (n instanceof Element && MODS_NS.equals(n.getNamespaceURI())
          && name.equals(n.getLocalName())) {
        out.add((Element) n);
      }
    }
    return out;
  }

  /** The non-blank texts of the named children of each parent, in document order. */
  private static List<String> texts(List<Element> parents, String name) {
    List<String> out = new ArrayList<String>();
    for (Element parent : parents) {
      for (Element child : children(parent, name)) {
        String s = trimmed(child);
        if (s != null) {
          out.add(s);
        }
      }
    }
    return out;
  }

  private static String first(List<Element> parents, String name) {
    List<String> all = texts(parents, name);
    return all.isEmpty() ? null : all.get(0);
  }

  private static String trimmed(Element e) {
    String s = e.getTextContent().replaceAll("\\s+", " ").trim();
    return s.isEmpty() ? null : s;
  }

  private static Integer integer(String s) {
    return s == null || s.isEmpty() ? null : Integer.valueOf(s);
  }

  private static String required(String value, String field, String packageId) {
    if (value == null) {
      throw new GovDataException("MODS record of " + packageId + " has no " + field);
    }
    return value;
  }
}
