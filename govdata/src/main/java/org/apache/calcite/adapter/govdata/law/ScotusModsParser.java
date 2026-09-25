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

import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * Reads the case-level fields of one GovInfo United States Reports granule's MODS record.
 *
 * <p>Only the record's own elements are read: the {@code relatedItem type="host"} block that
 * follows describes the enclosing volume (with its own title and term) and is skipped, as are
 * the publisher and distributor names. The parse is a StAX pull over the stream, so the record
 * is never held whole.
 *
 * <p>GovInfo's curated outcome fields ({@code disposition}, {@code partyWinning}, the votes and
 * the litigant types) come from the Supreme Court Database and are present for volumes 2 through
 * 582; volume 583 onward carries only the citation, title and a year-level decision date. A field
 * the record does not carry is absent from the result — never defaulted.
 *
 * <p>A {@code decisionDate} marked {@code notSpecified} (for example {@code 2017-01-01} with
 * {@code notSpecified="day-month"}) is a year only, so it yields {@code decision_year} and no
 * {@code decision_date}.
 */
final class ScotusModsParser {

  /** Element path of a single-valued text leaf to the key it is stored under. */
  private static final Map<String, String> TEXT = new HashMap<String, String>();

  /** Element path of a repeating text leaf to the key of the list it is appended to. */
  private static final Map<String, String> LISTS = new HashMap<String, String>();

  private static final String X = "mods/extension/";

  static {
    TEXT.put("mods/titleInfo/title", "case_title");
    TEXT.put(X + "accessId", "granule_id");
    TEXT.put(X + "usCitation", "us_citation");
    TEXT.put(X + "fullCitation", "full_citation");
    TEXT.put(X + "courtTerm", "court_term");
    TEXT.put(X + "docketId", "govinfo_docket_id");
    TEXT.put(X + "petitioner", "petitioner_type");
    TEXT.put(X + "respondent", "respondent_type");
    TEXT.put(X + "jurisdiction", "jurisdiction");
    TEXT.put(X + "caseOrigin", "case_origin");
    TEXT.put(X + "caseSource", "case_source");
    TEXT.put(X + "certReason", "cert_reason");
    TEXT.put(X + "chiefJustice", "chief_justice");
    TEXT.put(X + "dateArgument", "argument_date");
    TEXT.put(X + "authorityDecision", "authority_decision");
    TEXT.put(X + "decisionType", "decision_type");
    TEXT.put(X + "unconstitutionality", "unconstitutionality");
    TEXT.put(X + "disposition", "disposition");
    TEXT.put(X + "partyWinning", "party_winning");
    TEXT.put(X + "majorityOpinion/writer", "opinion_writer");
    TEXT.put(X + "majorityOpinion/assigner", "opinion_assigner");
    TEXT.put(X + "reportNumber", "first_page");
    TEXT.put(X + "sequenceNumber", "sequence_number");
    TEXT.put(X + "votes/majority", "votes_majority");
    TEXT.put(X + "votes/minority", "votes_minority");
    TEXT.put(X + "decisionDate", "decision_date");

    LISTS.put(X + "law/type", "law_types");
    LISTS.put(X + "law/legalProvisions", "legal_provisions");
  }

  /** Keys parsed to integers. */
  private static final String[] INTEGER_KEYS =
      {"first_page", "sequence_number", "votes_majority", "votes_minority"};

  private ScotusModsParser() {
  }

  /**
   * Parses one granule's MODS record.
   *
   * @return the case-level fields, in a stable order; absent fields are not present
   */
  static Map<String, Object> parse(InputStream mods) throws XMLStreamException {
    XMLInputFactory factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);

    Map<String, Object> row = new LinkedHashMap<String, Object>();
    List<String> lcTopics = new ArrayList<String>();
    List<String> issueAreas = new ArrayList<String>();
    List<String> lawTypes = new ArrayList<String>();
    List<String> legalProvisions = new ArrayList<String>();

    Deque<String> path = new ArrayDeque<String>();
    StringBuilder text = new StringBuilder();
    String subjectAuthority = null;
    boolean dateIsYearOnly = false;

    XMLStreamReader in = factory.createXMLStreamReader(mods);
    try {
      while (in.hasNext()) {
        int event = in.next();
        if (event == XMLStreamConstants.START_ELEMENT) {
          path.addLast(in.getLocalName());
          text.setLength(0);
          String p = joined(path);
          if ((X + "subject").equals(p)) {
            subjectAuthority = in.getAttributeValue(null, "authority");
          } else if ((X + "decisionDate").equals(p)) {
            dateIsYearOnly = in.getAttributeValue(null, "notSpecified") != null;
          }
        } else if (event == XMLStreamConstants.CHARACTERS || event == XMLStreamConstants.CDATA) {
          text.append(in.getText());
        } else if (event == XMLStreamConstants.END_ELEMENT) {
          String p = joined(path);
          String value = text.toString().trim();
          if (!value.isEmpty()) {
            String key = TEXT.get(p);
            if (key != null && !row.containsKey(key)) {
              row.put(key, value);
            } else if ((X + "subject/topic").equals(p)) {
              if ("llc".equals(subjectAuthority)) {
                lcTopics.add(value);
              } else if ("scdb".equals(subjectAuthority)) {
                issueAreas.add(value);
              }
            } else if (LISTS.containsKey(p)) {
              (LISTS.get(p).equals("law_types") ? lawTypes : legalProvisions).add(value);
            }
          }
          path.removeLast();
          text.setLength(0);
        }
      }
    } finally {
      in.close();
    }

    finish(row, dateIsYearOnly);
    putIfAny(row, "lc_topics", lcTopics);
    putIfAny(row, "scdb_issue_areas", issueAreas);
    putIfAny(row, "law_types", lawTypes);
    putIfAny(row, "legal_provisions", legalProvisions);
    return row;
  }

  /** Splits the decision date into year and (when it is a full date) date; parses integers. */
  private static void finish(Map<String, Object> row, boolean dateIsYearOnly) {
    Object date = row.get("decision_date");
    if (date != null) {
      String d = (String) date;
      if (d.length() < 4) {
        throw new IllegalArgumentException("MODS decisionDate is not a date: " + d);
      }
      row.put("decision_year", Integer.valueOf(parseInt("decisionDate year", d.substring(0, 4))));
      if (dateIsYearOnly) {
        row.remove("decision_date");
      }
    }
    for (String key : INTEGER_KEYS) {
      Object v = row.get(key);
      if (v != null) {
        row.put(key, Integer.valueOf(parseInt(key, (String) v)));
      }
    }
  }

  private static int parseInt(String what, String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("MODS " + what + " is not an integer: " + value, e);
    }
  }

  private static void putIfAny(Map<String, Object> row, String key, List<String> values) {
    if (!values.isEmpty()) {
      row.put(key, values);
    }
  }

  private static String joined(Deque<String> path) {
    StringBuilder sb = new StringBuilder();
    for (String part : path) {
      if (sb.length() > 0) {
        sb.append('/');
      }
      sb.append(part);
    }
    return sb.toString();
  }
}
