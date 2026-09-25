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

import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.StorageAwareDataProvider;
import org.apache.calcite.adapter.file.etl.VariableResolver;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;
import org.apache.calcite.adapter.govdata.GovDataException;
import org.apache.calcite.adapter.govdata.ZipDownloadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * DataProvider for the {@code bills} table and its child tables ({@code bill_actions},
 * {@code bill_cosponsors}, {@code bill_committees}, {@code bill_committee_activities},
 * {@code bill_subjects}, {@code bill_text_versions}, {@code bill_related_bills},
 * {@code bill_amendments}, {@code bill_amendment_actions}, {@code bill_amendment_cosponsors},
 * {@code bill_action_committees}, {@code bill_recorded_votes}, {@code bill_titles},
 * {@code bill_summaries}, {@code bill_cbo_cost_estimates}, {@code bill_committee_reports},
 * {@code bill_notes}), sourced from the GovInfo Bill Status bulk repository.
 *
 * <p>Where the source spells the same field two ways (e.g. {@code number} vs the legacy
 * {@code billNumber}, {@code text} vs {@code cdata/text}) both spellings are mapped to the one
 * column. Not modelled: derived aggregates ({@code actions/actionByCounts},
 * {@code actionTypeCounts}), name parts and identifiers already covered by the full name
 * ({@code firstName}, {@code lastName}, {@code identifiers}), hyperlink-only decorations
 * ({@code links}), legacy-only {@code createDate}, {@code calendarNumbers} and {@code version}.
 *
 * <p>One zip per (Congress, bill type) at
 * {@code https://www.govinfo.gov/bulkdata/BILLSTATUS/{congress}/{type}/BILLSTATUS-{congress}-{type}.zip}
 * holds one XML file per bill. Every table reads the same cached zip and differs only in which
 * rows it emits, so the raw download happens once per (Congress, bill type, refresh month).
 *
 * <p>Streaming: the zip is read one entry at a time, each entry is parsed with a StAX pull parser,
 * and rows are handed to the pipeline lazily. Nothing larger than a single bill's rows is ever held
 * in memory.
 */
public class CongressBillStatusProvider implements StorageAwareDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(CongressBillStatusProvider.class);

  static final String TABLE_BILLS = "bills";
  static final String TABLE_ACTIONS = "bill_actions";
  static final String TABLE_COSPONSORS = "bill_cosponsors";
  static final String TABLE_COMMITTEES = "bill_committees";
  static final String TABLE_COMMITTEE_ACTIVITIES = "bill_committee_activities";
  static final String TABLE_SUBJECTS = "bill_subjects";
  static final String TABLE_TEXT_VERSIONS = "bill_text_versions";
  static final String TABLE_RELATED_BILLS = "bill_related_bills";
  static final String TABLE_AMENDMENTS = "bill_amendments";
  static final String TABLE_AMENDMENT_ACTIONS = "bill_amendment_actions";
  static final String TABLE_AMENDMENT_COSPONSORS = "bill_amendment_cosponsors";
  static final String TABLE_ACTION_COMMITTEES = "bill_action_committees";
  static final String TABLE_RECORDED_VOTES = "bill_recorded_votes";
  static final String TABLE_TITLES = "bill_titles";
  static final String TABLE_SUMMARIES = "bill_summaries";
  static final String TABLE_CBO_ESTIMATES = "bill_cbo_cost_estimates";
  static final String TABLE_COMMITTEE_REPORTS = "bill_committee_reports";
  static final String TABLE_NOTES = "bill_notes";

  private static final java.util.Set<String> TABLES = new java.util.HashSet<String>(
      java.util.Arrays.asList(TABLE_BILLS, TABLE_ACTIONS, TABLE_COSPONSORS, TABLE_COMMITTEES,
          TABLE_COMMITTEE_ACTIVITIES, TABLE_SUBJECTS, TABLE_TEXT_VERSIONS, TABLE_RELATED_BILLS,
          TABLE_AMENDMENTS, TABLE_AMENDMENT_ACTIONS, TABLE_AMENDMENT_COSPONSORS,
          TABLE_ACTION_COMMITTEES, TABLE_RECORDED_VOTES, TABLE_TITLES, TABLE_SUMMARIES,
          TABLE_CBO_ESTIMATES, TABLE_COMMITTEE_REPORTS, TABLE_NOTES));

  private static final String P = "billStatus/bill/";

  /** Exact element path of a repeating item to the scope of the node opened for it. */
  private static final Map<String, String> ITEMS = new HashMap<String, String>();

  /** Exact element path of a leaf to the key it is stored under on the innermost open node. */
  private static final Map<String, String> LEAVES = new HashMap<String, String>();

  static {
    // Bill-level fields. Sponsor and law fields sit under repeating items that are not opened
    // as nodes, so they land on the bill itself and the first listed item wins.
    leaves("", "number", "number", "type", "type", "congress", "congress",
        "originChamber", "origin_chamber", "introducedDate", "introduced_date",
        "title", "title", "updateDate", "update_date", "legislationUrl", "legislation_url",
        "policyArea/name", "policy_area", "latestAction/actionDate", "latest_action_date",
        "latestAction/text", "latest_action_text",
        "sponsors/item/bioguideId", "sponsor_bioguide_id",
        "sponsors/item/fullName", "sponsor_full_name", "sponsors/item/party", "sponsor_party",
        "sponsors/item/state", "sponsor_state", "sponsors/item/district", "sponsor_district",
        "laws/item/type", "law_type", "laws/item/number", "law_number");

    item("action", "actions/item");
    leaves("actions/item/", "actionDate", "action_date", "actionTime", "action_time",
        "text", "text", "type", "action_type", "actionCode", "action_code",
        "sourceSystem/name", "source_system");

    item("cosponsor", "cosponsors/item");
    leaves("cosponsors/item/", "bioguideId", "bioguide_id", "fullName", "full_name",
        "party", "party", "state", "state", "district", "district",
        "sponsorshipDate", "sponsorship_date", "isOriginalCosponsor", "is_original_cosponsor",
        "sponsorshipWithdrawnDate", "withdrawn_date");

    item("committee", "committees/item");
    leaves("committees/item/", "systemCode", "system_code", "name", "name",
        "chamber", "chamber", "type", "type");
    item("subcommittee", "committees/item/subcommittees/item");
    leaves("committees/item/subcommittees/item/", "systemCode", "system_code", "name", "name");
    item("activity", "committees/item/activities/item");
    item("activity", "committees/item/subcommittees/item/activities/item");
    leaves("committees/item/activities/item/", "name", "name", "date", "date");
    leaves("committees/item/subcommittees/item/activities/item/", "name", "name",
        "date", "date");

    item("subject", "subjects/legislativeSubjects/item");
    leaves("subjects/legislativeSubjects/item/", "name", "name", "updateDate", "update_date");

    item("text_version", "textVersions/item");
    leaves("textVersions/item/", "type", "version_type", "date", "version_date");
    item("text_format", "textVersions/item/formats/item");
    leaves("textVersions/item/formats/item/", "type", "format_type", "url", "url");

    item("related_bill", "relatedBills/item");
    leaves("relatedBills/item/", "title", "title", "congress", "congress", "number", "number",
        "type", "type", "latestAction/actionDate", "latest_action_date",
        "latestAction/actionTime", "latest_action_time", "latestAction/text",
        "latest_action_text");
    item("relationship", "relatedBills/item/relationshipDetails/item");
    leaves("relatedBills/item/relationshipDetails/item/", "type", "relationship_type",
        "identifiedBy", "identified_by");

    // An amendment's own sponsors, cosponsors and actions are opened only so they can be
    // counted; their fields are not mapped. Sponsor fields land on the amendment (first wins).
    item("amendment", "amendments/amendment");
    item("amendment_action", "amendments/amendment/actions/actions/item");
    item("amendment_cosponsor", "amendments/amendment/cosponsors/item");
    leaves("amendments/amendment/", "number", "number", "type", "type", "chamber", "chamber",
        "updateDate", "update_date", "submittedDate", "submitted_date",
        "proposedDate", "proposed_date", "description", "description", "purpose", "purpose",
        "latestAction/actionDate", "latest_action_date", "latestAction/text",
        "latest_action_text", "sponsors/item/bioguideId", "sponsor_bioguide_id",
        "sponsors/item/fullName", "sponsor_full_name", "sponsors/item/party", "sponsor_party",
        "sponsors/item/state", "sponsor_state", "sponsors/item/district", "sponsor_district",
        "sponsors/item/name", "sponsor_name", "amendedAmendment/number",
        "amended_amendment_number", "amendedAmendment/type", "amended_amendment_type",
        "onBehalfOfSponsor/item/bioguideId", "on_behalf_of_bioguide_id",
        "onBehalfOfSponsor/item/fullName", "on_behalf_of_full_name",
        "onBehalfOfSponsor/item/type", "on_behalf_of_type",
        "latestAction/actionTime", "latest_action_time",
        "amendmentsToAmendment/count", "amendments_to_amendment_count");
    leaves("amendments/amendment/actions/actions/item/", "actionDate", "action_date",
        "actionTime", "action_time", "text", "text", "type", "action_type",
        "actionCode", "action_code", "sourceSystem/name", "source_system");
    leaves("amendments/amendment/cosponsors/item/", "bioguideId", "bioguide_id",
        "fullName", "full_name", "party", "party", "state", "state",
        "sponsorshipDate", "sponsorship_date", "isOriginalCosponsor", "is_original_cosponsor",
        "sponsorshipWithdrawnDate", "withdrawn_date");

    // Roll-call votes hang off a bill action, an amendment action, or (legacy files) the bill.
    item("recorded_vote", "actions/item/recordedVotes/recordedVote");
    item("recorded_vote", "amendments/amendment/actions/actions/item/recordedVotes/recordedVote");
    item("recorded_vote", "recordedVotes/recordedVote");
    for (String at : new String[] {"actions/item/recordedVotes/recordedVote/",
        "amendments/amendment/actions/actions/item/recordedVotes/recordedVote/",
        "recordedVotes/recordedVote/"}) {
      leaves(at, "chamber", "chamber", "congress", "congress", "date", "date",
          "rollNumber", "roll_number", "sessionNumber", "session_number", "url", "url",
          "fullActionName", "full_action_name");
    }

    item("action_committee", "actions/item/committees/item");
    leaves("actions/item/committees/item/", "systemCode", "system_code", "name", "name");
    leaves("actions/item/", "calendarNumber/calendar", "calendar_number");

    item("title", "titles/item");
    leaves("titles/item/", "title", "title", "titleType", "title_type",
        "titleTypeCode", "title_type_code", "billTextVersionCode", "bill_text_version_code",
        "billTextVersionName", "bill_text_version_name", "chamberCode", "chamber_code",
        "chamberName", "chamber_name", "parentTitleType", "parent_title_type",
        "updateDate", "update_date", "sourceSystem/name", "source_system");

    // Two spellings of one summary: summaries/summary (current) and summaries/billSummaries/item
    // (legacy, which also carries name and lastSummaryUpdateDate); the text sits in either
    // text or cdata/text.
    item("summary", "summaries/summary");
    item("summary", "summaries/billSummaries/item");
    for (String at : new String[] {"summaries/summary/", "summaries/billSummaries/item/"}) {
      leaves(at, "versionCode", "version_code", "actionDate", "action_date",
          "actionDesc", "action_description", "updateDate", "update_date",
          "text", "summary_text", "cdata/text", "summary_text");
    }
    leaves("summaries/billSummaries/item/", "name", "summary_name",
        "lastSummaryUpdateDate", "last_summary_update_date");

    item("cbo_estimate", "cboCostEstimates/item");
    leaves("cboCostEstimates/item/", "pubDate", "pub_date", "title", "title", "url", "url",
        "description", "description");

    item("committee_report", "committeeReports/committeeReport");
    leaves("committeeReports/committeeReport/", "citation", "citation");

    item("note", "notes/item");
    leaves("notes/item/", "text", "note_text", "cdata/text", "note_text");
    item("note_link", "notes/item/links/link");
    leaves("notes/item/links/link/", "name", "name", "url", "url");

    // Legacy and current spellings of bill-level fields.
    leaves("", "billNumber", "number", "billType", "type",
        "constitutionalAuthorityStatementText", "constitutional_authority_statement",
        "cdata/constitutionalAuthorityStatementText", "constitutional_authority_statement",
        "sponsors/item/isByRequest", "sponsor_is_by_request",
        "updateDateIncludingText", "update_date_including_text",
        "latestAction/actionTime", "latest_action_time",
        "onBehalfOfSponsor/item/bioguideId", "on_behalf_of_bioguide_id",
        "onBehalfOfSponsor/item/fullName", "on_behalf_of_full_name",
        "onBehalfOfSponsor/item/type", "on_behalf_of_type");
  }

  private static void item(String scope, String suffix) {
    ITEMS.put(P + suffix, scope);
  }

  /** Registers {@code prefix + path -> key} pairs, given as alternating path and key. */
  private static void leaves(String prefix, String... pathsAndKeys) {
    for (int i = 0; i < pathsAndKeys.length; i += 2) {
      LEAVES.put(P + prefix + pathsAndKeys[i], pathsAndKeys[i + 1]);
    }
  }

  private static final XMLInputFactory XML_FACTORY = newXmlFactory();

  private static XMLInputFactory newXmlFactory() {
    XMLInputFactory f = XMLInputFactory.newInstance();
    f.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);
    f.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
    return f;
  }

  private StorageProvider storageProvider;
  private String cacheBaseDir;

  @Override public void setStorageProvider(StorageProvider sp, String cacheDir) {
    this.storageProvider = sp;
    this.cacheBaseDir = cacheDir;
  }

  private StorageProvider storageProvider() {
    if (storageProvider == null) {
      storageProvider = StorageProviderFactory.createForGovDataCache();
      cacheBaseDir = StorageProviderFactory.getGovDataCacheDir();
    }
    return storageProvider;
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String table = config.getName();
    if (!TABLES.contains(table)) {
      throw new GovDataException("CongressBillStatusProvider does not serve table '" + table + "'");
    }
    String congress = required(variables, "congress");
    String billType = required(variables, "bill_type");
    String refresh = required(variables, "refresh_month");

    String url = VariableResolver.substitute(config.getSource().getUrl(), variables);
    StorageProvider sp = storageProvider();
    String cachePath = sp.resolvePath(cacheBaseDir, String.format(Locale.US,
        "bill_status/congress=%s/bill_type=%s/refresh=%s/BILLSTATUS.zip",
        congress, billType, refresh));

    File zipFile = File.createTempFile("billstatus-", ".zip");
    zipFile.deleteOnExit();
    try {
      if (sp.exists(cachePath)) {
        try (InputStream in = sp.openInputStream(cachePath)) {
          Files.copy(in, zipFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
      } else {
        ZipDownloadUtils.downloadToFile(url, null, zipFile);
        try (InputStream in = new FileInputStream(zipFile)) {
          sp.writeFile(cachePath, in);
        }
      }
      LOGGER.info("{}: streaming {} (congress={}, type={})", table, url, congress, billType);
      return new RowIterator(table, congress, billType, new ZipFile(zipFile), zipFile);
    } catch (IOException | RuntimeException e) {
      Files.deleteIfExists(zipFile.toPath());
      throw e;
    }
  }

  private static String required(Map<String, String> variables, String name) {
    String v = variables.get(name);
    if (v == null || v.isEmpty()) {
      throw new GovDataException("CongressBillStatusProvider: dimension '" + name
          + "' is required but was not supplied");
    }
    return v;
  }

  /** One element of the bill, its leaf fields, and the repeating items nested under it. */
  static final class Node {
    final Map<String, String> fields = new LinkedHashMap<String, String>();
    private final Map<String, List<Node>> children = new HashMap<String, List<Node>>();

    List<Node> children(String scope) {
      List<Node> list = children.get(scope);
      if (list == null) {
        list = new ArrayList<Node>();
        children.put(scope, list);
      }
      return list;
    }

    String get(String key) {
      return fields.get(key);
    }
  }

  /** Everything one BILLSTATUS file carries that the tables use. */
  static final class BillRecord {
    final Node root = new Node();
    final Map<String, String> bill = root.fields;
  }

  /**
   * Parses one BILLSTATUS XML document with a StAX pull parser. Repeating items become child
   * nodes of the innermost open node; an empty element is treated as an absent value.
   */
  static BillRecord parse(InputStream in) throws XMLStreamException {
    BillRecord rec = new BillRecord();
    XMLStreamReader r = XML_FACTORY.createXMLStreamReader(in);
    try {
      Deque<String> paths = new ArrayDeque<String>();
      Deque<Node> open = new ArrayDeque<Node>();
      Deque<String> openPaths = new ArrayDeque<String>();
      open.push(rec.root);
      while (r.hasNext()) {
        int ev = r.next();
        if (ev == XMLStreamConstants.START_ELEMENT) {
          String name = r.getLocalName();
          String path = paths.isEmpty() ? name : paths.peek() + "/" + name;
          String scope = ITEMS.get(path);
          if (scope != null) {
            Node node = new Node();
            open.peek().children(scope).add(node);
            open.push(node);
            openPaths.push(path);
          }
          String key = LEAVES.get(path);
          if (key != null) {
            // getElementText consumes the matching END_ELEMENT, so the leaf is never pushed.
            String text = r.getElementText().trim();
            if (!text.isEmpty()) {
              open.peek().fields.putIfAbsent(key, text);
            }
            continue;
          }
          paths.push(path);
        } else if (ev == XMLStreamConstants.END_ELEMENT) {
          String path = paths.pop();
          if (!openPaths.isEmpty() && openPaths.peek().equals(path)) {
            openPaths.pop();
            open.pop();
          }
        }
      }
    } finally {
      r.close();
    }
    for (String key : new String[] {"congress", "type", "number"}) {
      if (!rec.bill.containsKey(key)) {
        throw new GovDataException("BILLSTATUS document has no bill/" + key);
      }
    }
    return rec;
  }

  private static Integer integer(String text) {
    return text == null ? null : Integer.valueOf(text);
  }

  private static Map<String, Object> keyed(BillRecord rec) {
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("congress", Integer.valueOf(rec.bill.get("congress")));
    row.put("bill_number", Integer.valueOf(rec.bill.get("number")));
    return row;
  }

  private static void copy(Map<String, Object> row, Node from, String... pairs) {
    for (int i = 0; i < pairs.length; i += 2) {
      row.put(pairs[i], from.get(pairs[i + 1]));
    }
  }

  /** Turns a parsed bill into the rows for {@code table}. */
  static List<Map<String, Object>> rows(String table, BillRecord rec) {
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    Node bill = rec.root;

    if (TABLE_BILLS.equals(table)) {
      Map<String, Object> row = keyed(rec);
      copy(row, bill, "origin_chamber", "origin_chamber", "introduced_date", "introduced_date",
          "title", "title", "policy_area", "policy_area",
          "sponsor_bioguide_id", "sponsor_bioguide_id", "sponsor_full_name", "sponsor_full_name",
          "sponsor_party", "sponsor_party", "sponsor_state", "sponsor_state",
          "sponsor_district", "sponsor_district", "latest_action_date", "latest_action_date",
          "latest_action_text", "latest_action_text", "law_type", "law_type",
          "law_number", "law_number", "update_date", "update_date",
          "legislation_url", "legislation_url", "latest_action_time", "latest_action_time",
          "update_date_including_text", "update_date_including_text",
          "constitutional_authority_statement", "constitutional_authority_statement",
          "on_behalf_of_bioguide_id", "on_behalf_of_bioguide_id",
          "on_behalf_of_full_name", "on_behalf_of_full_name",
          "on_behalf_of_type", "on_behalf_of_type");
      row.put("sponsor_is_by_request", parseYesNo(bill.get("sponsor_is_by_request")));
      row.put("action_count", Integer.valueOf(bill.children("action").size()));
      row.put("cosponsor_count", Integer.valueOf(bill.children("cosponsor").size()));
      out.add(row);
    } else if (TABLE_ACTIONS.equals(table)) {
      // BILLSTATUS lists actions newest-first; the sequence is numbered oldest-first so an
      // action's seq stays fixed when later actions are added.
      List<Node> actions = bill.children("action");
      int n = actions.size();
      for (int i = 0; i < n; i++) {
        Map<String, Object> row = keyed(rec);
        row.put("action_seq", Integer.valueOf(n - i));
        copy(row, actions.get(i), "action_date", "action_date", "action_time", "action_time",
            "text", "text", "action_type", "action_type", "action_code", "action_code",
            "source_system", "source_system", "calendar_number", "calendar_number");
        out.add(row);
      }
    } else if (TABLE_COSPONSORS.equals(table)) {
      for (Node c : bill.children("cosponsor")) {
        Map<String, Object> row = keyed(rec);
        copy(row, c, "bioguide_id", "bioguide_id", "full_name", "full_name", "party", "party",
            "state", "state", "district", "district", "sponsorship_date", "sponsorship_date");
        row.put("is_original_cosponsor", parseBoolean(c.get("is_original_cosponsor")));
        row.put("withdrawn_date", c.get("withdrawn_date"));
        out.add(row);
      }
    } else if (TABLE_COMMITTEES.equals(table)) {
      for (Node c : bill.children("committee")) {
        Map<String, Object> row = keyed(rec);
        copy(row, c, "committee_system_code", "system_code", "committee_name", "name",
            "chamber", "chamber", "committee_type", "type");
        row.put("parent_committee_system_code", null);
        out.add(row);
        for (Node sub : c.children("subcommittee")) {
          Map<String, Object> subRow = keyed(rec);
          copy(subRow, sub, "committee_system_code", "system_code", "committee_name", "name");
          subRow.put("chamber", null);
          subRow.put("committee_type", null);
          subRow.put("parent_committee_system_code", c.get("system_code"));
          out.add(subRow);
        }
      }
    } else if (TABLE_COMMITTEE_ACTIVITIES.equals(table)) {
      for (Node c : bill.children("committee")) {
        addActivities(out, rec, c);
        for (Node sub : c.children("subcommittee")) {
          addActivities(out, rec, sub);
        }
      }
    } else if (TABLE_SUBJECTS.equals(table)) {
      for (Node s : bill.children("subject")) {
        Map<String, Object> row = keyed(rec);
        copy(row, s, "subject_name", "name", "update_date", "update_date");
        out.add(row);
      }
    } else if (TABLE_TEXT_VERSIONS.equals(table)) {
      for (Node v : bill.children("text_version")) {
        List<Node> formats = v.children("text_format");
        if (formats.isEmpty()) {
          // A version with no published format is still a version; keep it with no url.
          out.add(textVersionRow(rec, v, null));
        }
        for (Node f : formats) {
          out.add(textVersionRow(rec, v, f));
        }
      }
    } else if (TABLE_RELATED_BILLS.equals(table)) {
      for (Node rb : bill.children("related_bill")) {
        List<Node> rels = rb.children("relationship");
        if (rels.isEmpty()) {
          out.add(relatedBillRow(rec, rb, null));
        }
        for (Node rel : rels) {
          out.add(relatedBillRow(rec, rb, rel));
        }
      }
    } else if (TABLE_AMENDMENTS.equals(table)) {
      for (Node a : bill.children("amendment")) {
        Map<String, Object> row = keyed(rec);
        row.put("amendment_type", a.get("type"));
        row.put("amendment_number", integer(a.get("number")));
        copy(row, a, "chamber", "chamber", "sponsor_bioguide_id", "sponsor_bioguide_id",
            "sponsor_full_name", "sponsor_full_name", "sponsor_name", "sponsor_name",
            "sponsor_party", "sponsor_party", "sponsor_state", "sponsor_state",
            "sponsor_district", "sponsor_district", "description", "description",
            "purpose", "purpose", "submitted_date", "submitted_date",
            "proposed_date", "proposed_date", "latest_action_date", "latest_action_date",
            "latest_action_text", "latest_action_text", "update_date", "update_date",
            "amended_amendment_type", "amended_amendment_type",
            "latest_action_time", "latest_action_time",
            "on_behalf_of_bioguide_id", "on_behalf_of_bioguide_id",
            "on_behalf_of_full_name", "on_behalf_of_full_name",
            "on_behalf_of_type", "on_behalf_of_type");
        row.put("amended_amendment_number", integer(a.get("amended_amendment_number")));
        row.put("amendments_to_amendment_count",
            integer(a.get("amendments_to_amendment_count")));
        row.put("action_count", Integer.valueOf(a.children("amendment_action").size()));
        row.put("cosponsor_count", Integer.valueOf(a.children("amendment_cosponsor").size()));
        out.add(row);
      }
    } else if (TABLE_AMENDMENT_ACTIONS.equals(table)) {
      // No sequence number: amendment actions are not in date order in the source.
      for (Node a : bill.children("amendment")) {
        for (Node act : a.children("amendment_action")) {
          Map<String, Object> row = amendmentKey(rec, a);
          copy(row, act, "action_date", "action_date", "action_time", "action_time",
              "text", "text", "action_type", "action_type", "action_code", "action_code",
              "source_system", "source_system");
          out.add(row);
        }
      }
    } else if (TABLE_AMENDMENT_COSPONSORS.equals(table)) {
      for (Node a : bill.children("amendment")) {
        for (Node c : a.children("amendment_cosponsor")) {
          Map<String, Object> row = amendmentKey(rec, a);
          copy(row, c, "bioguide_id", "bioguide_id", "full_name", "full_name", "party", "party",
              "state", "state", "sponsorship_date", "sponsorship_date");
          row.put("is_original_cosponsor", parseBoolean(c.get("is_original_cosponsor")));
          row.put("withdrawn_date", c.get("withdrawn_date"));
          out.add(row);
        }
      }
    } else if (TABLE_ACTION_COMMITTEES.equals(table)) {
      List<Node> actions = bill.children("action");
      int n = actions.size();
      for (int i = 0; i < n; i++) {
        for (Node c : actions.get(i).children("action_committee")) {
          Map<String, Object> row = keyed(rec);
          row.put("action_seq", Integer.valueOf(n - i));
          copy(row, c, "committee_system_code", "system_code", "committee_name", "name");
          out.add(row);
        }
      }
    } else if (TABLE_RECORDED_VOTES.equals(table)) {
      // Votes on a bill's own actions, then (legacy files) votes recorded on the bill itself,
      // then votes on each amendment's actions.
      List<Node> actions = bill.children("action");
      int n = actions.size();
      for (int i = 0; i < n; i++) {
        for (Node v : actions.get(i).children("recorded_vote")) {
          out.add(voteRow(rec, null, actions.get(i), v));
        }
      }
      for (Node v : bill.children("recorded_vote")) {
        out.add(voteRow(rec, null, null, v));
      }
      for (Node a : bill.children("amendment")) {
        for (Node act : a.children("amendment_action")) {
          for (Node v : act.children("recorded_vote")) {
            out.add(voteRow(rec, a, act, v));
          }
        }
      }
    } else if (TABLE_TITLES.equals(table)) {
      for (Node t : bill.children("title")) {
        Map<String, Object> row = keyed(rec);
        copy(row, t, "title_type", "title_type", "title_type_code", "title_type_code",
            "title", "title", "bill_text_version_code", "bill_text_version_code",
            "bill_text_version_name", "bill_text_version_name", "chamber_code", "chamber_code",
            "chamber_name", "chamber_name", "parent_title_type", "parent_title_type",
            "source_system", "source_system", "update_date", "update_date");
        out.add(row);
      }
    } else if (TABLE_SUMMARIES.equals(table)) {
      for (Node sm : bill.children("summary")) {
        Map<String, Object> row = keyed(rec);
        copy(row, sm, "version_code", "version_code", "action_date", "action_date",
            "action_description", "action_description", "summary_name", "summary_name",
            "update_date", "update_date", "last_summary_update_date",
            "last_summary_update_date", "summary_text", "summary_text");
        out.add(row);
      }
    } else if (TABLE_CBO_ESTIMATES.equals(table)) {
      for (Node e : bill.children("cbo_estimate")) {
        Map<String, Object> row = keyed(rec);
        copy(row, e, "pub_date", "pub_date", "title", "title", "url", "url",
            "description", "description");
        out.add(row);
      }
    } else if (TABLE_COMMITTEE_REPORTS.equals(table)) {
      for (Node r : bill.children("committee_report")) {
        Map<String, Object> row = keyed(rec);
        copy(row, r, "citation", "citation");
        out.add(row);
      }
    } else if (TABLE_NOTES.equals(table)) {
      for (Node note : bill.children("note")) {
        List<Node> links = note.children("note_link");
        if (links.isEmpty()) {
          out.add(noteRow(rec, note, null));
        }
        for (Node l : links) {
          out.add(noteRow(rec, note, l));
        }
      }
    } else {
      throw new GovDataException("No row builder for table '" + table + "'");
    }
    return out;
  }

  private static Map<String, Object> amendmentKey(BillRecord rec, Node amendment) {
    Map<String, Object> row = keyed(rec);
    row.put("amendment_type", amendment.get("type"));
    row.put("amendment_number", integer(amendment.get("number")));
    return row;
  }

  /** A roll-call vote; {@code amendment} and {@code action} are null when not applicable. */
  private static Map<String, Object> voteRow(BillRecord rec, Node amendment, Node action,
      Node vote) {
    Map<String, Object> row = keyed(rec);
    row.put("amendment_type", amendment == null ? null : amendment.get("type"));
    row.put("amendment_number", amendment == null ? null : integer(amendment.get("number")));
    row.put("action_date", action == null ? null : action.get("action_date"));
    row.put("action_text", action == null ? null : action.get("text"));
    row.put("vote_chamber", vote.get("chamber"));
    row.put("vote_congress", integer(vote.get("congress")));
    row.put("session_number", integer(vote.get("session_number")));
    row.put("roll_number", integer(vote.get("roll_number")));
    row.put("vote_date", vote.get("date"));
    row.put("url", vote.get("url"));
    row.put("full_action_name", vote.get("full_action_name"));
    return row;
  }

  private static Map<String, Object> noteRow(BillRecord rec, Node note, Node link) {
    Map<String, Object> row = keyed(rec);
    row.put("note_text", note.get("note_text"));
    row.put("link_name", link == null ? null : link.get("name"));
    row.put("link_url", link == null ? null : link.get("url"));
    return row;
  }

  private static Boolean parseYesNo(String text) {
    if (text == null) {
      return null;
    }
    if ("Y".equals(text)) {
      return Boolean.TRUE;
    }
    if ("N".equals(text)) {
      return Boolean.FALSE;
    }
    throw new GovDataException("Unexpected Y/N value in BILLSTATUS: '" + text + "'");
  }

  private static void addActivities(List<Map<String, Object>> out, BillRecord rec,
      Node committee) {
    for (Node a : committee.children("activity")) {
      Map<String, Object> row = keyed(rec);
      row.put("committee_system_code", committee.get("system_code"));
      copy(row, a, "activity_name", "name", "activity_date", "date");
      out.add(row);
    }
  }

  private static Map<String, Object> textVersionRow(BillRecord rec, Node version, Node format) {
    Map<String, Object> row = keyed(rec);
    copy(row, version, "version_type", "version_type", "version_date", "version_date");
    row.put("format_type", format == null ? null : format.get("format_type"));
    row.put("url", format == null ? null : format.get("url"));
    return row;
  }

  private static Map<String, Object> relatedBillRow(BillRecord rec, Node related, Node rel) {
    Map<String, Object> row = keyed(rec);
    row.put("related_congress", integer(related.get("congress")));
    String type = related.get("type");
    // Lowercased so it joins to the bill_type partition column, which uses the URL slugs.
    row.put("related_bill_type", type == null ? null : type.toLowerCase(Locale.ROOT));
    row.put("related_bill_number", integer(related.get("number")));
    copy(row, related, "related_title", "title", "related_latest_action_date",
        "latest_action_date", "related_latest_action_time", "latest_action_time",
        "related_latest_action_text", "latest_action_text");
    row.put("relationship_type", rel == null ? null : rel.get("relationship_type"));
    row.put("identified_by", rel == null ? null : rel.get("identified_by"));
    return row;
  }

  private static Boolean parseBoolean(String text) {
    if (text == null) {
      return null;
    }
    if ("True".equals(text)) {
      return Boolean.TRUE;
    }
    if ("False".equals(text)) {
      return Boolean.FALSE;
    }
    throw new GovDataException("Unexpected boolean value in BILLSTATUS: '" + text + "'");
  }

  /** Lazily walks the zip, parsing one bill file at a time. */
  private static final class RowIterator implements Iterator<Map<String, Object>> {
    private final String table;
    private final String expectedCongress;
    private final String expectedType;
    private final ZipFile zip;
    private final File zipFile;
    private final Enumeration<? extends ZipEntry> entries;
    private final Deque<Map<String, Object>> pending = new ArrayDeque<Map<String, Object>>();
    private boolean closed;

    RowIterator(String table, String expectedCongress, String expectedType, ZipFile zip,
        File zipFile) {
      this.table = table;
      this.expectedCongress = expectedCongress;
      this.expectedType = expectedType;
      this.zip = zip;
      this.zipFile = zipFile;
      this.entries = zip.entries();
    }

    @Override public boolean hasNext() {
      while (pending.isEmpty()) {
        if (closed || !entries.hasMoreElements()) {
          close();
          return false;
        }
        ZipEntry entry = entries.nextElement();
        if (entry.isDirectory() || !entry.getName().endsWith(".xml")) {
          continue;
        }
        try (InputStream in = zip.getInputStream(entry)) {
          BillRecord rec = parse(in);
          if (!expectedCongress.equals(rec.bill.get("congress"))
              || !expectedType.equals(rec.bill.get("type").toLowerCase(Locale.ROOT))) {
            throw new GovDataException(entry.getName() + " is congress "
                + rec.bill.get("congress") + " type " + rec.bill.get("type")
                + " but the batch is congress " + expectedCongress + " type " + expectedType);
          }
          pending.addAll(rows(table, rec));
        } catch (IOException | XMLStreamException e) {
          close();
          throw new GovDataException("Failed to parse " + entry.getName(), e);
        }
      }
      return true;
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return pending.poll();
    }

    private void close() {
      if (closed) {
        return;
      }
      closed = true;
      try {
        zip.close();
        Files.deleteIfExists(zipFile.toPath());
      } catch (IOException e) {
        throw new GovDataException("Failed to release " + zipFile, e);
      }
    }
  }
}
