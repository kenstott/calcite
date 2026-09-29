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

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * DataProvider for {@code omb_apportionments} — every OMB-approved apportionment (SF-132)
 * schedule line for one federal fiscal year, from the public apportionment site
 * {@code apportionment-public.max.gov}.
 *
 * <p>The site has no API. Its landing page is one large HTML index whose links point at three
 * renditions (XLSX, PDF, JSON) of each approved apportionment, one file per account
 * iteration. The provider streams the landing page for the {@code /JSON/} links of the
 * requested fiscal year, then fetches each JSON file through the raw cache and emits one row
 * per schedule line, lazily, one file at a time.
 *
 * <p>A file is {@code header + ScheduleData[] + FootnoteData[]}. Footnotes are file-level
 * free text, and are where withholding, deferral and rescission-proposal language sits, so
 * each line carries the text of the footnote it references. A footnote that no line
 * references is emitted as its own row with a null {@code line_number}, so no footnote text
 * is dropped.
 */
public class OmbApportionmentsProvider implements CachingDataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(OmbApportionmentsProvider.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String BASE = "https://apportionment-public.max.gov";
  private static final String HREF = "href=\"";

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    String year = variables.get("effective_year");
    if (year == null || year.isEmpty()) {
      year = variables.get("year");
    }
    final String fy = year.trim();
    // Validates the dimension value as a year; the same string is the site's folder name.
    Integer.parseInt(fy);

    final String prefix = "/Fiscal%20Year%20" + fy + "/";
    List<String> paths = new ArrayList<String>();
    InputStream index = rawCache.openStream(BASE + "/",
        () -> FiscalHttp.openGetWithRetry(BASE + "/").getInputStream());
    try {
      scanLinks(index, prefix, paths);
    } finally {
      index.close();
    }
    LOGGER.info("omb_apportionments: {} JSON apportionment files listed for FY{}", paths.size(), fy);
    return new FileRowIterator(paths, rawCache);
  }

  /**
   * Streams the index page and collects every {@code href} under {@code prefix} that is a
   * JSON rendition. The page is tens of MB, so it is scanned character by character rather
   * than held as a string.
   */
  static void scanLinks(InputStream in, String prefix, List<String> out) throws IOException {
    BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), 1 << 16);
    StringBuilder value = new StringBuilder();
    int matched = 0;
    int c;
    boolean inValue = false;
    while ((c = r.read()) != -1) {
      if (inValue) {
        if (c == '"') {
          inValue = false;
          String href = value.toString();
          if (href.startsWith(prefix) && href.endsWith(".json") && href.contains("/JSON/")) {
            out.add(href);
          }
          value.setLength(0);
        } else {
          value.append((char) c);
        }
      } else if (c == HREF.charAt(matched)) {
        matched++;
        if (matched == HREF.length()) {
          inValue = true;
          matched = 0;
        }
      } else {
        matched = c == HREF.charAt(0) ? 1 : 0;
      }
    }
  }

  /** Lazily expands one JSON file at a time into rows. */
  private static final class FileRowIterator implements Iterator<Map<String, Object>> {
    private final Iterator<String> paths;
    private final RawCache rawCache;
    private final Deque<Map<String, Object>> pending = new ArrayDeque<Map<String, Object>>();

    FileRowIterator(List<String> paths, RawCache rawCache) {
      this.paths = paths.iterator();
      this.rawCache = rawCache;
    }

    @Override public boolean hasNext() {
      while (pending.isEmpty() && paths.hasNext()) {
        try {
          expand(paths.next());
        } catch (IOException e) {
          throw new IllegalStateException("omb_apportionments: file fetch/parse failed", e);
        }
      }
      return !pending.isEmpty();
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return pending.poll();
    }

    private void expand(final String path) throws IOException {
      final String url = BASE + path;
      JsonNode root;
      InputStream in = rawCache.openStream(url,
          () -> FiscalHttp.openGetWithRetry(url).getInputStream());
      try {
        root = MAPPER.readTree(in);
      } finally {
        in.close();
      }
      JsonNode schedule = root.path("ScheduleData");
      if (!schedule.isArray()) {
        throw new IOException("no ScheduleData array in " + url);
      }
      Map<String, String> footnotes = new LinkedHashMap<String, String>();
      for (JsonNode f : root.path("FootnoteData")) {
        footnotes.put(text(f, "FootnoteNumber"), text(f, "FootnoteText"));
      }
      Set<String> referenced = new HashSet<String>();
      Map<String, Object> header = new LinkedHashMap<String, Object>();
      header.put("file_id", root.path("FileId").asLong());
      header.put("file_name", text(root, "FileName"));
      header.put("folder", text(root, "Folder"));
      header.put("approval_timestamp", text(root, "ApprovalTimestamp"));
      header.put("approver_title", text(root, "ApproverTitle"));
      header.put("funds_provided_by", text(root, "FundsProvidedBy"));

      for (JsonNode line : schedule) {
        Map<String, Object> row = new LinkedHashMap<String, Object>(header);
        row.put("budget_agency_title", text(line, "BudgetAgencyTitle"));
        row.put("budget_bureau_title", text(line, "BudgetBureauTitle"));
        row.put("account_title", text(line, "AccountTitle"));
        row.put("cgac_agency", text(line, "CgacAgency"));
        row.put("cgac_account", text(line, "CgacAcct"));
        row.put("allocation_agency_code", text(line, "AllocationAgencyCode"));
        row.put("allocation_subaccount", text(line, "AllocationSubacct"));
        row.put("begin_poa", text(line, "BeginPoa"));
        row.put("end_poa", text(line, "EndPoa"));
        row.put("availability_type_code", text(line, "AvailabilityTypeCode"));
        row.put("iteration", text(line, "Iteration"));
        row.put("tafs_iteration_id", line.path("TafsIterationId").asLong());
        row.put("line_number", text(line, "LineNumber"));
        row.put("line_split", text(line, "LineSplit"));
        row.put("line_description", text(line, "LineDescription"));
        row.put("approved_amount", line.path("ApprovedAmount").asDouble());
        String fn = text(line, "FootnoteNumber");
        row.put("footnote_number", fn);
        if (fn != null) {
          referenced.add(fn);
          row.put("footnote_text", footnotes.get(fn));
        }
        pending.add(row);
      }
      for (Map.Entry<String, String> f : footnotes.entrySet()) {
        if (!referenced.contains(f.getKey())) {
          Map<String, Object> row = new LinkedHashMap<String, Object>(header);
          row.put("footnote_number", f.getKey());
          row.put("footnote_text", f.getValue());
          pending.add(row);
        }
      }
    }

    /** Blank strings are the source's encoding of "not applicable"; they become null. */
    private static String text(JsonNode node, String field) {
      JsonNode v = node.get(field);
      if (v == null || v.isNull()) {
        return null;
      }
      String s = v.asText();
      return s.isEmpty() ? null : s;
    }
  }
}
