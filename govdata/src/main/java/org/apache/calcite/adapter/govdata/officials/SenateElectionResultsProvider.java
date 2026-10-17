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
package org.apache.calcite.adapter.govdata.officials;

import org.apache.calcite.adapter.file.etl.CachingDataProvider;
import org.apache.calcite.adapter.file.etl.CsvRecordReader;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.file.etl.RawCache;
import org.apache.calcite.adapter.govdata.housing.HmdaLoanLevelAggregateProvider.ResumableHttpStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * DataProvider for {@code officials.senate_election_results}: the MIT Election Data + Science Lab
 * "U.S. Senate statewide returns 1976-2024" (Harvard Dataverse doi:10.7910/DVN/PEJ5QU, CC0), one
 * row per candidate (or write-in / residual bucket) per party line per statewide Senate election
 * round.
 *
 * <p>The file is located through the Dataverse dataset API (the latest released version, the one
 * file named {@code YYYY-YYYY-senate-state.tab}) and downloaded in its original CSV form by file
 * id, so a new release (a new file id) is picked up and cached under its own key. The dataset has
 * no guestbook, so the download works without interaction. The parse is strict: the header must
 * carry exactly the nineteen published columns, and any unrecognised stage, boolean, party,
 * version or number fails the fetch.
 */
public class SenateElectionResultsProvider implements CachingDataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SenateElectionResultsProvider.class);

  static final String TABLE = "senate_election_results";
  static final String DATASET_URL =
      "https://dataverse.harvard.edu/api/datasets/:persistentId?persistentId=doi:10.7910/DVN/PEJ5QU";
  static final String FILE_URL = "https://dataverse.harvard.edu/api/access/datafile/";
  private static final Pattern FILE_LABEL = Pattern.compile("\\d{4}-\\d{4}-senate-state\\.tab");
  private static final String USER_AGENT = "Apache-Calcite-GovData/1.0";
  private static final long BACKOFF_MS = 5_000L;
  private static final int MAX_ATTEMPTS = 3;

  static final String[] HEADERS = {"year", "state", "state_po", "state_fips", "state_cen",
      "state_ic", "office", "district", "stage", "special", "candidate", "party_detailed",
      "writein", "mode", "candidatevotes", "totalvotes", "unofficial", "version",
      "party_simplified"};

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    byte[] meta = download(DATASET_URL, null, "Dataverse Senate dataset metadata");
    JsonNode version = latestVersion(new String(meta, StandardCharsets.UTF_8));
    String release = release(version);
    String fileId = fileId(version);
    final String url = FILE_URL + fileId + "?format=original";
    LOGGER.info("senate_election_results: {} from {}", release, url);
    byte[] csv = download(url, rawCache, "MIT Election Lab Senate returns");
    List<Map<String, Object>> rows = parse(new BufferedReader(
        new InputStreamReader(new ByteArrayInputStream(csv), StandardCharsets.UTF_8)),
        release);
    LOGGER.info("senate_election_results: {} rows", Integer.valueOf(rows.size()));
    return rows.iterator();
  }

  private static byte[] download(final String url, RawCache rawCache, final String label)
      throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try (InputStream in = rawCache == null
              ? ResumableHttpStream.open(label, USER_AGENT, url, BACKOFF_MS)
              : rawCache.openStream(url,
                  () -> ResumableHttpStream.open(label, USER_AGENT, url, BACKOFF_MS));
           ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        byte[] buf = new byte[1 << 16];
        int n;
        while ((n = in.read(buf)) != -1) {
          out.write(buf, 0, n);
        }
        return out.toByteArray();
      } catch (IOException e) {
        last = e;
        LOGGER.warn("senate_election_results: attempt {}/{} failed for {}: {}",
            Integer.valueOf(attempt), Integer.valueOf(MAX_ATTEMPTS), url, e.getMessage());
      }
    }
    throw last;
  }

  /** The {@code data.latestVersion} node of the Dataverse dataset response; must be RELEASED. */
  static JsonNode latestVersion(String json) throws IOException {
    JsonNode root = new ObjectMapper().readTree(json);
    if (!"OK".equals(root.path("status").asText())) {
      throw new IOException("Dataverse Senate dataset: status is '"
          + root.path("status").asText() + "'");
    }
    JsonNode v = root.path("data").path("latestVersion");
    if (!"RELEASED".equals(v.path("versionState").asText())) {
      throw new IOException("Dataverse Senate dataset: latest version state is '"
          + v.path("versionState").asText() + "', expected RELEASED");
    }
    return v;
  }

  /** The id of the one state-level returns file in the version. */
  static String fileId(JsonNode version) throws IOException {
    String id = null;
    for (JsonNode f : version.path("files")) {
      if (FILE_LABEL.matcher(f.path("label").asText()).matches()) {
        if (id != null) {
          throw new IOException("Dataverse Senate dataset: more than one file matches "
              + FILE_LABEL.pattern());
        }
        if (f.path("restricted").asBoolean(true)) {
          throw new IOException("Dataverse Senate dataset: file " + f.path("label").asText()
              + " is restricted");
        }
        id = f.path("dataFile").path("id").asText();
      }
    }
    if (id == null || id.isEmpty()) {
      throw new IOException("Dataverse Senate dataset: no file matches " + FILE_LABEL.pattern());
    }
    return id;
  }

  /** Source note stored on every row: dataset, version number and release date. */
  static String release(JsonNode version) throws IOException {
    if (!version.path("versionNumber").canConvertToInt() || version.path("releaseTime").isMissingNode()) {
      throw new IOException("Dataverse Senate dataset: version number or release time missing");
    }
    return "MIT Election Data + Science Lab, U.S. Senate statewide returns 1976-2024, "
        + "Harvard Dataverse doi:10.7910/DVN/PEJ5QU v" + version.path("versionNumber").asInt()
        + " (released " + version.path("releaseTime").asText().substring(0, 10) + ")";
  }

  /** Parses the Senate returns CSV into one row per record. */
  static List<Map<String, Object>> parse(BufferedReader reader, String release)
      throws IOException {
    Map<String, Integer> idx = ElectionResultsSupport.header(reader, TABLE, HEADERS);
    int width = HEADERS.length;
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Integer> lineNo = new HashMap<String, Integer>();
    int line = 1;
    String record;
    while ((record = CsvRecordReader.readRecord(reader)) != null) {
      line++;
      if (record.trim().isEmpty()) {
        continue;
      }
      List<String> f = ElectionResultsSupport.fields(record, width, TABLE, line);
      String office = ElectionResultsSupport.required(f, idx, "office", TABLE, line);
      if (!"US SENATE".equalsIgnoreCase(office)) {
        throw new IOException(TABLE + " line " + line + ": office '" + office
            + "', expected US SENATE");
      }
      String district = ElectionResultsSupport.required(f, idx, "district", TABLE, line);
      if (!"statewide".equalsIgnoreCase(district)) {
        throw new IOException(TABLE + " line " + line + ": district '" + district
            + "', expected statewide");
      }
      String mode = ElectionResultsSupport.required(f, idx, "mode", TABLE, line)
          .toLowerCase(Locale.ROOT);
      if (!"total".equals(mode)) {
        throw new IOException(TABLE + " line " + line + ": mode '" + mode
            + "'; per-mode rows would double count the total and are not handled");
      }
      int year = ElectionResultsSupport.intValue(
          ElectionResultsSupport.required(f, idx, "year", TABLE, line), "year", TABLE, line);
      String stateFips = ElectionResultsSupport.stateFips(
          ElectionResultsSupport.required(f, idx, "state_fips", TABLE, line), TABLE, line);
      boolean special = ElectionResultsSupport.bool(
          ElectionResultsSupport.required(f, idx, "special", TABLE, line), "special", TABLE,
          line);
      String stage = stage(ElectionResultsSupport.required(f, idx, "stage", TABLE, line), line);
      boolean writein = ElectionResultsSupport.bool(
          ElectionResultsSupport.required(f, idx, "writein", TABLE, line), "writein", TABLE,
          line);
      String candidate = ElectionResultsSupport.optional(f, idx, "candidate", null);
      String simplified = ElectionResultsSupport.optional(f, idx, "party_simplified", null);

      Map<String, Object> m = new LinkedHashMap<String, Object>();
      m.put("year", Integer.valueOf(year));
      m.put("state_fips", stateFips);
      m.put("state_name", ElectionResultsSupport.stateName(
          ElectionResultsSupport.required(f, idx, "state", TABLE, line)));
      m.put("state_postal", ElectionResultsSupport.postal(
          ElectionResultsSupport.required(f, idx, "state_po", TABLE, line), TABLE, line));
      m.put("special", Boolean.valueOf(special));
      m.put("stage", stage);
      String key = year + "|" + stateFips + "|" + special + "|" + stage;
      Integer prev = lineNo.get(key);
      int next = prev == null ? 1 : prev.intValue() + 1;
      lineNo.put(key, Integer.valueOf(next));
      m.put("line_no", Integer.valueOf(next));
      m.put("candidate", candidate);
      m.put("candidate_kind", ElectionResultsSupport.candidateKind(candidate, writein, TABLE,
          line));
      String party = ElectionResultsSupport.optional(f, idx, "party_detailed", null);
      m.put("party_published", party);
      m.put("party_major", ElectionResultsSupport.majorForSenate(party, simplified, TABLE, line));
      m.put("writein", Boolean.valueOf(writein));
      long votes = ElectionResultsSupport.longValue(
          ElectionResultsSupport.required(f, idx, "candidatevotes", TABLE, line),
          "candidatevotes", TABLE, line);
      long total = ElectionResultsSupport.longValue(
          ElectionResultsSupport.required(f, idx, "totalvotes", TABLE, line), "totalvotes",
          TABLE, line);
      m.put("candidate_votes", Long.valueOf(votes));
      m.put("total_votes", Long.valueOf(total));
      // The Lab marks an uncontested race as one vote for the candidate out of a total of one.
      m.put("uncontested", Boolean.valueOf(votes == 1 && total == 1));
      m.put("unofficial", Boolean.valueOf(ElectionResultsSupport.bool(
          ElectionResultsSupport.required(f, idx, "unofficial", TABLE, line), "unofficial",
          TABLE, line)));
      m.put("version_date", ElectionResultsSupport.versionDate(
          ElectionResultsSupport.required(f, idx, "version", TABLE, line), TABLE, line));
      m.put("source_release", release);
      rows.add(m);
    }
    if (rows.isEmpty()) {
      throw new IOException(TABLE + ": no data rows");
    }
    return rows;
  }

  /** gen, pre, runoff or pri; the 2022 file's "GEN RUNOFF" is the general-election runoff. */
  private static String stage(String v, int line) throws IOException {
    String s = v.toLowerCase(Locale.ROOT);
    switch (s) {
    case "gen":
    case "pre":
    case "pri":
    case "runoff":
      return s;
    case "gen runoff":
      return "runoff";
    default:
      throw new IOException(TABLE + " line " + line + ": stage '" + v
          + "' is not gen, pre, pri, runoff or GEN RUNOFF");
    }
  }
}
