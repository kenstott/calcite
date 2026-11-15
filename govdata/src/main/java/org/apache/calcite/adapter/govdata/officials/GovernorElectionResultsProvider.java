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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * DataProvider for {@code officials.governor_election_results}: the governor rows of "Candidates in
 * American General Elections" (Cha and Kuriwaki, Harvard Dataverse doi:10.7910/DVN/DGDRDT, CC0), one
 * row per candidate per statewide governor election.
 *
 * <p>The file is located through the Dataverse dataset API (the latest released version, the one
 * file named {@code candidates_YYYY-YYYY.tab}) and downloaded by file id, so a new release is
 * picked up and cached under its own key. The tab-separated file holds President, House, Senate
 * and Governor rows; only office G is kept. The parse is strict: the header must carry exactly the
 * fourteen published columns and any unrecognised office, type, party or number fails the fetch.
 */
public class GovernorElectionResultsProvider implements CachingDataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GovernorElectionResultsProvider.class);

  static final String TABLE = "governor_election_results";
  static final String DATASET_URL =
      "https://dataverse.harvard.edu/api/datasets/:persistentId?persistentId=doi:10.7910/DVN/DGDRDT";
  static final String FILE_URL = "https://dataverse.harvard.edu/api/access/datafile/";
  private static final Pattern FILE_LABEL = Pattern.compile("candidates_\\d{4}-\\d{4}\\.tab");
  private static final String USER_AGENT = "Apache-Calcite-GovData/1.0";
  private static final long BACKOFF_MS = 5_000L;
  private static final int MAX_ATTEMPTS = 3;

  static final String[] HEADERS = {"year", "state", "office", "dist", "type", "nextup", "party",
      "party_formal", "name_snyder", "inc", "candidatevotes", "totalvotes", "won", "runoff"};

  private static final Set<String> OFFICES = new HashSet<String>(Arrays.asList("P", "H", "S", "G"));
  private static final Set<String> PARTIES = new HashSet<String>(
      Arrays.asList("D", "R", "I", "Lbt", "Grn", "NPA", "Other"));

  private static final Map<String, String[]> BY_POSTAL = new HashMap<String, String[]>();

  static {
    for (String[] s : FecHouseResultsParser.STATES) {
      BY_POSTAL.put(s[0], s);
    }
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    byte[] meta = download(DATASET_URL, null, "Dataverse candidates dataset metadata");
    JsonNode version = latestVersion(new String(meta, StandardCharsets.UTF_8));
    String release = release(version);
    final String url = FILE_URL + fileId(version);
    LOGGER.info("governor_election_results: {} from {}", release, url);
    byte[] tab = download(url, rawCache, "Candidates in American General Elections");
    List<Map<String, Object>> rows = parse(new BufferedReader(
        new InputStreamReader(new ByteArrayInputStream(tab), StandardCharsets.UTF_8)), release);
    LOGGER.info("governor_election_results: {} rows", Integer.valueOf(rows.size()));
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
        LOGGER.warn("governor_election_results: attempt {}/{} failed for {}: {}",
            Integer.valueOf(attempt), Integer.valueOf(MAX_ATTEMPTS), url, e.getMessage());
      }
    }
    throw last;
  }

  /** The {@code data.latestVersion} node of the Dataverse dataset response; must be RELEASED. */
  static JsonNode latestVersion(String json) throws IOException {
    JsonNode root = new ObjectMapper().readTree(json);
    if (!"OK".equals(root.path("status").asText())) {
      throw new IOException("Dataverse candidates dataset: status is '"
          + root.path("status").asText() + "'");
    }
    JsonNode v = root.path("data").path("latestVersion");
    if (!"RELEASED".equals(v.path("versionState").asText())) {
      throw new IOException("Dataverse candidates dataset: latest version state is '"
          + v.path("versionState").asText() + "', expected RELEASED");
    }
    return v;
  }

  /** The id of the one candidates tab file in the version. */
  static String fileId(JsonNode version) throws IOException {
    String id = null;
    for (JsonNode f : version.path("files")) {
      if (FILE_LABEL.matcher(f.path("label").asText()).matches()) {
        if (id != null) {
          throw new IOException("Dataverse candidates dataset: more than one file matches "
              + FILE_LABEL.pattern());
        }
        if (f.path("restricted").asBoolean(true)) {
          throw new IOException("Dataverse candidates dataset: file " + f.path("label").asText()
              + " is restricted");
        }
        id = f.path("dataFile").path("id").asText();
      }
    }
    if (id == null || id.isEmpty()) {
      throw new IOException("Dataverse candidates dataset: no file matches "
          + FILE_LABEL.pattern());
    }
    return id;
  }

  /** Source note stored on every row: dataset, version number and release date. */
  static String release(JsonNode version) throws IOException {
    if (!version.path("versionNumber").canConvertToInt()
        || version.path("releaseTime").isMissingNode()) {
      throw new IOException("Dataverse candidates dataset: version number or release time missing");
    }
    return "Cha and Kuriwaki, Candidates in American General Elections, Harvard Dataverse "
        + "doi:10.7910/DVN/DGDRDT v" + version.path("versionNumber").asInt()
        + " (released " + version.path("releaseTime").asText().substring(0, 10) + ")";
  }

  /** Parses the candidates file into one row per governor-race candidate; other offices skipped. */
  static List<Map<String, Object>> parse(BufferedReader reader, String release)
      throws IOException {
    String headerRecord = CsvRecordReader.readRecord(reader);
    if (headerRecord == null) {
      throw new IOException(TABLE + ": empty file");
    }
    List<String> cols = CsvRecordReader.splitFields(headerRecord, '\t');
    if (!cols.equals(Arrays.asList(HEADERS))) {
      throw new IOException(TABLE + ": header is " + cols + ", expected " + Arrays.asList(HEADERS)
          + "; the release layout changed and the parser must be reviewed");
    }
    Map<String, Integer> idx = new HashMap<String, Integer>();
    for (int i = 0; i < HEADERS.length; i++) {
      idx.put(HEADERS[i], Integer.valueOf(i));
    }
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Integer> lineNo = new HashMap<String, Integer>();
    int line = 1;
    String record;
    while ((record = CsvRecordReader.readRecord(reader)) != null) {
      line++;
      if (record.trim().isEmpty()) {
        continue;
      }
      List<String> f = CsvRecordReader.splitFields(record, '\t');
      if (f.size() != HEADERS.length) {
        throw new IOException(TABLE + " line " + line + ": " + f.size() + " fields, expected "
            + HEADERS.length);
      }
      String office = ElectionResultsSupport.required(f, idx, "office", TABLE, line);
      if (!OFFICES.contains(office)) {
        throw new IOException(TABLE + " line " + line + ": office '" + office
            + "' is not P, H, S or G");
      }
      if (!"G".equals(office)) {
        continue;
      }
      String dist = ElectionResultsSupport.optional(f, idx, "dist", null);
      if (dist != null) {
        throw new IOException(TABLE + " line " + line + ": governor row has district '" + dist
            + "'");
      }
      String type = ElectionResultsSupport.required(f, idx, "type", TABLE, line);
      if (!"G".equals(type) && !"S".equals(type)) {
        throw new IOException(TABLE + " line " + line + ": type '" + type
            + "' is not G (general) or S (special)");
      }
      int year = intValue(
          ElectionResultsSupport.required(f, idx, "year", TABLE, line), "year", TABLE, line);
      String postal = ElectionResultsSupport.postal(
          ElectionResultsSupport.required(f, idx, "state", TABLE, line), TABLE, line);
      String[] st = BY_POSTAL.get(postal);
      if (st == null) {
        throw new IOException(TABLE + " line " + line + ": state '" + postal
            + "' is not one of the 50 states");
      }
      String party = ElectionResultsSupport.required(f, idx, "party", TABLE, line);
      if (!PARTIES.contains(party)) {
        throw new IOException(TABLE + " line " + line + ": party '" + party
            + "' is not one of " + PARTIES);
      }
      boolean special = "S".equals(type);
      String key = year + "|" + st[1] + "|" + special;
      Integer prev = lineNo.get(key);
      int next = prev == null ? 1 : prev.intValue() + 1;
      lineNo.put(key, Integer.valueOf(next));

      Map<String, Object> m = new LinkedHashMap<String, Object>();
      m.put("year", Integer.valueOf(year));
      m.put("state_fips", st[1]);
      m.put("state_name", st[2]);
      m.put("state_postal", postal);
      m.put("special", Boolean.valueOf(special));
      m.put("line_no", Integer.valueOf(next));
      m.put("candidate", ElectionResultsSupport.required(f, idx, "name_snyder", TABLE, line));
      m.put("party", party);
      m.put("party_formal", ElectionResultsSupport.optional(f, idx, "party_formal", null));
      m.put("party_major", "D".equals(party) ? "D" : "R".equals(party) ? "R" : "OTHER");
      String inc = ElectionResultsSupport.optional(f, idx, "inc", null);
      m.put("incumbent_code",
          inc == null ? null : Integer.valueOf(intValue(inc, "inc", TABLE,
              line)));
      m.put("won", Boolean.valueOf(flag(ElectionResultsSupport.required(f, idx, "won", TABLE,
          line), "won", line)));
      String runoff = ElectionResultsSupport.optional(f, idx, "runoff", null);
      m.put("runoff_flag", runoff == null ? null
          : Boolean.valueOf(flag(runoff, "runoff", line)));
      m.put("nextup_year", Integer.valueOf(intValue(
          ElectionResultsSupport.required(f, idx, "nextup", TABLE, line), "nextup", TABLE,
          line)));
      m.put("candidate_votes", Long.valueOf(longValue(
          ElectionResultsSupport.required(f, idx, "candidatevotes", TABLE, line),
          "candidatevotes", TABLE, line)));
      m.put("total_votes", Long.valueOf(longValue(
          ElectionResultsSupport.required(f, idx, "totalvotes", TABLE, line), "totalvotes",
          TABLE, line)));
      m.put("source_release", release);
      rows.add(m);
    }
    if (rows.isEmpty()) {
      throw new IOException(TABLE + ": no governor rows");
    }
    return rows;
  }

  /**
   * The file is a Stata export, so integers arrive as "2006.0" and large counts in scientific
   * notation ("1.0094822E7"). Only a value that is exactly a whole number is accepted.
   */
  private static long whole(String v, String col, int line) throws IOException {
    try {
      return new BigDecimal(v).longValueExact();
    } catch (NumberFormatException | ArithmeticException e) {
      throw new IOException(TABLE + " line " + line + ": " + col + " '" + v
          + "' is not a whole number", e);
    }
  }

  private static int intValue(String v, String col, String table, int line) throws IOException {
    long n = whole(v, col, line);
    if (n < Integer.MIN_VALUE || n > Integer.MAX_VALUE) {
      throw new IOException(table + " line " + line + ": " + col + " '" + v + "' is out of range");
    }
    return (int) n;
  }

  private static long longValue(String v, String col, String table, int line)
      throws IOException {
    long n = whole(v, col, line);
    if (n < 0) {
      throw new IOException(table + " line " + line + ": " + col + " '" + v + "' is negative");
    }
    return n;
  }

  private static boolean flag(String v, String col, int line) throws IOException {
    int n = intValue(v, col, TABLE, line);
    if (n != 0 && n != 1) {
      throw new IOException(TABLE + " line " + line + ": " + col + " '" + v + "' is not 0 or 1");
    }
    return n == 1;
  }
}
