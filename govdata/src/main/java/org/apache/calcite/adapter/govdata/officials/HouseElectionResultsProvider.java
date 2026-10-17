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

/**
 * DataProvider for {@code officials.house_election_results}: the MIT Election Data + Science Lab
 * U.S. House returns 1976-2018 (CC0), one row per candidate (or write-in / residual bucket) per
 * party line per district election round.
 *
 * <p>The Lab's current House release (1976-2024, Harvard Dataverse doi:10.7910/DVN/IG0UN2) sits
 * behind a Dataverse guestbook that demands a name, e-mail, institution and position before the
 * API serves the file, so it cannot be fetched non-interactively. The Lab's own GitHub repository
 * MEDSL/constituency-returns carries the earlier release of the same series (1976-2018); this
 * provider reads that file at a pinned commit. The file is ISO-8859-1 (the Lab wrote it from R on
 * Windows) and marks missing values with an unquoted {@code NA}.
 */
public class HouseElectionResultsProvider implements CachingDataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(HouseElectionResultsProvider.class);

  static final String TABLE = "house_election_results";
  static final String COMMIT = "cca36bc159334e1e2ae82904a5dd0a8a299c9124";
  static final String URL = "https://raw.githubusercontent.com/MEDSL/constituency-returns/"
      + COMMIT + "/1976-2018-house.csv";
  static final String RELEASE = "MIT Election Data + Science Lab, U.S. House returns 1976-2018, "
      + "github.com/MEDSL/constituency-returns commit " + COMMIT.substring(0, 10)
      + " (committed 2019-05-24)";
  private static final String USER_AGENT = "Apache-Calcite-GovData/1.0";
  private static final long BACKOFF_MS = 5_000L;
  private static final int MAX_ATTEMPTS = 3;

  static final String[] HEADERS = {"year", "state", "state_po", "state_fips", "state_cen",
      "state_ic", "office", "district", "stage", "runoff", "special", "candidate", "party",
      "writein", "mode", "candidatevotes", "totalvotes", "unofficial", "version"};

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables, RawCache rawCache) throws IOException {
    byte[] csv = download(rawCache);
    List<Map<String, Object>> rows = parse(new BufferedReader(
        new InputStreamReader(new ByteArrayInputStream(csv), StandardCharsets.ISO_8859_1)));
    LOGGER.info("house_election_results: {} rows from {}", Integer.valueOf(rows.size()), URL);
    return rows.iterator();
  }

  private static byte[] download(RawCache rawCache) throws IOException {
    IOException last = null;
    for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try (InputStream in = rawCache.openStream(URL,
              () -> ResumableHttpStream.open("MIT Election Lab House returns", USER_AGENT, URL,
                  BACKOFF_MS));
           ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        byte[] buf = new byte[1 << 16];
        int n;
        while ((n = in.read(buf)) != -1) {
          out.write(buf, 0, n);
        }
        return out.toByteArray();
      } catch (IOException e) {
        last = e;
        LOGGER.warn("house_election_results: attempt {}/{} failed for {}: {}",
            Integer.valueOf(attempt), Integer.valueOf(MAX_ATTEMPTS), URL, e.getMessage());
      }
    }
    throw last;
  }

  /** Parses the House returns CSV into one row per record. */
  static List<Map<String, Object>> parse(BufferedReader reader) throws IOException {
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
      if (!"US House".equalsIgnoreCase(office)) {
        throw new IOException(TABLE + " line " + line + ": office '" + office
            + "', expected US House");
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
      int district = ElectionResultsSupport.intValue(
          ElectionResultsSupport.required(f, idx, "district", TABLE, line), "district", TABLE,
          line);
      if (district < 0 || district > 60) {
        throw new IOException(TABLE + " line " + line + ": district " + district
            + " outside 0-60");
      }
      boolean special = ElectionResultsSupport.bool(
          ElectionResultsSupport.required(f, idx, "special", TABLE, line), "special", TABLE,
          line);
      String runoffRaw = ElectionResultsSupport.optional(f, idx, "runoff", "NA");
      Boolean runoff = runoffRaw == null ? null : Boolean.valueOf(
          ElectionResultsSupport.bool(runoffRaw, "runoff", TABLE, line));
      String stage = ElectionResultsSupport.optional(f, idx, "stage", "NA");
      if (stage != null && !stage.equals("gen") && !stage.equals("pri")) {
        throw new IOException(TABLE + " line " + line + ": stage '" + stage
            + "' is not gen, pri or NA");
      }
      boolean writein = ElectionResultsSupport.bool(
          ElectionResultsSupport.required(f, idx, "writein", TABLE, line), "writein", TABLE,
          line);
      String candidate = ElectionResultsSupport.optional(f, idx, "candidate", "NA");
      String party = ElectionResultsSupport.optional(f, idx, "party", "NA");

      Map<String, Object> m = new LinkedHashMap<String, Object>();
      m.put("year", Integer.valueOf(year));
      m.put("state_fips", stateFips);
      m.put("state_name", ElectionResultsSupport.stateName(
          ElectionResultsSupport.required(f, idx, "state", TABLE, line)));
      m.put("state_postal", ElectionResultsSupport.postal(
          ElectionResultsSupport.required(f, idx, "state_po", TABLE, line), TABLE, line));
      m.put("district", Integer.valueOf(district));
      m.put("at_large", Boolean.valueOf(district == 0));
      m.put("cd_fips", stateFips + String.format(Locale.ROOT, "%02d", Integer.valueOf(district)));
      m.put("special", Boolean.valueOf(special));
      m.put("runoff", runoff);
      m.put("stage", stage);
      String key = year + "|" + stateFips + "|" + district + "|" + special + "|" + runoff + "|"
          + stage;
      Integer prev = lineNo.get(key);
      int next = prev == null ? 1 : prev.intValue() + 1;
      lineNo.put(key, Integer.valueOf(next));
      m.put("line_no", Integer.valueOf(next));
      m.put("candidate", candidate);
      m.put("candidate_kind", ElectionResultsSupport.candidateKind(candidate, writein, TABLE,
          line));
      m.put("party_published", party);
      m.put("party_major", ElectionResultsSupport.majorFromHouseParty(party));
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
      m.put("source_release", RELEASE);
      rows.add(m);
    }
    if (rows.isEmpty()) {
      throw new IOException(TABLE + ": no data rows");
    }
    return rows;
  }
}
