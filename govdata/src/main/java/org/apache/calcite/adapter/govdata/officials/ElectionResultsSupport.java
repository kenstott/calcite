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

import org.apache.calcite.adapter.file.etl.CsvRecordReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Strict parsing helpers shared by the MIT Election Data + Science Lab House and Senate providers.
 * Every helper fails with an {@link IOException} naming the table, the line and the offending
 * value; nothing is defaulted.
 */
final class ElectionResultsSupport {

  /**
   * Candidate labels that the Lab publishes in the candidate column but that are not people:
   * residual vote buckets (over/under/blank/void votes, scattering, "other"). Compared in lower
   * case after trimming. A name outside this list is a candidate.
   */
  private static final Set<String> RESIDUAL_LABELS = new HashSet<String>(Arrays.asList(
      "other", "scatter", "scattering", "blank vote", "blank votes", "blank vote/scattering",
      "blank vote/void vote/scattering", "over vote", "over votes", "overvote", "overvotes",
      "under vote", "under votes", "undervote", "undervotes", "void", "void vote",
      "none of these, candidates", "none of these candidates"));

  /** Senate party labels (upper case) that are the Democratic Party. */
  private static final Set<String> SENATE_DEMOCRATIC = new HashSet<String>(Arrays.asList(
      "DEMOCRAT", "DEMOCRATIC", "DEMOCRATIC-FARMER-LABOR", "DEMOCRATIC-NONPARTISAN LEAGUE",
      "DEMOCRATIC-NPL", "DEMOCRATIC/WORKING FAMILIES", "DEMOCRAT (NOT IDENTIFIED ON BALLOT)"));

  /** Senate party labels (upper case) that are the Republican Party. */
  private static final Set<String> SENATE_REPUBLICAN = new HashSet<String>(Arrays.asList(
      "REPUBLICAN", "GOP", "REPUBLICAN/CONSERVATIVE", "REPUBLICAN (NOT IDENTIFIED ON BALLOT)"));

  /** Party labels (lower case) the House file publishes that are the Democratic Party. */
  private static final Set<String> HOUSE_DEMOCRATIC = new HashSet<String>(Arrays.asList(
      "democrat", "democratic-farmer-labor"));

  /**
   * Party labels (lower case) the House file publishes that are the Republican Party: Minnesota's
   * Independent-Republican Party was the state's Republican Party until 1995.
   */
  private static final Set<String> HOUSE_REPUBLICAN = new HashSet<String>(Arrays.asList(
      "republican", "independent-republican"));

  private ElectionResultsSupport() { }

  /** Reads the header record and returns column name to index; the set must match exactly. */
  static Map<String, Integer> header(BufferedReader reader, String table, String[] expected)
      throws IOException {
    String record = CsvRecordReader.readRecord(reader);
    if (record == null) {
      throw new IOException(table + ": empty file");
    }
    List<String> cols = CsvRecordReader.splitFields(record, ',');
    Map<String, Integer> idx = new HashMap<String, Integer>();
    for (int i = 0; i < cols.size(); i++) {
      String name = cols.get(i).trim().toLowerCase(Locale.ROOT);
      if (idx.put(name, Integer.valueOf(i)) != null) {
        throw new IOException(table + ": duplicate header column '" + name + "'");
      }
    }
    for (String e : expected) {
      if (!idx.containsKey(e)) {
        throw new IOException(table + ": header lacks column '" + e + "'; header is " + cols);
      }
    }
    if (idx.size() != expected.length) {
      List<String> extra = new ArrayList<String>(idx.keySet());
      extra.removeAll(Arrays.asList(expected));
      throw new IOException(table + ": header has unexpected columns " + extra
          + "; the release layout changed and the parser must be reviewed");
    }
    return idx;
  }

  /** Splits a data record and checks it has exactly the header's field count. */
  static List<String> fields(String record, int width, String table, int line)
      throws IOException {
    List<String> f = CsvRecordReader.splitFields(record, ',');
    if (f.size() != width) {
      throw new IOException(table + " line " + line + ": " + f.size() + " fields, expected "
          + width);
    }
    return f;
  }

  static String required(List<String> f, Map<String, Integer> idx, String col, String table,
      int line) throws IOException {
    String v = f.get(idx.get(col).intValue()).trim();
    if (v.isEmpty()) {
      throw new IOException(table + " line " + line + ": empty value in required column " + col);
    }
    return v;
  }

  /** An optional text column: an empty field is null; {@code missingMarker} (e.g. NA) is null. */
  static String optional(List<String> f, Map<String, Integer> idx, String col,
      String missingMarker) {
    String v = f.get(idx.get(col).intValue()).trim();
    if (v.isEmpty() || (missingMarker != null && v.equals(missingMarker))) {
      return null;
    }
    return v;
  }

  static int intValue(String v, String col, String table, int line) throws IOException {
    try {
      return Integer.parseInt(wholeNumber(v));
    } catch (NumberFormatException e) {
      throw new IOException(table + " line " + line + ": " + col + " '" + v
          + "' is not an integer", e);
    }
  }

  static long longValue(String v, String col, String table, int line) throws IOException {
    long n;
    try {
      n = Long.parseLong(wholeNumber(v));
    } catch (NumberFormatException e) {
      throw new IOException(table + " line " + line + ": " + col + " '" + v
          + "' is not an integer", e);
    }
    if (n < 0) {
      throw new IOException(table + " line " + line + ": " + col + " '" + v + "' is negative");
    }
    return n;
  }

  /** The Lab's Senate file writes integers as "7310.0"; any other fraction is an error. */
  private static String wholeNumber(String v) {
    return v.endsWith(".0") ? v.substring(0, v.length() - 2) : v;
  }

  static boolean bool(String v, String col, String table, int line) throws IOException {
    if ("TRUE".equalsIgnoreCase(v)) {
      return true;
    }
    if ("FALSE".equalsIgnoreCase(v)) {
      return false;
    }
    throw new IOException(table + " line " + line + ": " + col + " '" + v
        + "' is not TRUE or FALSE");
  }

  /** 1 to 2 digit numeric FIPS to the 2-digit string, within the 1-56 state range. */
  static String stateFips(String v, String table, int line) throws IOException {
    int n = intValue(v, "state_fips", table, line);
    if (n < 1 || n > 56) {
      throw new IOException(table + " line " + line + ": state_fips " + n + " outside 1-56");
    }
    return String.format(Locale.ROOT, "%02d", Integer.valueOf(n));
  }

  /** Postal code: exactly two upper-case letters. */
  static String postal(String v, String table, int line) throws IOException {
    if (v.length() != 2 || !Character.isUpperCase(v.charAt(0))
        || !Character.isUpperCase(v.charAt(1))) {
      throw new IOException(table + " line " + line + ": state_po '" + v
          + "' is not a 2-letter postal code");
    }
    return v;
  }

  /** "NEW YORK" and "New York" to "New York"; "DISTRICT OF COLUMBIA" keeps "of" lower case. */
  static String stateName(String v) {
    StringBuilder sb = new StringBuilder();
    for (String word : v.trim().toLowerCase(Locale.ROOT).split(" +")) {
      if (sb.length() > 0) {
        sb.append(' ');
      }
      if (word.equals("of")) {
        sb.append(word);
      } else {
        sb.append(Character.toUpperCase(word.charAt(0))).append(word.substring(1));
      }
    }
    return sb.toString();
  }

  /** The Lab's release date: yyyyMMdd (all but the 2024 Senate partial) or MM/dd/yy, to ISO. */
  static String versionDate(String v, String table, int line) throws IOException {
    if (v.matches("\\d{8}")) {
      return v.substring(0, 4) + "-" + v.substring(4, 6) + "-" + v.substring(6, 8);
    }
    if (v.matches("\\d{2}/\\d{2}/\\d{2}")) {
      return "20" + v.substring(6, 8) + "-" + v.substring(0, 2) + "-" + v.substring(3, 5);
    }
    throw new IOException(table + " line " + line + ": version '" + v
        + "' is neither yyyyMMdd nor MM/dd/yy");
  }

  /** candidate, write_in_aggregate (write-ins lumped in one row) or residual (vote bucket). */
  static String candidateKind(String candidate, boolean writein, String table, int line)
      throws IOException {
    if (candidate == null) {
      if (!writein) {
        throw new IOException(table + " line " + line
            + ": candidate is empty on a row that is not a write-in");
      }
      return "write_in_aggregate";
    }
    String lower = candidate.toLowerCase(Locale.ROOT);
    if (lower.equals("write-in") || lower.equals("writein")) {
      return "write_in_aggregate";
    }
    if (RESIDUAL_LABELS.contains(lower)) {
      return "residual";
    }
    return "candidate";
  }

  /**
   * D, R or OTHER for a Senate row from the party label and the Lab's simplified party. The label
   * decides when it is one of the listed Democratic or Republican labels, because the Lab's
   * simplified column is wrong for some of them (2022 "DEMOCRATIC" in Illinois and Maryland and a
   * "REPUBLICAN (NOT IDENTIFIED ON BALLOT)" label are simplified to OTHER). A label that
   * contradicts the simplified party, or an unlisted label the Lab simplified to DEMOCRAT or
   * REPUBLICAN, fails: it needs a person to add it to the lists. With no label, only a DEMOCRAT or
   * REPUBLICAN simplified party counts; otherwise the party is unknown (null).
   */
  static String majorForSenate(String label, String simplified, String table, int line)
      throws IOException {
    if (simplified != null && !simplified.equals("DEMOCRAT") && !simplified.equals("REPUBLICAN")
        && !simplified.equals("LIBERTARIAN") && !simplified.equals("OTHER")) {
      throw new IOException(table + " line " + line + ": party_simplified '" + simplified
          + "' is not DEMOCRAT, REPUBLICAN, LIBERTARIAN or OTHER");
    }
    String fromSimplified = "DEMOCRAT".equals(simplified) ? "D"
        : "REPUBLICAN".equals(simplified) ? "R" : null;
    if (label == null) {
      return fromSimplified;
    }
    String upper = label.toUpperCase(Locale.ROOT);
    String fromLabel = SENATE_DEMOCRATIC.contains(upper) ? "D"
        : SENATE_REPUBLICAN.contains(upper) ? "R" : null;
    if (fromLabel != null) {
      if (fromSimplified != null && !fromSimplified.equals(fromLabel)) {
        throw new IOException(table + " line " + line + ": party '" + label
            + "' contradicts party_simplified '" + simplified + "'");
      }
      return fromLabel;
    }
    if (fromSimplified != null) {
      throw new IOException(table + " line " + line + ": party '" + label
          + "' is not a listed Democratic or Republican label but party_simplified is '"
          + simplified + "'");
    }
    return "OTHER";
  }

  /** D or R from the House file's party text: only the listed labels map to D and R. */
  static String majorFromHouseParty(String party) {
    if (party == null) {
      return null;
    }
    String lower = party.toLowerCase(Locale.ROOT);
    if (HOUSE_DEMOCRATIC.contains(lower)) {
      return "D";
    }
    if (HOUSE_REPUBLICAN.contains(lower)) {
      return "R";
    }
    return "OTHER";
  }
}
