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

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts the bills a lobbying activity's free-text description names.
 *
 * <p>Rules (also summarised in the {@code lobbying_activity_bills} table comment):
 * <ul>
 *   <li><b>Forms recognised</b> — "H.R. 1", "HR 1", "HR1", "H.R. No. 1", "S. 123", "S123",
 *       "H.J.Res. 5", "S.J.Res. 5", "H.Con.Res. 5", "S.Con.Res. 5", "H.Res. 5", "S.Res. 5"
 *       (periods, spaces and case of "Res"/"Con"/"J" are free; the "H", "S" and "R" are
 *       upper case). A bare "S" must be followed by a period or directly by the digits.</li>
 *   <li><b>Not recognised</b> — a number that only follows a list ("H.R. 1, 2 and 3" yields
 *       H.R. 1), and spelled-out names ("House Bill 1"). Those are missed, never guessed.</li>
 *   <li><b>Congress</b> — taken from an explicit tag when one is adjacent: after the
 *       reference ("H.R. 1 (118th)", "H.R. 1, 118th Congress", "H.R. 1 of the 118th Congress")
 *       or directly before it ("118th Congress: H.R. 1"); the ordinal must be followed by
 *       "Congress"/"Cong." or a closing parenthesis, so "21st Century" is not a tag. Otherwise it is the Congress in
 *       session on the filing's posting date ({@link #congressOn}). A tag that names a Congress
 *       later than the one in session when the filing was posted cannot be right: the row is
 *       kept with a null Congress and basis {@code unresolved_tag} instead of being guessed.</li>
 *   <li>One row per (Congress, type, number) within an activity; an explicit tag wins over an
 *       inferred one for the same bill and Congress.</li>
 * </ul>
 */
final class LobbyingBillReferences {

  static final String EXPLICIT = "explicit_congress_tag";
  static final String INFERRED = "inferred_from_posted_date";
  static final String UNRESOLVED = "unresolved_tag";

  private static final Pattern REFERENCE = Pattern.compile(
      "(?<![A-Za-z0-9.])"
      + "(H\\.?\\s?J\\.?\\s?(?i:Res)\\.?"
      + "|S\\.?\\s?J\\.?\\s?(?i:Res)\\.?"
      + "|H\\.?\\s?(?i:Con)\\.?\\s?(?i:Res)\\.?"
      + "|S\\.?\\s?(?i:Con)\\.?\\s?(?i:Res)\\.?"
      + "|H\\.?\\s?(?i:Res)\\.?"
      + "|S\\.?\\s?(?i:Res)\\.?"
      + "|H\\.?\\s?R\\.?"
      + "|S\\.\\s?"
      + "|S(?=\\d))"
      + "\\s*(?:(?i:No)\\.?\\s*)?(\\d{1,5})(?!\\d)");

  // An ordinal is a Congress tag only when "Congress"/"Cong." follows it or it closes a
  // parenthesis, so "21st Century" is not one. Markdown emphasis around the reference is skipped.
  private static final Pattern TAG_AFTER = Pattern.compile(
      "^[*_]*\\s*[(\\[,]?\\s*(?:(?i:of|in)\\s+the\\s+)?(\\d{1,3})(?:st|nd|rd|th)"
      + "(?:\\s*(?i:Cong)|\\s*[)\\]])");

  private static final Pattern TAG_BEFORE = Pattern.compile(
      "(\\d{1,3})(?:st|nd|rd|th)\\s+(?i:Cong(?:ress|\\.)?)[\\s,:;\\-–(]*$");

  private LobbyingBillReferences() {
  }

  /** One bill named by a description. */
  static final class Ref {
    final Integer congress;
    final String billType;
    final int billNumber;
    final String matchBasis;
    final String referenceText;

    Ref(Integer congress, String billType, int billNumber, String matchBasis,
        String referenceText) {
      this.congress = congress;
      this.billType = billType;
      this.billNumber = billNumber;
      this.matchBasis = matchBasis;
      this.referenceText = referenceText;
    }
  }

  /** The Congress in session on a date; a Congress begins on 3 January of each odd year. */
  static int congressOn(LocalDate d) {
    int y = d.getYear();
    boolean beforeSwearing = (y % 2 == 1) && d.getDayOfYear() < 3;
    int startYear = (y % 2 == 1) ? y : y - 1;
    if (beforeSwearing) {
      startYear -= 2;
    }
    return (startYear - 1789) / 2 + 1;
  }

  /** The bill_type partition value used by the bills table, from the matched prefix. */
  static String billType(String prefix) {
    String p = prefix.replaceAll("[^A-Za-z]", "").toUpperCase(Locale.ROOT);
    switch (p) {
    case "HR": return "hr";
    case "S": return "s";
    case "HJRES": return "hjres";
    case "SJRES": return "sjres";
    case "HCONRES": return "hconres";
    case "SCONRES": return "sconres";
    case "HRES": return "hres";
    case "SRES": return "sres";
    default:
      throw new IllegalStateException("Unmapped bill prefix: " + prefix);
    }
  }

  static List<Ref> extract(String description, LocalDate posted) {
    List<Ref> out = new ArrayList<Ref>();
    if (description == null) {
      return out;
    }
    int postedCongress = congressOn(posted);
    Map<String, Ref> byKey = new LinkedHashMap<String, Ref>();
    Matcher m = REFERENCE.matcher(description);
    while (m.find()) {
      String type = billType(m.group(1));
      int number = Integer.parseInt(m.group(2));
      if (number == 0) {
        continue;
      }
      Integer tag = null;
      Matcher after = TAG_AFTER.matcher(description.substring(m.end()));
      if (after.find()) {
        tag = Integer.valueOf(after.group(1));
      } else {
        int from = Math.max(0, m.start() - 40);
        Matcher before = TAG_BEFORE.matcher(description.substring(from, m.start()));
        if (before.find()) {
          tag = Integer.valueOf(before.group(1));
        }
      }
      Ref ref;
      String text = m.group().trim();
      if (tag == null) {
        ref = new Ref(Integer.valueOf(postedCongress), type, number, INFERRED, text);
      } else if (tag.intValue() < 1 || tag.intValue() > postedCongress) {
        ref = new Ref(null, type, number, UNRESOLVED, text);
      } else {
        ref = new Ref(tag, type, number, EXPLICIT, text);
      }
      String key = ref.congress + "|" + type + "|" + number
          + (ref.congress == null ? "|" + tag : "");
      Ref prior = byKey.get(key);
      if (prior == null || (INFERRED.equals(prior.matchBasis) && EXPLICIT.equals(ref.matchBasis))) {
        byKey.put(key, ref);
      }
    }
    out.addAll(byKey.values());
    return out;
  }
}
