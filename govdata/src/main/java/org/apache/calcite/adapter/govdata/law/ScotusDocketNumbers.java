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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads the Court's docket numbers off the first pages of an opinion.
 *
 * <p>The syllabus header of every opinion carries the docket number of the case, followed by
 * the argument and decision dates: {@code No. 23-583. Argued October 15, 2024-Decided December
 * 10, 2024}. Variations handled:
 * <ul>
 *   <li>an application, {@code No. 24A910}, and an application linked to a later petition,
 *       {@code No. 13A1003 (13-854)}, both of whose numbers are returned;</li>
 *   <li>an original-jurisdiction case, {@code No. 137, Orig.}, returned as {@code 137-Orig};</li>
 *   <li>a consolidated case, whose header names only the lead docket and whose footnote lists
 *       the rest: {@code *Together with No. 24-657, Firebaugh v. Garland, ...}. A companion
 *       named without a number (e.g. "see this Court's Rule 12.4") contributes none, because
 *       the text does not give one.</li>
 * </ul>
 *
 * <p>The dash is normalised to a hyphen. The lead docket comes first. An empty list means no
 * docket number was found; nothing is guessed.
 */
public final class ScotusDocketNumbers {

  /** Pages of the opinion that are examined; the header and its footnote are on the first. */
  private static final int PAGES = 3;

  private static final String NUMBER = "\\d{1,3}\\s*[–—\\-]\\s*\\d+|\\d{1,3}[AO]\\d+|\\d{1,4}";

  private static final String DATES =
      "(?:Argued|Decided|Submitted|Reargued|Final Decree|Decree)";

  /** The header's docket number, identified by the argument or decision date that follows it. */
  private static final Pattern LEAD =
      Pattern.compile("\\bNo\\.\\s*(" + NUMBER + ")(?:\\s*,\\s*(Orig)\\.)?"
          + "(?:\\s*\\(\\s*(" + NUMBER + ")\\s*\\))?\\s*\\.?\\s+" + DATES);

  /**
   * The header of an opinion as first released, before it is reprinted in a preliminary print:
   * {@code SUPREME COURT OF THE UNITED STATES No. 26A308 ... ON APPLICATION FOR STAY [September
   * 25, 2026]}, with no argument or decision date after the number. A consolidated case lists
   * its numbers there ({@code Nos. 24-656 and 24-657}).
   */
  private static final Pattern RELEASED_HEADER =
      Pattern.compile("SUPREME COURT OF THE UNITED STATES\\s+Nos?\\.\\s*((?:" + NUMBER + ")"
          + "(?:\\s*(?:,\\s*and\\s+|,\\s*|\\s+and\\s+|\\s*&\\s*)(?:" + NUMBER + "))*)");

  private static final Pattern EACH_NUMBER = Pattern.compile(NUMBER);

  private static final Pattern TOGETHER =
      Pattern.compile("\\*\\s*Together with(.{0,600}?)(?:also on|Page Proof|Cite as|Syllabus|$)");

  private static final Pattern COMPANION =
      Pattern.compile("\\bNos?\\.\\s*(" + NUMBER + ")(?:\\s*,\\s*(Orig)\\.)?"
          + "(?:\\s*\\(\\s*(" + NUMBER + ")\\s*\\))?");

  private static final Pattern WS = Pattern.compile("[\\s\\u00a0\\u2007\\u2009\\u202f]+");

  private ScotusDocketNumbers() {
  }

  /**
   * Returns the docket numbers found on the opinion's first pages, lead docket first.
   *
   * @param pageTexts extracted text of each page, first page first; only the first
   *     {@value #PAGES} are read
   */
  public static List<String> parse(List<String> pageTexts) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < PAGES && i < pageTexts.size(); i++) {
      sb.append(pageTexts.get(i) == null ? "" : pageTexts.get(i)).append(' ');
    }
    String text = WS.matcher(sb).replaceAll(" ");

    List<String> numbers = new ArrayList<String>();
    Matcher released = RELEASED_HEADER.matcher(text);
    Matcher lead = LEAD.matcher(text);
    if (!lead.find()) {
      if (released.find()) {
        Matcher each = EACH_NUMBER.matcher(released.group(1));
        while (each.find()) {
          add(numbers, each.group(), false);
        }
      }
      return numbers;
    }
    add(numbers, lead.group(1), lead.group(2) != null);
    add(numbers, lead.group(3), false);

    Matcher together = TOGETHER.matcher(text);
    if (together.find()) {
      Matcher companion = COMPANION.matcher(together.group(1));
      while (companion.find()) {
        add(numbers, companion.group(1), companion.group(2) != null);
        add(numbers, companion.group(3), false);
      }
    }
    return numbers;
  }

  private static void add(List<String> numbers, String number, boolean original) {
    if (number == null) {
      return;
    }
    String normalised = number.replaceAll("\\s+", "").replaceAll("[–—]", "-");
    if (original) {
      normalised = normalised + "-Orig";
    }
    if (!numbers.contains(normalised)) {
      numbers.add(normalised);
    }
  }
}
