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
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Derives the disposition of a decided Supreme Court case from the text of its opinion PDF.
 *
 * <p>Used where no curated outcome exists: the slip opinions on supremecourt.gov (term 2018
 * onward) and volume 583 of the United States Reports (term 2017), whose GovInfo MODS carries
 * no disposition. Earlier volumes take their outcome from the MODS instead.
 *
 * <p>The syllabus of a merits opinion ends with a line naming the judgment below and what the
 * Court did with it, e.g. {@code 77 F. 4th 1077, vacated and remanded.}, immediately before
 * the sentence "X, J., delivered the opinion of the Court". That line is the disposition. Two
 * kinds of opinion have no such line and are recognised separately: a per curiam that dismisses
 * the writ as improvidently granted, and an order on an application (docket number
 * {@code NNANNN}) that grants or denies it.
 *
 * <p>The text is what the PDF font encoding yields, in which the "fi" ligature loses its "i"
 * ("affrmed", "fnal"). The verb pattern accepts that spelling and the result is normalised to
 * "affirmed". The opinion text itself is not repaired here.
 *
 * <p>Only the first {@value #MAX_PAGES} pages are read; the syllabus is always among them, so a
 * caller can stream pages and stop consuming after that.
 *
 * <p>A consolidated case whose dockets were decided differently has a syllabus line with one
 * clause per docket ("No. 14-1468, ..., reversed and remanded; No. 14-1470, ..., affirmed;
 * ..."). The first clause is returned, which is the convention GovInfo's MODS follows.
 */
public final class ScotusDispositionParser {

  /** Pages of the opinion that are examined; the syllabus ends well within them. */
  public static final int MAX_PAGES = 12;

  /** How the disposition was found. */
  public enum Kind {
    /** The syllabus disposition line of a merits opinion. */
    SYLLABUS_LINE,
    /** A per curiam dismissing the writ of certiorari as improvidently granted. */
    DISMISSED_IMPROVIDENTLY,
    /** An order granting or denying an application (docket number of the form 24A910). */
    APPLICATION,
    /** No disposition could be derived, e.g. an original-jurisdiction case. */
    NONE
  }

  /** MODS's decision type for a case the Court split evenly, which affirms the judgment below. */
  public static final String EQUALLY_DIVIDED_VOTE = "Equally divided vote";

  /**
   * The outcome of one parse. {@link #disposition} is null exactly when {@link #kind} is NONE;
   * {@link #decisionType} is {@link #EQUALLY_DIVIDED_VOTE} when the Court affirmed by an equally
   * divided vote and null otherwise.
   */
  public static final class Result {
    public final Kind kind;
    public final String disposition;
    public final String decisionType;

    Result(Kind kind, String disposition) {
      this(kind, disposition, null);
    }

    Result(Kind kind, String disposition, String decisionType) {
      this.kind = kind;
      this.disposition = disposition;
      this.decisionType = decisionType;
    }

    @Override public String toString() {
      return kind + (disposition == null ? "" : ": " + disposition);
    }
  }

  private static final Result NONE = new Result(Kind.NONE, null);

  private static final String WS = "[\\s\\u00a0\\u2007\\u2009\\u202f]+";

  private static final String VERB = "(?:af+i?rmed|reversed|vacated|dismissed)";

  /** One or more verbs joined by ", ", " and " or ", and ", each optionally "in part". */
  private static final String PHRASE =
      "(?:" + VERB + "(?: in part)?(?:,?(?: and| but)? (?:" + VERB + "|remanded)(?: in part)?)*"
          + "(?: by an equally divided Court)?|remanded)";

  /** A disposition phrase that follows a reporter citation, a page range, or a semicolon. */
  private static final Pattern DISPOSITION =
      Pattern.compile("(?:(?<=[\\d)])\\s*,\\s*|(?<=\\d\\.)\\s+|(?<=;)\\s*(?:judgment\\s+)?)"
          + "(" + PHRASE + ")[.;]", Pattern.CASE_INSENSITIVE);

  private static final Pattern PAGE_RANGE = Pattern.compile("Pp?\\. [\\d–\\-, ]+\\.");

  private static final Pattern SYLLABUS_END =
      Pattern.compile("delivered the opinion|announced the judgment of the Court"
          + "|PER CURIAM\\.|Per Curiam\\.\\s");

  private static final Pattern DISMISSED_AS_IMPROVIDENTLY =
      Pattern.compile("writ of certiorari is dismissed as improvidently granted",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern APPLICATION_DOCKET = Pattern.compile("No\\.\\s*\\d+A\\d+");

  private static final Pattern APPLICATION_RULING =
      Pattern.compile("\\b(?:is|are)\\s+(granted|denied)(\\s+in\\s+part)?\\b"
          + "|\\b(granted|denied)\\s+in\\s+part\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern SOFT_HYPHEN_BREAK = Pattern.compile("-\\n(?=[a-z])");

  private static final Pattern PAGE_PROOF_BANNER =
      Pattern.compile("\\n?Page Proof Pending Publication\\n?");

  /** How much of the opening text is searched for a docket number. */
  private static final int DOCKET_SEARCH_CHARS = 1500;

  /** How much of the opening text is searched for the dismissal sentence. */
  private static final int DISMISSAL_SEARCH_CHARS = 6000;

  private static final int FALLBACK_PAGES = 4;

  private ScotusDispositionParser() {
  }

  /**
   * Derives the disposition from the page texts of one opinion, in page order.
   *
   * @param pageTexts extracted text of each page, first page first; at most
   *     {@link #MAX_PAGES} are read
   */
  public static Result parse(List<String> pageTexts) {
    List<String> pages = new ArrayList<String>(MAX_PAGES);
    for (String page : pageTexts) {
      if (pages.size() == MAX_PAGES) {
        break;
      }
      pages.add(page == null ? "" : page);
    }

    String text = join(pages, pages.size());
    text = PAGE_PROOF_BANNER.matcher(text).replaceAll(" ");
    text = SOFT_HYPHEN_BREAK.matcher(text).replaceAll("");

    String full = collapse(text);

    Matcher end = SYLLABUS_END.matcher(text);
    String syllabus =
        collapse(end.find() ? text.substring(0, end.start()) : join(pages, FALLBACK_PAGES));
    Result line = syllabusLine(syllabus);
    if (line != null) {
      return line;
    }

    // An application that also granted certiorari and vacated the judgment has a syllabus
    // line and is handled above; a plain grant or denial has none.
    if (APPLICATION_DOCKET.matcher(full.substring(0, Math.min(DOCKET_SEARCH_CHARS, full.length())))
        .find()) {
      return applicationRuling(text);
    }

    if (DISMISSED_AS_IMPROVIDENTLY.matcher(
        full.substring(0, Math.min(DISMISSAL_SEARCH_CHARS, full.length()))).find()) {
      return new Result(Kind.DISMISSED_IMPROVIDENTLY, "Dismissed as improvidently granted");
    }
    return NONE;
  }

  /**
   * The first disposition clause of the syllabus's final disposition sentence, which starts
   * after the last "Pp. 12-15." page range; without one, the last clause found.
   */
  private static Result syllabusLine(String syllabus) {
    List<int[]> clauses = new ArrayList<int[]>();
    List<String> phrases = new ArrayList<String>();
    Matcher m = DISPOSITION.matcher(syllabus);
    while (m.find()) {
      clauses.add(new int[] {m.start(1), m.end(1)});
      phrases.add(m.group(1));
    }
    if (clauses.isEmpty()) {
      return null;
    }
    int lastPageRangeEnd = -1;
    Matcher pp = PAGE_RANGE.matcher(syllabus);
    while (pp.find()) {
      lastPageRangeEnd = pp.end();
    }
    String chosen = phrases.get(phrases.size() - 1);
    if (lastPageRangeEnd >= 0) {
      for (int i = 0; i < clauses.size(); i++) {
        if (clauses.get(i)[0] >= lastPageRangeEnd) {
          chosen = phrases.get(i);
          break;
        }
      }
    }
    boolean dividedVote = chosen.toLowerCase(Locale.ROOT).contains("equally divided");
    return new Result(Kind.SYLLABUS_LINE, normalise(chosen),
        dividedVote ? EQUALLY_DIVIDED_VOTE : null);
  }

  /** The last grant or denial before the order's closing "It is so ordered". */
  private static Result applicationRuling(String text) {
    String flat = collapse(text);
    int ordered = flat.indexOf("It is so ordered");
    String order = ordered < 0 ? flat : flat.substring(0, ordered);
    Matcher m = APPLICATION_RULING.matcher(order);
    String verb = null;
    boolean inPart = false;
    while (m.find()) {
      if (m.group(1) != null) {
        verb = m.group(1);
        inPart = m.group(2) != null;
      } else {
        verb = m.group(3);
        inPart = true;
      }
    }
    if (verb == null) {
      return NONE;
    }
    String label = "Application " + verb.toLowerCase(Locale.ROOT) + (inPart ? " in part" : "");
    return new Result(Kind.APPLICATION, label);
  }

  /** GovInfo MODS's wording for a judgment affirmed in one part and changed in another. */
  private static final String MIXED = "Affirmed and reversed (or vacated) in part";

  /** MODS's label for every affirmance; it has no separate "affirmed and remanded". */
  private static final String AFFIRMED = "Affirmed (includes modified)";

  /**
   * Lower-cases the phrase and repairs the ligature spelling, returning it in GovInfo MODS's
   * vocabulary so that text-derived and MODS-supplied dispositions share one set of values: a
   * judgment affirmed in part and reversed or vacated in part is "Affirmed and reversed (or
   * vacated) in part [and remanded]", and an affirmance, with or without a remand or an equally
   * divided vote, is "Affirmed (includes modified)". That folds a remand into an affirmance, as
   * MODS did for every such case examined; the divided vote is reported as the decision type.
   */
  private static String normalise(String phrase) {
    String s = phrase.toLowerCase(Locale.ROOT).replaceAll("af+i?rmed", "affirmed")
        .replace(" by an equally divided court", "").trim();
    if (s.contains("affirmed") && s.contains("in part")
        && (s.contains("reversed") || s.contains("vacated"))) {
      return s.contains("remanded") ? MIXED + " and remanded" : MIXED;
    }
    if (s.equals("affirmed") || s.equals("affirmed and remanded")) {
      return AFFIRMED;
    }
    return Character.toUpperCase(s.charAt(0)) + s.substring(1);
  }

  private static String join(List<String> pages, int count) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count && i < pages.size(); i++) {
      if (i > 0) {
        sb.append('\n');
      }
      sb.append(pages.get(i));
    }
    return sb.toString();
  }

  private static String collapse(String s) {
    return s.replaceAll(WS, " ").trim();
  }
}
