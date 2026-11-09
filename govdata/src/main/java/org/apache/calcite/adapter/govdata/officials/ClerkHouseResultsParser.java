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

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads the U.S. House general-election returns out of the Clerk of the House's "Statistics of the
 * Presidential and Congressional Election" PDF (public domain, one volume per election, published
 * the spring after it). It is the official 2024 source: the MIT Election Lab's 1976-2024 file is
 * behind a Dataverse guestbook and the FEC has published no 2024 House results workbook.
 *
 * <p>The PDF lists, per state, one line per candidate party line: {@code 1. Nick LaLota,
 * Republican ..... 200,802}. The number prefix opens a district; a line with only party labels
 * (Conservative, Common Sense) is another party line of the candidate above it. Parsing is strict:
 * a line of an unknown shape, a party label that is not in the vocabulary below, a state whose
 * districts are not 1..N for its 2020-census seat count, a seat total other than 435, or a
 * party split of the winners other than the release's known one fails the fetch.
 *
 * <p>Row conventions follow {@link FecHouseResultsParser}: one row per candidate per party line,
 * stage gen, party_major D/R/OTHER, write-in lumps as candidate_kind write_in_aggregate, and
 * total_votes the sum of the rows kept. Blank, void, over and under votes are vote buckets, not
 * votes for anyone, and are not loaded (the FEC series has none either).
 */
final class ClerkHouseResultsParser {

  static final String TABLE = "house_election_results";
  static final String SOURCE = "CLERK";

  /** One pinned release of the Clerk's statistics volume. */
  static final class Release {
    final int year;
    final String url;
    final String publishedText;
    /** Number of districts won by a Democratic and by a Republican line, from the result itself. */
    final int democraticSeats;
    final int republicanSeats;
    final String description;

    Release(int year, String url, String publishedText, int democraticSeats,
        int republicanSeats) {
      this.year = year;
      this.url = url;
      this.publishedText = publishedText;
      this.democraticSeats = democraticSeats;
      this.republicanSeats = republicanSeats;
      this.description = "Clerk of the House, Statistics of the Presidential and Congressional "
          + "Election of November 5, " + year + ", published " + publishedText + ", " + url;
    }
  }

  static final Release[] RELEASES = {
      new Release(2024,
          "https://clerk.house.gov/member_info/electionInfo/2024/statistics2024.pdf",
          "March 10, 2025", 215, 220)};

  /** Seats per state after the 2020 census (435 in all). */
  private static final Map<String, Integer> SEATS = new HashMap<String, Integer>();

  static {
    String[] pairs = {"AL7", "AK1", "AZ9", "AR4", "CA52", "CO8", "CT5", "DE1", "FL28", "GA14",
        "HI2", "ID2", "IL17", "IN9", "IA4", "KS4", "KY6", "LA6", "ME2", "MD8", "MA9", "MI13",
        "MN8", "MS4", "MO8", "MT2", "NE3", "NV4", "NH2", "NJ12", "NM3", "NY26", "NC14", "ND1",
        "OH15", "OK5", "OR6", "PA17", "RI2", "SC7", "SD1", "TN9", "TX38", "UT4", "VT1", "VA11",
        "WA10", "WV2", "WI8", "WY1"};
    for (String p : pairs) {
      SEATS.put(p.substring(0, 2), Integer.valueOf(p.substring(2)));
    }
  }

  /** Headings of the delegate jurisdictions, whose sections are not loaded. */
  private static final Set<String> NON_STATE_HEADINGS = new HashSet<String>(Arrays.asList(
      "DISTRICT OF COLUMBIA", "PUERTO RICO", "AMERICAN SAMOA", "GUAM", "NORTHERN MARIANA ISLANDS",
      "VIRGIN ISLANDS", "U.S. VIRGIN ISLANDS", "UNITED STATES VIRGIN ISLANDS"));

  /** Party labels the volume prints on House lines. A label not in this set fails the fetch. */
  private static final Set<String> PARTIES = new HashSet<String>(Arrays.asList(
      "Republican", "Democrat", "Democratic-Farmer-Labor", "Democratic-Nonpartisan League",
      "Libertarian", "Independent", "Green", "Conservative", "Working Families", "Constitution",
      "Working Class", "Unity Party", "Approval Voting Party", "Unaffiliated", "U.S. Taxpayers",
      "No Political Party", "Independent American", "Common Sense", "Pacific Green",
      "No Party Affiliation", "No Party", "Alliance", "Independence", "No Party Preference",
      "American Constitution", "Forward Party", "Libertarian Party of Florida", "Nonpartisan",
      "Unenrolled Independent", "Constitutional Conservative", "Better Party", "No Slogan",
      "Why Not Wynn", "Join The Revolution", "Peace Freedom Liberty", "Common Sense Independent",
      "Labour", "Socialist Workers", "C4C 2024", "All Of Us!", "Social Activist",
      "Truth Freedom Prosperity", "Common Sense Suffolk", "Truth", "LaRouche", "United Citizens",
      "United Utah", "Green Mountain Peace and Justice"));

  private static final Set<String> DEMOCRATIC = new HashSet<String>(Arrays.asList(
      "Democrat", "Democratic-Farmer-Labor", "Democratic-Nonpartisan League"));

  /** Lines that are not votes for anyone (or, for the ranked-choice tabulation, are its totals). */
  private static final Set<String> RESIDUAL = new HashSet<String>(Arrays.asList(
      "Blank", "Blanks", "Blank Votes", "Void", "Over Votes", "Under Votes", "Continuing Ballots",
      "Exhausted Ballots"));

  /** Lines that lump write-ins together. */
  private static final Set<String> AGGREGATE = new HashSet<String>(Arrays.asList(
      "Scattering", "All Others", "Other Write-ins", "Miscellaneous"));

  /** Districts whose published counts are first-choice counts of a ranked-choice contest. */
  private static final Set<String> FIRST_CHOICE_ONLY =
      new HashSet<String>(Arrays.asList("AK-0"));

  /** The running header of the volume's first page. */
  private static final String MASTHEAD = "https://clerk.house";

  private static final Pattern VALUE_LINE =
      Pattern.compile("^(?:(\\d{1,2})\\.\\s+)?(.+?)\\s*\\.{3,}\\s*([\\d,]+|\\(1\\))$");
  /** The footnote that explains a "(1)" in place of a vote count. */
  private static final Pattern NO_OPPOSITION_NOTE = Pattern.compile(
      "^\\d?According to .+ law, the names of those with no opposition are not printed on the "
          + "ballot\\.$");
  private static final Pattern WRITE_IN = Pattern.compile("^Write-in(?: \\(.*\\))?$");
  private static final Pattern PAGE_NUMBER = Pattern.compile("^\\d{1,3}$");
  private static final Pattern CONTINUED = Pattern.compile("^(.+?)—Continued$");

  private ClerkHouseResultsParser() { }

  /** Extracts the text of the PDF and parses it. */
  static List<Map<String, Object>> parsePdf(byte[] pdf, Release rel) throws IOException {
    String text;
    try (PDDocument doc = PDDocument.load(new ByteArrayInputStream(pdf))) {
      PDFTextStripper stripper = new PDFTextStripper();
      stripper.setSortByPosition(true);
      text = stripper.getText(doc);
    }
    return parse(text, rel);
  }

  /** One party line of a candidate in a district. */
  private static final class Line {
    final int lineNo;
    final String candidate;
    final String party;
    final boolean writeIn;
    final boolean aggregate;
    final long votes;

    Line(int lineNo, String candidate, String party, boolean writeIn, boolean aggregate,
        long votes) {
      this.lineNo = lineNo;
      this.candidate = candidate;
      this.party = party;
      this.writeIn = writeIn;
      this.aggregate = aggregate;
      this.votes = votes;
    }
  }

  /** One district of one state while it is being read. */
  private static final class District {
    final int number;
    final int firstLine;
    final List<Line> lines = new ArrayList<Line>();
    Long continuingBallots;
    /** True when the volume prints "(1)" for the sole candidate instead of a vote count. */
    boolean unopposed;

    District(int number, int firstLine) {
      this.number = number;
      this.firstLine = firstLine;
    }
  }

  private static IOException fail(Release rel, int line, String msg) {
    return new IOException(TABLE + " " + rel.year + " Clerk PDF line " + line + ": " + msg
        + "; the volume changed and the parser must be reviewed");
  }

  /** Parses the extracted text of the volume into rows. */
  static List<Map<String, Object>> parse(String text, Release rel) throws IOException {
    Map<String, String[]> byName = new HashMap<String, String[]>();
    for (String[] s : FecHouseResultsParser.STATES) {
      byName.put(s[2].toUpperCase(Locale.ROOT), s);
    }
    Map<String, List<District>> byState = new LinkedHashMap<String, List<District>>();
    String[] lines = text.split("\\r?\\n");
    String[] state = null;
    boolean inHouse = false;
    District district = null;
    boolean atLarge = false;
    Set<String> noOppositionNote = new HashSet<String>();
    int n = 0;
    for (String raw : lines) {
      n++;
      String s = raw.replace(' ', ' ').trim();
      if (s.isEmpty() || PAGE_NUMBER.matcher(s).matches() || s.startsWith(MASTHEAD)) {
        continue;
      }
      String head = s;
      Matcher cont = CONTINUED.matcher(s);
      if (cont.matches()) {
        head = cont.group(1).trim();
      }
      if (byName.containsKey(head) || NON_STATE_HEADINGS.contains(head)) {
        if (!cont.matches()) {
          district = null;
        }
        state = byName.get(head);
        inHouse = false;
        atLarge = false;
        if (state != null && !byState.containsKey(state[0])) {
          byState.put(state[0], new ArrayList<District>());
        }
        continue;
      }
      if (head.startsWith("FOR ")) {
        inHouse = head.equals("FOR UNITED STATES REPRESENTATIVE");
        atLarge = false;
        if (!cont.matches()) {
          district = null;
        }
        continue;
      }
      if (s.startsWith("Recapitulation of Votes Cast")) {
        inHouse = false;
        continue;
      }
      if (state != null && NO_OPPOSITION_NOTE.matcher(s).matches()) {
        noOppositionNote.add(state[0]);
        continue;
      }
      if (!inHouse || state == null) {
        continue;
      }
      if (s.equals("AT LARGE")) {
        atLarge = true;
        continue;
      }
      Matcher m = VALUE_LINE.matcher(s);
      if (!m.matches()) {
        throw fail(rel, n, "unrecognised line in " + state[2] + "'s representative section: '"
            + s + "'");
      }
      boolean unopposed = m.group(3).equals("(1)");
      // The Lab's marker for an unopposed race: one vote of a total of one.
      long votes = unopposed ? 1L : Long.parseLong(m.group(3).replace(",", ""));
      String body = m.group(2).trim();
      List<District> list = byState.get(state[0]);
      if (m.group(1) != null) {
        district = new District(Integer.parseInt(m.group(1)), n);
        list.add(district);
      } else if (atLarge && district == null) {
        district = new District(0, n);
        list.add(district);
      }
      if (district == null) {
        throw fail(rel, n, "a line before the first district of " + state[2] + ": '" + s + "'");
      }
      if (unopposed) {
        if (m.group(1) == null) {
          throw fail(rel, n, "'(1)' on a line that does not open a district: '" + s + "'");
        }
        district.unopposed = true;
      }
      addLine(rel, n, district, body, votes);
    }
    for (Map.Entry<String, List<District>> e : byState.entrySet()) {
      for (District d : e.getValue()) {
        if (d.unopposed && (!noOppositionNote.contains(e.getKey()) || d.lines.size() != 1)) {
          throw fail(rel, d.firstLine, e.getKey() + " district " + d.number
              + " has no vote count but is not a sole candidate with the no-opposition note");
        }
      }
    }
    return finish(rel, byState);
  }

  private static void addLine(Release rel, int n, District d, String body, long votes)
      throws IOException {
    int next = d.lines.size() + 1;
    if (body.equals("Continuing Ballots")) {
      d.continuingBallots = Long.valueOf(votes);
      return;
    }
    if (RESIDUAL.contains(body)) {
      return;
    }
    if (AGGREGATE.contains(body) || WRITE_IN.matcher(body).matches()) {
      d.lines.add(new Line(next, null, null, true, true, votes));
      return;
    }
    // A line made only of party labels is another party line of the candidate above it.
    if (onlyParties(body)) {
      Line prev = d.lines.isEmpty() ? null : d.lines.get(d.lines.size() - 1);
      if (prev == null || prev.aggregate) {
        throw fail(rel, n, "party line '" + body + "' has no candidate above it");
      }
      d.lines.add(new Line(next, prev.candidate, body, prev.writeIn, false, votes));
      return;
    }
    int cut = body.lastIndexOf(", ");
    String name = null;
    String party = null;
    // Longest known label that ends the line, so a label with a comma is not split.
    for (String label : PARTIES) {
      if (body.endsWith(", " + label)
          && (party == null || label.length() > party.length())) {
        party = label;
        name = body.substring(0, body.length() - label.length() - 2);
      }
    }
    boolean writeIn = false;
    if (party == null && cut > 0) {
      String tail = body.substring(cut + 2);
      if (WRITE_IN.matcher(tail).matches()) {
        party = tail;
        name = body.substring(0, cut);
        writeIn = true;
      }
    }
    if (party == null || name == null || name.isEmpty()) {
      throw fail(rel, n, "'" + body + "' is neither a candidate with a known party label nor a "
          + "known bucket");
    }
    d.lines.add(new Line(next, name, party, writeIn, false, votes));
  }

  private static boolean onlyParties(String body) {
    for (String part : body.split(", ")) {
      if (!PARTIES.contains(part)) {
        return false;
      }
    }
    return true;
  }

  private static String major(Line l) {
    if (l.writeIn) {
      return "OTHER";
    }
    if ("Republican".equals(l.party)) {
      return "R";
    }
    return DEMOCRATIC.contains(l.party) ? "D" : "OTHER";
  }

  private static List<Map<String, Object>> finish(Release rel, Map<String, List<District>> byState)
      throws IOException {
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    int seats = 0;
    int democratic = 0;
    int republican = 0;
    for (String[] st : FecHouseResultsParser.STATES) {
      List<District> districts = byState.get(st[0]);
      int expected = SEATS.get(st[0]).intValue();
      if (districts == null || districts.isEmpty()) {
        throw new IOException(TABLE + " " + rel.year + " Clerk PDF: no representative section "
            + "for " + st[2]);
      }
      Set<Integer> numbers = new TreeSet<Integer>();
      for (District d : districts) {
        if (!numbers.add(Integer.valueOf(d.number))) {
          throw fail(rel, d.firstLine, st[2] + " district " + d.number + " appears twice");
        }
      }
      Set<Integer> want = new TreeSet<Integer>();
      for (int i = expected == 1 ? 0 : 1; i <= (expected == 1 ? 0 : expected); i++) {
        want.add(Integer.valueOf(i));
      }
      if (!numbers.equals(want)) {
        throw new IOException(TABLE + " " + rel.year + " Clerk PDF: " + st[2] + " has districts "
            + numbers + ", expected " + want);
      }
      seats += districts.size();
      for (District d : districts) {
        String winner = winner(rel, st, d);
        if ("D".equals(winner)) {
          democratic++;
        } else if ("R".equals(winner)) {
          republican++;
        }
        emit(rel, st, d, out);
      }
    }
    if (seats != 435) {
      throw new IOException(TABLE + " " + rel.year + " Clerk PDF: " + seats + " seats, expected "
          + "435");
    }
    if (democratic != rel.democraticSeats || republican != rel.republicanSeats) {
      throw new IOException(TABLE + " " + rel.year + " Clerk PDF: leaders are " + democratic
          + " Democratic and " + republican + " Republican districts, expected "
          + rel.democraticSeats + " and " + rel.republicanSeats);
    }
    return out;
  }

  /** D, R or OTHER for the candidate with the most votes (all party lines summed). */
  private static String winner(Release rel, String[] st, District d) throws IOException {
    Map<String, Long> sum = new LinkedHashMap<String, Long>();
    Map<String, String> major = new HashMap<String, String>();
    for (Line l : d.lines) {
      if (l.aggregate) {
        continue;
      }
      Long prev = sum.get(l.candidate);
      sum.put(l.candidate, Long.valueOf((prev == null ? 0L : prev.longValue()) + l.votes));
      String lm = major(l);
      String cur = major.get(l.candidate);
      if (cur == null || "OTHER".equals(cur)) {
        major.put(l.candidate, lm);
      }
    }
    String leader = null;
    long best = -1;
    boolean tie = false;
    for (Map.Entry<String, Long> e : sum.entrySet()) {
      if (e.getValue().longValue() > best) {
        best = e.getValue().longValue();
        leader = e.getKey();
        tie = false;
      } else if (e.getValue().longValue() == best) {
        tie = true;
      }
    }
    if (leader == null || tie) {
      throw fail(rel, d.firstLine, st[2] + " district " + d.number + " has "
          + (leader == null ? "no candidate" : "a tied leader"));
    }
    return major.get(leader);
  }

  private static void emit(Release rel, String[] st, District d, List<Map<String, Object>> out)
      throws IOException {
    long total = 0;
    for (Line l : d.lines) {
      total += l.votes;
    }
    boolean ranked = d.continuingBallots != null
        || FIRST_CHOICE_ONLY.contains(st[0] + "-" + d.number);
    if (d.continuingBallots != null && d.continuingBallots.longValue() != total) {
      throw fail(rel, d.firstLine, st[2] + " district " + d.number + " rows sum to " + total
          + " but the continuing-ballot total is " + d.continuingBallots);
    }
    String cd = st[1] + String.format(Locale.ROOT, "%02d", Integer.valueOf(d.number));
    for (Line l : d.lines) {
      Map<String, Object> m = new LinkedHashMap<String, Object>();
      m.put("year", Integer.valueOf(rel.year));
      m.put("state_fips", st[1]);
      m.put("state_name", st[2]);
      m.put("state_postal", st[0]);
      m.put("district", Integer.valueOf(d.number));
      m.put("at_large", Boolean.valueOf(d.number == 0));
      m.put("cd_fips", cd);
      m.put("special", Boolean.FALSE);
      m.put("runoff", Boolean.FALSE);
      m.put("stage", "gen");
      m.put("line_no", Integer.valueOf(l.lineNo));
      m.put("candidate", l.candidate);
      m.put("candidate_kind", l.aggregate ? "write_in_aggregate" : "candidate");
      m.put("party_published", l.party == null ? null : l.party.toLowerCase(Locale.ROOT));
      m.put("party_major", l.aggregate ? null : major(l));
      m.put("writein", Boolean.valueOf(l.writeIn));
      m.put("candidate_votes", Long.valueOf(l.votes));
      m.put("total_votes", Long.valueOf(total));
      m.put("uncontested", Boolean.valueOf(d.unopposed));
      m.put("unofficial", Boolean.FALSE);
      m.put("version_date", null);
      m.put("source_release", rel.description);
      m.put("source", SOURCE);
      m.put("ranked_choice", Boolean.valueOf(ranked));
      m.put("first_choice_votes",
          FIRST_CHOICE_ONLY.contains(st[0] + "-" + d.number) ? Long.valueOf(l.votes) : null);
      out.add(m);
    }
  }
}
