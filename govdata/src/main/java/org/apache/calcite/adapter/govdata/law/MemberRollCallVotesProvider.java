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
// storage-provider-guard:ignore-file - audited: every filesystem op here targets a local temp
// file from StagingDirectory.newTempFile, never an object-store URI.

import org.apache.calcite.adapter.file.StagingDirectory;
import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.govdata.GovDataException;
import org.apache.calcite.adapter.govdata.ZipDownloadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * DataProvider for {@code member_roll_call_votes}: how each member of the House and Senate voted
 * on each roll call, one row per member per vote.
 *
 * <p>Sources are the chambers' own roll-call XML, no key needed. The House Clerk publishes
 * {@code clerk.house.gov/evs/{year}/roll{NNN}.xml} (1990 onward), numbered from 1 within the
 * calendar year with no index file, so the provider reads rolls in order until the first
 * unpublished one (see {@code downloadHouse}). The
 * Senate publishes {@code senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/
 * vote_{congress}_{session}_{NNNNN}.xml} (101st Congress, 1989 onward) and a per-session menu,
 * {@code roll_call_lists/vote_menu_{congress}_{session}.xml}, that lists the vote numbers; a
 * missing Senate vote file answers with a redirect to an HTML page rather than a 404, so the menu
 * is the only safe way to enumerate. The {@code year} dimension is the calendar year; the
 * Congress and session follow from it.
 *
 * <p>A file that cannot be fetched or parsed throws: the Senate menu names every vote, and the
 * House sequence has no gaps, so a missing roll is a fault to fix, not a vote to skip.
 */
public class MemberRollCallVotesProvider implements DataProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(MemberRollCallVotesProvider.class);

  static final String TABLE = "member_roll_call_votes";

  /** First calendar year of the House Clerk's roll-call XML (verified live 2026-11-15). */
  static final int HOUSE_FIRST_YEAR = 1990;

  /** First calendar year of the Senate's roll-call XML, the 101st Congress (verified live). */
  static final int SENATE_FIRST_YEAR = 1989;

  private static final String HOUSE_SITE = "https://clerk.house.gov/evs";
  private static final String SENATE_SITE = "https://www.senate.gov/legislative/LIS";

  /** The House Clerk prints the month in capitals in its older files ("3-JAN-2001"). */
  private static final DateTimeFormatter HOUSE_DATE =
      new DateTimeFormatterBuilder().parseCaseInsensitive()
          .appendPattern("d-MMM-uuuu").toFormatter(Locale.US);
  private static final DateTimeFormatter SENATE_DATE =
      DateTimeFormatter.ofPattern("MMMM d, uuuu", Locale.US);

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    if (!TABLE.equals(config.getName())) {
      throw new GovDataException("MemberRollCallVotesProvider does not serve table '"
          + config.getName() + "'");
    }
    String yearText = variables.get("year");
    if (yearText == null || yearText.isEmpty()) {
      throw new IOException(TABLE + ": the 'year' dimension is required");
    }
    int year = Integer.parseInt(yearText);
    if (year < SENATE_FIRST_YEAR) {
      throw new IOException(TABLE + ": no roll-call XML before " + SENATE_FIRST_YEAR
          + " (requested " + year + ")");
    }
    int congress = congressOf(year);
    int session = year % 2 == 1 ? 1 : 2;

    List<Integer> senateRolls = fetchSenateRollNumbers(congress, session);
    LOGGER.info("{}: {} Senate menu lists {} roll calls", TABLE, year, senateRolls.size());
    return new RowIterator(year, congress, session, senateRolls);
  }

  /** The Congress sitting in a calendar year: the 1st met in 1789, one per two years. */
  static int congressOf(int year) {
    return (year - 1789) / 2 + 1;
  }

  private static List<Integer> fetchSenateRollNumbers(int congress, int session)
      throws IOException {
    String url = SENATE_SITE + "/roll_call_lists/vote_menu_" + congress + "_" + session + ".xml";
    List<Integer> numbers = new ArrayList<Integer>();
    File xml = StagingDirectory.newTempFile("senate-vote-menu-", ".xml");
    try {
      ZipDownloadUtils.downloadToFile(url, null, xml);
      try (InputStream in = new FileInputStream(xml)) {
        XMLStreamReader r = newReader(in);
        while (r.hasNext()) {
          if (r.next() == XMLStreamConstants.START_ELEMENT
              && "vote_number".equals(r.getLocalName())) {
            numbers.add(Integer.valueOf(r.getElementText().trim()));
          }
        }
      } catch (XMLStreamException e) {
        throw new IOException(url + ": failed to parse the vote menu", e);
      }
    } finally {
      Files.deleteIfExists(xml.toPath());
    }
    return numbers;
  }

  private static XMLStreamReader newReader(InputStream in) throws XMLStreamException {
    XMLInputFactory f = XMLInputFactory.newInstance();
    f.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);
    f.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
    return f.createXMLStreamReader(in);
  }

  /** Reads the Senate votes named by the menu, then the House rolls until the first unpublished one. */
  private static final class RowIterator implements Iterator<Map<String, Object>> {
    private final int year;
    private final int congress;
    private final int session;
    private final Deque<Integer> senateRolls;
    private int nextHouseRoll = 1;
    private boolean houseDone;
    private final Deque<Map<String, Object>> rows = new ArrayDeque<Map<String, Object>>();

    RowIterator(int year, int congress, int session, List<Integer> senateRolls) {
      this.year = year;
      this.congress = congress;
      this.session = session;
      this.senateRolls = new ArrayDeque<Integer>(senateRolls);
      this.houseDone = year < HOUSE_FIRST_YEAR;
    }

    @Override public boolean hasNext() {
      while (rows.isEmpty()) {
        try {
          if (!senateRolls.isEmpty()) {
            readSenate(senateRolls.removeFirst().intValue(), rows);
          } else if (!houseDone) {
            readHouse(nextHouseRoll++, rows);
          } else {
            return false;
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
      return true;
    }

    @Override public Map<String, Object> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return rows.removeFirst();
    }

    private void readSenate(int roll, Deque<Map<String, Object>> out) throws IOException {
      String url = String.format(Locale.ROOT,
          "%s/roll_call_votes/vote%d%d/vote_%d_%d_%05d.xml", SENATE_SITE, congress, session,
          congress, session, roll);
      File xml = StagingDirectory.newTempFile("senate-vote-", ".xml");
      try {
        ZipDownloadUtils.downloadToFile(url, null, xml);
        try (InputStream in = new FileInputStream(xml)) {
          parseSenate(in, url, year, congress, session, roll, out);
        } catch (XMLStreamException e) {
          throw new IOException(url + ": failed to parse", e);
        }
      } finally {
        Files.deleteIfExists(xml.toPath());
      }
    }

    private void readHouse(int roll, Deque<Map<String, Object>> out) throws IOException {
      String url = houseUrl(year, roll);
      File xml = StagingDirectory.newTempFile("house-roll-", ".xml");
      try {
        if (!downloadHouse(url, xml)) {
          // The Clerk answers an unpublished roll with HTTP 200 and an error body (404 for some
          // numbers), so the end of the year is the first unpublished roll with none published
          // after it. A published roll further on means this one is a hole to fix, not an end.
          for (int ahead = 1; ahead <= HOUSE_END_PROBE; ahead++) {
            File probe = StagingDirectory.newTempFile("house-probe-", ".xml");
            try {
              if (downloadHouse(houseUrl(year, roll + ahead), probe)) {
                throw new IOException(url + ": unpublished, but roll " + (roll + ahead)
                    + " is published; a hole in the Clerk's sequence");
              }
            } finally {
              Files.deleteIfExists(probe.toPath());
            }
          }
          houseDone = true;
          LOGGER.info("{}: {} House has {} roll calls", TABLE, year, roll - 1);
          return;
        }
        try (InputStream in = new FileInputStream(xml)) {
          parseHouse(in, url, year, congress, session, roll, out);
        } catch (XMLStreamException e) {
          throw new IOException(url + ": failed to parse", e);
        }
      } finally {
        Files.deleteIfExists(xml.toPath());
      }
    }
  }

  /** How many rolls past the first unpublished one are checked before calling it the end. */
  private static final int HOUSE_END_PROBE = 5;

  private static String houseUrl(int year, int roll) {
    return String.format(Locale.ROOT, "%s/%d/roll%03d.xml", HOUSE_SITE, year, roll);
  }

  /**
   * Downloads a House roll file; false when the Clerk reports it unpublished (404, or the 200
   * "Error sanitizing file" body it serves for a roll that does not exist). The error body also
   * says "try again", so it is re-requested before being believed.
   */
  private static boolean downloadHouse(String url, File dest) throws IOException {
    for (int attempt = 1; attempt <= 3; attempt++) {
      try {
        ZipDownloadUtils.downloadToFile(url, null, dest);
      } catch (FileNotFoundException e) {
        return false;
      }
      byte[] head = new byte[64];
      int n;
      try (InputStream in = new FileInputStream(dest)) {
        n = in.read(head);
      }
      if (n <= 0 || !new String(head, 0, n, java.nio.charset.StandardCharsets.UTF_8)
          .startsWith("<xml>Error sanitizing file")) {
        return true;
      }
      try {
        Thread.sleep(1000L * attempt);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted reading " + url, ie);
      }
    }
    return false;
  }

  /** Parses one Senate roll-call XML into one row per member. */
  static void parseSenate(InputStream in, String url, int year, int congress, int session,
      int roll, Deque<Map<String, Object>> out) throws XMLStreamException, IOException {
    XMLStreamReader r = newReader(in);
    String voteDate = null;
    String question = null;
    String result = null;
    String document = null;
    List<Map<String, Object>> members = new ArrayList<Map<String, Object>>();
    Map<String, Object> member = null;
    List<String> path = new ArrayList<String>();
    while (r.hasNext()) {
      int ev = r.next();
      if (ev == XMLStreamConstants.START_ELEMENT) {
        String name = r.getLocalName();
        path.add(name);
        if ("member".equals(name)) {
          member = new LinkedHashMap<String, Object>();
        } else if (member != null) {
          String text = r.getElementText().trim();
          path.remove(path.size() - 1);
          switch (name) {
          case "last_name": member.put("member_last_name", text); break;
          case "first_name": member.put("member_first_name", text); break;
          case "party": member.put("party", text); break;
          case "state": member.put("state", text); break;
          case "vote_cast": member.put("vote_cast", text); break;
          case "lis_member_id": member.put("lis_member_id", text); break;
          default: break;
          }
        } else if ("vote_date".equals(name) && path.size() == 2) {
          voteDate = r.getElementText().trim();
          path.remove(path.size() - 1);
        } else if ("question".equals(name) && path.size() == 2) {
          question = r.getElementText().trim();
          path.remove(path.size() - 1);
        } else if ("vote_result".equals(name) && path.size() == 2) {
          result = r.getElementText().trim();
          path.remove(path.size() - 1);
        } else if ("document_name".equals(name)) {
          document = r.getElementText().trim();
          path.remove(path.size() - 1);
        }
      } else if (ev == XMLStreamConstants.END_ELEMENT) {
        if ("member".equals(r.getLocalName()) && member != null) {
          members.add(member);
          member = null;
        }
        if (!path.isEmpty()) {
          path.remove(path.size() - 1);
        }
      }
    }
    if (members.isEmpty()) {
      throw new IOException(url + ": no <member> elements; the Senate roll-call XML changed shape");
    }
    if (voteDate == null) {
      throw new IOException(url + ": no <vote_date>");
    }
    String isoDate = isoDate(voteDate.split(",")[0] + "," + voteDate.split(",")[1], SENATE_DATE,
        url);
    for (Map<String, Object> m : members) {
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("year", Integer.valueOf(year));
      row.put("chamber", "Senate");
      row.put("congress", Integer.valueOf(congress));
      row.put("session_number", Integer.valueOf(session));
      row.put("roll_number", Integer.valueOf(roll));
      row.put("vote_date", isoDate);
      putIfSet(row, "vote_question", question);
      putIfSet(row, "vote_result", result);
      putIfSet(row, "legis_num", document);
      row.putAll(m);
      row.put("source_url", url);
      out.addLast(row);
    }
  }

  /** Parses one House roll-call XML into one row per recorded vote. */
  static void parseHouse(InputStream in, String url, int year, int congress, int session,
      int roll, Deque<Map<String, Object>> out) throws XMLStreamException, IOException {
    XMLStreamReader r = newReader(in);
    String voteDate = null;
    String question = null;
    String result = null;
    String legisNum = null;
    String voteDesc = null;
    List<Map<String, Object>> members = new ArrayList<Map<String, Object>>();
    Map<String, Object> member = null;
    while (r.hasNext()) {
      if (r.next() != XMLStreamConstants.START_ELEMENT) {
        continue;
      }
      String name = r.getLocalName();
      switch (name) {
      case "action-date": voteDate = r.getElementText().trim(); break;
      case "vote-question": question = r.getElementText().trim(); break;
      case "vote-result": result = r.getElementText().trim(); break;
      case "legis-num": legisNum = r.getElementText().trim(); break;
      case "vote-desc": voteDesc = r.getElementText().trim(); break;
      case "recorded-vote": member = new LinkedHashMap<String, Object>(); break;
      case "legislator":
        if (member == null) {
          throw new IOException(url + ": <legislator> outside <recorded-vote>");
        }
        putIfSet(member, "bioguide_id", r.getAttributeValue(null, "name-id"));
        putIfSet(member, "party", r.getAttributeValue(null, "party"));
        putIfSet(member, "state", r.getAttributeValue(null, "state"));
        member.put("member_last_name", r.getElementText().trim());
        break;
      case "vote":
        if (member == null) {
          throw new IOException(url + ": <vote> outside <recorded-vote>");
        }
        member.put("vote_cast", r.getElementText().trim());
        members.add(member);
        member = null;
        break;
      default: break;
      }
    }
    if (members.isEmpty() && voteDesc != null && voteDesc.contains("was vacated")) {
      // The Clerk keeps the number of a roll that was vacated by unanimous consent and publishes
      // it with no recorded votes; it records no member's vote, so it contributes no rows.
      return;
    }
    if (members.isEmpty()) {
      throw new IOException(url + ": no <recorded-vote> elements; the House roll-call XML "
          + "changed shape");
    }
    if (voteDate == null) {
      throw new IOException(url + ": no <action-date>");
    }
    String isoDate = isoDate(voteDate, HOUSE_DATE, url);
    for (Map<String, Object> m : members) {
      Map<String, Object> row = new LinkedHashMap<String, Object>();
      row.put("year", Integer.valueOf(year));
      row.put("chamber", "House");
      row.put("congress", Integer.valueOf(congress));
      row.put("session_number", Integer.valueOf(session));
      row.put("roll_number", Integer.valueOf(roll));
      row.put("vote_date", isoDate);
      putIfSet(row, "vote_question", question);
      putIfSet(row, "vote_result", result);
      putIfSet(row, "legis_num", legisNum);
      row.putAll(m);
      row.put("source_url", url);
      out.addLast(row);
    }
  }

  private static void putIfSet(Map<String, Object> row, String key, String value) {
    if (value != null && !value.isEmpty()) {
      row.put(key, value);
    }
  }

  private static String isoDate(String text, DateTimeFormatter format, String url)
      throws IOException {
    try {
      return LocalDate.parse(text.trim(), format).toString();
    } catch (java.time.format.DateTimeParseException e) {
      throw new IOException(url + ": unparseable vote date '" + text + "'", e);
    }
  }
}
