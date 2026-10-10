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

import org.apache.calcite.adapter.file.etl.DataProvider;
import org.apache.calcite.adapter.file.etl.EtlPipelineConfig;
import org.apache.calcite.adapter.govdata.GovDataException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataProvider for the {@code fd_*} tables: the U.S. Senate's financial disclosures from the
 * Senate eFD system (efdsearch.senate.gov) — annual, candidate and new-filer reports (Parts 1-10),
 * periodic transaction reports (PTRs) and due-date-extension notices — for senators, former
 * senators and candidates.
 *
 * <p>The {@code year} dimension is the year a report was <em>filed</em> (the listing's
 * submitted-date window), not the calendar year it covers. One eFD session reads the year's whole
 * listing, then fetches each annual/candidate report and each PTR as HTML once; every table the
 * provider serves is a projection of that one read, which {@link #load(int)} keeps for the next
 * table of the same year.
 *
 * <p>Coverage is bounded by what eFD serves as HTML. Scanned paper filings ({@code
 * /search/view/paper/}) have no text and are excluded by design; their count per year is logged. A
 * report whose view URL redirects to the site root (an expired candidate report) is recorded in
 * {@code fd_filings} with {@code view_status = 'unavailable'} and yields no detail rows.
 *
 * <p>Use of these reports is restricted by 5 U.S.C. § 13107(c) (no credit-rating use, no soliciting
 * money; commercial use only by news and communications media). That is carried by the table
 * comments and the product's terms, not enforced here.
 */
public class SenateFinancialDisclosureProvider implements DataProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SenateFinancialDisclosureProvider.class);

  private static final List<String> STATES = Arrays.asList("AL", "AK", "AZ", "AR", "CA", "CO", "CT",
      "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN",
      "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
      "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY");

  private static final Pattern MDY = Pattern.compile("^(\\d{2})/(\\d{2})/(\\d{4})$");
  private static final Pattern DOLLARS = Pattern.compile("\\$([\\d,]+(?:\\.\\d+)?)");
  private static final Pattern CY = Pattern.compile("\\bCY (\\d{4})");
  private static final Pattern AMENDMENT = Pattern.compile("\\(Amendment(?: (\\d+))?\\)");

  private static final int CACHED_YEARS = 2;

  private static final Map<Integer, YearData> CACHE = new LinkedHashMap<Integer, YearData>();
  private static final SenateEfdSession SESSION = new SenateEfdSession();

  /** One filing with everything read from its view page. */
  static final class Filed {
    final SenateEfdListing.Filing listing;
    /** parsed, unavailable, or extension_notice. */
    final String viewStatus;
    final String calendarYear;
    final Map<String, List<Map<String, String>>> parts;
    final List<Map<String, String>> ptr;

    Filed(SenateEfdListing.Filing listing, String viewStatus, String calendarYear,
        Map<String, List<Map<String, String>>> parts, List<Map<String, String>> ptr) {
      this.listing = listing;
      this.viewStatus = viewStatus;
      this.calendarYear = calendarYear;
      this.parts = parts;
      this.ptr = ptr;
    }
  }

  /** A filing year read from eFD. */
  static final class YearData {
    final List<Filed> filings = new ArrayList<Filed>();
    int paper;
  }

  @Override public Iterator<Map<String, Object>> fetch(EtlPipelineConfig config,
      Map<String, String> variables) throws IOException {
    String table = config.getName();
    String year = variables.get("year");
    if (year == null || year.isEmpty()) {
      throw new IOException(table + ": the 'year' dimension is required");
    }
    YearData data = load(Integer.parseInt(year));
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    for (Filed f : data.filings) {
      rows.addAll(rowsFor(table, f));
    }
    LOGGER.info("{}: {} -> {} rows from {} non-paper filings ({} paper filings excluded)", table,
        year, rows.size(), data.filings.size(), data.paper);
    return rows.iterator();
  }

  private static synchronized YearData load(int year) throws IOException {
    YearData cached = CACHE.get(year);
    if (cached != null) {
      return cached;
    }
    YearData data = read(year);
    CACHE.put(year, data);
    while (CACHE.size() > CACHED_YEARS) {
      CACHE.remove(CACHE.keySet().iterator().next());
    }
    return data;
  }

  private static YearData read(int year) throws IOException {
    List<SenateEfdListing.Filing> listing = SenateEfdListing.forYear(SESSION, year, STATES);
    YearData data = new YearData();
    int unavailable = 0;
    for (SenateEfdListing.Filing f : listing) {
      if (f.kind.equals("paper")) {
        data.paper++;
      } else if (f.kind.startsWith("extension-notice")) {
        data.filings.add(new Filed(f, "extension_notice", null, null, null));
      } else if (f.kind.equals("annual") || f.kind.equals("ptr")) {
        SenateEfdSession.View view = SESSION.getView(f.viewPath());
        if (view.unavailable) {
          unavailable++;
          data.filings.add(new Filed(f, "unavailable", null, null, null));
        } else if (f.kind.equals("ptr")) {
          data.filings.add(new Filed(f, "parsed", null, null,
              SenateEfdReportParser.parsePtr(view.body)));
        } else {
          data.filings.add(new Filed(f, "parsed", SenateEfdReportParser.calendarYear(view.body),
              SenateEfdReportParser.parseAnnual(view.body), null));
        }
      } else {
        throw new GovDataException("eFD " + year + ": unrecognized report kind '" + f.kind
            + "' for filing " + f.uuid + " (" + f.title + ")");
      }
    }
    LOGGER.info("eFD {}: {} listed, {} paper (excluded), {} unavailable (view redirects to the site "
        + "root), {} recorded", year, listing.size(), data.paper, unavailable, data.filings.size());
    return data;
  }

  private static List<Map<String, Object>> rowsFor(String table, Filed f) {
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    if (table.equals("fd_filings")) {
      out.add(filingRow(f));
      return out;
    }
    if (table.equals("fd_transactions")) {
      if (f.ptr != null) {
        add(out, f, "ptr", f.ptr);
      }
      if (f.parts != null) {
        add(out, f, "annual_4b", f.parts.get("4b"));
      }
      return out;
    }
    if (f.parts == null) {
      return out;
    }
    switch (table) {
    case "fd_assets":
      add(out, f, null, f.parts.get("3"));
      break;
    case "fd_income":
      add(out, f, "1", f.parts.get("1"));
      add(out, f, "2", f.parts.get("2"));
      break;
    case "fd_liabilities":
      add(out, f, null, f.parts.get("7"));
      break;
    case "fd_gifts":
      add(out, f, null, f.parts.get("5"));
      break;
    case "fd_travel":
      add(out, f, null, f.parts.get("6"));
      break;
    case "fd_positions":
      add(out, f, null, f.parts.get("8"));
      break;
    case "fd_agreements":
      add(out, f, null, f.parts.get("9"));
      break;
    case "fd_compensation":
      add(out, f, null, f.parts.get("10"));
      break;
    default:
      throw new GovDataException("SenateFinancialDisclosureProvider does not serve table '"
          + table + "'");
    }
    return out;
  }

  /** Appends one row per source row; {@code source} is the part/origin tag where a table mixes. */
  private static void add(List<Map<String, Object>> out, Filed f, String source,
      List<Map<String, String>> rows) {
    if (rows == null) {
      return;
    }
    int position = 0;
    for (Map<String, String> r : rows) {
      position++;
      Map<String, Object> row = base(f);
      boolean isTransaction = r.containsKey("Transaction Date");
      if (isTransaction) {
        transaction(row, source, r);
      } else if (r.containsKey("Asset")) {
        asset(row, r);
      } else if (source != null && (source.equals("1") || source.equals("2"))) {
        income(row, source, r);
      } else {
        detail(row, r);
      }
      row.put("row_seq", Integer.valueOf(position));
      out.add(row);
    }
  }

  private static Map<String, Object> base(Filed f) {
    SenateEfdListing.Filing l = f.listing;
    Map<String, Object> row = new LinkedHashMap<String, Object>();
    row.put("chamber", "senate");
    row.put("filing_id", l.uuid);
    row.put("filer_first_name", l.firstName);
    row.put("filer_last_name", l.lastName);
    row.put("report_title", l.title);
    row.put("filing_date", iso(l.filedDate));
    if (f.calendarYear != null) {
      row.put("calendar_year", Integer.valueOf(f.calendarYear));
    }
    row.put("filing_url", SenateEfdSession.SITE + l.viewPath());
    return row;
  }

  private static Map<String, Object> filingRow(Filed f) {
    SenateEfdListing.Filing l = f.listing;
    Map<String, Object> row = base(f);
    row.remove("calendar_year");
    row.put("filer_type", l.filerType);
    if (l.state != null) {
      row.put("state", l.state);
    }
    row.put("report_kind", l.kind.startsWith("extension-notice") ? "extension_notice" : l.kind);
    row.put("report_type", reportType(l.title));
    Matcher cy = CY.matcher(l.title);
    if (cy.find()) {
      row.put("calendar_year", Integer.valueOf(cy.group(1)));
    } else if (f.calendarYear != null) {
      row.put("calendar_year", Integer.valueOf(f.calendarYear));
    }
    Matcher am = AMENDMENT.matcher(l.title);
    boolean amended = am.find();
    row.put("is_amendment", Boolean.valueOf(amended));
    if (amended && am.group(1) != null) {
      row.put("amendment_number", Integer.valueOf(am.group(1)));
    }
    row.put("view_status", f.viewStatus);
    return row;
  }

  private static String reportType(String title) {
    if (title.contains("Due Date Extension")) {
      return "Due Date Extension";
    }
    String t = AMENDMENT.matcher(title).replaceAll("").trim();
    int cut = t.indexOf(" for ");
    return (cut > 0 ? t.substring(0, cut) : t).trim();
  }

  private static void asset(Map<String, Object> row, Map<String, String> r) {
    String no = r.get("#");
    row.put("item_no", no);
    int dot = no.indexOf('.');
    if (dot > 0) {
      row.put("parent_item_no", no.substring(0, dot));
    }
    row.put("asset_name", r.get("Asset"));
    putText(row, "asset_detail", r.get("Asset detail"));
    row.put("asset_type", r.get("Asset Type"));
    putText(row, "asset_type_detail", r.get("Asset Type detail"));
    row.put("owner", r.get("Owner"));
    amount(row, "value", r.get("Value"));
    putText(row, "income_type", r.get("Income Type"));
    amount(row, "income", r.get("Income"));
  }

  private static void income(Map<String, Object> row, String part, Map<String, String> r) {
    row.put("source_part", part);
    row.put("item_no", r.get("#"));
    if (part.equals("1")) {
      putText(row, "activity_date", r.get("Date"));
      row.put("income_type", r.get("Activity"));
      putText(row, "payer", r.get("Who Paid?"));
      putText(row, "recipient", r.get("Who received payment?"));
      amount(row, "amount", r.get("Amount"));
    } else {
      putText(row, "recipient", r.get("Who Was Paid"));
      row.put("income_type", r.get("Type"));
      putText(row, "payer", r.get("Who Paid"));
      amount(row, "amount", r.get("Amount Paid"));
    }
    putText(row, "comments", r.get("Comments"));
  }

  private static void transaction(Map<String, Object> row, String source,
      Map<String, String> r) {
    row.put("source_part", source);
    row.put("item_no", r.get("#"));
    String owner = r.get("Owner");
    row.put("owner", owner);
    String code = ownerCode(owner);
    if (code != null) {
      row.put("owner_code", code);
    }
    putText(row, "ticker", r.get("Ticker"));
    row.put("asset_name", r.get("Asset Name"));
    putText(row, "asset_type", r.get("Asset Type"));
    row.put("transaction_type",
        r.containsKey("Transaction Type") ? r.get("Transaction Type") : r.get("Type"));
    row.put("transaction_date", iso(r.get("Transaction Date")));
    amount(row, "amount", r.get("Amount"));
    putText(row, "comment", r.containsKey("Comments") ? r.get("Comments") : r.get("Comment"));
  }

  /** Remaining parts: map each header to a snake_case column by the table's own header text. */
  private static void detail(Map<String, Object> row, Map<String, String> r) {
    row.put("item_no", r.get("#"));
    for (Map.Entry<String, String> e : r.entrySet()) {
      String col = DETAIL_COLUMNS.get(e.getKey());
      if (col == null) {
        continue;
      }
      if (col.equals("value") || col.equals("amount")) {
        amount(row, col, e.getValue());
      } else {
        putText(row, col, e.getValue());
      }
    }
  }

  private static final Map<String, String> DETAIL_COLUMNS = new LinkedHashMap<String, String>();

  static {
    // Part 5 gifts
    DETAIL_COLUMNS.put("Date", "event_date");
    DETAIL_COLUMNS.put("Recipient", "recipient");
    DETAIL_COLUMNS.put("Gift", "gift");
    DETAIL_COLUMNS.put("Value", "value");
    DETAIL_COLUMNS.put("From", "gift_from");
    // Part 6 travel
    DETAIL_COLUMNS.put("Date(s)", "event_date");
    DETAIL_COLUMNS.put("Traveler(s)", "travelers");
    DETAIL_COLUMNS.put("Travel Type", "travel_type");
    DETAIL_COLUMNS.put("Itinerary", "itinerary");
    DETAIL_COLUMNS.put("Reimbursed For", "reimbursed_for");
    DETAIL_COLUMNS.put("Who Paid", "who_paid");
    // Part 7 liabilities
    DETAIL_COLUMNS.put("Incurred", "incurred");
    DETAIL_COLUMNS.put("Debtor", "debtor");
    DETAIL_COLUMNS.put("Type", "item_type");
    DETAIL_COLUMNS.put("Points", "points");
    DETAIL_COLUMNS.put("Rate (Term)", "rate_term");
    DETAIL_COLUMNS.put("Amount", "amount");
    DETAIL_COLUMNS.put("Creditor", "creditor");
    // Part 8 positions
    DETAIL_COLUMNS.put("Position Dates", "position_dates");
    DETAIL_COLUMNS.put("Position Held", "position_held");
    DETAIL_COLUMNS.put("Entity", "entity");
    DETAIL_COLUMNS.put("Entity Type", "entity_type");
    // Part 9 agreements
    DETAIL_COLUMNS.put("Parties Involved", "parties");
    DETAIL_COLUMNS.put("Status and Terms", "status_and_terms");
    // Part 10 compensation
    DETAIL_COLUMNS.put("Source", "source");
    DETAIL_COLUMNS.put("Duties", "duties");
    DETAIL_COLUMNS.put("Comments", "comments");
  }

  private static String ownerCode(String owner) {
    switch (owner) {
    case "Self":
      return null;
    case "Spouse":
      return "SP";
    case "Joint":
      return "JT";
    case "Child":
      return "DC";
    default:
      throw new GovDataException("eFD transaction owner '" + owner + "' is not Self, Spouse, "
          + "Joint or Child");
    }
  }

  private static void putText(Map<String, Object> row, String col, String text) {
    if (text != null && !text.isEmpty()) {
      row.put(col, text);
    }
  }

  /** Puts {@code <prefix>_range} (the text as printed) plus {@code _min}/{@code _max} dollars. */
  private static void amount(Map<String, Object> row, String prefix, String text) {
    if (text == null || text.isEmpty()) {
      return;
    }
    row.put(prefix + "_range", text);
    Long[] bounds = bounds(text);
    if (bounds[0] != null) {
      row.put(prefix + "_min", bounds[0]);
    }
    if (bounds[1] != null) {
      row.put(prefix + "_max", bounds[1]);
    }
  }

  /**
   * Dollar bounds of an amount as eFD prints it: {@code "$1,001 - $15,000"} is a bracket,
   * {@code "Over $5,000,000"} and {@code "> $1,000"} are open-ended above, {@code "$2,148.46"} is
   * one exact figure (rounded to whole dollars), and {@code "None (or less than $201)"} states no
   * figure and has no bounds. A bracket followed by {@code " Other $N"} is bounded by the bracket.
   */
  static Long[] bounds(String text) {
    // An income cell can carry a bracket and then an exact "Other $12,460.00" figure; the bounds
    // are the bracket's, and the range text keeps both as printed.
    int other = text.indexOf(" Other ");
    Matcher m = DOLLARS.matcher(other > 0 ? text.substring(0, other) : text);
    List<Long> n = new ArrayList<Long>();
    while (m.find()) {
      n.add(new BigDecimal(m.group(1).replace(",", "")).setScale(0, RoundingMode.HALF_UP)
          .longValueExact());
    }
    String t = text.trim();
    if (t.startsWith("None") || t.contains("less than") || n.isEmpty()) {
      return new Long[] {null, null};
    }
    if (n.size() == 2) {
      return new Long[] {n.get(0), n.get(1)};
    }
    if (n.size() == 1 && (t.startsWith("Over") || t.startsWith(">"))) {
      return new Long[] {n.get(0), null};
    }
    if (n.size() == 1) {
      return new Long[] {n.get(0), n.get(0)};
    }
    throw new GovDataException("eFD amount '" + text + "' has " + n.size() + " dollar figures");
  }

  /** {@code "09/12/2025"} to {@code "2025-09-12"}. */
  static String iso(String mdy) {
    Matcher m = MDY.matcher(mdy);
    if (!m.matches()) {
      throw new GovDataException("eFD date '" + mdy + "' is not MM/DD/YYYY");
    }
    return m.group(3) + "-" + m.group(1) + "-" + m.group(2);
  }
}
