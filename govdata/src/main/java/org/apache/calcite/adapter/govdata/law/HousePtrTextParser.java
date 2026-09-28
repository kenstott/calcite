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
 * Parses the transaction table out of a House Periodic Transaction Report (PTR), the STOCK Act
 * filing type {@code "P"} in {@link HouseFinancialDisclosureIndex}.
 *
 * <p>The PDF is machine-generated from a fixed template, but its label text (column headers,
 * "Filing Status", "Description") is set in a font whose glyphs PDFBox/PDF text extraction reads
 * as control characters or drops outright — only the filer's own data survives extraction
 * cleanly. So rather than parsing labels, this reads the one part of each row that is always
 * clean: the transaction-type/date/date/amount line, which is generated as plain text and always
 * immediately follows the asset's {@code [TYPE]} code with no other row's data between them.
 * Everything between the end of one row's asset-type bracket and the start of the next is that
 * row's asset name and ticker (after stripping an owner code); a matching {@code : New} or
 * {@code : Amended} in the short span right after the amount is that row's filing status. Any
 * further free-text notes (the source's "Location"/"Description"/"Comment" lines, and, across a
 * page break, repeated header/footer boilerplate) are not extracted — a page break lands inside
 * that free text on a multi-page filing, so no delimiter reliably ends it.
 */
final class HousePtrTextParser {

  /** One transaction row, in document order. */
  static final class Row {
    final String ownerCode;
    final String assetName;
    final String ticker;
    final String assetTypeCode;
    final String transactionType;
    final String transactionDate;
    final String notificationDate;
    final String amountRange;
    final Long amountMin;
    final Long amountMax;
    final String filingStatus;

    Row(String ownerCode, String assetName, String ticker, String assetTypeCode,
        String transactionType, String transactionDate, String notificationDate,
        String amountRange, Long amountMin, Long amountMax, String filingStatus) {
      this.ownerCode = ownerCode;
      this.assetName = assetName;
      this.ticker = ticker;
      this.assetTypeCode = assetTypeCode;
      this.transactionType = transactionType;
      this.transactionDate = transactionDate;
      this.notificationDate = notificationDate;
      this.amountRange = amountRange;
      this.amountMin = amountMin;
      this.amountMax = amountMax;
      this.filingStatus = filingStatus;
    }
  }

  /** The table header's last cell; the transaction table body starts right after it. */
  private static final String TABLE_HEADER_END = "$200?";

  /**
   * {@code [ST] S (partial) 07/28/202508/11/2025$1,001 - $15,000}: the asset-type bracket,
   * transaction type, two MM/DD/YYYY dates and an amount range, with no guaranteed whitespace
   * between the dates or between the second date and the amount — the source emits them back to
   * back with no separator when the layout has no room for one.
   */
  // Between the second date and the amount, a joint holding over the top disclosure bracket
  // carries an extra annotation token ("Spouse/DC Over $1,000,000", confirmed live 2026-09-27,
  // filing 20033695) that plain \s* does not allow for; (?:\S+\s+){0,3}? tolerates up to three
  // such tokens without letting the match run away into an unrelated later amount. The
  // quantifier must be LAZY: a greedy {0,3} backtracks from 3 tokens down, and since the
  // amount alternation's last branch (a bare $N with no bracket) can match just the tail of a
  // real "$1,001 - $15,000" bracket, greedy backtracking finds that shorter match first and
  // silently drops the "$1,001 - " lower bound from every ordinary bracket, not just the
  // annotated case (confirmed live 2026-09-27 against a real filing's plain bracket).
  //
  // The bracket's first letter is lower-cased regardless of which letter it is ("[gS]" for GS,
  // "[sT]" for ST, "[oI]" for OI — confirmed live 2026-09-27, filings 20020238/20016125/20012722):
  // the source's font substitution table mis-maps that glyph position, not any specific letter.
  // [A-Za-z] tolerates it; the captured code is upper-cased when the row is built.
  private static final Pattern ANCHOR = Pattern.compile(
      "\\[([A-Za-z]{1,4})\\]\\s*(P|E|S\\s*\\(partial\\)|S)\\s+(\\d{2}/\\d{2}/\\d{4})\\s*"
      + "(\\d{2}/\\d{2}/\\d{4})\\s*(?:\\S+\\s+){0,3}?(Over\\s*\\$[\\d,]+|\\$[\\d,]+\\s*-\\s*\\$[\\d,]+"
      + "|\\$[\\d,]+(?:\\.\\d{2})?)");

  private static final Pattern OWNER_PREFIX = Pattern.compile("^(SP|JT|DC)\\s+(.*)$", Pattern.DOTALL);

  /**
   * Finds an owner code anywhere in the leading span, not just at its start. A row's own SP/JT/DC
   * marker is a reliable anchor: clipping to its LAST occurrence discards any preceding free text
   * unconditionally, including a previous row's subholding/description note whose own
   * sentence-ending period happens to be a {@link #PROTECTED_ABBREVIATION} (e.g. "R.W. Allen &amp;
   * Associates, Inc." immediately followed by "SP Albemarle Corporation..." — confirmed live
   * 2026-09-27, filing 20024277 — {@link #stripLeadingFreeText} alone leaves "Inc." unsplit and
   * the note attached to the next row's asset name).
   */
  private static final Pattern OWNER_CODE_TOKEN = Pattern.compile("\\b(?:SP|JT|DC)\\s");

  private static final Pattern TRAILING_TICKER =
      Pattern.compile("^(.*)\\(([A-Z0-9.\\-]{1,8})\\)\\s*$", Pattern.DOTALL);

  private static final Pattern FILING_STATUS = Pattern.compile(":\\s*(New|Amended)\\b");

  /** How far past an anchor's end to look for its filing-status marker. */
  private static final int FILING_STATUS_WINDOW = 200;

  /**
   * Corporate suffixes that end in a period without ending a sentence, so a free-text note
   * ("Sold 31,600 shares. SP NVIDIA...") isn't confused with an asset name that legitimately
   * contains one ("Alphabet Inc. - Class A Common Stock"). Case-insensitive, and including
   * foreign issuers' own legal-entity suffixes (N.V., S.A., A.G.) — confirmed live 2026-09-28:
   * "HSBC Holdings plc" (lower-case "plc") and "NXP Semiconductors N.V." both lost their name to
   * an unprotected period once the ticker parenthetical right after them was mistaken for the
   * end of an unrelated note.
   */
  private static final Pattern PROTECTED_ABBREVIATION =
      Pattern.compile("(?i)\\b(Inc|Corp|Co|Ltd|Cos|Plc|N\\.V|S\\.A|A\\.G|N\\.A|S\\.p\\.A)\\.");

  // A sentence period, or (confirmed live 2026-09-27, filing 20034201) "/share" ending a clause
  // of a broker's consolidated sale note ("... AAPL – 20.313 shares sold @ $253.45/share Apple
  // Inc. - Common Stock (AAPL)") — that note has no terminal period before the next asset name,
  // so the period-only split leaves the whole note attached.
  private static final Pattern NOTE_BREAK = Pattern.compile("\\.\\s+|/share\\s+");

  /**
   * A garbled label's colon: every label this template prints ("Filing Status", "Subholding Of",
   * "Location", "Description") loses its own letters to the same font-decoding failure that drops
   * the table headers, leaving only a single surviving letter ("F", "O", "L", "D") right before
   * the colon. An asset name that itself contains a colon (an exchange-prefixed ticker, e.g.
   * "NYSEARCA: DIA" — confirmed live 2026-09-27, filing 20034201) keeps its full preceding word
   * intact, so requiring a 1-2 letter token before the colon tells the two apart: only the last
   * label colon (not the last colon of any kind) marks where the real row text starts.
   */
  private static final Pattern LABEL_COLON = Pattern.compile("\\b[A-Za-z]{1,2}\\s*:");

  /**
   * The pre-2018 PTR template has no asset-type-code column at all (no {@code [TYPE]} bracket,
   * no {@code TABLE_HEADER_END}) — its header row ends in the bare word "Amount" instead of
   * "Cap. Gains &gt; $200?". Case varies with the same font-substitution quirk seen throughout
   * this era's filings ("amount", "aMount", ...), so this is matched case-insensitively.
   */
  private static final Pattern LEGACY_TABLE_HEADER_END = Pattern.compile("(?i)\\bamount\\b");

  /**
   * {@code (BRK.B) P 02/27/2015 02/27/2015 $25,000,001 - $50,000,000}: unlike the 2018+ template,
   * there is no asset-type bracket to anchor on, so this anchors on the transaction-type letter
   * itself, followed by two dates and an amount. Confirmed live 2026-09-28 against real 2015-2016
   * filings: unlike the 2018+ template, the two dates are separated by whitespace (never smashed
   * together) but the month/day may be a single digit with no zero-padding ("12/2/2015"), and the
   * type letter itself is sometimes lower-cased by the same font substitution that affects owner
   * codes and tickers in this era.
   */
  private static final Pattern LEGACY_ANCHOR = Pattern.compile(
      "\\b([PESpes])\\s*(\\(partial\\))?\\s+(\\d{1,2}/\\d{1,2}/\\d{4})\\s+"
      + "(\\d{1,2}/\\d{1,2}/\\d{4})\\s+(Over\\s*\\$[\\d,]+|\\$[\\d,]+\\s*-\\s*\\$[\\d,]+"
      + "|\\$[\\d,]+(?:\\.\\d{2})?)");

  /**
   * Case-insensitive counterpart of {@link #OWNER_PREFIX}: confirmed live 2026-09-28, filing
   * 20006000 (Hon. Raúl M. Grijalva) — "sP First Trust..." lower-cases the owner code itself, not
   * just the asset name or ticker.
   */
  private static final Pattern LEGACY_OWNER_PREFIX =
      Pattern.compile("(?i)^(SP|JT|DC)\\s+(.*)$", Pattern.DOTALL);

  /** Case-insensitive counterpart of {@link #OWNER_CODE_TOKEN}. */
  private static final Pattern LEGACY_OWNER_CODE_TOKEN = Pattern.compile("(?i)\\b(?:SP|JT|DC)\\s");

  /**
   * Case-insensitive counterpart of {@link #TRAILING_TICKER}, allowing a lower-cased letter
   * inside the ticker itself (confirmed live 2026-09-28: "(MXWl)", "(SEDg)", "(CgNX)", "(aYI)").
   * A pre-2018 filing may have no ticker at all (e.g. a municipal bond), in which case this simply
   * fails to match and the whole span is kept as the asset name, exactly like the 2018+ template.
   */
  private static final Pattern LEGACY_TRAILING_TICKER =
      Pattern.compile("^(.*)\\(([A-Za-z0-9.\\-]{1,8})\\)\\s*$", Pattern.DOTALL);

  private HousePtrTextParser() {
  }

  /**
   * @param pages the PDF's pages, in order, as extracted text
   * @return one row per transaction, in the order the PTR lists them
   */
  static List<Row> parse(List<String> pages) {
    StringBuilder sb = new StringBuilder();
    for (String page : pages) {
      sb.append(page).append(' ');
    }
    String flattened = normalize(sb.toString());
    return flattened.contains(TABLE_HEADER_END) ? parseModern(flattened) : parseLegacy(flattened);
  }

  private static List<Row> parseModern(String flattened) {
    int headerEnd = flattened.indexOf(TABLE_HEADER_END);
    int spanStart = headerEnd < 0 ? 0 : headerEnd + TABLE_HEADER_END.length();

    List<Row> rows = new ArrayList<Row>();
    Matcher anchor = ANCHOR.matcher(flattened);
    int searchFrom = spanStart;
    while (searchFrom <= flattened.length() && anchor.find(searchFrom)) {
      String span = flattened.substring(spanStart, anchor.start());
      Matcher labelColon = LABEL_COLON.matcher(span);
      int afterLabelStart = 0;
      while (labelColon.find()) {
        afterLabelStart = labelColon.end();
      }
      // A labeled field ("Subholding Of", "Location", "Description") prints its value on the
      // same line as its label and nothing else — the source's own line break, not the colon, is
      // what ends the value. Skipping only past the colon left the value itself (e.g.
      // "Morgan Stanley IRA - X141") attached as a prefix to the next row's asset name whenever
      // that value has no sentence-ending period of its own to trigger stripLeadingFreeText's
      // split (confirmed live 2026-09-28, filing 20022571: ~26% of this table's rows carried a
      // leading subholding/account description in asset_name). If the label's line has no
      // newline before the next anchor (a value that wraps across a page break, per this class's
      // own javadoc), there is no reliable boundary and the old behavior is kept.
      int labelLineEnd = span.indexOf('\n', afterLabelStart);
      if (labelLineEnd >= 0) {
        afterLabelStart = labelLineEnd + 1;
      }
      String afterLabel = span.substring(afterLabelStart).trim();

      String ownerCode = null;
      String preTicker;
      boolean stripFreeText;
      Matcher ownerToken = OWNER_CODE_TOKEN.matcher(afterLabel);
      int lastOwnerStart = -1;
      while (ownerToken.find()) {
        lastOwnerStart = ownerToken.start();
      }
      if (lastOwnerStart >= 0) {
        Matcher owner = OWNER_PREFIX.matcher(afterLabel.substring(lastOwnerStart).trim());
        if (owner.matches()) {
          ownerCode = owner.group(1);
          preTicker = owner.group(2);
          stripFreeText = false;
        } else {
          preTicker = afterLabel;
          stripFreeText = true;
        }
      } else {
        preTicker = afterLabel;
        stripFreeText = true;
      }

      // The trailing "(TICKER)" is extracted before any free-text/note splitting runs, not
      // after: splitting first (the old order) treated an unprotected period inside the asset
      // name itself (a foreign issuer's "N.V."/"S.A." suffix right before its ticker) as a note
      // boundary, discarding the whole name and keeping only the ticker (confirmed live
      // 2026-09-28, filing 20022571: "NXP Semiconductors N.V. (NXPI)" produced ticker=NXPI,
      // asset_name=null).
      String ticker = null;
      String withoutTicker = preTicker;
      Matcher tickerMatch = TRAILING_TICKER.matcher(preTicker);
      if (tickerMatch.matches()) {
        withoutTicker = tickerMatch.group(1);
        ticker = tickerMatch.group(2);
      }
      String assetName = stripFreeText ? stripLeadingFreeText(withoutTicker) : withoutTicker.trim();

      String assetTypeCode = anchor.group(1).toUpperCase(java.util.Locale.ROOT);
      String transactionType = anchor.group(2).replaceAll("\\s+", " ").trim();
      String transactionDate = isoDate(anchor.group(3));
      String notificationDate = isoDate(anchor.group(4));
      String amountRange = anchor.group(5).replaceAll("\\s+", " ").trim();

      // The filing-status window must not cross into the next row's own anchor match: two
      // consecutive same-day transactions can pack the next row's "[TYPE] P MM/DD/YYYY..." within
      // FILING_STATUS_WINDOW chars of this row's anchor end, and a spurious ":  New"/":  Amended"
      // match past that point would push spanStart beyond the next iteration's anchor.start(),
      // making the next substring(spanStart, anchor.start()) call throw (confirmed live
      // 2026-09-27, filing 8220... range).
      Matcher nextAnchorPeek = ANCHOR.matcher(flattened);
      int windowEnd = Math.min(flattened.length(), anchor.end() + FILING_STATUS_WINDOW);
      if (nextAnchorPeek.find(anchor.end())) {
        windowEnd = Math.min(windowEnd, nextAnchorPeek.start());
      }
      String filingStatus = null;
      Matcher status = FILING_STATUS.matcher(flattened.substring(anchor.end(), windowEnd));
      int filingStatusEnd = anchor.end();
      if (status.find()) {
        filingStatus = status.group(1);
        filingStatusEnd = anchor.end() + status.end();
      }

      long[] range = parseAmountRange(amountRange);
      rows.add(new Row(ownerCode, assetName, ticker, assetTypeCode, transactionType,
          transactionDate, notificationDate, amountRange,
          range[0] < 0 ? null : Long.valueOf(range[0]),
          range[1] < 0 ? null : Long.valueOf(range[1]), filingStatus));

      spanStart = filingStatusEnd;
      searchFrom = anchor.end();
    }
    return rows;
  }

  /**
   * Pre-2018 PTR template (2015-2017): no asset-type-code column, so {@code assetTypeCode} is
   * always {@code null}. Otherwise mirrors {@link #parseModern}'s row-extraction shape (label
   * skipping, owner-code clipping, trailing-ticker stripping, filing-status window) with
   * case-insensitive owner/ticker/type patterns for this era's font substitutions.
   */
  private static List<Row> parseLegacy(String flattened) {
    Matcher headerEndMatch = LEGACY_TABLE_HEADER_END.matcher(flattened);
    int spanStart = headerEndMatch.find() ? headerEndMatch.end() : 0;

    List<Row> rows = new ArrayList<Row>();
    Matcher anchor = LEGACY_ANCHOR.matcher(flattened);
    int searchFrom = spanStart;
    while (searchFrom <= flattened.length() && anchor.find(searchFrom)) {
      String span = flattened.substring(spanStart, anchor.start());
      Matcher labelColon = LABEL_COLON.matcher(span);
      int afterLabelStart = 0;
      while (labelColon.find()) {
        afterLabelStart = labelColon.end();
      }
      int labelLineEnd = span.indexOf('\n', afterLabelStart);
      if (labelLineEnd >= 0) {
        afterLabelStart = labelLineEnd + 1;
      }
      String afterLabel = span.substring(afterLabelStart).trim();

      String ownerCode = null;
      String preTicker;
      boolean stripFreeText;
      Matcher ownerToken = LEGACY_OWNER_CODE_TOKEN.matcher(afterLabel);
      int lastOwnerStart = -1;
      while (ownerToken.find()) {
        lastOwnerStart = ownerToken.start();
      }
      if (lastOwnerStart >= 0) {
        Matcher owner = LEGACY_OWNER_PREFIX.matcher(afterLabel.substring(lastOwnerStart).trim());
        if (owner.matches()) {
          ownerCode = owner.group(1).toUpperCase(java.util.Locale.ROOT);
          preTicker = owner.group(2);
          stripFreeText = false;
        } else {
          preTicker = afterLabel;
          stripFreeText = true;
        }
      } else {
        preTicker = afterLabel;
        stripFreeText = true;
      }

      String ticker = null;
      String withoutTicker = preTicker;
      Matcher tickerMatch = LEGACY_TRAILING_TICKER.matcher(preTicker);
      if (tickerMatch.matches()) {
        withoutTicker = tickerMatch.group(1);
        ticker = tickerMatch.group(2).toUpperCase(java.util.Locale.ROOT);
      }
      String assetName = stripFreeText ? stripLeadingFreeText(withoutTicker) : withoutTicker.trim();

      String transactionTypeLetter = anchor.group(1).toUpperCase(java.util.Locale.ROOT);
      String transactionType =
          anchor.group(2) != null ? transactionTypeLetter + " (partial)" : transactionTypeLetter;
      String transactionDate = isoDateFlexible(anchor.group(3));
      String notificationDate = isoDateFlexible(anchor.group(4));
      String amountRange = anchor.group(5).replaceAll("\\s+", " ").trim();

      Matcher nextAnchorPeek = LEGACY_ANCHOR.matcher(flattened);
      int windowEnd = Math.min(flattened.length(), anchor.end() + FILING_STATUS_WINDOW);
      if (nextAnchorPeek.find(anchor.end())) {
        windowEnd = Math.min(windowEnd, nextAnchorPeek.start());
      }
      String filingStatus = null;
      Matcher status = FILING_STATUS.matcher(flattened.substring(anchor.end(), windowEnd));
      int filingStatusEnd = anchor.end();
      if (status.find()) {
        filingStatus = status.group(1);
        filingStatusEnd = anchor.end() + status.end();
      }

      long[] range = parseAmountRange(amountRange);
      rows.add(new Row(ownerCode, assetName, ticker, null, transactionType,
          transactionDate, notificationDate, amountRange,
          range[0] < 0 ? null : Long.valueOf(range[0]),
          range[1] < 0 ? null : Long.valueOf(range[1]), filingStatus));

      spanStart = filingStatusEnd;
      searchFrom = anchor.end();
    }
    return rows;
  }

  /**
   * The text between the end of one row's filing status (or the table header, for the first row)
   * and the start of the next row's asset-type bracket holds that next row's owner code and
   * asset name — plus, when the previous row carried a free-text note, that note's tail, ending
   * in its own sentence period. Splitting on the last sentence break (a period whose corporate
   * abbreviations have been protected first) discards the note and keeps the asset name.
   */
  private static String stripLeadingFreeText(String text) {
    String protectedText = PROTECTED_ABBREVIATION.matcher(text).replaceAll("$1\u0001");
    String[] parts = NOTE_BREAK.split(protectedText);
    String tail = parts[parts.length - 1];
    return tail.replace('\u0001', '.').trim();
  }

  /**
   * Collapses every run of non-printable-or-whitespace bytes to a single space, except a line
   * break: a run containing one is collapsed to a single {@code \n} instead. The line break is
   * kept because it is the only reliable boundary between a labeled field's value and the next
   * row's own text (see the label-line handling in {@link #parse}); collapsing it away like any
   * other whitespace is what let a "Subholding Of" value bleed into the next asset name.
   */
  private static String normalize(String text) {
    StringBuilder out = new StringBuilder(text.length());
    // Whitespace runs are buffered and only flushed once their extent (and whether any char in
    // the run was a line break) is known, so a run like "  \n" collapses to one "\n" rather than
    // a stray " \n".
    boolean inRun = false;
    boolean runHasNewline = false;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      boolean isNewline = c == '\n' || c == '\r';
      boolean isSpace = c < 0x20 || c == 0x7F || Character.isWhitespace(c);
      if (isSpace) {
        inRun = true;
        runHasNewline |= isNewline;
      } else {
        if (inRun) {
          out.append(runHasNewline ? '\n' : ' ');
          inRun = false;
          runHasNewline = false;
        }
        out.append(c);
      }
    }
    if (inRun) {
      out.append(runHasNewline ? '\n' : ' ');
    }
    return out.toString();
  }

  /** {@code MM/DD/YYYY} to {@code YYYY-MM-DD}. */
  private static String isoDate(String date) {
    return date.substring(6, 10) + "-" + date.substring(0, 2) + "-" + date.substring(3, 5);
  }

  /**
   * {@code MM/DD/YYYY} or {@code M/D/YYYY} to {@code YYYY-MM-DD}: unlike {@link #isoDate}, the
   * pre-2018 template does not always zero-pad the month/day (confirmed live 2026-09-28,
   * "12/2/2015"), so fixed character offsets do not apply and the field widths must be split out.
   */
  private static String isoDateFlexible(String date) {
    String[] parts = date.split("/");
    String month = parts[0].length() == 1 ? "0" + parts[0] : parts[0];
    String day = parts[1].length() == 1 ? "0" + parts[1] : parts[1];
    return parts[2] + "-" + month + "-" + day;
  }

  /**
   * @return {min, max}; {@code max} is -1 ("not present") only for an open-ended "Over $N" —
   *     an exact amount with no bracket (e.g. an IRA sale below the disclosure threshold) sets
   *     both to that same figure
   */
  private static long[] parseAmountRange(String range) {
    if (range.startsWith("Over")) {
      return new long[] {parseDollars(range.substring(4)), -1L};
    }
    String[] parts = range.split("-");
    if (parts.length > 1) {
      return new long[] {parseDollars(parts[0]), parseDollars(parts[1])};
    }
    long exact = parseDollars(parts[0]);
    return new long[] {exact, exact};
  }

  /** {@code "$2,722.50"} to {@code 2723} (rounded to the nearest dollar). */
  private static long parseDollars(String token) {
    return Math.round(Double.parseDouble(token.replaceAll("[^0-9.]", "")));
  }
}
