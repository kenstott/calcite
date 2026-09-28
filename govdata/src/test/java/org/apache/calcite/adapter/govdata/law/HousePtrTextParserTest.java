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

import org.apache.calcite.adapter.govdata.law.HousePtrTextParser.Row;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for {@link HousePtrTextParser}. The page-0 fixture is the verbatim text PDFBox
 * extracts from a real filing (House Clerk PTR, filing ID 20024277, Hon. Richard W. Allen,
 * fetched and extracted live 2026-09-27) — the label text (headers, "Filing Status", "Subholding
 * Of") comes through as runs of {@code \0} control characters exactly as the source PDF's label
 * font decodes, which is why the parser reads only the anchor line and ignores labels.
 */
@Tag("unit")
class HousePtrTextParserTest {

  private static final String ALLEN_20024277_PAGE_0 =
      "P\0\0\0\0\0\0\0 T\0\0\0\0\0\0\0\0\0\0 R\0\0\0\0\0\n"
      + "Clerk of the House of Representatives • Legislative Resource Center • 135 "
      + "Cannon Building • Washington, DC 20515\n"
      + "F\0\0\0\0 I\0\0\0\0\0\0\0\0\0\0\n"
      + "Name: Hon. Richard W. Allen\n"
      + "Status: Member\n"
      + "State/District:GA12\n"
      + "T\0\0\0\0\0\0\0\0\0\0\0\n"
      + "ID Owner Asset Transaction\nType\nDate Notification\nDate\nAmount Cap.\nGains >\n$200?\n"
      + "SP Albemarle Corporation (ALB) [ST] S 12/21/202301/08/2024$1,001 - $15,000\n"
      + "F\0\0\0\0\0 S\0\0\0\0\0: New\n"
      + "S\0\0\0\0\0\0\0\0\0 O\0: R.W. Allen & Associates, Inc.\n"
      + "SP Albemarle Corporation (ALB) [ST] S 12/21/202301/08/2024$1,001 - $15,000\n"
      + "F\0\0\0\0\0 S\0\0\0\0\0: New\n"
      + "S\0\0\0\0\0\0\0\0\0 O\0: LIVTR\n"
      + "SP Charles Schwab Corporation (SCHW)\n[ST]\nP 12/14/202301/08/2024$50,001 -\n$100,000\n"
      + "F\0\0\0\0\0 S\0\0\0\0\0: New\n"
      + "S\0\0\0\0\0\0\0\0\0 O\0: R.W. Allen & Associates, Inc.\n"
      + "SP NextEra Energy, Inc. (NEE) [ST] S 12/21/202301/08/2024$15,001 -\n$50,000\n"
      + "F\0\0\0\0\0 S\0\0\0\0\0: New\n"
      + "S\0\0\0\0\0\0\0\0\0 O\0: R.W. Allen & Associates, Inc.\n"
      + "* For the complete list of asset type abbreviations, please visit "
      + "https://fd.house.gov/reference/asset-type-codes.aspx.\n"
      + "A\0\0\0\0 C\0\0\0\0 D\0\0\0\0\0\0\n"
      + "LIVTR (Owner: SP)\nL\0\0\0\0\0\0\0: US\n"
      + "R.W. Allen & Associates, Inc. (Owner: SP)\nL\0\0\0\0\0\0\0: US\n"
      + "Filing ID #20024277";

  @Test void realFilingFourTransactionsInDocumentOrder() {
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(ALLEN_20024277_PAGE_0));
    assertEquals(4, rows.size());

    Row r1 = rows.get(0);
    assertEquals("SP", r1.ownerCode);
    assertEquals("Albemarle Corporation", r1.assetName);
    assertEquals("ALB", r1.ticker);
    assertEquals("ST", r1.assetTypeCode);
    assertEquals("S", r1.transactionType);
    assertEquals("2023-12-21", r1.transactionDate);
    assertEquals("2024-01-08", r1.notificationDate);
    assertEquals("$1,001 - $15,000", r1.amountRange);
    assertEquals(Long.valueOf(1001L), r1.amountMin);
    assertEquals(Long.valueOf(15000L), r1.amountMax);
    assertEquals("New", r1.filingStatus);

    // Same asset/amount filed twice under a different sub-holding: rows stay independent.
    Row r2 = rows.get(1);
    assertEquals("Albemarle Corporation", r2.assetName);
    assertEquals("ALB", r2.ticker);

    // A purchase, and the amount range wraps across a line break ("$50,001 -\n$100,000") with
    // no whitespace before it either ("...01/08/2024$50,001").
    Row r3 = rows.get(2);
    assertEquals("Charles Schwab Corporation", r3.assetName);
    assertEquals("SCHW", r3.ticker);
    assertEquals("P", r3.transactionType);
    assertEquals("$50,001 - $100,000", r3.amountRange);
    assertEquals(Long.valueOf(50001L), r3.amountMin);
    assertEquals(Long.valueOf(100000L), r3.amountMax);

    Row r4 = rows.get(3);
    assertEquals("NextEra Energy, Inc.", r4.assetName);
    assertEquals("NEE", r4.ticker);
    assertEquals(Long.valueOf(15001L), r4.amountMin);
    assertEquals(Long.valueOf(50000L), r4.amountMax);
  }

  @Test void openEndedTopBracketHasNoUpperBound() {
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(
        "$200?\nBerkshire Hathaway Inc (BRK) [ST] P 02/27/2015 02/27/2015 Over $50,000,000 "
        + "F: New"));
    assertEquals(1, rows.size());
    Row r = rows.get(0);
    assertEquals("Over $50,000,000", r.amountRange);
    assertEquals(Long.valueOf(50000000L), r.amountMin);
    assertNull(r.amountMax);
  }

  /**
   * Real filing 20033695 (Hon. Doris O. Matsui, fetched and extracted live 2026-09-27): a
   * spouse/dependent-child holding over the top disclosure bracket prints an extra "Spouse/DC"
   * annotation token between the notification date and the amount, and the dates/amount run
   * together across a line break with no separating whitespace at all.
   */
  @Test void spouseOverTopBracketAnnotationTokenBeforeAmount() {
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(
        "$200?\nSP U.S. Treasury Note due 2/28/2029\n[GS]\nP 12/15/202512/16/2025 Spouse/DC Over\n"
        + "$1,000,000\nF\0\0\0\0\0 S\0\0\0\0\0: New"));
    assertEquals(1, rows.size());
    Row r = rows.get(0);
    assertEquals("GS", r.assetTypeCode);
    assertEquals("P", r.transactionType);
    assertEquals("2025-12-15", r.transactionDate);
    assertEquals("2025-12-16", r.notificationDate);
    assertEquals("Over $1,000,000", r.amountRange);
    assertEquals(Long.valueOf(1000000L), r.amountMin);
    assertNull(r.amountMax);
    assertEquals("New", r.filingStatus);
  }

  @Test void ordinaryBracketRetainsItsLowerBoundWhenFollowedByMoreText() {
    // The bare-$N amount alternative (for an exact, non-bracket figure) must not be allowed to
    // match just the tail of a real "$low - $high" bracket once a greedy lookahead is involved.
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(
        "$200?\nAT&T Inc. (T) [ST] S (partial) 03/16/2026 03/16/2026 $1,001 - $15,000\n"
        + "F\0\0\0\0\0 S\0\0\0\0\0: New\nS\0\0\0\0\0\0\0\0\0 O\0: Putnam Investments"));
    assertEquals(1, rows.size());
    Row r = rows.get(0);
    assertEquals("$1,001 - $15,000", r.amountRange);
    assertEquals(Long.valueOf(1001L), r.amountMin);
    assertEquals(Long.valueOf(15000L), r.amountMax);
  }

  @Test void partialSaleTransactionType() {
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(
        "$200?\nJT Equity Commonwealth (EQC) [ST] S (partial) 12/13/2024 12/20/2024 "
        + "$1,001 - $15,000 F: New"));
    assertEquals(1, rows.size());
    Row r = rows.get(0);
    assertEquals("JT", r.ownerCode);
    assertEquals("S (partial)", r.transactionType);
  }

  @Test void dependentChildOwnerAndTickerWithEmbeddedPeriod() {
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(
        "$200?\nDC Berkshire Hathaway Inc (BRK.B) [ST] P 12/13/2024 12/20/2024 "
        + "$1,001 - $15,000 F: New"));
    assertEquals(1, rows.size());
    Row r = rows.get(0);
    assertEquals("DC", r.ownerCode);
    assertEquals("BRK.B", r.ticker);
  }

  @Test void noAssetTypeBracketAfterLastRowLeavesFilingStatusNullWhenAbsent() {
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(
        "$200?\nSome Private Fund LP [OT] E 01/02/2024 01/10/2024 $15,001 - $50,000"));
    assertEquals(1, rows.size());
    assertNull(rows.get(0).filingStatus);
    assertNull(rows.get(0).ticker);
    assertEquals("OT", rows.get(0).assetTypeCode);
    assertEquals("E", rows.get(0).transactionType);
  }

  /**
   * Reproduces a real crash (McCaul filings, live 2026-09-27): when a row has no filing-status
   * marker of its own within {@code FILING_STATUS_WINDOW} chars, the window used to be searched
   * unclamped and could match a *later* row's "F: New" past the very next row's anchor, pushing
   * {@code spanStart} beyond that next anchor's start and throwing
   * {@code StringIndexOutOfBoundsException} on the following iteration's {@code substring} call.
   */
  @Test void closelyPackedRowsWithNoInterveningFilingStatusDoesNotOverrunNextAnchor() {
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(
        "$200?\nDC A [ST] P 01/02/2024 01/10/2024 $1,001 - $15,000 \n"
        + "DC B [ST] S 02/02/2024 02/10/2024 $1,001 - $15,000 F: New F: New"));
    assertEquals(2, rows.size());
    Row r1 = rows.get(0);
    assertEquals("A", r1.assetName);
    assertNull(r1.filingStatus);
    Row r2 = rows.get(1);
    assertEquals("B", r2.assetName);
    assertEquals("New", r2.filingStatus);
  }

  /**
   * Real filings 20020238/20016125/20012722 (fetched and extracted live 2026-09-28): the
   * bracket's first letter is lower-cased regardless of which letter it is — not a fixed
   * per-letter substitution — so the parser must accept any case and upper-case the result.
   */
  @Test void assetTypeCodeFirstLetterLowerCasedByFontIsUpperCasedInResult() {
    List<Row> gs = HousePtrTextParser.parse(Arrays.asList(
        "$200?\nMet govt Nashville 5% [gS] P 12/20/2021 01/17/2022 $50,001 - $100,000 F: New"));
    assertEquals("GS", gs.get(0).assetTypeCode);

    List<Row> st = HousePtrTextParser.parse(Arrays.asList(
        "$200?\nNorthwest Natural Holding Company (NWN) [sT] P 02/14/2020 02/20/2020 "
        + "$1,001 - $15,000 F: New"));
    assertEquals("ST", st.get(0).assetTypeCode);

    List<Row> oi = HousePtrTextParser.parse(Arrays.asList(
        "$200?\nDon Beyer Motors Inc. [oI] S 10/07/2019 10/15/2019 $1,001 - $15,000 F: New"));
    assertEquals("OI", oi.get(0).assetTypeCode);
  }

  /**
   * Real filing 20022571 (Hon. Lois Frankel, fetched and extracted live 2026-09-28): a
   * "Subholding Of" note with no owner code (SP/JT/DC) sits on its own line before the next
   * row's asset name, with no other delimiter between them — only the line break tells them
   * apart, so collapsing line breaks like any other whitespace left the note's value
   * ("Morgan Stanley IRA - X141") attached as a prefix to that next row's asset name.
   */
  @Test void subholdingNoteWithoutOwnerCodeDoesNotBleedIntoNextAssetName() {
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(
        "$200?\nCitigroup, Inc. (C) [ST] P 02/22/2023 03/14/2023 $1,001 - $15,000\n"
        + "F: New\nS O: Morgan Stanley IRA - X141\n"
        + "JP Morgan Chase & Co. (JPM) [ST] P 03/02/2023 03/14/2023 $1,001 - $15,000\n"
        + "F: New"));
    assertEquals(2, rows.size());
    assertEquals("Citigroup, Inc.", rows.get(0).assetName);
    assertEquals("JP Morgan Chase & Co.", rows.get(1).assetName);
    assertEquals("JPM", rows.get(1).ticker);
  }

  /**
   * Real filing 20022571 (Hon. Lois Frankel, fetched and extracted live 2026-09-28): "NXP
   * Semiconductors N.V." carries an internal, unprotected period right before its ticker
   * parenthetical — splitting on that period before extracting the ticker used to discard the
   * whole name, leaving asset_name null and ticker "NXPI".
   */
  @Test void foreignLegalSuffixNameNotSwallowedByFreeTextSplit() {
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(
        "$200?\nPNC Financial Services Group, Inc. (PNC) [ST] S (partial) 02/22/2023 "
        + "03/14/2023 $1,001 - $15,000\nF: New\nS O: Morgan Stanley IRA - X141\n"
        + "NXP Semiconductors N.V. (NXPI)\n[ST]\nS (partial) 02/22/2023 03/14/2023 "
        + "$1,001 - $15,000\nF: New"));
    assertEquals(2, rows.size());
    assertEquals("NXP Semiconductors N.V.", rows.get(1).assetName);
    assertEquals("NXPI", rows.get(1).ticker);
  }

  @Test void exactAmountWithNoBracketSetsMinEqualsMax() {
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(
        "$200?\nIRA Distribution [OT] S 03/01/2024 03/05/2024 $2,722.50 F: New"));
    assertEquals(1, rows.size());
    Row r = rows.get(0);
    assertEquals(Long.valueOf(2723L), r.amountMin);
    assertEquals(Long.valueOf(2723L), r.amountMax);
  }

  /**
   * Real filing 20002776 (Hon. Brad Ashford, fetched and PDFBox-extracted live 2026-09-28): the
   * pre-2018 PTR template has no {@code [TYPE]} bracket and no "Cap. Gains &gt; $200?" header —
   * the ticker parenthetical sits directly after the asset name, and dates are space-separated
   * rather than smashed together. "Berkshire hathaway Inc. New" is the issuer's own registered
   * name (a 1996 reclassification suffix), not a stray "Filing Status: New" value.
   */
  @Test void legacyTemplateOwnerCodeAndMultiLineAmountWrap() {
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(
        "PerioDic tranSaction rePort\n"
        + "filer information\nname: Brad Ashford\nStatus: Member\nState/District: NE02\n"
        + "tranSactionS\n"
        + "iD owner asset transaction\ntype\nDate notification\nDate\namount\n"
        + "DC Berkshire hathaway Inc. New (BRK.B) P 02/27/2015 02/27/2015 $25,000,001 -\n"
        + "$50,000,000\n"
        + "FILINg STATUS: New\n"
        + "SUBhOLDINg OF: Dependent Child Stock Account\n"
        + "DESCRIPTION: 250 BRK B shares at $148 per share.\n"
        + "SP Union Pacific Corporation (UNP) S 03/12/2015 03/12/2015 $15,001 - $50,000\n"
        + "FILINg STATUS: New\n"
        + "SUBhOLDINg OF: Ann Ashford Stock Account\n"
        + "DESCRIPTION: 220 Shares of UNP at $114 per share."));
    assertEquals(2, rows.size());

    Row r1 = rows.get(0);
    assertEquals("DC", r1.ownerCode);
    assertEquals("Berkshire hathaway Inc. New", r1.assetName);
    assertEquals("BRK.B", r1.ticker);
    assertNull(r1.assetTypeCode);
    assertEquals("P", r1.transactionType);
    assertEquals("2015-02-27", r1.transactionDate);
    assertEquals("2015-02-27", r1.notificationDate);
    assertEquals(Long.valueOf(25000001L), r1.amountMin);
    assertEquals(Long.valueOf(50000000L), r1.amountMax);
    assertEquals("New", r1.filingStatus);

    Row r2 = rows.get(1);
    assertEquals("SP", r2.ownerCode);
    assertEquals("Union Pacific Corporation", r2.assetName);
    assertEquals("UNP", r2.ticker);
    assertEquals("S", r2.transactionType);
    assertEquals("2015-03-12", r2.transactionDate);
  }

  /**
   * Real filing 20004336 (Hon. Suzan K. DelBene, fetched and PDFBox-extracted live 2026-09-28):
   * single-digit, non-zero-padded dates ("12/2/2015") and a ticker whose letters are partly
   * lower-cased by this era's font substitution ("MXWl", "CgNX", "aYI", "SuNE") — confirming the
   * upper-casing and flexible date width both hold across a real multi-row filing, including an
   * owner-code switch (JT to SP) mid-filing with no free text between rows.
   */
  @Test void legacyTemplateSingleDigitDatesAndLowerCasedTickerLetters() {
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(
        "filer information\nname: Hon. Suzan K. DelBene\nState/District: Wa01\n"
        + "tranSactionS\niD owner asset transaction\ntype\nDate notification\nDate\namount\n"
        + "JT acuity Brands Inc (aYI) S 12/22/2015 12/22/2015 $1,001 - $15,000\n"
        + "FIlINg STaTuS: New\n"
        + "JT Cognex Corporation (CgNX) P 12/2/2015 12/2/2015 $15,001 - $50,000\n"
        + "FIlINg STaTuS: New\n"
        + "JT Maxwell Technologies, Inc. (MXWl) P 12/2/2015 12/2/2015 $1,001 - $15,000\n"
        + "FIlINg STaTuS: New\n"
        + "SP Microsoft Corporation (MSFT) P 12/31/2015 12/31/2015 $15,001 - $50,000\n"
        + "FIlINg STaTuS: New\n"
        + "JT SunEdison, Inc. (SuNE) P 12/22/2015 12/22/2015 $1,001 - $15,000\n"
        + "FIlINg STaTuS: New"));
    assertEquals(5, rows.size());

    assertEquals("AYI", rows.get(0).ticker);
    assertEquals("acuity Brands Inc", rows.get(0).assetName);

    Row cognex = rows.get(1);
    assertEquals("CGNX", cognex.ticker);
    assertEquals("2015-12-02", cognex.transactionDate);
    assertEquals("2015-12-02", cognex.notificationDate);

    assertEquals("MXWL", rows.get(2).ticker);

    Row msft = rows.get(3);
    assertEquals("SP", msft.ownerCode);
    assertEquals("Microsoft Corporation", msft.assetName);
    assertEquals("MSFT", msft.ticker);

    Row sune = rows.get(4);
    assertEquals("JT", sune.ownerCode);
    assertEquals("SUNE", sune.ticker);
  }

  /**
   * Real filing 20005764 (Hon. David E. Price, fetched and PDFBox-extracted live 2026-09-28): a
   * municipal bond has no ticker at all — the same "no bracket, no ticker" case the 2018+
   * template handles by leaving {@code ticker} null, confirming the pre-2018 anchor (which does
   * not require a ticker to match) does the same rather than dropping the row.
   */
  @Test void legacyTemplateRowWithNoTickerAtAllStillParses() {
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(
        "filer information\nname: Hon. David E. Price\nState/District: NC04\n"
        + "tranSactionS\niD owner asset transaction\ntype\nDate notification\nDate\namount\n"
        + "Metlife, Inc. (MET) P 08/3/2016 08/8/2016 $1,001 - $15,000\n"
        + "FIlINg sTaTus: New\n"
        + "N.C. Capital Facilities Bonds - Duke\nuniversity\n"
        + "P 08/3/2016 08/8/2016 $1,001 - $15,000\n"
        + "FIlINg sTaTus: New\n"
        + "DEsCRIPTION: 10,000 units\n"
        + "Thermo Fisher scientific Inc (TMO) P 08/3/2016 08/8/2016 $1,001 - $15,000\n"
        + "FIlINg sTaTus: New"));
    assertEquals(3, rows.size());
    assertNull(rows.get(1).ticker);
    assertEquals("TMO", rows.get(2).ticker);
  }

  /**
   * Real filing 20006000 (Hon. Raúl M. Grijalva, fetched and PDFBox-extracted live 2026-09-28):
   * the owner code itself is lower-cased ("sP" rather than "SP") by this era's font substitution,
   * and a ticker sits alone on its own line after the asset name wraps.
   */
  @Test void legacyTemplateLowerCasedOwnerCodeAndWrappedTickerLine() {
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(
        "filer information\nname: Hon. Raúl M. Grijalva\nState/District: aZ03\n"
        + "tranSactionS\niD owner asset transaction\ntype\nDate notification\nDate\namount\n"
        + "sP First Trust Vl Dividend (FVD) P 09/20/2016 10/5/2016 $1,001 - $15,000\n"
        + "FIlING sTaTus: New\n"
        + "sP sPDR s&P 500 (sPY) P 09/20/2016 10/5/2016 $1,001 - $15,000\n"
        + "FIlING sTaTus: New\n"
        + "sP Vanguard Mid-Cap Growth ETF - DNQ\n(VOT)\nP 09/20/2016 10/5/2016 $1,001 - $15,000\n"
        + "FIlING sTaTus: New"));
    assertEquals(3, rows.size());
    assertEquals("SP", rows.get(0).ownerCode);
    assertEquals("FVD", rows.get(0).ticker);
    assertEquals("SPY", rows.get(1).ticker);
    Row vot = rows.get(2);
    assertEquals("SP", vot.ownerCode);
    assertEquals("VOT", vot.ticker);
    assertEquals("Vanguard Mid-Cap Growth ETF - DNQ", vot.assetName);
  }

  /**
   * Real filing 20011886 (Hon. Mikie Sherrill, fetched and extracted live 2026-09-28): the same
   * font-substitution quirk documented for the pre-2018 template also hits 2018+ filings — a
   * ticker with a lower-cased letter ("aSMl", "gOOgl") failed the modern trailing-ticker pattern's
   * upper-case-only character class, so the whole "(ticker)" span stayed attached to the asset
   * name and the row's own ticker column came out null. ~30 tickers in that one filing alone were
   * affected.
   */
  @Test void lowerCasedModernTickerLetterStillExtracted() {
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(
        "$200?\nASML Holding N.V. - ADS represents 1 ordinary share (aSMl) [ST] "
        + "S 05/28/2019 06/18/2019 $1,001 - $15,000\nFIlINg STaTuS: New"));
    assertEquals(1, rows.size());
    assertEquals("ASML", rows.get(0).ticker);
    assertEquals("ASML Holding N.V. - ADS represents 1 ordinary share", rows.get(0).assetName);
  }

  /**
   * Real filing 20011886 (Hon. Mikie Sherrill, fetched and extracted live 2026-09-28): the
   * source reprints the column header ("... Amount Cap. Gains > $200?") at the top of every
   * continuation page. A row landing right after such a break had that reprinted header as a
   * literal prefix on its own asset name; only the document's first header occurrence (used to
   * find where the table body starts) is real, every later one is a repeat to be discarded the
   * same way a label's colon is discarded.
   */
  @Test void repeatedPageHeaderDoesNotBleedIntoAssetName() {
    List<Row> rows = HousePtrTextParser.parse(Arrays.asList(
        "$200?\nDiageo plc (DEO) [ST] S 05/28/2019 06/18/2019 $1,001 - $15,000\nFIlINg STaTuS: New\n"
        + "ID Owner Asset Transaction\nType\nDate Notification\nDate\nAmount Cap.\nGains >\n$200?\n"
        + "DNB ASA SPONSORED ADR Representing 10 (DNHBY) [ST] "
        + "S 05/28/2019 06/18/2019 $1,001 - $15,000\nFIlINg STaTuS: New"));
    assertEquals(2, rows.size());
    assertEquals("DNB ASA SPONSORED ADR Representing 10", rows.get(1).assetName);
    assertEquals("DNHBY", rows.get(1).ticker);
  }
}
