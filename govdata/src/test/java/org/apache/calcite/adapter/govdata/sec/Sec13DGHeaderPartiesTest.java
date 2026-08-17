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
package org.apache.calcite.adapter.govdata.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Reads both parties of a Schedule 13D/G out of an EDGAR SGML submission header.
 *
 * <p>A 13D/G is one entity reporting ownership in another, so a row naming only one side does
 * not record the relationship it exists for. Before this parser, {@code subject_cik} was a local
 * declared null and never assigned — null on all 147,700 rows, which also left the table's
 * declared {@code sortOrder: [subject_cik, ...]} sorting on nothing — and {@code filer_cik} was
 * set to the filing CIK, which is usually the SUBJECT's rather than the filer's.
 */
@Tag("unit")
public class Sec13DGHeaderPartiesTest {

  /**
   * The header of accession 0001193125-19-159860, fetched verbatim from
   * {@code https://www.sec.gov/Archives/edgar/data/1731831/000119312519159860/}
   * {@code 0001193125-19-159860.hdr.sgml}, trimmed to the two party blocks.
   *
   * <p>This filing is the concrete counterexample to the old assumption: the filing CIK is
   * 0001731831 (Eidos Therapeutics — the SUBJECT), while the filer is BridgeBio Pharma,
   * 0001743881. The old code wrote 0001731831 into filer_cik and null into subject_cik, getting
   * both fields wrong from one filing.
   */
  private static final String REAL_HEADER =
      "ACCESSION NUMBER:\t\t0001193125-19-159860\n"
      + "CONFORMED SUBMISSION TYPE:\tSC 13D/A\n"
      + "FILED AS OF DATE:\t\t20190530\n"
      + "\n"
      + "SUBJECT COMPANY:\t\n"
      + "\n"
      + "\tCOMPANY DATA:\t\n"
      + "\t\tCOMPANY CONFORMED NAME:\t\t\tEidos Therapeutics, Inc.\n"
      + "\t\tCENTRAL INDEX KEY:\t\t\t0001731831\n"
      + "\t\tSTANDARD INDUSTRIAL CLASSIFICATION:\tPHARMACEUTICAL PREPARATIONS [2834]\n"
      + "\t\tIRS NUMBER:\t\t\t\t463733671\n"
      + "\n"
      + "\tFILING VALUES:\n"
      + "\t\tFORM TYPE:\t\tSC 13D/A\n"
      + "\t\tSEC FILE NUMBER:\t005-90540\n"
      + "\n"
      + "FILED BY:\t\t\n"
      + "\n"
      + "\tCOMPANY DATA:\t\n"
      + "\t\tCOMPANY CONFORMED NAME:\t\t\tBridgeBio Pharma, Inc.\n"
      + "\t\tCENTRAL INDEX KEY:\t\t\t0001743881\n"
      + "\t\tSTANDARD INDUSTRIAL CLASSIFICATION:\tPHARMACEUTICAL PREPARATIONS [2834]\n"
      + "\t\tIRS NUMBER:\t\t\t\t000000000\n";

  @Test void readsBothPartiesFromARealHeader() {
    assertArrayEquals(new String[] {"0001731831", "0001743881"},
        XbrlToParquetConverter.parseHeaderPartyCiks(REAL_HEADER),
        "subject is the issuer (Eidos), filer is the reporting owner (BridgeBio) — the filing"
            + " CIK 0001731831 is the SUBJECT, which is why filer_cik = cik was wrong");
  }

  @Test void doesNotLeakOneSectionsCikIntoTheOther() {
    // The bug this guards is subtler than a wrong regex: if the section boundary is not honoured,
    // the FILED BY lookup scans forward past its own block, or the SUBJECT lookup runs to the end
    // of the header and picks up the filer's key. Either way both fields get the same CIK and the
    // relationship silently collapses to self-ownership — which is what filer_cik = cik produced.
    String[] parties = XbrlToParquetConverter.parseHeaderPartyCiks(REAL_HEADER);
    org.junit.jupiter.api.Assertions.assertNotEquals(parties[0], parties[1],
        "the two parties of a 13D/G must never resolve to the same CIK");
  }

  @Test void handlesFiledByAppearingBeforeSubjectCompany() {
    // Block order is not guaranteed by EDGAR, and a parser that assumes SUBJECT-then-FILED-BY
    // would read the wrong block's key rather than fail visibly.
    int subjectAt = REAL_HEADER.indexOf("SUBJECT COMPANY:");
    int filerAt = REAL_HEADER.indexOf("FILED BY:");
    String swapped = REAL_HEADER.substring(filerAt) + "\n" + REAL_HEADER.substring(subjectAt, filerAt);

    assertArrayEquals(new String[] {"0001731831", "0001743881"},
        XbrlToParquetConverter.parseHeaderPartyCiks(swapped),
        "each CIK must follow its own label regardless of which block comes first");
  }

  @Test void returnsNullForAMissingPartyRatherThanTheOthersCik() {
    // Rule: an unknown counterparty is recoverable on a later run; a plausible-looking wrong CIK
    // is not, because nothing downstream can tell it from a real one.
    String subjectOnly = REAL_HEADER.substring(0, REAL_HEADER.indexOf("FILED BY:"));
    String[] parties = XbrlToParquetConverter.parseHeaderPartyCiks(subjectOnly);

    assertArrayEquals(new String[] {"0001731831", null}, parties,
        "absent FILED BY block must yield null, never the subject's CIK");
  }

  @Test void returnsNullsForAHeaderWithNoPartyBlocks() {
    String[] parties = XbrlToParquetConverter.parseHeaderPartyCiks(
        "ACCESSION NUMBER:\t\t0001193125-19-159860\nCONFORMED SUBMISSION TYPE:\tSC 13D/A\n");
    assertNull(parties[0], "no SUBJECT COMPANY block means no subject CIK");
    assertNull(parties[1], "no FILED BY block means no filer CIK");
  }

  @Test void zeroPadsAShortCikToTenDigits() {
    // EDGAR writes some keys unpadded; the cik column is 10-digit zero-padded throughout, and an
    // unpadded value would silently fail every join against it.
    String header = "SUBJECT COMPANY:\n\t\tCENTRAL INDEX KEY:\t\t\t320193\n"
        + "FILED BY:\n\t\tCENTRAL INDEX KEY:\t\t\t1067983\n";
    assertArrayEquals(new String[] {"0000320193", "0001067983"},
        XbrlToParquetConverter.parseHeaderPartyCiks(header));
  }
}
