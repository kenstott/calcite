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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A combined Schedule TO/13D is submitted as {@code SC TO-T/A} (or another Schedule TO type)
 * while its party blocks carry {@code SC 13D/A}; the document has no 13D cover page, so its
 * figures must not be read from it.
 */
@Tag("unit")
public class Sec13DGCombinedTenderOfferTest {

  /** Header of accession 0001104659-26-013052, trimmed to the type lines. */
  private static final String COMBINED_HEADER =
      "<SEC-HEADER>0001104659-26-013052.hdr.sgml : 20260211\n"
      + "<ACCESSION-NUMBER>0001104659-26-013052\n"
      + "<TYPE>SC TO-T/A\n"
      + "<SUBJECT-COMPANY>\n"
      + "<FILING-VALUES>\n"
      + "<FORM-TYPE>SC 13D/A\n";

  @Test void tenderOfferSubmissionTypeIsCombined() {
    assertTrue(XbrlToParquetConverter.isCombinedTenderOffer(COMBINED_HEADER));
  }

  @Test void plainThirteenDIsNotCombined() {
    assertFalse(XbrlToParquetConverter.isCombinedTenderOffer(
        "<ACCESSION-NUMBER>0001193125-19-159860\n<TYPE>SC 13D/A\n<FORM-TYPE>SC 13D/A\n"));
  }

  @Test void formTypeInPartyBlockAloneDoesNotCount() {
    assertFalse(XbrlToParquetConverter.isCombinedTenderOffer(
        "<TYPE>SC 13G/A\n<FORM-TYPE>SC TO-T/A\n"));
  }

  @Test void missingHeaderIsNotCombined() {
    assertFalse(XbrlToParquetConverter.isCombinedTenderOffer(null));
  }
}
