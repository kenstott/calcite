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
package org.apache.calcite.adapter.govdata.banking;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression for kenstott/govdata-ops#241: FFIEC's Cloudflare gate can return 200 OK with a
 * CAPTCHA HTML body instead of a zip. Before this fix, {@code openZip} accepted any 200 response
 * body as a zip stream; the failure only surfaced downstream in
 * {@code findAggregateA11Entry} as "no *_Aggr_A11.dat entry found", outside the retry loop, so
 * the intermittent gate never got the retry treatment that a real 403 does.
 *
 * <p>Covers the magic-byte check that now guards {@code openZip} before returning a
 * {@code ZipInputStream}.
 */
@Tag("unit")
class CraSmallBusinessLendingTransformerTest {

  @Test void detectsStandardLocalFileHeaderMagic() {
    // PK\x03\x04 - the standard first-record marker every non-empty zip starts with.
    assertTrue(CraSmallBusinessLendingTransformer.isZipMagic(
        new byte[] {(byte) 0x50, (byte) 0x4B, (byte) 0x03, (byte) 0x04}));
  }

  @Test void detectsEmptyArchiveEndOfCentralDirectoryMagic() {
    // PK\x05\x06 - end-of-central-directory signature (valid empty zip).
    assertTrue(CraSmallBusinessLendingTransformer.isZipMagic(
        new byte[] {(byte) 0x50, (byte) 0x4B, (byte) 0x05, (byte) 0x06}));
  }

  @Test void detectsSpannedArchiveMarker() {
    // PK\x07\x08 - spanned/split archive marker (valid, though unusual for FFIEC).
    assertTrue(CraSmallBusinessLendingTransformer.isZipMagic(
        new byte[] {(byte) 0x50, (byte) 0x4B, (byte) 0x07, (byte) 0x08}));
  }

  @Test void rejectsHtmlPrefixLikeFfiecCaptchaResponse() {
    // "<htm" - the actual first bytes an FFIEC CAPTCHA HTML response starts with.
    assertFalse(CraSmallBusinessLendingTransformer.isZipMagic(
        new byte[] {(byte) '<', (byte) 'h', (byte) 't', (byte) 'm'}));
  }

  @Test void rejectsAllZeroesEmptyResponse() {
    assertFalse(CraSmallBusinessLendingTransformer.isZipMagic(new byte[] {0, 0, 0, 0}));
  }

  @Test void rejectsPkPrefixWithWrongTrailer() {
    // PK\x99\x99 - starts with the two ASCII bytes of "PK" but no valid zip trailer.
    assertFalse(CraSmallBusinessLendingTransformer.isZipMagic(
        new byte[] {(byte) 0x50, (byte) 0x4B, (byte) 0x99, (byte) 0x99}));
  }
}
