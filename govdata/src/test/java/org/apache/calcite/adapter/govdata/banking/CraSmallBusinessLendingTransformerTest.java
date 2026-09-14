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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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

  /**
   * Regression for kenstott/govdata-ops#241's follow-up: the volume-triggered CAPTCHA gate
   * (confirmed live - a burst of back-to-back requests trips it on every request, including
   * years that had each individually just succeeded) cannot be out-waited by the original
   * few-second linear backoff, since Cloudflare's own challenge-cookie default lifetime is far
   * longer. A CAPTCHA-classified failure must always get the longer, fixed backoff regardless
   * of which attempt number it is; any other transient IOException keeps the original linear
   * schedule.
   */
  @Test void captchaClassifiedFailureAlwaysGetsTheLongerFixedBackoff() {
    IOException captcha = new CraSmallBusinessLendingTransformer.FfiecCaptchaException(
        "CRA aggregate download returned non-zip body");

    long backoffAttempt1 = CraSmallBusinessLendingTransformer.backoffForRetry(captcha, 1);
    long backoffAttempt4 = CraSmallBusinessLendingTransformer.backoffForRetry(captcha, 4);

    assertEquals(backoffAttempt1, backoffAttempt4,
        "a CAPTCHA-classified failure's backoff must not scale with attempt number - "
            + "retrying sooner cannot succeed against a still-active challenge");
    assertTrue(backoffAttempt1 > 5_000L,
        "CAPTCHA backoff must be meaningfully longer than the original few-second linear "
            + "backoff, since that window is known too short to out-wait the challenge: "
            + backoffAttempt1);
  }

  @Test void genericTransientFailureKeepsTheOriginalLinearBackoff() {
    IOException generic = new IOException("CRA aggregate download HTTP 500");

    long backoffAttempt1 = CraSmallBusinessLendingTransformer.backoffForRetry(generic, 1);
    long backoffAttempt2 = CraSmallBusinessLendingTransformer.backoffForRetry(generic, 2);

    assertTrue(backoffAttempt2 > backoffAttempt1,
        "a generic transient failure must still scale backoff with attempt number: "
            + backoffAttempt1 + " then " + backoffAttempt2);
  }

  // ── Member-name regression (kenstott/govdata-ops#241, second root cause) ────────────────
  // Pre-2016 expanded-aggregate zips don't carry a per-table cra{year}_Aggr_A11.dat member;
  // they carry one combined exp_aggr.dat holding every aggregate table (A1-1, A1-1a, A1-2*,
  // A2-*) concatenated. Confirmed live against the real 2011 and 2013 zips. The member
  // matcher must accept both naming eras, and the line filter must keep only A1-1 records.

  /** Builds an in-memory zip of (name, content) pairs. */
  private static ZipInputStream zipOf(String... nameContentPairs) throws IOException {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    try (ZipOutputStream zos = new ZipOutputStream(buf)) {
      for (int i = 0; i < nameContentPairs.length; i += 2) {
        zos.putNextEntry(new ZipEntry(nameContentPairs[i]));
        zos.write(nameContentPairs[i + 1].getBytes(StandardCharsets.ISO_8859_1));
        zos.closeEntry();
      }
    }
    return new ZipInputStream(new ByteArrayInputStream(buf.toByteArray()));
  }

  @Test void modernPerTableMemberIsSelected() throws IOException {
    ZipInputStream zis = zipOf("cra2016_Aggr_A11.dat", "whatever");
    ZipEntry found = CraSmallBusinessLendingTransformer.findAggregateA11Entry(zis, "test");
    assertEquals("cra2016_Aggr_A11.dat", found.getName());
  }

  @Test void legacyCombinedMemberIsSelected() throws IOException {
    ZipInputStream zis = zipOf("exp_aggr.dat", "whatever");
    ZipEntry found = CraSmallBusinessLendingTransformer.findAggregateA11Entry(zis, "test");
    assertEquals("exp_aggr.dat", found.getName());
  }

  @Test void unrelatedMemberNamesReturnNull() throws IOException {
    ZipInputStream zis = zipOf("cra2016_Aggr_A12.dat", "x", "readme.txt", "y");
    assertNull(CraSmallBusinessLendingTransformer.findAggregateA11Entry(zis, "test"),
        "a zip with no A1-1 member under either naming era must report no match");
  }

  @Test void a11LineFilterKeepsOnlyTheA11Table() {
    // Real table_id prefixes from the 2013 exp_aggr.dat: A1-1 (wanted), A1-1a (sibling
    // table - only the trailing space distinguishes it), A1-2, A2-1 (other tables).
    assertTrue(CraSmallBusinessLendingTransformer.isAggregateA11Line(
        "A1-1 20134148059101800301.01NS103"));
    assertFalse(CraSmallBusinessLendingTransformer.isAggregateA11Line(
        "A1-1a20134148059101800301.01NS103"));
    assertFalse(CraSmallBusinessLendingTransformer.isAggregateA11Line(
        "A1-2 20134148059101800301.01NS106"));
    assertFalse(CraSmallBusinessLendingTransformer.isAggregateA11Line(
        "A2-1 2013414805910180"));
  }

  @Test void parsesRealLegacyA11Record() {
    // First A1-1 record of the real 2013 exp_aggr.dat (activity year 2013, 116 chars):
    // Texas (48), county 059, Abilene MSA 10180, tract 0301.01, middle income (103),
    // tract-x-income-group grain (blank report_level).
    String line =
        "A1-1 20134148059101800301.01NS103   0000000058000000110000000000050000001005"
            + "0000000004000000207800000000240000000543";
    assertEquals(116, line.length(), "A1-1 records are 116 chars - fixture drifted");

    Map<String, Object> row = CraSmallBusinessLendingTransformer.parseLine(line);
    assertEquals(2013, row.get("activity_year"));
    assertEquals("48", row.get("state_fips"));
    assertEquals("059", row.get("county_code"));
    assertEquals("48059", row.get("county_fips"));
    assertEquals("10180", row.get("msa_md"));
    assertEquals("0301.01", row.get("census_tract"));
    assertEquals("N", row.get("split_county"));
    assertEquals("S", row.get("population_classification"));
    assertEquals("103", row.get("income_group_total"));
    assertNull(row.get("report_level"), "blank report_level = tract-x-income-group grain");
    assertEquals(58L, row.get("loans_lt_100k_count"));
    assertEquals(1100L, row.get("loans_lt_100k_amount"));
    assertEquals(5L, row.get("loans_100k_to_250k_count"));
    assertEquals(1005L, row.get("loans_100k_to_250k_amount"));
    assertEquals(4L, row.get("loans_250k_to_1m_count"));
    assertEquals(2078L, row.get("loans_250k_to_1m_amount"));
    assertEquals(24L, row.get("loans_to_small_biz_lt_1m_rev_count"));
    assertEquals(543L, row.get("loans_to_small_biz_lt_1m_rev_amount"));
  }
}
