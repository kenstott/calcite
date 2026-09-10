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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link SecretRedaction}. */
@Tag("unit")
public class SecretRedactionTest {

  /** The exact shape found in 731 production tracker rows. */
  @Test void redactsNassQueryStringKey() {
    String actual = SecretRedaction.redact(
        "HTTP 403 from https://quickstats.nass.usda.gov/api/api_GET/?key=ABCD-1234-EFGH-5678"
            + "&commodity_desc=CORN&year=2019");
    assertFalse(actual.contains("ABCD-1234-EFGH-5678"), "key value must not survive");
    assertTrue(actual.contains("key=<redacted>"), "parameter name is kept for diagnosis");
    assertTrue(actual.contains("commodity_desc=CORN"), "non-secret params are untouched");
    assertTrue(actual.contains("year=2019"), "trailing params survive redaction");
  }

  @Test void redactsCensusStyleApiKeyParam() {
    String actual =
        SecretRedaction.redact("https://api.census.gov/data/2020/dec?get=NAME&key=deadbeefcafe");
    assertEquals("https://api.census.gov/data/2020/dec?get=NAME&key=<redacted>", actual);
  }

  @Test void redactsEachAliasAndIsCaseInsensitive() {
    String[] params = {"api_key", "api-key", "apikey", "key", "token", "access_token",
        "auth", "secret", "password", "signature"};
    for (String p : params) {
      String actual = SecretRedaction.redact("https://h/x?" + p.toUpperCase() + "=s3cr3tvalue");
      assertFalse(actual.contains("s3cr3tvalue"), p + " must be redacted regardless of case");
    }
  }

  @Test void redactsHeaderStyleCredentials() {
    assertFalse(SecretRedaction.redact("X-Api-Key: abc123xyz").contains("abc123xyz"));
    assertFalse(SecretRedaction.redact("Authorization: Bearer tok_live_9").contains("tok_live_9"));
  }

  /**
   * Matching is anchored on the whole parameter name. A name merely ENDING in "key" is ordinary
   * data — redacting it would quietly destroy the diagnostic this column exists to carry.
   */
  @Test void leavesNonCredentialParamsAlone() {
    String url = "https://h/x?monkey=curious&sortkey=asc&keyword=corn&primary_key=7";
    assertEquals(url, SecretRedaction.redact(url));
  }

  @Test void leavesOrdinaryErrorTextAlone() {
    String msg = "HTTP 500 from https://h/x?state_alpha=NE&year=2019";
    assertEquals(msg, SecretRedaction.redact(msg));
  }

  @Test void handlesNullAndEmpty() {
    assertEquals(null, SecretRedaction.redact(null));
    assertEquals("", SecretRedaction.redact(""));
  }

  @Test void redactsEveryOccurrenceNotJustTheFirst() {
    String actual = SecretRedaction.redact("first https://h/a?key=aaa then https://h/b?key=bbb");
    assertFalse(actual.contains("aaa"), "first occurrence redacted");
    assertFalse(actual.contains("bbb"), "second occurrence redacted too");
  }

  /** A key at the very end of the string has no trailing delimiter to anchor on. */
  @Test void redactsKeyAtEndOfString() {
    assertFalse(SecretRedaction.redact("https://h/x?a=1&key=tail").contains("tail"));
  }
}
