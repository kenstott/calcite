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
package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Remaining-lifetime arithmetic behind {@link R2CredentialProvider#resolveOrFetch(String, long)}. */
@Tag("unit")
class R2CredentialProviderTest {

  private static Map<String, String> expiringIn(long millis) {
    Map<String, String> creds = new HashMap<>();
    creds.put("expiresAtMillis", Long.toString(System.currentTimeMillis() + millis));
    return creds;
  }

  /** A set with eight minutes left must not satisfy a thirty-minute minimum -- the spawn that
   *  took such a set hung mid-walk on 403s once it expired. */
  @Test void shortLivedSetFallsBelowMinimum() {
    long remaining = R2CredentialProvider.remainingMillis(expiringIn(8L * 60_000L));
    assertTrue(remaining < 30L * 60_000L, "remaining " + remaining);
    assertTrue(remaining > 6L * 60_000L, "remaining " + remaining);
  }

  @Test void freshSetMeetsMinimum() {
    assertTrue(R2CredentialProvider.remainingMillis(expiringIn(60L * 60_000L))
        >= 30L * 60_000L);
  }

  @Test void unstampedSetNeverExpires() {
    assertEquals(Long.MAX_VALUE, R2CredentialProvider.remainingMillis(new HashMap<>()));
  }

  @Test void unparsableStampHasNoLifetime() {
    Map<String, String> creds = new HashMap<>();
    creds.put("expiresAtMillis", "soon");
    assertEquals(0L, R2CredentialProvider.remainingMillis(creds));
  }
}
