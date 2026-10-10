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
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The data server signs every connection in with its API key as the password. A refusal is
 * told apart by its SQLSTATE from a server that is merely not ready, and named for the user.
 */
@Tag("unit")
class PgwireGovDataConnectorSignInTest {

  @Test void aRefusedKeyIsNamedAndIsNotAServerStillStarting() {
    String refusal = PgwireGovDataConnector.signInRefusal("28P01");
    assertNotNull(refusal);
    assertTrue(refusal.contains("API key"), refusal);
    assertTrue(refusal.contains("ASKAMERICA_API_KEY"), refusal);
  }

  @Test void aKeyServiceThatDidNotAnswerSaysToTryAgain() {
    String refusal = PgwireGovDataConnector.signInRefusal("08006");
    assertNotNull(refusal);
    assertTrue(refusal.contains("key service"), refusal);
  }

  @Test void aLockedOutKeySaysSo() {
    assertNotNull(PgwireGovDataConnector.signInRefusal("28000"));
  }

  @Test void anyOtherErrorIsNotASignInRefusal() {
    assertNull(PgwireGovDataConnector.signInRefusal("57P03"));
    assertNull(PgwireGovDataConnector.signInRefusal("XX000"));
    assertNull(PgwireGovDataConnector.signInRefusal(null));
  }
}
