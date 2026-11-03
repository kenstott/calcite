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

/** The SEC User-Agent comes from the schema YAML when the model does not name one. */
@Tag("unit")
class EdgarUserAgentTest {

  @Test void aModelWithoutEdgarUserAgentGetsTheSchemaYamlValue() {
    String ua = EdgarRateLimiter.userAgent();
    assertFalse(ua.isEmpty());
    assertFalse(ua.contains("${"), ua);
    assertTrue(ua.contains("@"), "SEC requires a contact: " + ua);
  }
}
