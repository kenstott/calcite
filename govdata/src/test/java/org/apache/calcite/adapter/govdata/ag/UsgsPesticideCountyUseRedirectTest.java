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
package org.apache.calcite.adapter.govdata.ag;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("unit")
class UsgsPesticideCountyUseRedirectTest {

  /** Plain HTTP gets a 403 from ScienceBase, so the item URL must be returned unrequested. */
  @Test void scienceBaseUrlEndsTheRedirectChain() throws IOException {
    String item = "https://www.sciencebase.gov/catalog/item/5e95c12282ce172707f2524e";
    assertEquals(item, UsgsPesticideCountyUseProvider.followRedirect(item, "test"));
  }
}
