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
package org.apache.calcite.adapter.govdata.fiscal;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests resolving the current SNAP zip from the FNS landing page. */
@Tag("unit")
class SnapBenefitsProviderTest {

  @Test void picksHighestReleaseNumber() throws IOException {
    String html = "<a href=\"/sites/default/files/resource-files/snap-zip-fy69tocurrent-9.zip\">"
        + "<a href=\"/sites/default/files/resource-files/snap-zip-fy69tocurrent-10.zip\">";
    assertEquals(
        "https://www.fna.usda.gov/sites/default/files/resource-files/"
            + "snap-zip-fy69tocurrent-10.zip",
        SnapBenefitsProvider.currentZipUrl(html));
  }

  @Test void missingLinkFails() {
    assertThrows(IOException.class, () -> SnapBenefitsProvider.currentZipUrl("<html></html>"));
  }
}
