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
package org.apache.calcite.adapter.govdata.energy;

import org.apache.calcite.adapter.file.etl.SkippedBatchException;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests how {@link Eia861ArchiveProvider} locates a year's archive on the EIA-861 page. */
@Tag("unit")
class Eia861ArchiveProviderTest {

  /** The link forms of the live landing page: newest under zip/, older under archive/zip/. */
  private static final String PAGE = "<a href=\"zip/f8612025.zip\">ZIP</a>"
      + "<a href=\"archive/zip/f8612024.zip\">ZIP</a>"
      + "<a href=\"zip/f8612023er.zip\">Early release</a>"
      + "<a href=\"archive/zip/f8612022.zip\">ZIP</a>";

  @Test void readsEachYearsLocationFromThePage() throws Exception {
    assertEquals("zip/f8612025.zip", Eia861ArchiveProvider.archiveLink(PAGE, 2025));
    assertEquals("archive/zip/f8612024.zip", Eia861ArchiveProvider.archiveLink(PAGE, 2024));
    assertEquals("archive/zip/f8612022.zip", Eia861ArchiveProvider.archiveLink(PAGE, 2022));
  }

  @Test void skipsAYearThatIsNotPublishedYet() {
    assertThrows(SkippedBatchException.class,
        () -> Eia861ArchiveProvider.archiveLink(PAGE, 2026));
  }

  @Test void failsOnAMissingYearWithinThePublishedRange() {
    // 2023 is linked only as an early release, which is not the final archive.
    IOException e = assertThrows(IOException.class,
        () -> Eia861ArchiveProvider.archiveLink(PAGE, 2023));
    assertFalse(e instanceof SkippedBatchException);
  }

  @Test void failsOnAPageWithoutArchiveLinks() {
    IOException e = assertThrows(IOException.class,
        () -> Eia861ArchiveProvider.archiveLink("<html>moved</html>", 2025));
    assertFalse(e instanceof SkippedBatchException);
  }
}
