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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * sec.gov renders FilingSummary.xml only for a filing carrying XBRL, so the feed store answers a
 * request for it as absent when it holds an accession with none, and leaves every doubtful case
 * to be fetched.
 */
@Tag("unit")
class SecFeedStoreKnownAbsentTest {

  private static final String BASE = "https://www.sec.gov/Archives/edgar/data/1132651/";
  private static final String ACC = "000143774924007444";

  @AfterEach void uninstall() {
    SecFeedStore.uninstall();
  }

  private static void accession(File root, String acc, String types, String htmlName,
      String html) throws IOException {
    File dir = new File(root, acc);
    assertTrue(dir.mkdirs());
    StringBuilder items = new StringBuilder();
    for (String t : types.split(",")) {
      if (items.length() > 0) {
        items.append(',');
      }
      items.append("{\"name\":\"d-").append(items.length()).append("\",\"type\":\"")
          .append(t).append("\",\"size\":\"1\"}");
    }
    Files.write(new File(dir, "index.json").toPath(),
        ("{\"directory\":{\"name\":\"x\",\"item\":[" + items + "]}}")
            .getBytes(StandardCharsets.UTF_8));
    if (htmlName != null) {
      Files.write(new File(dir, htmlName).toPath(), html.getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test void aHeldFilingWithNoXbrlHasNoFilingSummary(@TempDir File root) throws Exception {
    accession(root, ACC, "DEF 14A,GRAPHIC", "def14a.htm", "<html><body>proxy</body></html>");
    SecFeedStore.install(root.getAbsolutePath());
    assertTrue(SecFeedStore.knownAbsent(BASE + ACC + "/FilingSummary.xml"));
  }

  @Test void aFilingWithAnXbrlExhibitMayHaveOne(@TempDir File root) throws Exception {
    accession(root, ACC, "10-K,EX-101.INS", "k.htm", "<html></html>");
    SecFeedStore.install(root.getAbsolutePath());
    assertFalse(SecFeedStore.knownAbsent(BASE + ACC + "/FilingSummary.xml"));
  }

  @Test void aFilingWithAnInlineXbrlHeaderMayHaveOne(@TempDir File root) throws Exception {
    StringBuilder pad = new StringBuilder();
    for (int i = 0; i < 150000; i++) { // 1.5 MB: the header lies beyond the first 1 MB read
      pad.append("0123456789");
    }
    accession(root, ACC, "10-Q", "q.htm", pad + "<body><IX:Header>x</ix:header>");
    SecFeedStore.install(root.getAbsolutePath());
    assertFalse(SecFeedStore.knownAbsent(BASE + ACC + "/FilingSummary.xml"));
  }

  @Test void anAccessionTheTreeDoesNotHoldIsLeftToBeFetched(@TempDir File root) throws Exception {
    accession(root, ACC, "DEF 14A", "def14a.htm", "<html></html>");
    SecFeedStore.install(root.getAbsolutePath());
    assertFalse(SecFeedStore.knownAbsent(BASE + "000000000024000001/FilingSummary.xml"));
  }

  @Test void aFilingSummaryTheFeedCarriesIsServedNotCalledAbsent(@TempDir File root)
      throws Exception {
    accession(root, ACC, "8-K", "e.htm", "<html></html>");
    Files.write(new File(root, ACC + "/FilingSummary.xml").toPath(),
        "<FilingSummary/>".getBytes(StandardCharsets.UTF_8));
    SecFeedStore.install(root.getAbsolutePath());
    assertFalse(SecFeedStore.knownAbsent(BASE + ACC + "/FilingSummary.xml"));
  }

  @Test void otherUrlsAndNoStoreAreNeverAbsent(@TempDir File root) throws Exception {
    assertFalse(SecFeedStore.knownAbsent(BASE + ACC + "/FilingSummary.xml"));
    accession(root, ACC, "DEF 14A", "def14a.htm", "<html></html>");
    SecFeedStore.install(root.getAbsolutePath());
    assertFalse(SecFeedStore.knownAbsent(BASE + ACC + "/R1.htm"));
    assertFalse(SecFeedStore.knownAbsent("https://data.sec.gov/submissions/CIK0000019617.json"));
  }
}
