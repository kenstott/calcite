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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** How the feedDir setting decides whether filing documents are read locally or fetched. */
@Tag("unit")
class SecFeedStoreConfigTest {

  private static final String ACC = "000143774924007444";
  private static final String URL =
      "https://www.sec.gov/Archives/edgar/data/1132651/" + ACC + "/doc.xml";

  @AfterEach void uninstall() {
    SecFeedStore.uninstall();
  }

  @Test void aConfiguredTreeAnswersADocumentWithoutAnHttpCall(@TempDir File root)
      throws Exception {
    File dir = new File(root, ACC);
    assertTrue(dir.mkdirs());
    Files.write(new File(dir, "doc.xml").toPath(), "<a/>".getBytes(StandardCharsets.UTF_8));
    SecFeedStore.install(root.getAbsolutePath());
    assertEquals("<a/>", SecFeedStore.text(URL));
    assertNull(SecFeedStore.text(URL.replace(ACC, "000143774924999999")));
  }

  @Test void aConfiguredButMissingDirectoryFailsByName(@TempDir File root) {
    String missing = new File(root, "no-such-feed").getAbsolutePath();
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> SecFeedStore.install(missing));
    assertTrue(e.getMessage().contains(missing), e.getMessage());
  }

  @Test void anEmptyFeedDirFetchesLive() throws Exception {
    SecFeedStore.install("");
    assertNull(SecFeedStore.bytes(URL));
  }

  /** The schema ships in the pgwire-govdata bundle too, where a worker-only default path made
   *  the sec schema, and the whole data server, fail to start (release 0.124.5). */
  @Test void theSchemaYamlConfiguresNoFeedByDefault() {
    assertEquals("${SEC_FEED_DIR:}",
        org.apache.calcite.adapter.govdata.GovDataUtils.loadSchemaSetting(
            SecSchemaFactory.class, "/sec/sec-schema.yaml", "feedDir"));
  }

  /** The ETL worker still reads the on-disk feed: its launch scripts set SEC_FEED_DIR. */
  @Test void theWorkerScriptsPointAtTheOnDiskFeed() throws Exception {
    java.nio.file.Path common = java.nio.file.Paths.get(
        SecFeedStoreConfigTest.class.getResource("/sec/sec-schema.yaml").toURI())
        .getParent().getParent().getParent().getParent().getParent()
        .resolve("scripts/parallel/common.sh");
    String text = new String(java.nio.file.Files.readAllBytes(common),
        java.nio.charset.StandardCharsets.UTF_8);
    assertTrue(text.contains(
        "export SEC_FEED_DIR=\"${SEC_FEED_DIR-/mnt/wsltmp/govdata/edgar-feed}\""), common.toString());
  }
}
