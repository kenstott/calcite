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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** A zero-byte document in the unpacked feed is not held, so it is fetched rather than parsed empty. */
@Tag("unit")
class SecFeedStoreEmptyDocumentTest {

  private static final String BASE = "https://www.sec.gov/Archives/edgar/data/1387135/";
  private static final String ACC = "000161577415002765";

  @AfterEach void uninstall() {
    SecFeedStore.uninstall();
  }

  @Test void emptyFileIsNotHeldAndNonEmptyIs(@TempDir File root) throws Exception {
    File dir = new File(root, ACC);
    Files.createDirectories(dir.toPath());
    Files.write(new File(dir, "q_cal.xml").toPath(), new byte[0]);
    Files.write(new File(dir, "q_pre.xml").toPath(), "<a/>".getBytes(StandardCharsets.UTF_8));
    SecFeedStore.install(root.getAbsolutePath());
    assertNull(SecFeedStore.bytes(BASE + ACC + "/q_cal.xml"));
    assertArrayEquals("<a/>".getBytes(StandardCharsets.UTF_8),
        SecFeedStore.bytes(BASE + ACC + "/q_pre.xml"));
  }
}
