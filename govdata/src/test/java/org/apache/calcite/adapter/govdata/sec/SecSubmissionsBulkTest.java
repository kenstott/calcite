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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The nightly submissions.zip answers the per-filer submissions URLs, and only those. */
@Tag("unit")
class SecSubmissionsBulkTest {

  private static File zip(File dir) throws Exception {
    File f = new File(dir, "submissions.zip");
    try (ZipOutputStream z = new ZipOutputStream(new FileOutputStream(f))) {
      put(z, "CIK0000019617.json", "{\"name\":\"JPMORGAN CHASE & CO\",\"filings\":{}}");
      put(z, "CIK0000019617-submissions-001.json", "{\"accessionNumber\":[\"a\"]}");
      put(z, "placeholder.txt", "not a filer");
    }
    return f;
  }

  private static void put(ZipOutputStream z, String name, String body) throws Exception {
    z.putNextEntry(new ZipEntry(name));
    z.write(body.getBytes(StandardCharsets.UTF_8));
    z.closeEntry();
  }

  @Test void aFilerAndItsOlderPagesAreAnsweredFromTheFile(@TempDir File dir) throws Exception {
    try (SecSubmissionsBulk bulk = SecSubmissionsBulk.open(zip(dir))) {
      assertEquals("{\"name\":\"JPMORGAN CHASE & CO\",\"filings\":{}}",
          bulk.resolve("https://data.sec.gov/submissions/CIK0000019617.json"));
      assertEquals("{\"accessionNumber\":[\"a\"]}",
          bulk.resolve("https://data.sec.gov/submissions/CIK0000019617-submissions-001.json"));
      assertEquals(2, bulk.hits());
    }
  }

  @Test void aFilerNotInTheFileIsNullSoTheCallerFetchesIt(@TempDir File dir) throws Exception {
    try (SecSubmissionsBulk bulk = SecSubmissionsBulk.open(zip(dir))) {
      assertNull(bulk.resolve("https://data.sec.gov/submissions/CIK0000000002.json"));
      assertEquals(1, bulk.misses());
    }
  }

  @Test void otherUrlsAndOtherEntriesAreNeverServed(@TempDir File dir) throws Exception {
    try (SecSubmissionsBulk bulk = SecSubmissionsBulk.open(zip(dir))) {
      assertNull(bulk.resolve("https://www.sec.gov/Archives/edgar/data/19617/x.json"));
      assertNull(bulk.resolve("https://data.sec.gov/submissions/placeholder.txt"));
      assertNull(bulk.resolve("https://data.sec.gov/submissions/../CIK0000019617.json"));
      assertEquals(0, bulk.hits());
    }
  }

  @Test void aConfiguredFileThatIsMissingIsAnError(@TempDir File dir) {
    assertThrows(IllegalStateException.class,
        () -> SecSubmissionsBulk.install(new File(dir, "absent.zip").getAbsolutePath()));
  }
}
