/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.storage.S3StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.etl.FilingMetadataPatchJob;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Runs {@link FilingMetadataPatchJob} against Cloudflare R2.
 *
 * <p>Checkpoints are written to {@code /tmp/filing_patch_checkpoints/patch_<year>.ckpt}.
 * Re-running any test resumes from where it left off — already-processed paths are skipped.
 *
 * <p>Invoke via Gradle:
 * <pre>
 *   R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... \
 *     ./gradlew :govdata:test -PincludeTags=integration \
 *     --tests "*FilingMetadataPatchJobRunner.testPatchYear2023*"
 * </pre>
 */
class FilingMetadataPatchJobRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(FilingMetadataPatchJobRunner.class);

  private static final String R2_ENDPOINT =
      "https://21cd637936a05913431a608f3f6d73bb.r2.cloudflarestorage.com";
  private static final String BUCKET = "govdata-parquet-v1";

  /** Apple (0000320193), Microsoft (0000789019), Amazon (0001018724). */
  private static final String SAMPLE_CIKS = "0000320193,0000789019,0001018724";

  private static final File CHECKPOINT_DIR = new File(
      System.getProperty("user.home"), "filing_patch_checkpoints");

  private StorageProvider buildStorage() {
    String accessKeyId = System.getenv("R2_ACCESS_KEY_ID");
    String secretAccessKey = System.getenv("R2_SECRET_ACCESS_KEY");
    if (accessKeyId == null || secretAccessKey == null) {
      throw new IllegalStateException(
          "Set R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY environment variables");
    }
    Map<String, Object> config = new HashMap<>();
    config.put("accessKeyId", accessKeyId);
    config.put("secretAccessKey", secretAccessKey);
    config.put("endpoint", R2_ENDPOINT);
    config.put("region", "auto");
    config.put("directory", "s3://" + BUCKET + "/");
    return new S3StorageProvider(config);
  }

  @Test
  @Tag("integration")
  void testPatchSampleCiksDryRun() throws Exception {
    FilingMetadataPatchJob job = new FilingMetadataPatchJob(buildStorage(), true);
    job.patchSampleCiks(Arrays.asList(SAMPLE_CIKS.split(",")));
    LOGGER.info("Dry-run complete: scanned={} patched={} skipped={}",
        job.getFilesScanned(), job.getFilesPatched(), job.getFilesSkipped());
  }

  @Test
  @Tag("integration")
  void testPatchSampleCiksLive() throws Exception {
    FilingMetadataPatchJob job = new FilingMetadataPatchJob(buildStorage(), false);
    job.patchSampleCiks(Arrays.asList(SAMPLE_CIKS.split(",")));
    LOGGER.info("Live patch complete: scanned={} patched={} skipped={}",
        job.getFilesScanned(), job.getFilesPatched(), job.getFilesSkipped());
  }

  @Test
  @Tag("integration")
  void testPatchYear2010() throws Exception {
    patchYear("2010");
  }

  @Test
  @Tag("integration")
  void testPatchYear2011() throws Exception {
    patchYear("2011");
  }

  @Test
  @Tag("integration")
  void testPatchYear2012() throws Exception {
    patchYear("2012");
  }

  @Test
  @Tag("integration")
  void testPatchYear2013() throws Exception {
    patchYear("2013");
  }

  @Test
  @Tag("integration")
  void testPatchYear2014() throws Exception {
    patchYear("2014");
  }

  @Test
  @Tag("integration")
  void testPatchYear2015() throws Exception {
    patchYear("2015");
  }

  @Test
  @Tag("integration")
  void testPatchYear2016() throws Exception {
    patchYear("2016");
  }

  @Test
  @Tag("integration")
  void testPatchYear2017() throws Exception {
    patchYear("2017");
  }

  @Test
  @Tag("integration")
  void testPatchYear2018() throws Exception {
    patchYear("2018");
  }

  @Test
  @Tag("integration")
  void testPatchYear2019() throws Exception {
    patchYear("2019");
  }

  @Test
  @Tag("integration")
  void testPatchYear2020() throws Exception {
    patchYear("2020");
  }

  @Test
  @Tag("integration")
  void testPatchYear2021() throws Exception {
    patchYear("2021");
  }

  @Test
  @Tag("integration")
  void testPatchYear2022() throws Exception {
    patchYear("2022");
  }

  @Test
  @Tag("integration")
  void testPatchYear2023() throws Exception {
    patchYear("2023");
  }

  @Test
  @Tag("integration")
  void testPatchYear2024() throws Exception {
    patchYear("2024");
  }

  @Test
  @Tag("integration")
  void testPatchYear2025() throws Exception {
    patchYear("2025");
  }

  @Test
  @Tag("integration")
  void testPatchYear2026() throws Exception {
    patchYear("2026");
  }

  private void patchYear(String year) throws Exception {
    FilingMetadataPatchJob job =
        new FilingMetadataPatchJob(buildStorage(), false, CHECKPOINT_DIR);
    job.patchYear(year);
    LOGGER.info("Year={} patch complete: scanned={} patched={} skipped={}",
        year, job.getFilesScanned(), job.getFilesPatched(), job.getFilesSkipped());
  }
}
