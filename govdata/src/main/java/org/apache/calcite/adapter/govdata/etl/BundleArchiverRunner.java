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
package org.apache.calcite.adapter.govdata.etl;

import org.apache.calcite.adapter.file.etl.cache.BundleArchiver;
import org.apache.calcite.adapter.file.storage.S3StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Standalone runner for {@link BundleArchiver}.
 *
 * <p>Uploads a local raw cache directory to S3/R2 without running a full ETL.
 * Useful for retrying a failed archive after a network timeout.
 *
 * <p>Usage:
 * <pre>
 *   java -cp calcite-govdata-*-all.jar \
 *     org.apache.calcite.adapter.govdata.etl.BundleArchiverRunner \
 *     --schema edu \
 *     --local-cache-dir /path/to/.aperio/edu/cache/raw \
 *     --cache-directory s3://govdata-raw-v1/edu \
 *     [--bundle-id run-20260509T2130]
 * </pre>
 *
 * <p>Required environment variables:
 * <ul>
 *   <li>{@code AWS_ACCESS_KEY_ID}</li>
 *   <li>{@code AWS_SECRET_ACCESS_KEY}</li>
 *   <li>{@code AWS_ENDPOINT_OVERRIDE}</li>
 * </ul>
 */
public class BundleArchiverRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(BundleArchiverRunner.class);

  private BundleArchiverRunner() {
  }

  public static void main(String[] args) throws Exception {
    String schema = null;
    String localCacheDir = null;
    String cacheDirectory = null;
    String bundleId = null;

    for (int i = 0; i < args.length - 1; i++) {
      switch (args[i]) {
      case "--schema":
        schema = args[++i];
        break;
      case "--local-cache-dir":
        localCacheDir = args[++i];
        break;
      case "--cache-directory":
        cacheDirectory = args[++i];
        break;
      case "--bundle-id":
        bundleId = args[++i];
        break;
      default:
        break;
      }
    }

    if (schema == null || localCacheDir == null || cacheDirectory == null) {
      LOGGER.error("Usage: BundleArchiverRunner"
          + " --schema <name>"
          + " --local-cache-dir <path>"
          + " --cache-directory <s3://bucket/prefix>"
          + " [--bundle-id <id>]"
          + " | Required env vars: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_OVERRIDE");
      System.exit(1);
    }

    String accessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
    String secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");

    if (accessKeyId == null || secretAccessKey == null || endpoint == null) {
      LOGGER.error("Missing required env vars: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_OVERRIDE");
      System.exit(1);
    }

    if (bundleId == null) {
      bundleId = "run-" + new SimpleDateFormat("yyyyMMdd'T'HHmm").format(new Date());
    }

    Map<String, Object> s3Config = new HashMap<String, Object>();
    s3Config.put("accessKeyId", accessKeyId);
    s3Config.put("secretAccessKey", secretAccessKey);
    s3Config.put("endpoint", endpoint);
    s3Config.put("region", "auto");
    s3Config.put("directory", cacheDirectory);

    StorageProvider storageProvider = new S3StorageProvider(s3Config);

    LOGGER.info("Archiving {} -> {} (bundle: {})", localCacheDir, cacheDirectory, bundleId);
    BundleArchiver.archive(localCacheDir, storageProvider, schema, bundleId);
    LOGGER.info("Done.");
  }
}
