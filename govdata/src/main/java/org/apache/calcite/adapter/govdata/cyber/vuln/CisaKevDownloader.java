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
package org.apache.calcite.adapter.govdata.cyber.vuln;

import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.govdata.cyber.AbstractCyberDownloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Downloads the CISA Known Exploited Vulnerabilities (KEV) catalog and writes parquet.
 *
 * <p>Source: {@code https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json}
 * — a single JSON file with the full catalog.
 *
 * <p>The JSON is passed to {@link CisaKevResponseTransformer} which produces a flat
 * JSON array keyed on {@code cve_id}, then written to
 * {@code {parquetDirectory}/type=kev_catalog.parquet}.
 *
 * <p>There is no delta endpoint. The full file is downloaded and the
 * transformer stores {@code catalog_version} per-row so downstream SQL can
 * detect when the catalog has been refreshed.
 */
public class CisaKevDownloader extends AbstractCyberDownloader {

  private static final Logger LOGGER = LoggerFactory.getLogger(CisaKevDownloader.class);

  /** CISA KEV catalog JSON endpoint. */
  static final String KEV_URL =
      "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json";

  /** Parquet file path relative to parquetDirectory. */
  static final String PARQUET_REL_PATH = "type=kev_catalog.parquet";

  private final CisaKevResponseTransformer transformer;

  public CisaKevDownloader(StorageProvider storageProvider, String parquetDirectory) {
    super(storageProvider, parquetDirectory);
    this.transformer = new CisaKevResponseTransformer();
  }

  @Override public void download() throws IOException, InterruptedException {
    LOGGER.info("CISA KEV: fetching {}", KEV_URL);
    String json = fetchString(KEV_URL,
        "Accept", "application/json",
        "User-Agent", "Apache Calcite GovData (govdata@apache.org)");

    LOGGER.info("CISA KEV: transforming to row JSON");
    String jsonArray = transformer.transform(json, buildContext(KEV_URL));

    LOGGER.info("CISA KEV: writing parquet");
    writeJsonToParquet(jsonArray, "kev_catalog", PARQUET_REL_PATH);
  }
}
