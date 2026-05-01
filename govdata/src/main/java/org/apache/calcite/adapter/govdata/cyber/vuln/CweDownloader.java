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
 * Downloads the MITRE CWE catalog and materializes it as parquet.
 *
 * <p>Source: {@code https://cwe.mitre.org/data/xml/cwec_latest.xml.zip}
 * — a ZIP containing a single XML file with all CWE entries.
 *
 * <p>The XML is decompressed and passed to {@link CweResponseTransformer}
 * which produces a JSON array, then written to
 * {@code {parquetDirectory}/type=cwe_catalog.parquet} via the StorageProvider.
 *
 * <p>CWE is a dimension table referenced by NVD, OSV, and CISA KEV, so it
 * should be downloaded before other vulnerability tables.
 */
public class CweDownloader extends AbstractCyberDownloader {

  private static final Logger LOGGER = LoggerFactory.getLogger(CweDownloader.class);

  /** MITRE CWE catalog ZIP (always points to the latest release). */
  static final String CWE_ZIP_URL = "https://cwe.mitre.org/data/xml/cwec_latest.xml.zip";

  /** Parquet file path relative to parquetDirectory. */
  static final String PARQUET_REL_PATH = "type=cwe_catalog.parquet";

  private final CweResponseTransformer transformer;

  public CweDownloader(StorageProvider storageProvider, String parquetDirectory) {
    super(storageProvider, parquetDirectory);
    this.transformer = new CweResponseTransformer();
  }

  @Override public void download() throws IOException, InterruptedException {
    LOGGER.info("CWE: fetching {}", CWE_ZIP_URL);
    byte[] zipBytes = fetchBytes(CWE_ZIP_URL,
        "User-Agent", "Apache Calcite GovData (govdata@apache.org)");

    LOGGER.info("CWE: decompressing ZIP ({} bytes)", zipBytes.length);
    String xml = extractXmlFromZip(zipBytes, ".xml");

    LOGGER.info("CWE: transforming XML to row JSON");
    String jsonArray = transformer.transform(xml, buildContext(CWE_ZIP_URL));

    LOGGER.info("CWE: writing parquet");
    writeJsonToParquet(jsonArray, "cwe_catalog", PARQUET_REL_PATH);
  }
}
