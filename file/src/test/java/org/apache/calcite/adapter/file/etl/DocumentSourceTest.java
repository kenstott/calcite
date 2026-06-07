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
package org.apache.calcite.adapter.file.etl;

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@link DocumentSource} construction and configuration.
 */
@Tag("unit")
class DocumentSourceTest {

  @TempDir
  Path tempDir;

  private StorageProvider storageProvider;

  @BeforeEach
  void setUp() {
    storageProvider = new LocalFileStorageProvider();
  }

  @Test void testDocumentSourceConstruction() {
    Map<String, Object> docMap = new HashMap<String, Object>();
    docMap.put("metadataUrl", "https://data.sec.gov/submissions/CIK{cik}.json");
    docMap.put("documentUrl", "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}");
    docMap.put("documentConverter", "org.apache.calcite.adapter.file.converters.XbrlToParquetConverter");

    HttpSourceConfig.DocumentSourceConfig docConfig =
        HttpSourceConfig.DocumentSourceConfig.fromMap(docMap);

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://data.sec.gov/")
        .documentSource(docConfig)
        .build();

    DocumentSource source =
        new DocumentSource(config, storageProvider, tempDir.toString());
    assertNotNull(source);
  }

  @Test void testDocumentSourceWithHeaders() {
    Map<String, Object> docMap = new HashMap<String, Object>();
    docMap.put("metadataUrl", "https://data.sec.gov/submissions/CIK{cik}.json");
    docMap.put("documentUrl", "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}");
    docMap.put("documentConverter", "org.apache.calcite.adapter.file.converters.XbrlToParquetConverter");

    HttpSourceConfig.DocumentSourceConfig docConfig =
        HttpSourceConfig.DocumentSourceConfig.fromMap(docMap);

    Map<String, String> headers = new HashMap<String, String>();
    headers.put("User-Agent", "test@example.com");
    headers.put("Accept", "application/json");

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://data.sec.gov/")
        .documentSource(docConfig)
        .headers(headers)
        .build();

    DocumentSource source =
        new DocumentSource(config, storageProvider, tempDir.toString());
    assertNotNull(source);
  }

  @Test void testDocumentSourceWithCacheDirectory() throws IOException {
    Path cacheDir = tempDir.resolve("cache");
    Files.createDirectories(cacheDir);

    Map<String, Object> docMap = new HashMap<String, Object>();
    docMap.put("metadataUrl", "https://data.sec.gov/submissions/CIK{cik}.json");
    docMap.put("documentUrl", "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}");
    docMap.put("documentConverter", "test.converter");

    HttpSourceConfig.DocumentSourceConfig docConfig =
        HttpSourceConfig.DocumentSourceConfig.fromMap(docMap);

    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://data.sec.gov/")
        .documentSource(docConfig)
        .build();

    DocumentSource source =
        new DocumentSource(config, storageProvider, cacheDir.toString());
    assertNotNull(source);
  }

  @Test void testDocumentSourceConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("metadataUrl", "https://api.example.com/meta/{cik}");
    map.put("documentUrl", "https://api.example.com/docs/{id}");
    map.put("extractionType", "xbrl");
    map.put("documentConverter", "com.example.Converter");
    map.put("responseTransformer", "com.example.Transformer");

    HttpSourceConfig.DocumentSourceConfig config =
        HttpSourceConfig.DocumentSourceConfig.fromMap(map);

    assertNotNull(config);
    assertNotNull(config.getMetadataUrl());
    assertNotNull(config.getDocumentUrl());
  }

  @Test void testDocumentSourceConfigFromNullMap() {
    HttpSourceConfig.DocumentSourceConfig config =
        HttpSourceConfig.DocumentSourceConfig.fromMap(null);
    // null input should return null
    assertEquals(null, config);
  }

  private static void assertEquals(Object expected, Object actual) {
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual);
  }
}
