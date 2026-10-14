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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A URL a local copy can answer must not be fetched; one the copy lacks, or one asked for live,
 * must be. The host below does not resolve, so any request that reaches the network fails.
 */
@Tag("unit")
class DocumentSourceLocalContentTest {

  private static final String PREFIX = "https://bulk-test.invalid/submissions/";

  @TempDir
  Path tempDir;

  @AfterEach void clear() {
    DocumentSource.clearLocalContentResolvers();
  }

  private DocumentSource source() {
    Map<String, Object> docMap = new HashMap<String, Object>();
    docMap.put("metadataUrl", PREFIX + "CIK{cik}.json");
    docMap.put("documentUrl", "https://bulk-test.invalid/Archives/{cik}/{accession}");
    docMap.put("documentConverter", "org.apache.calcite.adapter.file.converters.XbrlToParquetConverter");
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://bulk-test.invalid/")
        .documentSource(HttpSourceConfig.DocumentSourceConfig.fromMap(docMap))
        .build();
    return new DocumentSource(config, new LocalFileStorageProvider(), tempDir.toString());
  }

  @Test void aLocallyAnsweredUrlIsNotFetched() throws IOException {
    DocumentSource.registerLocalContentResolver(PREFIX,
        url -> url.endsWith("CIK0000000001.json") ? "{\"local\":true}" : null);
    assertEquals("{\"local\":true}", source().fetchUrlContent(PREFIX + "CIK0000000001.json"));
  }

  @Test void aUrlTheLocalCopyLacksIsFetched() {
    DocumentSource.registerLocalContentResolver(PREFIX, url -> null);
    assertThrows(IOException.class, () -> source().fetchUrlContent(PREFIX + "CIK0000000002.json"));
  }

  @Test void aLiveFetchIgnoresTheLocalCopy() {
    DocumentSource.registerLocalContentResolver(PREFIX, url -> "{\"local\":true}");
    assertThrows(IOException.class,
        () -> source().fetchUrlContentLive(PREFIX + "CIK0000000001.json"));
  }

  @Test void hasLocalContentFollowsTheRegisteredPrefix() {
    DocumentSource source = source();
    assertFalse(source.hasLocalContent(PREFIX + "CIK0000000001.json"));
    DocumentSource.registerLocalContentResolver(PREFIX, url -> null);
    assertTrue(source.hasLocalContent(PREFIX + "CIK0000000001.json"));
    assertFalse(source.hasLocalContent("https://elsewhere.invalid/x"));
  }
}
