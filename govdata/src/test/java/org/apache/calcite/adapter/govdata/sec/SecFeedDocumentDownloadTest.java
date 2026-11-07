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

import org.apache.calcite.adapter.file.etl.DocumentSource;
import org.apache.calcite.adapter.file.etl.HttpSourceConfig;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * The sec {@code documentUrl} for a filing's primary document lies under the feed store's prefix,
 * so with the document unpacked into the feed the document ETL's download is answered from the
 * feed: the bytes reach the cache unchanged and no request is made.
 */
@Tag("unit")
class SecFeedDocumentDownloadTest {

  @AfterEach void uninstall() {
    SecFeedStore.uninstall();
  }

  @Test void aHeldPrimaryDocumentIsCachedFromTheFeed(@TempDir File feed, @TempDir File cache)
      throws Exception {
    String acc = "000106299325017537";
    byte[] xml = "<edgarSubmission><x>é</x></edgarSubmission>"
        .getBytes(StandardCharsets.UTF_8);
    File dir = new File(feed, acc);
    if (!dir.mkdirs()) {
      throw new IllegalStateException("cannot create " + dir);
    }
    Files.write(new File(dir, "primary_doc.xml").toPath(), xml);
    SecFeedStore.install(feed.getAbsolutePath());

    Map<String, Object> docMap = new HashMap<String, Object>();
    docMap.put("documentUrl", "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{document}");
    HttpSourceConfig config = HttpSourceConfig.builder()
        .url("https://www.sec.gov/")
        .documentSource(HttpSourceConfig.DocumentSourceConfig.fromMap(docMap))
        .build();
    DocumentSource source =
        new DocumentSource(config, new LocalFileStorageProvider(), cache.getAbsolutePath());

    Map<String, String> vars = new HashMap<String, String>();
    vars.put("cik", "0000855887");
    vars.put("accession", acc);
    vars.put("document", "primary_doc.xml");
    String cached = source.downloadDocument(vars);

    assertArrayEquals(xml, Files.readAllBytes(new File(cached).toPath()));
  }
}
