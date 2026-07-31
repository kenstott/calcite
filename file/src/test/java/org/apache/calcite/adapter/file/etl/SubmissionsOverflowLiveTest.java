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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Live check of the overflow fix against the exact filing that exposed the bug.
 *
 * <p>CIK 0001390777, accession 0001390777-21-000072 logged "submissions.json miss ... mapSize=1001"
 * during the 2022 institutional_holdings repair on 2026-07-30. Traced: the accession is outside
 * that CIK's filings.recent window and the reprocess path never fetched filings.files.
 *
 * <p>Fixed to one filing whose contents will not change: EDGAR filings are immutable once
 * accepted, so the expected value below is stable.
 */
@Tag("integration")
class SubmissionsOverflowLiveTest {

  @Test void testKnownOverflowAccessionIsNowFound() throws Exception {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("url", "https://example.com/api");
    HttpSourceConfig config = HttpSourceConfig.fromMap(configMap);

    DocumentSource source = new DocumentSource(config, null, "/tmp/cache-live") {
      @Override public String fetchUrlContent(String url) throws java.io.IOException {
        java.net.URL u;
        try {
          u = new java.net.URI(url).toURL();
        } catch (java.net.URISyntaxException e) {
          throw new java.io.IOException(e);
        }
        java.net.HttpURLConnection c = (java.net.HttpURLConnection) u.openConnection();
        c.setRequestProperty("User-Agent", "Kenneth Stott kennethstott@gmail.com");
        c.setConnectTimeout(30000);
        c.setReadTimeout(30000);
        try (java.io.InputStream is = c.getInputStream()) {
          return new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
      }
    };

    DocumentETLProcessor processor =
        new DocumentETLProcessor(config, null, "/output", "/cache", null);
    Method fetch = DocumentETLProcessor.class.getDeclaredMethod(
        "fetchPrimaryDocumentFromSubmissions", DocumentSource.class, String.class, String.class);
    fetch.setAccessible(true);

    String result =
        (String) fetch.invoke(processor, source, "0001390777", "0001390777-21-000072");

    assertEquals("xslForm13F_X01/primary_doc.xml", result,
        "the overflow page must be reached and this accession's primaryDocument found in it");
  }
}
