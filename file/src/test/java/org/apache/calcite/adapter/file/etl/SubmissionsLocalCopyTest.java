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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A submissions JSON answered by a local copy (a nightly bulk file) lags the live source, so an
 * accession missing from it is rechecked live before it counts as absent; a hit never costs a
 * live request, and with no local copy nothing changes.
 */
@Tag("unit")
class SubmissionsLocalCopyTest {

  private static final String URL = "https://data.sec.gov/submissions/CIK0000000001.json";

  private static final String SNAPSHOT =
      "{\"filings\":{\"recent\":{\"accessionNumber\":[\"0000000001-26-000001\"],"
      + "\"primaryDocument\":[\"old.htm\"]},\"files\":[]}}";

  private static final String LIVE =
      "{\"filings\":{\"recent\":{\"accessionNumber\":[\"0000000001-26-000002\","
      + "\"0000000001-26-000001\"],\"primaryDocument\":[\"today.htm\",\"old.htm\"]},"
      + "\"files\":[]}}";

  /** Answers locally from SNAPSHOT (when local) and live from LIVE, counting each. */
  private static final class StubDocumentSource extends DocumentSource {
    final boolean local;
    int localFetches;
    int liveFetches;

    StubDocumentSource(boolean local) {
      super(minimalConfig(), null, "/cache");
      this.local = local;
    }

    @Override public boolean hasLocalContent(String url) {
      return local;
    }

    @Override public String fetchUrlContent(String url) throws IOException {
      localFetches++;
      return local ? SNAPSHOT : LIVE;
    }

    @Override public String fetchUrlContentLive(String url) throws IOException {
      liveFetches++;
      return LIVE;
    }
  }

  private static HttpSourceConfig minimalConfig() {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put("url", "https://example.com/api");
    return HttpSourceConfig.fromMap(configMap);
  }

  private static DocumentETLProcessor newProcessor() {
    org.apache.calcite.adapter.file.storage.StorageProvider mockStorage =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.storage.StorageProvider.class);
    org.apache.calcite.adapter.file.converters.FileConverter mockConverter =
        org.mockito.Mockito.mock(org.apache.calcite.adapter.file.converters.FileConverter.class);
    return new DocumentETLProcessor(minimalConfig(), mockStorage, "/output", "/cache",
        mockConverter);
  }

  private static String fetch(DocumentETLProcessor processor, DocumentSource source,
      String accession) throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "fetchPrimaryDocumentFromSubmissions", DocumentSource.class, String.class, String.class);
    m.setAccessible(true);
    return (String) m.invoke(processor, source, "0000000001", accession);
  }

  @Test void anAccessionFiledSinceTheSnapshotIsFoundLiveOnce() throws Exception {
    StubDocumentSource source = new StubDocumentSource(true);
    DocumentETLProcessor processor = newProcessor();

    assertEquals("today.htm", fetch(processor, source, "0000000001-26-000002"));
    assertEquals(1, source.localFetches);
    assertEquals(1, source.liveFetches);

    // The live answer is kept for this CIK: more lookups cost nothing.
    assertEquals("old.htm", fetch(processor, source, "0000000001-26-000001"));
    assertEquals("today.htm", fetch(processor, source, "0000000001-26-000002"));
    assertEquals(1, source.localFetches);
    assertEquals(1, source.liveFetches);
  }

  @Test void anAccessionInTheSnapshotCostsNoLiveRequest() throws Exception {
    StubDocumentSource source = new StubDocumentSource(true);
    DocumentETLProcessor processor = newProcessor();

    assertEquals("old.htm", fetch(processor, source, "0000000001-26-000001"));
    assertEquals(1, source.localFetches);
    assertEquals(0, source.liveFetches);
  }

  @Test void anAccessionAbsentEverywhereIsOneLiveRecheckThenNull() throws Exception {
    StubDocumentSource source = new StubDocumentSource(true);
    DocumentETLProcessor processor = newProcessor();

    assertNull(fetch(processor, source, "0000000001-26-000099"));
    assertNull(fetch(processor, source, "0000000001-26-000098"));
    assertEquals(1, source.liveFetches);
  }

  @Test void withNoLocalCopyNothingChanges() throws Exception {
    StubDocumentSource source = new StubDocumentSource(false);
    DocumentETLProcessor processor = newProcessor();

    assertNull(fetch(processor, source, "0000000001-26-000099"));
    assertEquals(1, source.localFetches);
    assertEquals(0, source.liveFetches);
  }
}
