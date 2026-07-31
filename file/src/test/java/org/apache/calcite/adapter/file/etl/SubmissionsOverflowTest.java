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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for finding a filing's primary document beyond a prolific filer's newest submissions.
 *
 * <p>EDGAR's {@code submissions.json} carries a filer's newest ~1000 submissions under
 * {@code filings.recent}; anything older is named in {@code filings.files} and lives on separate
 * pages. Looking only at {@code recent} makes an old accession indistinguishable from one that was
 * never filed — the SEC reprocess path did exactly that, and 194 thirteen-F filings from
 * 2019-2021 lost their institutional_holdings rows in part because of it. index.json resolution
 * later fixed most of the population; this fixes the specific miss that produced the log line
 * {@code submissions.json miss [diag N]: mapSize=1001}.
 */
@Tag("unit")
class SubmissionsOverflowTest {

  /** Routes fetchUrlContent to canned responses keyed by URL, and counts calls per URL. */
  private static final class StubDocumentSource extends DocumentSource {
    final Map<String, String> responses = new HashMap<String, String>();
    final Map<String, Integer> fetchCounts = new HashMap<String, Integer>();

    StubDocumentSource() {
      super(minimalConfig(), null, "/cache");
    }

    @Override public String fetchUrlContent(String url) throws IOException {
      fetchCounts.merge(url, 1, Integer::sum);
      String body = responses.get(url);
      if (body == null) {
        throw new IOException("no stub for " + url);
      }
      return body;
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

  private static String fetch(DocumentETLProcessor processor, DocumentSource source, String cik,
      String accession) throws Exception {
    Method m = DocumentETLProcessor.class.getDeclaredMethod(
        "fetchPrimaryDocumentFromSubmissions", DocumentSource.class, String.class, String.class);
    m.setAccessible(true);
    return (String) m.invoke(processor, source, cik, accession);
  }

  private static final String RECENT_ONLY =
      "{\"filings\":{\"recent\":{"
      + "\"accessionNumber\":[\"0000000001-21-000099\"],"
      + "\"primaryDocument\":[\"newest.htm\"]},"
      + "\"files\":[]}}";

  private static final String RECENT_WITH_OVERFLOW =
      "{\"filings\":{\"recent\":{"
      + "\"accessionNumber\":[\"0000000001-21-000099\"],"
      + "\"primaryDocument\":[\"newest.htm\"]},"
      + "\"files\":[{\"name\":\"CIK0000000001-submissions-001.json\","
      + "\"filingCount\":1,\"filingFrom\":\"2019-01-01\",\"filingTo\":\"2019-12-31\"}]}}";

  private static final String OVERFLOW_PAGE =
      "{\"accessionNumber\":[\"0000000001-19-000004\"],"
      + "\"primaryDocument\":[\"oldest.htm\"]}";

  @Test void testRecentSubmissionsAreFoundWithoutTouchingOverflow() throws Exception {
    StubDocumentSource source = new StubDocumentSource();
    source.responses.put("https://data.sec.gov/submissions/CIK0000000001.json", RECENT_ONLY);
    DocumentETLProcessor processor = newProcessor();

    assertEquals("newest.htm",
        fetch(processor, source, "0000000001", "0000000001-21-000099"));
  }

  /**
   * An accession outside filings.recent is found once the overflow page is fetched.
   *
   * <p>This is the case that previously returned null and produced the miss log line — the exact
   * failure traced in a 2019-2021 institutional_holdings gap.
   */
  @Test void testOldAccessionIsFoundInOverflow() throws Exception {
    StubDocumentSource source = new StubDocumentSource();
    source.responses.put("https://data.sec.gov/submissions/CIK0000000001.json",
        RECENT_WITH_OVERFLOW);
    source.responses.put(
        "https://data.sec.gov/submissions/CIK0000000001-submissions-001.json", OVERFLOW_PAGE);
    DocumentETLProcessor processor = newProcessor();

    assertEquals("oldest.htm",
        fetch(processor, source, "0000000001", "0000000001-19-000004"));
  }

  /**
   * The overflow page is fetched only when a lookup actually misses recent.
   *
   * <p>Fetching it unconditionally would multiply request volume for the overwhelming majority of
   * filers who never approach 1000 recent submissions and have no overflow to gain from it.
   */
  @Test void testOverflowIsNotFetchedWhenRecentAlreadyAnswers() throws Exception {
    StubDocumentSource source = new StubDocumentSource();
    source.responses.put("https://data.sec.gov/submissions/CIK0000000001.json",
        RECENT_WITH_OVERFLOW);
    // Deliberately no stub for the overflow page — fetching it would throw and fail the test.
    DocumentETLProcessor processor = newProcessor();

    assertEquals("newest.htm",
        fetch(processor, source, "0000000001", "0000000001-21-000099"));
  }

  /**
   * A genuinely nonexistent accession is still reported as not found, not endlessly retried.
   *
   * <p>Every overflow page has to be checked before that conclusion is reached — the wanted
   * accession could be on any of them — but it must be reached, once, and not force a second
   * full sweep on the next call for the same CIK.
   */
  @Test void testAccessionAbsentFromEveryPageReturnsNull() throws Exception {
    StubDocumentSource source = new StubDocumentSource();
    source.responses.put("https://data.sec.gov/submissions/CIK0000000001.json",
        RECENT_WITH_OVERFLOW);
    source.responses.put(
        "https://data.sec.gov/submissions/CIK0000000001-submissions-001.json", OVERFLOW_PAGE);
    DocumentETLProcessor processor = newProcessor();

    assertNull(fetch(processor, source, "0000000001", "0000000001-05-999999"));

    String pageUrl = "https://data.sec.gov/submissions/CIK0000000001-submissions-001.json";
    fetch(processor, source, "0000000001", "0000000001-05-999999");
    assertEquals(Integer.valueOf(1), source.fetchCounts.get(pageUrl),
        "the overflow page must be fetched once per CIK, not once per repeated miss");
  }

  /**
   * Several accessions from the same CIK share one overflow fetch.
   *
   * <p>A prolific filer is exactly the filer with the most accessions in a batch and the most
   * overflow pages; fetching those pages per-accession rather than per-CIK would turn the fix
   * into the request-volume problem it exists to avoid.
   */
  @Test void testOverflowFetchedOnceAcrossMultipleAccessionsFromOneCik() throws Exception {
    StubDocumentSource source = new StubDocumentSource();
    source.responses.put("https://data.sec.gov/submissions/CIK0000000001.json",
        RECENT_WITH_OVERFLOW);
    source.responses.put(
        "https://data.sec.gov/submissions/CIK0000000001-submissions-001.json", OVERFLOW_PAGE);
    DocumentETLProcessor processor = newProcessor();

    fetch(processor, source, "0000000001", "0000000001-21-000099");
    fetch(processor, source, "0000000001", "0000000001-19-000004");
    fetch(processor, source, "0000000001", "0000000001-19-000004");

    assertEquals(Integer.valueOf(1),
        source.fetchCounts.get("https://data.sec.gov/submissions/CIK0000000001.json"));
    assertEquals(Integer.valueOf(1), source.fetchCounts.get(
        "https://data.sec.gov/submissions/CIK0000000001-submissions-001.json"));
  }

  /**
   * A page that fails to fetch does not hide the accessions on other pages.
   *
   * <p>A filer with enough history to need several overflow pages should not lose all of them to
   * one transient failure.
   */
  @Test void testOneFailingPageDoesNotBlockAnother() throws Exception {
    String twoPages =
        "{\"filings\":{\"recent\":{\"accessionNumber\":[],\"primaryDocument\":[]},"
        + "\"files\":["
        + "{\"name\":\"CIK0000000001-submissions-001.json\","
        + "\"filingCount\":1,\"filingFrom\":\"2019-01-01\",\"filingTo\":\"2019-12-31\"},"
        + "{\"name\":\"CIK0000000001-submissions-002.json\","
        + "\"filingCount\":1,\"filingFrom\":\"2010-01-01\",\"filingTo\":\"2010-12-31\"}"
        + "]}}";
    StubDocumentSource source = new StubDocumentSource();
    source.responses.put("https://data.sec.gov/submissions/CIK0000000001.json", twoPages);
    // Page 001 has no stub and will throw; page 002 is answered.
    source.responses.put(
        "https://data.sec.gov/submissions/CIK0000000001-submissions-002.json", OVERFLOW_PAGE);
    DocumentETLProcessor processor = newProcessor();

    assertEquals("oldest.htm",
        fetch(processor, source, "0000000001", "0000000001-19-000004"));
  }
}
