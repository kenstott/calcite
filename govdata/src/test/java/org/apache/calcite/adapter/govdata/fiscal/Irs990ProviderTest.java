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
package org.apache.calcite.adapter.govdata.fiscal;

import org.apache.calcite.adapter.file.etl.RawCache;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link Irs990Provider}: every filing the IRS index lists must come out as a row,
 * and a batch that cannot be read must fail the load instead of being skipped (#970).
 */
@Tag("unit")
class Irs990ProviderTest {

  private static final String BASE = "https://irs.test/990/xml/";
  private static final String HEADER = "RETURN_ID,FILING_TYPE,EIN,TAX_PERIOD,SUB_DATE,"
      + "TAXPAYER_NAME,RETURN_TYPE,DLN,OBJECT_ID,XML_BATCH_ID\n";

  private static String indexRow(String objectId, String batch) {
    return ",EFILE,123456789,202412,2025,ORG,990,1," + objectId + "," + batch + "\n";
  }

  private static Irs990Provider.Index index(String... rows) throws IOException {
    StringBuilder sb = new StringBuilder(HEADER);
    for (String r : rows) {
      sb.append(r);
    }
    return Irs990Provider.parseIndex(new BufferedReader(new StringReader(sb.toString())), "t");
  }

  private static Irs990Provider.ZipSet zips(Irs990Provider.Index idx) {
    Irs990Provider.ZipSet set = new Irs990Provider.ZipSet();
    for (String b : idx.batchIds) {
      set.paths.add(b.substring(0, 4) + "/" + b);
    }
    return set;
  }

  private static Irs990Provider.ZipSet zipSet(boolean legacy, String... paths) {
    Irs990Provider.ZipSet set = new Irs990Provider.ZipSet();
    set.legacy = legacy;
    set.paths.addAll(java.util.Arrays.asList(paths));
    return set;
  }

  private static String xml(String ein) {
    return "<Return><ReturnHeader><Filer><EIN>" + ein + "</EIN></Filer></ReturnHeader></Return>";
  }

  private static byte[] zip(String... objectIdsAndXml) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    java.util.zip.ZipOutputStream z = new java.util.zip.ZipOutputStream(bos);
    for (int i = 0; i < objectIdsAndXml.length; i += 2) {
      z.putNextEntry(new ZipEntry(objectIdsAndXml[i] + "_public.xml"));
      z.write(objectIdsAndXml[i + 1].getBytes(StandardCharsets.UTF_8));
      z.closeEntry();
    }
    z.close();
    return bos.toByteArray();
  }

  /** A zip whose entries are tagged Deflate64 (method 9) like the IRS 2025_TEOS_XML_11B batch. */
  private static byte[] deflate64Zip(String objectId, String xml) throws IOException {
    File f = File.createTempFile("irs990", ".zip");
    try {
      ZipArchiveOutputStream z = new ZipArchiveOutputStream(f);
      z.setLevel(0);  // stored deflate blocks, which Deflate64 shares
      ZipArchiveEntry e = new ZipArchiveEntry(objectId + "_public.xml");
      e.setMethod(ZipEntry.DEFLATED);
      z.putArchiveEntry(e);
      z.write(xml.getBytes(StandardCharsets.UTF_8));
      z.closeArchiveEntry();
      z.close();
      byte[] b = Files.readAllBytes(f.toPath());
      assertEquals(8, b[8]);  // local header compression method
      b[8] = 9;
      return b;
    } finally {
      f.delete();
    }
  }

  private static final class StubCache implements RawCache {
    final Map<String, byte[]> zips = new HashMap<String, byte[]>();
    final List<String> opened = new ArrayList<String>();

    @Override public InputStream openStream(String url) throws IOException {
      opened.add(url);
      byte[] b = zips.get(url);
      if (b == null) {
        throw new IOException("HTTP 404 for " + url);
      }
      return new ByteArrayInputStream(b);
    }

    @Override public InputStream openStream(String key, ContentSupplier onMiss)
        throws IOException {
      return onMiss.open();
    }

    @Override public boolean isEnabled() {
      return false;
    }
  }

  private static List<Map<String, Object>> drain(Iterator<Map<String, Object>> it) {
    List<Map<String, Object>> out = new ArrayList<Map<String, Object>>();
    while (it.hasNext()) {
      out.add(it.next());
    }
    return out;
  }

  @Test void everyListedFilingBecomesARow() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2025/2025_TEOS_XML_01A.zip", zip("100", xml("1"), "101", xml("2")));
    cache.zips.put(BASE + "2025/2025_TEOS_XML_02A.zip", zip("200", xml("3")));
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_01A"),
        indexRow("101", "2025_TEOS_XML_01A"), indexRow("200", "2025_TEOS_XML_02A"));
    List<Map<String, Object>> rows = drain(Irs990Provider.rows(BASE, zips(idx), idx, cache, "2025"));
    assertEquals(3, rows.size());
    assertEquals("100", rows.get(0).get("object_id"));
    assertEquals("000000003", rows.get(2).get("ein"));
  }

  @Test void failedBatchFailsTheLoadAfterBoundedRetries() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2025/2025_TEOS_XML_01A.zip", zip("100", xml("1")));
    // batch 02A is absent from the stub: every open throws
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_01A"),
        indexRow("200", "2025_TEOS_XML_02A"), indexRow("300", "2025_TEOS_XML_03A"));
    Iterator<Map<String, Object>> it = Irs990Provider.rows(BASE, zips(idx), idx, cache, "2025");
    assertTrue(it.hasNext());
    it.next();
    RuntimeException e = assertThrows(RuntimeException.class, it::hasNext);
    assertTrue(e.getCause().getMessage().contains("2025_TEOS_XML_02A.zip"), e.getMessage());
    int attempts = 0;
    for (String u : cache.opened) {
      if (u.endsWith("02A.zip")) {
        attempts++;
      }
    }
    assertEquals(3, attempts);
    assertTrue(!cache.opened.contains(BASE + "2025/2025_TEOS_XML_03A.zip"), "must not skip ahead");
  }

  @Test void lowerCaseBatchSuffixInIndexResolvesToTheUpperCaseZip() throws IOException {
    Irs990Provider.Index idx = index(indexRow("100", "2024_TEOS_XML_04a"));
    Irs990Provider.ZipSet found = Irs990Provider.discoverZips("b/", "2024", idx,
        probe("b/2024/2024_TEOS_XML_04A.zip", "b/2024/2024_TEOS_XML_01A.zip"));
    assertEquals(java.util.Arrays.asList("2024/2024_TEOS_XML_01A", "2024/2024_TEOS_XML_04A"),
        found.paths);
    assertTrue(!found.legacy);
  }

  private static Irs990Provider.Probe probe(final String... existing) {
    final java.util.Set<String> urls = new java.util.HashSet<String>(java.util.Arrays.asList(existing));
    return new Irs990Provider.Probe() {
      @Override public boolean exists(String url) {
        return urls.contains(url);
      }
    };
  }

  /** The 2025 defect: the index labels every filing 05A although the IRS split it into 05A+05B. */
  @Test void batchSplitAcrossTwoZipsButIndexedUnderOneIdIsRead() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2025/2025_TEOS_XML_05A.zip", zip("100", xml("1"), "101", xml("2")));
    cache.zips.put(BASE + "2025/2025_TEOS_XML_05B.zip", zip("102", xml("3"), "103", xml("4")));
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_05A"),
        indexRow("101", "2025_TEOS_XML_05A"), indexRow("102", "2025_TEOS_XML_05A"),
        indexRow("103", "2025_TEOS_XML_05A"));
    Irs990Provider.ZipSet found = Irs990Provider.discoverZips(BASE, "2025", idx,
        probe(BASE + "2025/2025_TEOS_XML_05A.zip", BASE + "2025/2025_TEOS_XML_05B.zip"));
    assertEquals(java.util.Arrays.asList("2025/2025_TEOS_XML_05A", "2025/2025_TEOS_XML_05B"),
        found.paths);
    assertEquals(4, drain(Irs990Provider.rows(BASE, found, idx, cache, "2025")).size());
  }

  private static Irs990Provider.Index noBatchIndex(String year, String... objectIds)
      throws IOException {
    StringBuilder sb = new StringBuilder(
        "RETURN_ID,FILING_TYPE,EIN,TAX_PERIOD,SUB_DATE,TAXPAYER_NAME,RETURN_TYPE,DLN,OBJECT_ID\n");
    for (String o : objectIds) {
      sb.append("1,EFILE,1,201812,1/1/" + year + ",A,990,1," + o + "\n");
    }
    return Irs990Provider.parseIndex(new BufferedReader(new StringReader(sb.toString())), "t");
  }

  /** 2020: no monthly zips, no XML_BATCH_ID; ids start with 2019 or 2020, in two series + CT1. */
  @Test void legacyYearReadsOwnAndPreviousSeriesAndSkipsOtherYearsFilings() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2019/download990xml_2019_1.zip",
        zip("201900000000000001", xml("1"), "201900000000000002", xml("9")));
    cache.zips.put(BASE + "2020/download990xml_2020_1.zip",
        zip("202000000000000001", xml("2"), "202000000000000002", xml("3")));
    cache.zips.put(BASE + "2020/2020_TEOS_XML_CT1.zip", zip("202000000000000002", xml("3")));
    // 201900000000000002 is listed by the 2019 index, not this one
    Irs990Provider.Index idx = noBatchIndex("2020", "201900000000000001", "202000000000000001",
        "202000000000000002");
    assertTrue(idx.batchIds.isEmpty());
    Irs990Provider.ZipSet found = Irs990Provider.discoverZips(BASE, "2020", idx,
        probe(BASE + "2019/download990xml_2019_1.zip", BASE + "2020/download990xml_2020_1.zip",
            BASE + "2020/2020_TEOS_XML_CT1.zip"));
    assertTrue(found.legacy);
    assertEquals(java.util.Arrays.asList("2019/download990xml_2019_1",
        "2020/download990xml_2020_1", "2020/2020_TEOS_XML_CT1"), found.paths);
    assertEquals(3, drain(Irs990Provider.rows(BASE, found, idx, cache, "2020")).size());
  }

  @Test void legacyYearStillFailsWhenAnIndexedFilingIsInNoZip() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2020/download990xml_2020_1.zip", zip("202000000000000001", xml("2")));
    Irs990Provider.Index idx = noBatchIndex("2020", "202000000000000001", "202000000000000009");
    Iterator<Map<String, Object>> it = Irs990Provider.rows(BASE,
        zipSet(true, "2020/download990xml_2020_1"), idx, cache, "2020");
    it.next();
    IllegalStateException e = assertThrows(IllegalStateException.class, it::hasNext);
    assertTrue(e.getMessage().contains("202000000000000009"), e.getMessage());
  }

  @Test void monthlyYearIndexWithoutBatchColumnIsRead() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2023/2023_TEOS_XML_01A.zip", zip("202300000000000001", xml("1")));
    cache.zips.put(BASE + "2023/2023_TEOS_XML_02A.zip", zip("202300000000000002", xml("2")));
    Irs990Provider.Index idx = noBatchIndex("2023", "202300000000000001", "202300000000000002");
    Irs990Provider.ZipSet found = Irs990Provider.discoverZips(BASE, "2023", idx,
        probe(BASE + "2023/2023_TEOS_XML_01A.zip", BASE + "2023/2023_TEOS_XML_02A.zip"));
    assertTrue(!found.legacy);
    assertEquals(2, drain(Irs990Provider.rows(BASE, found, idx, cache, "2023")).size());
  }

  @Test void indexedFilingFoundInNoZipFails() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2025/2025_TEOS_XML_05A.zip", zip("100", xml("1")));
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_05A"),
        indexRow("101", "2025_TEOS_XML_05A"));
    Iterator<Map<String, Object>> it = Irs990Provider.rows(BASE,
        zipSet(false, "2025/2025_TEOS_XML_05A"), idx, cache, "2025");
    it.next();
    IllegalStateException e = assertThrows(IllegalStateException.class, it::hasNext);
    assertTrue(e.getMessage().contains("[101]"), e.getMessage());
  }

  @Test void returnTheIndexDoesNotListFails() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2025/2025_TEOS_XML_05A.zip", zip("100", xml("1"), "999", xml("2")));
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_05A"));
    Iterator<Map<String, Object>> it = Irs990Provider.rows(BASE, zips(idx), idx, cache, "2025");
    it.next();
    IllegalStateException e = assertThrows(IllegalStateException.class, it::hasNext);
    assertTrue(e.getMessage().contains("999"), e.getMessage());
  }

  @Test void returnInTwoZipsFails() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2025/2025_TEOS_XML_05A.zip", zip("100", xml("1")));
    cache.zips.put(BASE + "2025/2025_TEOS_XML_05B.zip", zip("100", xml("1")));
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_05A"));
    Iterator<Map<String, Object>> it = Irs990Provider.rows(BASE,
        zipSet(false, "2025/2025_TEOS_XML_05A", "2025/2025_TEOS_XML_05B"), idx, cache, "2025");
    it.next();
    IllegalStateException e = assertThrows(IllegalStateException.class, it::hasNext);
    assertTrue(e.getMessage().contains("more than one zip"), e.getMessage());
  }

  @Test void batchNamedByIndexButNotPublishedFails() throws IOException {
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_05A"));
    assertThrows(IOException.class,
        () -> Irs990Provider.discoverZips(BASE, "2025", idx, probe(BASE + "2025/2025_TEOS_XML_01A.zip")));
  }

  @Test void noPublishedZipsFails() throws IOException {
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_05A"));
    assertThrows(IOException.class,
        () -> Irs990Provider.discoverZips(BASE, "2025", idx, probe()));
  }

  @Test void repeatedObjectIdInIndexFails() {
    assertThrows(IOException.class,
        () -> index(indexRow("100", "2025_TEOS_XML_01A"), indexRow("100", "2025_TEOS_XML_01A")));
  }

  @Test void deflate64BatchIsRead() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2025/2025_TEOS_XML_11B.zip", deflate64Zip("100", xml("1")));
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_11B"));
    assertEquals(1, drain(Irs990Provider.rows(BASE, zips(idx), idx, cache, "2025")).size());
  }

  @Test void batchShorterThanTheIndexListsFails() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2025/2025_TEOS_XML_01A.zip", zip("100", xml("1")));
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_01A"),
        indexRow("101", "2025_TEOS_XML_01A"));
    Iterator<Map<String, Object>> it = Irs990Provider.rows(BASE, zips(idx), idx, cache, "2025");
    it.next();
    IllegalStateException e = assertThrows(IllegalStateException.class, it::hasNext);
    assertTrue(e.getMessage().contains("wrote 1 filings but the index lists 2"), e.getMessage());
  }

  @Test void unparseableReturnFailsInsteadOfBeingSkipped() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2025/2025_TEOS_XML_01A.zip", zip("100", "<Return><oops>"));
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_01A"));
    Iterator<Map<String, Object>> it = Irs990Provider.rows(BASE, zips(idx), idx, cache, "2025");
    IllegalStateException e = assertThrows(IllegalStateException.class, it::hasNext);
    assertTrue(e.getMessage().contains("100"), e.getMessage());
  }

  @Test void returnWithoutEinFails() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2025/2025_TEOS_XML_01A.zip", zip("100", "<Return/>"));
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_01A"));
    Iterator<Map<String, Object>> it = Irs990Provider.rows(BASE, zips(idx), idx, cache, "2025");
    assertThrows(IllegalStateException.class, it::hasNext);
  }

  @Test void indexRowWithoutBatchFails() {
    assertThrows(IOException.class, () -> index(indexRow("100", "")));
  }
}
