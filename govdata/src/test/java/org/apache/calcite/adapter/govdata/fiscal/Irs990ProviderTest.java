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

  private static final String BASE = "https://irs.test/990/xml/2025/";
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
    cache.zips.put(BASE + "2025_TEOS_XML_01A.zip", zip("100", xml("1"), "101", xml("2")));
    cache.zips.put(BASE + "2025_TEOS_XML_02A.zip", zip("200", xml("3")));
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_01A"),
        indexRow("101", "2025_TEOS_XML_01A"), indexRow("200", "2025_TEOS_XML_02A"));
    List<Map<String, Object>> rows = drain(Irs990Provider.rows(BASE, idx, cache, "2025"));
    assertEquals(3, rows.size());
    assertEquals("100", rows.get(0).get("object_id"));
    assertEquals("000000003", rows.get(2).get("ein"));
  }

  @Test void failedBatchFailsTheLoadAfterBoundedRetries() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2025_TEOS_XML_01A.zip", zip("100", xml("1")));
    // batch 02A is absent from the stub: every open throws
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_01A"),
        indexRow("200", "2025_TEOS_XML_02A"), indexRow("300", "2025_TEOS_XML_03A"));
    Iterator<Map<String, Object>> it = Irs990Provider.rows(BASE, idx, cache, "2025");
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
    assertTrue(!cache.opened.contains(BASE + "2025_TEOS_XML_03A.zip"), "must not skip ahead");
  }

  @Test void lowerCaseBatchSuffixInIndexResolvesToTheUpperCaseZip() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2024_TEOS_XML_04A.zip", zip("100", xml("1")));
    Irs990Provider.Index idx = index(indexRow("100", "2024_TEOS_XML_04a"));
    assertEquals(1, drain(Irs990Provider.rows(BASE, idx, cache, "2024")).size());
  }

  @Test void deflate64BatchIsRead() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2025_TEOS_XML_11B.zip", deflate64Zip("100", xml("1")));
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_11B"));
    assertEquals(1, drain(Irs990Provider.rows(BASE, idx, cache, "2025")).size());
  }

  @Test void batchShorterThanTheIndexListsFails() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2025_TEOS_XML_01A.zip", zip("100", xml("1")));
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_01A"),
        indexRow("101", "2025_TEOS_XML_01A"));
    Iterator<Map<String, Object>> it = Irs990Provider.rows(BASE, idx, cache, "2025");
    it.next();
    IllegalStateException e = assertThrows(IllegalStateException.class, it::hasNext);
    assertTrue(e.getMessage().contains("yielded 1 rows but the index lists 2"), e.getMessage());
  }

  @Test void unparseableReturnFailsInsteadOfBeingSkipped() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2025_TEOS_XML_01A.zip", zip("100", "<Return><oops>"));
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_01A"));
    Iterator<Map<String, Object>> it = Irs990Provider.rows(BASE, idx, cache, "2025");
    IllegalStateException e = assertThrows(IllegalStateException.class, it::hasNext);
    assertTrue(e.getMessage().contains("100"), e.getMessage());
  }

  @Test void returnWithoutEinFails() throws IOException {
    StubCache cache = new StubCache();
    cache.zips.put(BASE + "2025_TEOS_XML_01A.zip", zip("100", "<Return/>"));
    Irs990Provider.Index idx = index(indexRow("100", "2025_TEOS_XML_01A"));
    Iterator<Map<String, Object>> it = Irs990Provider.rows(BASE, idx, cache, "2025");
    assertThrows(IllegalStateException.class, it::hasNext);
  }

  @Test void indexRowWithoutBatchFails() {
    assertThrows(IOException.class, () -> index(indexRow("100", "")));
  }
}
