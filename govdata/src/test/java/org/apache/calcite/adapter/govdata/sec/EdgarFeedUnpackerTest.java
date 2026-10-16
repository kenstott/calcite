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

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A day file is unpacked into a tree the ETL can read documents from instead of fetching them. */
@Tag("unit")
class EdgarFeedUnpackerTest {

  @AfterEach void uninstall() {
    SecFeedStore.uninstall();
  }

  private static byte[] dayFile() throws IOException {
    ByteArrayOutputStream raw = new ByteArrayOutputStream();
    try (TarArchiveOutputStream tar = new TarArchiveOutputStream(new GZIPOutputStream(raw))) {
      add(tar, "./0000805676-23-000155.nc", NcSubmissionTest.FORM4);
      add(tar, "./0001234567-23-000009.nc", NcSubmissionTest.EIGHT_K);
      add(tar, "./0009999999-23-000001.nc",
          NcSubmissionTest.EIGHT_K.replace("<TYPE>8-K\n<FILING-DATE>", "<TYPE>424B2\n<FILING-DATE>")
              .replace("0001234567-23-000009", "0009999999-23-000001"));
    }
    return raw.toByteArray();
  }

  private static void add(TarArchiveOutputStream tar, String name, String body)
      throws IOException {
    byte[] b = body.getBytes(StandardCharsets.ISO_8859_1);
    TarArchiveEntry e = new TarArchiveEntry(name);
    e.setSize(b.length);
    tar.putArchiveEntry(e);
    tar.write(b);
    tar.closeArchiveEntry();
  }

  @Test void documentsOfProcessedFormsAreUnpackedAndOthersAreSkipped(@TempDir File dir)
      throws Exception {
    EdgarFeedUnpacker.Stats stats =
        EdgarFeedUnpacker.unpack(new ByteArrayInputStream(dayFile()), dir);

    assertEquals(3, stats.submissions);
    assertEquals(2, stats.processed);
    assertTrue(new File(dir, "000123456723000009/acme-8k.htm").isFile());
    assertTrue(new File(dir, "000123456723000009/FilingSummary.xml").isFile());
    assertFalse(new File(dir, "000123456723000009/logo.jpg").exists(), "binaries are not unpacked");
    assertFalse(new File(dir, "000999999923000001").exists(), "424B2 is not a processed form");
    String index = new String(
        Files.readAllBytes(new File(dir, "000123456723000009/index.json").toPath()),
        StandardCharsets.UTF_8);
    assertTrue(index.contains("\"name\":\"acme-8k.htm\"") && index.contains("logo.jpg"), index);
  }

  @Test void renderingFilesNothingReadsAreListedButNotWritten(@TempDir File dir) throws Exception {
    ByteArrayOutputStream raw = new ByteArrayOutputStream();
    try (TarArchiveOutputStream tar = new TarArchiveOutputStream(new GZIPOutputStream(raw))) {
      String withR = NcSubmissionTest.EIGHT_K.replace("</SUBMISSION>",
          "<DOCUMENT>\n<TYPE>XML\n<SEQUENCE>4\n<FILENAME>R2.htm\n<DESCRIPTION>r\n<TEXT>\n"
              + "<html>R2</html>\n</TEXT>\n</DOCUMENT>\n</SUBMISSION>");
      add(tar, "./0001234567-23-000009.nc", withR);
    }
    EdgarFeedUnpacker.unpack(new ByteArrayInputStream(raw.toByteArray()), dir);

    assertFalse(new File(dir, "000123456723000009/R2.htm").exists());
    assertTrue(new File(dir, "000123456723000009/FilingSummary.xml").isFile());
    assertTrue(new String(
        Files.readAllBytes(new File(dir, "000123456723000009/index.json").toPath()),
        StandardCharsets.UTF_8).contains("R2.htm"));
  }

  @Test void theSubmissionHeaderIsWrittenAsTheHdrSgmlSidecar(@TempDir File dir) throws Exception {
    EdgarFeedUnpacker.unpack(new ByteArrayInputStream(dayFile()), dir);
    File hdr = new File(dir, "000123456723000009/0001234567-23-000009.hdr.sgml");
    assertTrue(hdr.isFile());
    String text = new String(Files.readAllBytes(hdr.toPath()), StandardCharsets.ISO_8859_1);
    assertTrue(text.contains("<ACCESSION-NUMBER>0001234567-23-000009")
        && text.contains("<CIK>0001234567") && !text.contains("<DOCUMENT>"), text);
  }

  @Test void exhibitsNothingReadsAreListedButNotWritten(@TempDir File dir) throws Exception {
    ByteArrayOutputStream raw = new ByteArrayOutputStream();
    try (TarArchiveOutputStream tar = new TarArchiveOutputStream(new GZIPOutputStream(raw))) {
      String doc = "<DOCUMENT>\n<TYPE>%s\n<SEQUENCE>%d\n<FILENAME>%s\n<DESCRIPTION>d\n<TEXT>\n"
          + "<html>x</html>\n</TEXT>\n</DOCUMENT>\n";
      String extra = String.format(doc, "EX-99.1", 5, "ex991.htm")
          + String.format(doc, "EX-21.1", 6, "ex21.htm")
          + String.format(doc, "EX-101.LAB", 7, "acme_lab.xml")
          + String.format(doc, "EX-101.PRE", 8, "acme_pre.xml");
      add(tar, "./0001234567-23-000009.nc",
          NcSubmissionTest.EIGHT_K.replace("</SUBMISSION>", extra + "</SUBMISSION>"));
    }
    EdgarFeedUnpacker.unpack(new ByteArrayInputStream(raw.toByteArray()), dir);

    File acc = new File(dir, "000123456723000009");
    assertTrue(new File(acc, "ex991.htm").isFile());
    assertTrue(new File(acc, "acme_pre.xml").isFile());
    assertFalse(new File(acc, "ex21.htm").exists());
    assertFalse(new File(acc, "acme_lab.xml").exists());
    assertTrue(new String(Files.readAllBytes(new File(acc, "index.json").toPath()),
        StandardCharsets.UTF_8).contains("ex21.htm"));
  }

  @Test void anInsiderFormAlsoGetsItsSubmissionTextFile(@TempDir File dir) throws Exception {
    EdgarFeedUnpacker.unpack(new ByteArrayInputStream(dayFile()), dir);
    File txt = new File(dir, "000080567623000155/0000805676-23-000155.txt");
    assertTrue(txt.isFile());
    assertTrue(new String(Files.readAllBytes(txt.toPath()), StandardCharsets.ISO_8859_1)
        .contains("<ownershipDocument>"));
    assertFalse(new File(dir, "000123456723000009/0001234567-23-000009.txt").exists());
  }

  @Test void aDocumentIsAnsweredWhicheverCikTheRequestNames(@TempDir File dir) throws Exception {
    EdgarFeedUnpacker.unpack(new ByteArrayInputStream(dayFile()), dir);
    SecFeedStore.install(dir.getAbsolutePath());

    String base = "https://www.sec.gov/Archives/edgar/data/";
    assertNotNull(SecFeedStore.bytes(base + "1234567/000123456723000009/FilingSummary.xml"));
    assertNotNull(SecFeedStore.bytes(base + "42/000123456723000009/FilingSummary.xml"));
    assertNotNull(SecFeedStore.bytes(base + "805676/0000805676-23-000155.txt"));
    assertNotNull(SecFeedStore.bytes(base + "1234567/000123456723000009/index.json"));
  }

  @Test void theDirectoryListingAndIndexPageAreBuiltFromTheUnpackedDocuments(@TempDir File dir)
      throws Exception {
    EdgarFeedUnpacker.unpack(new ByteArrayInputStream(dayFile()), dir);
    SecFeedStore.install(dir.getAbsolutePath());

    String base = "https://www.sec.gov/Archives/edgar/data/1234567/000123456723000009/";
    for (String url : new String[] {base, base.substring(0, base.length() - 1),
        base + "0001234567-23-000009-index.html", base + "0001234567-23-000009-index.htm"}) {
      byte[] bytes = SecFeedStore.bytes(url);
      assertNotNull(bytes, url);
      String html = new String(bytes, StandardCharsets.UTF_8);
      assertTrue(html.contains("<a href=\"/Archives/edgar/data/1234567/000123456723000009/acme-8k.htm\">"),
          html);
      assertTrue(html.contains("FilingSummary.xml") && html.contains("<td>8-K</td>"), html);
    }
    assertNull(SecFeedStore.bytes(
        "https://www.sec.gov/Archives/edgar/data/1/000000000023000001/"));
  }

  @Test void anythingTheTreeLacksIsNullSoItIsFetched(@TempDir File dir) throws Exception {
    EdgarFeedUnpacker.unpack(new ByteArrayInputStream(dayFile()), dir);
    SecFeedStore.install(dir.getAbsolutePath());

    String base = "https://www.sec.gov/Archives/edgar/data/";
    assertNull(SecFeedStore.bytes(base + "1234567/000123456723000009/missing.xml"));
    assertNull(SecFeedStore.bytes(base + "1234567/000000000023000001/FilingSummary.xml"));
    assertNull(SecFeedStore.bytes("https://data.sec.gov/submissions/CIK0000019617.json"));
    assertNull(SecFeedStore.bytes(base + "1/000123456723000009/../../x"));
  }

  @Test void withNoStoreInstalledNothingIsServed() throws Exception {
    assertNull(SecFeedStore.bytes(
        "https://www.sec.gov/Archives/edgar/data/1/000123456723000009/FilingSummary.xml"));
  }
}
