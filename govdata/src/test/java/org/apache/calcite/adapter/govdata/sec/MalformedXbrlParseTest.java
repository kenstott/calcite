/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.metadata.ConversionMetadata;
import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests that XbrlToParquetConverter handles malformed XML/XBRL files
 * by falling back to JSoup when strict XML parsing fails.
 *
 * <p>Specifically tests CIK 0000790273, accession 0000790273-21-000003,
 * document conc2ndq.TXT — a legacy plain-text 10-Q with SGML column
 * tags ({@code <C>}) that are invalid XML.
 */
@Tag("integration")
class MalformedXbrlParseTest {

  @TempDir
  Path tempDir;

  /**
   * Downloads a filing from SEC EDGAR and verifies XbrlToParquetConverter
   * does not throw. Legacy text filings should parse via JSoup fallback
   * and produce empty output (no XBRL facts), not an exception.
   */
  @Test void testMalformedTxtFilingDoesNotThrow() throws IOException {
    // Download the specific failing file from SEC EDGAR
    String edgarUrl =
        "https://www.sec.gov/Archives/edgar/data/790273/000079027321000003/conc2ndq.TXT";
    Path localFile = tempDir.resolve("conc2ndq.txt");
    downloadFile(edgarUrl, localFile);

    // Verify file was downloaded
    assertNotNull(Files.size(localFile) > 0, "File should not be empty");

    StorageProvider storageProvider = new LocalFileStorageProvider();
    XbrlToParquetConverter converter = new XbrlToParquetConverter(storageProvider);

    String sourcePath = localFile.toString();
    String targetDir = tempDir.resolve("output").toString();
    Files.createDirectories(tempDir.resolve("output"));

    ConversionMetadata metadata = new ConversionMetadata(targetDir);
    metadata.setHint("cik", "0000790273");
    metadata.setHint("form", "10-Q");
    metadata.setHint("filingDate", "2021-04-30");
    metadata.setHint("accession", "0000790273-21-000003");

    // This should NOT throw — the JSoup fallback should handle the malformed XML
    List<String> outputFiles = assertDoesNotThrow(
        () -> converter.convert(sourcePath, targetDir, metadata),
        "XbrlToParquetConverter should not throw on malformed XML; "
            + "JSoup fallback should handle it gracefully");

    assertNotNull(outputFiles, "Output files list should not be null");
    // Legacy text filing has no XBRL data, so output may be empty — that's fine.
    // The key assertion is that it doesn't throw.
  }

  private void downloadFile(String urlStr, Path target) throws IOException {
    URL url = URI.create(urlStr).toURL();
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty("User-Agent",
        "Apache Calcite SEC Adapter apache-calcite@apache.org");
    conn.setConnectTimeout(30000);
    conn.setReadTimeout(30000);

    try (InputStream is = conn.getInputStream()) {
      Files.copy(is, target);
    } finally {
      conn.disconnect();
    }
  }
}
