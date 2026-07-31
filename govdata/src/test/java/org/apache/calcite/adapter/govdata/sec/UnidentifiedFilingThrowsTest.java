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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * convertInternal's XBRL/XML path used to return an empty list (with only a WARN log) when the
 * document had no extractable CIK or usable period-end date. process8KHtml, process13FForm, and
 * process13DGForm all throw for the identical condition via unidentifiedFiling, so fix-sec.sh's
 * accession-level retry could reach the same root cause on three form types but never the fourth.
 * A code fix (or an EDGAR outage) affecting these fields left the XBRL-path filing permanently
 * recorded complete-and-empty, since retry only re-touches accessions with a FAILED marker.
 *
 * <p>These tests pin that convertInternal now throws instead, matching the other three paths.
 */
@Tag("unit")
class UnidentifiedFilingThrowsTest {

  @TempDir
  Path tempDir;

  private XbrlToParquetConverter newConverter() {
    StorageProvider storageProvider = new LocalFileStorageProvider();
    return new XbrlToParquetConverter(storageProvider);
  }

  /** No issuerCik, no XBRL context/identifier, and a non-numeric directory structure — every
   *  CIK-resolution strategy in extractCik fails, so this used to silently return no output. */
  @Test void testUnresolvableCikThrowsInsteadOfReturningEmpty() throws IOException {
    Path accessionDir = tempDir.resolve("data").resolve("filing-without-cik");
    Files.createDirectories(accessionDir);
    Path sourceFile = accessionDir.resolve("doc.xml");
    Files.write(sourceFile,
        "<?xml version=\"1.0\"?><root><unrelated>no identifying data here</unrelated></root>"
            .getBytes(StandardCharsets.UTF_8));

    String targetDir = tempDir.resolve("output").toString();
    Files.createDirectories(tempDir.resolve("output"));
    ConversionMetadata metadata = new ConversionMetadata(targetDir);

    IOException e = assertThrows(IOException.class,
        () -> newConverter().convert(sourceFile.toString(), targetDir, metadata),
        "A document with no resolvable CIK should throw so fix-sec.sh retries it, "
            + "not silently record it as processed-and-empty");
    assertTrue(e.getMessage() != null && e.getMessage().contains("Failed to convert XBRL to Parquet"),
        "Expected the convertInternal wrapper message, got: " + e.getMessage());
  }

  /** A resolvable CIK via issuerCik, but no period end date anywhere in the document — the
   *  fiscal-year-dependent path used to silently return no output for this too. */
  @Test void testMissingPeriodEndDateThrowsInsteadOfReturningEmpty() throws IOException {
    Path accessionDir = tempDir.resolve("data").resolve("filing-without-period");
    Files.createDirectories(accessionDir);
    Path sourceFile = accessionDir.resolve("doc.xml");
    Files.write(sourceFile,
        "<?xml version=\"1.0\"?><root><issuerCik>1234567890</issuerCik></root>"
            .getBytes(StandardCharsets.UTF_8));

    String targetDir = tempDir.resolve("output").toString();
    Files.createDirectories(tempDir.resolve("output"));
    ConversionMetadata metadata = new ConversionMetadata(targetDir);

    assertThrows(IOException.class,
        () -> newConverter().convert(sourceFile.toString(), targetDir, metadata),
        "A document with a resolvable CIK but no period end date should throw, not "
            + "silently record it as processed-and-empty");
  }
}
