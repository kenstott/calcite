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

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies DQ fixes in XbrlToParquetConverter for filing_metadata:
 * filing_type normalization, date normalization, fiscal_year_end, sic_code, ticker, fiscal_year.
 */
class FilingMetadataDqTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(FilingMetadataDqTest.class);

  // ---- Unit tests for static DQ helpers ----

  @Test
  @Tag("unit")
  void testNormalizeFilingType() {
    assertEquals("10-K",   XbrlToParquetConverter.normalizeFilingType("10K"));
    assertEquals("10-K",   XbrlToParquetConverter.normalizeFilingType("10-K"));
    assertEquals("10-Q",   XbrlToParquetConverter.normalizeFilingType("10Q"));
    assertEquals("10-Q",   XbrlToParquetConverter.normalizeFilingType("10-Q"));
    assertEquals("10-K/A", XbrlToParquetConverter.normalizeFilingType("10K/A"));
    assertEquals("10-K/A", XbrlToParquetConverter.normalizeFilingType("10KA"));
    assertEquals("10-Q/A", XbrlToParquetConverter.normalizeFilingType("10QA"));
    assertEquals("8-K",    XbrlToParquetConverter.normalizeFilingType("8-K"));
    assertEquals(null,     XbrlToParquetConverter.normalizeFilingType(null));
  }

  @Test
  @Tag("unit")
  void testNormalizeDateToIso() {
    assertEquals("2023-12-31", XbrlToParquetConverter.normalizeDateToIso("December 31, 2023"));
    assertEquals("2023-12-31", XbrlToParquetConverter.normalizeDateToIso("December 31 , 2023"));
    assertEquals("2023-12-31", XbrlToParquetConverter.normalizeDateToIso("Dec 31, 2023"));
    assertEquals("2023-12-31", XbrlToParquetConverter.normalizeDateToIso("12/31/2023"));
    assertEquals("2023-12-31", XbrlToParquetConverter.normalizeDateToIso("2023-12-31"));
    assertEquals("2023-09-30", XbrlToParquetConverter.normalizeDateToIso("September 30, 2023"));
    assertEquals(null,         XbrlToParquetConverter.normalizeDateToIso(null));
    assertEquals(null,         XbrlToParquetConverter.normalizeDateToIso("  "));
  }

  @Test
  @Tag("unit")
  void testNormalizeFiscalYearEnd() {
    assertEquals("--09-26", XbrlToParquetConverter.normalizeFiscalYearEnd("0926"));
    assertEquals("--12-31", XbrlToParquetConverter.normalizeFiscalYearEnd("1231"));
    assertEquals("--09-26", XbrlToParquetConverter.normalizeFiscalYearEnd("--09-26"));
    assertEquals(null,      XbrlToParquetConverter.normalizeFiscalYearEnd(null));
  }

  // ---- Integration test: run ETL on embedded XBRL resource, verify DQ columns ----

  @TempDir
  File tempDir;

  @Test
  @Tag("integration")
  void testAaplTenKDqColumns() throws Exception {
    String cik = "0000320193";
    String accession = "0000320193-23-000106";

    // Use embedded minimal XBRL resource — avoids SEC rate-limit issues
    java.net.URL resourceUrl = getClass().getClassLoader().getResource("aapl-10k-minimal.xml");
    assertNotNull(resourceUrl, "Test resource aapl-10k-minimal.xml must exist");
    File docFile = new File(resourceUrl.toURI());

    File outputDir = new File(tempDir, "parquet");
    outputDir.mkdirs();

    StorageProvider storage = new LocalFileStorageProvider();
    XbrlToParquetConverter converter = new XbrlToParquetConverter(storage, false);

    ConversionMetadata metadata = new ConversionMetadata(tempDir);
    metadata.setHint("cik", cik);
    metadata.setHint("form", "10-K");
    metadata.setHint("filingDate", "2023-11-03");
    metadata.setHint("accession", accession);

    String outputRelPath = "parquet/year=2023/0000320193_0000320193-23-000106_metadata.parquet";
    converter.convert(docFile.getAbsolutePath(), outputDir.getAbsolutePath(), metadata);

    // Find the metadata parquet file specifically
    File parquetFile = findParquetFile(outputDir, "_metadata.parquet");
    assertNotNull(parquetFile, "Parquet metadata file should have been written under " + outputDir);
    LOGGER.info("Reading parquet: {}", parquetFile.getAbsolutePath());

    List<String> filingTypes = new ArrayList<>();
    List<String> tickers = new ArrayList<>();
    List<String> sicCodes = new ArrayList<>();
    List<String> fiscalYearEnds = new ArrayList<>();
    List<String> periodsOfReport = new ArrayList<>();

    Configuration conf = new Configuration();
    try (ParquetReader<GenericRecord> reader = AvroParquetReader
        .<GenericRecord>builder(HadoopInputFile.fromPath(
            new Path(parquetFile.getAbsolutePath()), conf))
        .withConf(conf)
        .build()) {
      GenericRecord rec;
      while ((rec = reader.read()) != null) {
        Object ft = rec.get("filing_type");
        if (ft != null) filingTypes.add(ft.toString());
        Object tk = rec.get("ticker");
        if (tk != null) tickers.add(tk.toString());
        Object sic = rec.get("sic_code");
        if (sic != null) sicCodes.add(sic.toString());
        Object fye = rec.get("fiscal_year_end");
        if (fye != null) fiscalYearEnds.add(fye.toString());
        Object por = rec.get("period_of_report");
        if (por != null) periodsOfReport.add(por.toString());
      }
    }

    LOGGER.info("filing_type values: {}", filingTypes);
    LOGGER.info("ticker values: {}", tickers);
    LOGGER.info("sic_code values: {}", sicCodes);
    LOGGER.info("fiscal_year_end values: {}", fiscalYearEnds);
    LOGGER.info("period_of_report values: {}", periodsOfReport);

    assertTrue(filingTypes.contains("10-K"), "filing_type should be normalized to '10-K'");
    assertTrue(tickers.stream().anyMatch(v -> "AAPL".equalsIgnoreCase(v)),
        "ticker should be 'AAPL'");
    assertTrue(sicCodes.stream().anyMatch(v -> !v.isEmpty()),
        "sic_code should be populated from EDGAR submissions.json");
    assertTrue(fiscalYearEnds.stream().anyMatch(v -> v.startsWith("--")),
        "fiscal_year_end should be in --MM-DD format");
    assertTrue(periodsOfReport.stream().anyMatch(v -> v.matches("\\d{4}-\\d{2}-\\d{2}")),
        "period_of_report should be ISO date (YYYY-MM-DD)");
  }

  private File findParquetFile(File dir, String suffix) throws Exception {
    if (dir.isFile() && dir.getName().endsWith(suffix)) return dir;
    File[] children = dir.listFiles();
    if (children != null) {
      for (File child : children) {
        File found = findParquetFile(child, suffix);
        if (found != null) return found;
      }
    }
    return null;
  }
}
