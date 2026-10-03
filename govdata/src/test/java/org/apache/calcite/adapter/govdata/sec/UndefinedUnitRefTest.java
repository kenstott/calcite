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

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A fact whose unitRef the filing never declares must cost only that fact, not the whole
 * filing's facts.parquet.
 */
@Tag("unit")
class UndefinedUnitRefTest {

  @TempDir
  File tempDir;

  @Test void undeclaredUnitDropsOnlyThatFact() throws Exception {
    String xml = "<?xml version='1.0' encoding='UTF-8'?>"
        + "<xbrl xmlns='http://www.xbrl.org/2003/instance'"
        + " xmlns:dei='http://xbrl.sec.gov/dei/2022'"
        + " xmlns:us-gaap='http://fasb.org/us-gaap/2022'>"
        + "<dei:EntityRegistrantName contextRef='d1'>Test Co</dei:EntityRegistrantName>"
        + "<dei:DocumentType contextRef='d1'>10-K</dei:DocumentType>"
        + "<context id='d1'><entity><identifier scheme='http://www.sec.gov/CIK'>0000000001"
        + "</identifier></entity><period><instant>2023-12-31</instant></period></context>"
        + "<unit id='usd'><measure>iso4217:USD</measure></unit>"
        + "<us-gaap:Assets contextRef='d1' unitRef='usd' decimals='0'>500</us-gaap:Assets>"
        + "<dei:EntityPublicFloat contextRef='d1' unitRef='USD' decimals='0'>"
        + "700</dei:EntityPublicFloat>"
        + "</xbrl>";
    File doc = new File(tempDir, "undeclared-unit.xml");
    Files.write(doc.toPath(), xml.getBytes(StandardCharsets.UTF_8));
    File outputDir = new File(tempDir, "parquet");
    outputDir.mkdirs();

    ConversionMetadata metadata = new ConversionMetadata(tempDir);
    metadata.setHint("cik", "0000000001");
    metadata.setHint("form", "10-K");
    metadata.setHint("filingDate", "2024-02-01");
    metadata.setHint("accession", "0000000001-24-000001");

    XbrlToParquetConverter converter = new XbrlToParquetConverter(new LocalFileStorageProvider(),
        false, new HashSet<>(Collections.singletonList("financial_line_items")));
    List<String> outputs =
        converter.convert(doc.getAbsolutePath(), outputDir.getAbsolutePath(), metadata);

    String factsPath = null;
    for (String p : outputs) {
      if (p.endsWith("_facts.parquet")) {
        factsPath = p;
      }
    }
    assertNotNull(factsPath, "facts.parquet must still be written: " + outputs);

    List<String> concepts = new ArrayList<>();
    Configuration conf = new Configuration();
    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
        HadoopInputFile.fromPath(new Path(factsPath), conf)).withConf(conf).build()) {
      GenericRecord rec;
      while ((rec = reader.read()) != null) {
        concepts.add(String.valueOf(rec.get("concept")));
      }
    }
    assertTrue(concepts.contains("us-gaap:Assets"), "declared-unit fact kept: " + concepts);
    assertFalse(concepts.contains("dei:EntityPublicFloat"),
        "undeclared-unit fact dropped: " + concepts);
  }
}
