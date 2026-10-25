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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Document;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A plain-text main document from a filer with no XBRL (for example a shell company's
 * {@code r10q-0325.txt}) is a filing with no XBRL: it must produce its metadata row from the
 * submission hints, not fail for want of a CIK and period end date that only XBRL carries.
 */
@Tag("unit")
class PlainTextFilingTest {

  @TempDir
  Path tempDir;

  private static final String TEXT_10Q = "<DOCUMENT>\n<TYPE>10-Q\n<TEXT>\n"
      + "<TABLE>\n<CAPTION>\n<S> <C> <C>\nAssets 1,000 2,000\n</TABLE>\n"
      + "Part I. Financial Information & notes\n</TEXT>\n</DOCUMENT>\n";

  @Test void plainTextFilingProducesMetadataFromHints() throws IOException {
    Path src = tempDir.resolve("r10q-0325.txt");
    Files.write(src, TEXT_10Q.getBytes(StandardCharsets.UTF_8));
    String target = tempDir.resolve("out").toString();
    Files.createDirectories(tempDir.resolve("out"));

    ConversionMetadata metadata = new ConversionMetadata(target);
    metadata.setHint("cik", "0000918545");
    metadata.setHint("form", "10-Q");
    metadata.setHint("filingDate", "2025-08-14");
    metadata.setHint("accession", "0000918545-25-000003");

    List<String> out = new XbrlToParquetConverter(new LocalFileStorageProvider())
        .convert(src.toString(), target, metadata);

    assertEquals(1, out.size(), "metadata row only: " + out);
    assertTrue(out.get(0).endsWith("0000918545_0000918545-25-000003_metadata.parquet"), out.get(0));
    assertTrue(Files.exists(Path.of(out.get(0))), "metadata parquet written");
  }

  @Test void xbrlStructureIsDetectedWithAndWithoutPrefix() throws Exception {
    assertTrue(parse("<xbrli:xbrl xmlns:xbrli='http://www.xbrl.org/2003/instance'>"
        + "<xbrli:context id='c'/></xbrli:xbrl>"));
    assertTrue(parse("<xbrl><context id='c'/></xbrl>"));
    assertFalse(parse("<html><body><table><tr><td>Assets</td></tr></table></body></html>"));
  }

  /** Filers sometimes hard-wrap an ix:nonFraction name mid-token; that must not fail the filing. */
  @Test void wrappedFactNameDoesNotFailInlineXbrl() throws IOException {
    String htm = "<html xmlns:ix=\"http://www.xbrl.org/2013/inlineXBRL\">"
        + "<body><ix:header><ix:resources>"
        + "<xbrli:context id=\"c1\"><xbrli:entity><xbrli:identifier scheme=\"http://www.sec.gov/CIK\">"
        + "0001579241</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:instant>2024-12-31"
        + "</xbrli:instant></xbrli:period></xbrli:context></ix:resources></ix:header>"
        + "<ix:nonNumeric name=\"dei:EntityCentralIndexKey\" contextRef=\"c1\">0001579241</ix:nonNumeric>"
        + "<ix:nonFraction name=\"us-gaap:PropertyPlantAnd\nEquipmentGross\" contextRef=\"c1\""
        + " unitRef=\"usd\" decimals=\"-5\" scale=\"6\">1,234</ix:nonFraction>"
        + "</body></html>";
    Path src = tempDir.resolve("alle-20241231.htm");
    Files.write(src, htm.getBytes(StandardCharsets.UTF_8));

    Document doc = new XbrlToParquetConverter(new LocalFileStorageProvider())
        .parseInlineXbrl(src.toString());

    assertTrue(doc.getElementsByTagName("us-gaap:PropertyPlantAndEquipmentGross").getLength() > 0);
  }

  private static boolean parse(String xml) throws Exception {
    Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
        .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    return XbrlToParquetConverter.hasXbrlStructure(doc);
  }
}
