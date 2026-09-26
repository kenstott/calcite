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

import org.apache.calcite.adapter.file.storage.LocalFileStorageProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Document;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * A unit id is arbitrary per filer, so the canonical unit comes from the measure inside the
 * filing's own unit definition, not from the id.
 */
@Tag("unit")
class XbrlUnitResolutionTest {

  @TempDir
  Path tempDir;

  @Test void traditionalInstanceResolvesMeasures() throws Exception {
    String xml = "<?xml version='1.0'?>"
        + "<xbrl xmlns:xbrli='http://www.xbrl.org/2003/instance'>"
        + "<xbrli:unit id='U_iso4217USD'><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>"
        + "<xbrli:unit id='eur'><xbrli:measure>iso4217:EUR</xbrli:measure></xbrli:unit>"
        + "<xbrli:unit id='sh'><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unit>"
        + "<xbrli:unit id='p'><xbrli:measure>xbrli:pure</xbrli:measure></xbrli:unit>"
        + "<xbrli:unit id='eps'><xbrli:divide>"
        + "<xbrli:unitNumerator><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unitNumerator>"
        + "<xbrli:unitDenominator><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unitDenominator>"
        + "</xbrli:divide></xbrli:unit>"
        + "<xbrli:unit id='eurPerShare'><xbrli:divide>"
        + "<xbrli:unitNumerator><xbrli:measure>iso4217:EUR</xbrli:measure></xbrli:unitNumerator>"
        + "<xbrli:unitDenominator><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unitDenominator>"
        + "</xbrli:divide></xbrli:unit>"
        + "</xbrl>";
    Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
        .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

    Map<String, String> labels = XbrlToParquetConverter.resolveUnitLabels(doc);

    assertEquals("USD", labels.get("U_iso4217USD"));
    assertEquals("EUR", labels.get("eur"));
    assertEquals("shares", labels.get("sh"));
    assertEquals("pure", labels.get("p"));
    assertEquals("usdPerShare", labels.get("eps"));
    assertEquals("EUR/shares", labels.get("eurPerShare"));
  }

  @Test void inlineFilingWithOpaqueUnitIdsResolvesMeasures() throws Exception {
    String html = "<html xmlns:ix='http://www.xbrl.org/2013/inlineXBRL'"
        + " xmlns:xbrli='http://www.xbrl.org/2003/instance'><body>"
        + "<ix:header><ix:resources>"
        + "<xbrli:context id='c1'><xbrli:entity><xbrli:identifier scheme='http://www.sec.gov/CIK'>"
        + "0000000001</xbrli:identifier></xbrli:entity>"
        + "<xbrli:period><xbrli:instant>2022-12-31</xbrli:instant></xbrli:period></xbrli:context>"
        + "<xbrli:unit id='Unit12'><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>"
        + "<xbrli:unit id='Unit15'><xbrli:divide>"
        + "<xbrli:unitNumerator><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unitNumerator>"
        + "<xbrli:unitDenominator><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unitDenominator>"
        + "</xbrli:divide></xbrli:unit>"
        + "<xbrli:unit id='Unit18'><xbrli:measure>tbbk:segment</xbrli:measure></xbrli:unit>"
        + "</ix:resources></ix:header>"
        + "<ix:nonFraction name='us-gaap:Revenues' contextRef='c1' unitRef='Unit12' decimals='-3'"
        + " scale='3'>1,234</ix:nonFraction>"
        + "</body></html>";
    Path file = tempDir.resolve("filing.htm");
    Files.write(file, html.getBytes(StandardCharsets.UTF_8));

    XbrlToParquetConverter converter = new XbrlToParquetConverter(new LocalFileStorageProvider());
    Method parse = XbrlToParquetConverter.class.getDeclaredMethod("parseInlineXbrl", String.class);
    parse.setAccessible(true);
    Document doc = (Document) parse.invoke(converter, file.toString());
    assertNotNull(doc);

    Map<String, String> labels = XbrlToParquetConverter.resolveUnitLabels(doc);

    assertEquals("USD", labels.get("Unit12"));
    assertEquals("usdPerShare", labels.get("Unit15"));
    assertEquals("number", labels.get("Unit18"));
  }
}
