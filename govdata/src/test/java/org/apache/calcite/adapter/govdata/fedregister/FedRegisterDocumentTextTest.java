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
package org.apache.calcite.adapter.govdata.fedregister;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests summary/body text extraction from a Federal Register daily XML file. */
@Tag("unit")
class FedRegisterDocumentTextTest {

  private static final String XML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      + "<FEDREG><RULES>\n"
      + "<RULE><PREAMB><AGENCY>DOE</AGENCY><SUBJECT>Title</SUBJECT>\n"
      + "<SUM><HD SOURCE=\"HED\">SUMMARY:</HD><P>The agency\n   amends a rule &amp; more.</P></SUM>\n"
      + "</PREAMB>\n"
      + "<SUPLINF><HD SOURCE=\"HED\">SUPPLEMENTARY INFORMATION:</HD>\n"
      + "<P>First <E T=\"03\">emphasis</E> paragraph.</P><P>Second paragraph.</P></SUPLINF>\n"
      + "<FRDOC>[FR Doc. 2025-00001 Filed 1-2-25; 8:45 am]</FRDOC></RULE>\n"
      + "<RULE><PREAMB><SUBJECT>No body</SUBJECT></PREAMB>\n"
      + "<FRDOC>[FR Doc. 2025-00002 Filed 1-2-25; 8:45 am]</FRDOC></RULE>\n"
      + "<RULE><SUPLINF><P>Reprint.</P></SUPLINF>\n"
      + "<FRDOC>[FR Doc. 2025-00001 Filed 1-2-25; 8:45 am]</FRDOC></RULE>\n"
      + "</RULES><PRESDOCS><PRESDOCU><PROCL><HD>Proclamation</HD><P>By the President.</P>\n"
      + "<FRDOC>[FR Doc. 2025-00003 Filed 1-2-25; 8:45 am]</FRDOC></PROCL></PRESDOCU>\n"
      + "</PRESDOCS></FEDREG>";

  @Test void testExtractsSummaryBodyAndPresidentialText(@TempDir File dir) throws Exception {
    File f = new File(dir, "FR-2025-01-02.xml");
    Files.write(f.toPath(), XML.getBytes(StandardCharsets.UTF_8));
    Deque<Map<String, Object>> rows = new ArrayDeque<Map<String, Object>>();
    FedRegisterDocumentTextDataProvider.parseDailyFile(f, rows, new HashSet<String>());

    assertEquals(3, rows.size());
    Map<String, Object> first = rows.poll();
    assertEquals("2025-00001", first.get("document_number"));
    assertEquals("The agency amends a rule & more.", first.get("summary"));
    assertEquals("First emphasis paragraph.\n\nSecond paragraph.", first.get("body_text"));

    Map<String, Object> second = rows.poll();
    assertEquals("2025-00002", second.get("document_number"));
    assertNull(second.get("summary"));
    assertNull(second.get("body_text"));

    Map<String, Object> pres = rows.poll();
    assertEquals("2025-00003", pres.get("document_number"));
    assertNull(pres.get("summary"));
    assertEquals("Proclamation\n\nBy the President.", pres.get("body_text"));
  }
}
