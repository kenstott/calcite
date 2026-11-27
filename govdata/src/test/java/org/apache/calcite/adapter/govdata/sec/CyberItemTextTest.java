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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests for cutting the Item 1.05 section out of an 8-K primary document. */
@Tag("unit")
class CyberItemTextTest {

  private static String extract(String body) {
    return XbrlToParquetConverter.extractItemText("<html><body>" + body + "</body></html>",
        "1.05");
  }

  @Test void testSectionEndsAtTheNextItemHeader() {
    assertEquals("Material Cybersecurity Incidents. We detected an intrusion.",
        extract("<p>Item 1.05 Material Cybersecurity Incidents. We detected an intrusion.</p>"
            + "<p>Item 9.01 Financial Statements and Exhibits.</p>"));
  }

  @Test void testHeaderWithUnicodeSpaces() {
    assertEquals("Material Cybersecurity Incidents. We detected an intrusion.",
        extract("<p>Item&#8201;1.05 Material Cybersecurity Incidents. We detected an intrusion."
            + "</p><p>Item&#8195;9.01 Financial Statements and Exhibits.</p>"));
    assertEquals("Material Cybersecurity Incidents.",
        extract("<p>ITEM&#160;1.05 Material Cybersecurity Incidents.</p><p>SIGNATURES</p>"));
  }

  @Test void testDocumentWithoutTheItemHasNoText() {
    assertNull(extract("<p>Item 8.01 Other Events.</p>"));
  }
}
