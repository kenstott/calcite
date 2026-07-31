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

/**
 * Tests for stripping EDGAR's XSLT-viewer path off a {@code primaryDocument} filename.
 *
 * <p>submissions.json names, as the primary document, whichever file a browser should render.
 * For a filing that is not itself inline XBRL, that is an auto-generated HTML view nested under a
 * folder named for the stylesheet — not the underlying data file this pipeline parses. Using the
 * nested path verbatim downloads that HTML view under a name ending in {@code .xml}: the strict
 * parser fails on it, JSoup's lenient fallback "successfully" parses the HTML shell, and the
 * converter is handed a document with none of the fields it expects. One two-hour 13F repair
 * batch hit exactly this failure 879 times, on documents that were never malformed at all — the
 * rendered view was fetched in place of the real file, whose actual content parses cleanly.
 */
@Tag("unit")
class PrimaryDocumentNormalizationTest {

  /** Confirmed live: this exact HTML, including the unclosed &lt;meta&gt; that fails XML parsing. */
  @Test void testThirteenFViewerPathIsStripped() {
    assertEquals("primary_doc.xml",
        SecSchemaFactory.normalizePrimaryDocument("xslForm13F_X01/primary_doc.xml"));
  }

  /** Confirmed live for a real Form 4; the stylesheet folder name varies by EDGAR renderer version. */
  @Test void testFormFourViewerPathIsStripped() {
    assertEquals("ownership.xml",
        SecSchemaFactory.normalizePrimaryDocument("xslF345X05/ownership.xml"));
  }

  /**
   * Every observed variant across form types shares the pattern: an {@code xslXxx} stylesheet
   * folder, then the real file's own name.
   */
  @Test void testOtherObservedViewerPathsAreStripped() {
    assertEquals("primary_doc.xml",
        SecSchemaFactory.normalizePrimaryDocument("xslSCHEDULE_13G_X02/primary_doc.xml"));
    assertEquals("primary_doc.xml",
        SecSchemaFactory.normalizePrimaryDocument("xslN-PX_X01/primary_doc.xml"));
    assertEquals("doc4a.xml",
        SecSchemaFactory.normalizePrimaryDocument("xslF345X03/doc4a.xml"));
    assertEquals("primary_doc.xml",
        SecSchemaFactory.normalizePrimaryDocument("xsl144X01/primary_doc.xml"));
  }

  /**
   * Every inline-XBRL form type observed (10-K, 10-Q, 8-K, DEF 14A) reports a bare filename with
   * no viewer path — inline XBRL is the human- and machine-readable document in one file, so
   * there is no separate rendering to route around. The normalization must be a no-op here.
   */
  @Test void testBareFilenamePassesThroughUnchanged() {
    assertEquals("aapl-20240928.htm",
        SecSchemaFactory.normalizePrimaryDocument("aapl-20240928.htm"));
    assertEquals("primary_doc.xml", SecSchemaFactory.normalizePrimaryDocument("primary_doc.xml"));
  }

  @Test void testNullPassesThroughAsNull() {
    assertNull(SecSchemaFactory.normalizePrimaryDocument(null));
  }

  /**
   * Only the final path segment is kept, however many the value has.
   *
   * <p>Not observed in practice — every real value seen is exactly one folder deep — but the
   * document this pipeline parses is always the last-named file regardless of nesting depth.
   */
  @Test void testDeeplyNestedPathKeepsOnlyTheFinalSegment() {
    assertEquals("doc.xml", SecSchemaFactory.normalizePrimaryDocument("a/b/c/doc.xml"));
  }
}
