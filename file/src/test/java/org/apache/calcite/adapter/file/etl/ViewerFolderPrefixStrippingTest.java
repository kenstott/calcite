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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * {@code DocumentETLProcessor.stripViewerFolderPrefix} used to only strip the XSL-viewer
 * folder prefix for Forms 3/4/5 ({@code xslF345X03/wf-form4_xxx.xml}), leaving every other
 * form type — 13F-HR's {@code xslForm13F_X02/primary_doc.xml} included — fetching EDGAR's
 * rendered HTML view instead of the real document submissions.json names. That produced a
 * document the strict XML parser rejects (an unclosed HTML {@code <meta>} tag), which the
 * JSoup fallback parses into a near-empty structure, leaving {@code filing_metadata.company_
 * name} null for every 13F-HR filing. Confirmed live: CIK 0001067983, accession
 * 0001193125-26-226661 — before this fix, 6 of 6 sampled 13F-HR rows had a null company_name;
 * after, 0 of 6.
 */
@Tag("unit")
class ViewerFolderPrefixStrippingTest {

  @Test void testThirteenFViewerPathIsStripped() {
    assertEquals("primary_doc.xml",
        DocumentETLProcessor.stripViewerFolderPrefix("xslForm13F_X02/primary_doc.xml"));
  }

  @Test void testFormFourViewerPathIsStripped() {
    assertEquals("wf-form4_12345.xml",
        DocumentETLProcessor.stripViewerFolderPrefix("xslF345X03/wf-form4_12345.xml"));
  }

  @Test void testOtherObservedViewerPathsAreStripped() {
    assertEquals("primary_doc.xml",
        DocumentETLProcessor.stripViewerFolderPrefix("xslSCHEDULE_13G_X02/primary_doc.xml"));
    assertEquals("doc4a.xml",
        DocumentETLProcessor.stripViewerFolderPrefix("xslF345X01/doc4a.xml"));
  }

  @Test void testBareFilenamePassesThroughUnchanged() {
    assertEquals("aapl-20240928.htm",
        DocumentETLProcessor.stripViewerFolderPrefix("aapl-20240928.htm"));
    assertEquals("primary_doc.xml",
        DocumentETLProcessor.stripViewerFolderPrefix("primary_doc.xml"));
  }

  @Test void testNullPassesThroughAsNull() {
    assertNull(DocumentETLProcessor.stripViewerFolderPrefix(null));
  }

  @Test void testDeeplyNestedPathKeepsOnlyTheFinalSegment() {
    assertEquals("doc.xml", DocumentETLProcessor.stripViewerFolderPrefix("a/b/c/doc.xml"));
  }
}
