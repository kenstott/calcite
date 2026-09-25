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
package org.apache.calcite.adapter.govdata.ref;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link ChunkOrganizer#stripHtml}, used by the document-blob sources whose text
 * column holds HTML (law.bill_summaries, law.bills.constitutional_authority_statement).
 */
@Tag("unit")
class ChunkOrganizerHtmlStripTest {

  @Test void testParagraphsBecomeLinesAndInlineTagsAreDropped() {
    assertEquals("Laken Riley Act\nThis bill (aliens under law).",
        ChunkOrganizer.stripHtml("<p><strong>Laken Riley Act</strong></p>"
            + "<p>This bill (<em>aliens</em> under law).</p>"));
  }

  @Test void testListItemsGetOneLineEach() {
    assertEquals("one\ntwo", ChunkOrganizer.stripHtml("<ul><li>one</li><li>two</li></ul>"));
  }

  @Test void testKnownEntitiesAreDecoded() {
    assertEquals("A B & C <D> \"E\" ’",
        ChunkOrganizer.stripHtml("A&nbsp;B &amp; C &lt;D&gt; &quot;E&quot; &#8217;"));
  }

  @Test void testAnEscapedEntityIsNotDecodedTwice() {
    assertEquals("&lt;", ChunkOrganizer.stripHtml("&amp;lt;"));
  }

  @Test void testAnUnknownEntityIsLeftAsWritten() {
    assertEquals("caf&eacute;", ChunkOrganizer.stripHtml("caf&eacute;"));
  }

  @Test void testPreBlockWithAnchorKeepsTheLinkText() {
    assertEquals("[Congressional Record Volume 171][House]By Mr. E [www.gpo.gov]",
        ChunkOrganizer.stripHtml("<pre>[Congressional Record Volume 171][House]By Mr. E "
            + "[<a href=\"https://www.gpo.gov\">www.gpo.gov</a>]</pre>"));
  }

  @Test void testParamIsNotTreatedAsAParagraphTag() {
    assertEquals("ab", ChunkOrganizer.stripHtml("a<param x=\"1\">b"));
  }

  @Test void testMarkupOnlyInputBecomesEmpty() {
    assertEquals("", ChunkOrganizer.stripHtml("<p>&nbsp;</p><br/>"));
  }

  @Test void testRowConcatStripsOnlyTheHtmlColumn() {
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("title", "A & B");
    row.put("statement", "<pre>Article I, <a href=\"x\">section 8</a></pre>");
    assertEquals("title: A & B | statement: Article I, section 8",
        ChunkOrganizer.buildRowConcatText(row, Arrays.asList("title", "statement"),
            Collections.singleton("statement")));
  }

  @Test void testRowConcatOmitsAnHtmlColumnThatIsOnlyMarkup() {
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("title", "Only a title");
    row.put("statement", "<pre>&nbsp;</pre>");
    assertEquals("title: Only a title",
        ChunkOrganizer.buildRowConcatText(row, Arrays.asList("title", "statement"),
            Collections.singleton("statement")));
  }
}
