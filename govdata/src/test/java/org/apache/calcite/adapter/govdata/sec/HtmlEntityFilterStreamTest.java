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
package org.apache.calcite.adapter.govdata.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import javax.xml.parsers.DocumentBuilderFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The filter that makes EDGAR XML parseable must change nothing the parser would read correctly.
 * Four ways it used to change text: a reference that began a read chunk lost its {@code &}
 * ("COHEN amp; STEERS"), numeric references were escaped to literal text ("LEGGETT&#38; PLATT"),
 * text inside CDATA was escaped ("LEGGETT &amp; PLATT"), and "&amp;;" lost its semicolon.
 */
@Tag("unit")
class HtmlEntityFilterStreamTest {

  /** A source that hands out at most {@code chunk} bytes per read, to move read boundaries. */
  private static final class ChunkedStream extends InputStream {
    private final byte[] data;
    private final int chunk;
    private int pos;

    ChunkedStream(byte[] data, int chunk) {
      this.data = data;
      this.chunk = chunk;
    }

    @Override public int read() {
      return pos < data.length ? data[pos++] & 0xFF : -1;
    }

    @Override public int read(byte[] b, int off, int len) {
      if (pos >= data.length) {
        return -1;
      }
      int n = Math.min(Math.min(len, chunk), data.length - pos);
      System.arraycopy(data, pos, b, off, n);
      pos += n;
      return n;
    }

    @Override public int available() {
      return data.length - pos;
    }
  }

  private static String filterBulk(String in, int sourceChunk, int readSize) throws IOException {
    InputStream f = new XbrlToParquetConverter.HtmlEntityFilterStream(
        new ChunkedStream(in.getBytes(StandardCharsets.UTF_8), sourceChunk));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[readSize];
    int n;
    while ((n = f.read(buf, 0, buf.length)) != -1) {
      out.write(buf, 0, n);
    }
    return new String(out.toByteArray(), StandardCharsets.UTF_8);
  }

  private static String filterByByte(String in) throws IOException {
    InputStream f = new XbrlToParquetConverter.HtmlEntityFilterStream(
        new ByteArrayInputStream(in.getBytes(StandardCharsets.UTF_8)));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int b;
    while ((b = f.read()) != -1) {
      out.write(b);
    }
    return new String(out.toByteArray(), StandardCharsets.UTF_8);
  }

  private static String text(String xml) throws Exception {
    return DocumentBuilderFactory.newInstance().newDocumentBuilder()
        .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)))
        .getDocumentElement().getTextContent();
  }

  @Test void predefinedEntitiesPassThrough() throws IOException {
    assertEquals("<a>M &amp; T &lt;&gt; &quot;x&quot; &apos;y&apos;</a>",
        filterByByte("<a>M &amp; T &lt;&gt; &quot;x&quot; &apos;y&apos;</a>"));
  }

  @Test void bareAmpersandIsEscaped() throws IOException {
    assertEquals("<a>Smith &amp; Jones</a>", filterByByte("<a>Smith & Jones</a>"));
    assertEquals("<a>AT&amp;T</a>", filterByByte("<a>AT&T</a>"));
    assertEquals("<a>&amp;P500</a>", filterByByte("<a>&P500</a>"));
  }

  @Test void ampersandAtEndOfStreamIsEscaped() throws IOException {
    assertEquals("AT&amp;", filterByByte("AT&"));
    assertEquals("AT&amp;#", filterByByte("AT&#"));
  }

  @Test void numericReferencesToLegalCharactersAreKept() throws Exception {
    assertEquals("<a>LEGGETT&#38; PLATT</a>", filterByByte("<a>LEGGETT&#38; PLATT</a>"));
    assertEquals("<a>&#x26;&#X26;&#233;</a>", filterByByte("<a>&#x26;&#X26;&#233;</a>"));
    // The parser, not the filter, turns the reference into the character.
    assertEquals("LEGGETT& PLATT", text(filterByByte("<a>LEGGETT&#38; PLATT</a>")));
  }

  @Test void numericReferencesToIllegalCharactersAreEscapedNotFatal() throws Exception {
    assertEquals("<a>&amp;#0;</a>", filterByByte("<a>&#0;</a>"));
    assertEquals("<a>&amp;#x1;</a>", filterByByte("<a>&#x1;</a>"));
    assertEquals("<a>&amp;#xFFFFFFFFF;</a>", filterByByte("<a>&#xFFFFFFFFF;</a>"));
    assertEquals("&#0;", text(filterByByte("<a>&#0;</a>")));
  }

  @Test void malformedNumericReferenceIsEscapedWithoutLosingCharacters() throws IOException {
    assertEquals("<a>&amp;#;x</a>", filterByByte("<a>&#;x</a>"));
    assertEquals("<a>&amp;#12 x</a>", filterByByte("<a>&#12 x</a>"));
    assertEquals("<a>&amp;#x;</a>", filterByByte("<a>&#x;</a>"));
  }

  @Test void semicolonAfterBareAmpersandIsKept() throws IOException {
    assertEquals("<a>&amp;;</a>", filterByByte("<a>&;</a>"));
  }

  @Test void htmlNamedEntitiesBecomeNumericAndUnknownOnesBecomeText() throws Exception {
    assertEquals("<a>x&#160;y</a>", filterByByte("<a>x&nbsp;y</a>"));
    assertEquals("<a>&amp;foo;</a>", filterByByte("<a>&foo;</a>"));
    assertEquals("&foo;", text(filterByByte("<a>&foo;</a>")));
  }

  @Test void cdataSectionIsLeftVerbatim() throws Exception {
    String in = "<a><![CDATA[LEGGETT & PLATT &amp; S&P &#38; &foo;]]></a>";
    assertEquals(in, filterByByte(in));
    assertEquals("LEGGETT & PLATT &amp; S&P &#38; &foo;", text(filterByByte(in)));
  }

  @Test void textAfterCdataIsFilteredAgain() throws IOException {
    assertEquals("<a><![CDATA[x & y]]> then &amp;P and &amp; z</a>",
        filterByByte("<a><![CDATA[x & y]]> then &P and &amp; z</a>"));
  }

  @Test void cdataEndingAfterExtraBracketIsRecognised() throws IOException {
    assertEquals("<a><![CDATA[x]] & y]]]> &amp;</a>",
        filterByByte("<a><![CDATA[x]] & y]]]> &</a>"));
  }

  @Test void referenceBeginningAReadChunkKeepsItsAmpersand() throws Exception {
    // The source hands out 6 bytes at a time, so the "&amp;" lands at the start of a chunk.
    String in = "<a>COHEN &amp; STEERS INC</a>";
    for (int chunk = 1; chunk <= 12; chunk++) {
      for (int readSize = 1; readSize <= 12; readSize++) {
        assertEquals(in, filterBulk(in, chunk, readSize), "chunk=" + chunk + " read=" + readSize);
      }
    }
  }

  @Test void outputDoesNotDependOnReadBoundaries() throws IOException {
    String[] pieces = {"A", " ", "&amp;", "&", "&#38;", "&#x26;", "&#0;", "&nbsp;", "&foo;", "&;",
        "&#", "<![CDATA[", "]]>", "x & y", "]]", "<b>", "</b>", "&lt;", "é", "&P500", "S&P"};
    Random rnd = new Random(42);
    for (int trial = 0; trial < 300; trial++) {
      StringBuilder sb = new StringBuilder();
      int n = 1 + rnd.nextInt(30);
      for (int i = 0; i < n; i++) {
        sb.append(pieces[rnd.nextInt(pieces.length)]);
      }
      String in = sb.toString();
      String expected = filterByByte(in);
      for (int chunk : new int[] {1, 2, 3, 5, 8, 13, 64}) {
        for (int readSize : new int[] {1, 2, 3, 7, 16, 8192}) {
          assertEquals(expected, filterBulk(in, chunk, readSize),
              "input=" + in + " chunk=" + chunk + " read=" + readSize);
        }
      }
    }
  }

  @Test void nonAsciiTextIsUntouched() throws IOException {
    assertEquals("<a>Nestlé ’s</a>", filterByByte("<a>Nestlé ’s</a>"));
  }
}
