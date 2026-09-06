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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Unit tests for {@link CsvRecordReader#splitFields} and {@link CsvRecordReader#readRecord}. */
@Tag("unit")
public class CsvRecordReaderTest {

  private static List<String> split(String record) {
    return CsvRecordReader.splitFields(record, ',');
  }

  @Test void plainUnquoted() {
    assertEquals(java.util.Arrays.asList("a", "b", "c"), split("a,b,c"));
  }

  @Test void quotedNoSpace() {
    assertEquals(java.util.Arrays.asList("a", "b", "c"), split("\"a\",\"b\",\"c\""));
  }

  @Test void quotedWithCommaSpace_malwareBazaarStyle() {
    // abuse.ch / MalwareBazaar puts a space after each delimiter: "a", "b", "c"
    assertEquals(java.util.Arrays.asList("a", "b", "c"), split("\"a\", \"b\", \"c\""));
  }

  @Test void realMalwareBazaarRow_sha256Unquoted() {
    String sha = "e0646d7307c96469bb0cdd2a8b46c674c9445c889e60361364deb31d0b8b6529";
    String row = "\"2026-06-07 12:57:22\", \"" + sha + "\", \"5814e782497bfa006f379f4b13f04502\"";
    List<String> f = split(row);
    assertEquals("2026-06-07 12:57:22", f.get(0));
    assertEquals(sha, f.get(1));                                  // no leading space, no quotes
    assertEquals(true, f.get(1).matches("^[A-Fa-f0-9]{64}$"));    // passes the hash validation
  }

  @Test void embeddedDelimiterInsideQuotes() {
    assertEquals(java.util.Arrays.asList("a,b", "c"), split("\"a,b\",c"));
  }

  @Test void embeddedDelimiterInsideQuotes_withCommaSpace() {
    assertEquals(java.util.Arrays.asList("a, b", "c"), split("\"a, b\", \"c\""));
  }

  @Test void escapedQuotes() {
    assertEquals(java.util.Arrays.asList("a\"b", "c"), split("\"a\"\"b\",c"));
  }

  @Test void unquotedLeadingSpacePreserved() {
    // No quote follows the leading space -> field is left intact (no regression).
    assertEquals(java.util.Arrays.asList("a", " b"), split("a, b"));
  }

  @Test void emptyQuotedField() {
    assertEquals(java.util.Arrays.asList("a", "", "c"), split("\"a\", \"\", \"c\""));
  }

  private static BufferedReader reader(String text) {
    return new BufferedReader(new StringReader(text));
  }

  @Test void quotedDialect_joinsGenuineMultiLineQuotedField() throws IOException {
    // RFC4180 dialect (default/true): an odd double-quote count means the record
    // continues on the next physical line, joined with '\n'.
    BufferedReader r = reader("a,\"b\nstill b\",c\nnext,row,here\n");
    assertEquals("a,\"b\nstill b\",c", CsvRecordReader.readRecord(r, true));
    assertEquals("next,row,here", CsvRecordReader.readRecord(r, true));
    assertNull(CsvRecordReader.readRecord(r, true));
  }

  @Test void singleQuoteDialect_strayDoubleQuoteDoesNotMergeRecords() throws IOException {
    // Non-RFC4180 dialect (quoted=false), e.g. NBI bridges: '...' is the real text
    // qualifier and a literal " (mistyped apostrophe, inch mark) has no quoting
    // meaning. Each physical line must stay its own record even with an odd count.
    BufferedReader r = reader("01,'B\"HAM SOUTHERN RAILROAD',x\n02,'NEXT BRIDGE',y\n");
    assertEquals("01,'B\"HAM SOUTHERN RAILROAD',x", CsvRecordReader.readRecord(r, false));
    assertEquals("02,'NEXT BRIDGE',y", CsvRecordReader.readRecord(r, false));
    assertNull(CsvRecordReader.readRecord(r, false));
  }

  @Test void noArgOverload_defaultsToQuotedTrue() throws IOException {
    BufferedReader r = reader("a,\"b\nstill b\",c\n");
    assertEquals("a,\"b\nstill b\",c", CsvRecordReader.readRecord(r));
  }
}
