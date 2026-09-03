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
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers how the entries matched inside one archive are merged into the single file the raw cache
 * holds — specifically that rows are aligned by column NAME, so members covering different ranges
 * keep their values on the columns they were published under.
 */
@Tag("unit")
public class HttpSourceZipMergeTest {

  private static File csv(String content) throws IOException {
    File f = File.createTempFile("zip-merge-part-", ".csv");
    f.deleteOnExit();
    Files.write(f.toPath(), content.getBytes(StandardCharsets.UTF_8));
    return f;
  }

  private static List<String> merge(List<File> parts, boolean csvFormat) throws Exception {
    File out = File.createTempFile("zip-merge-out-", ".csv");
    out.deleteOnExit();
    List<String> names = new ArrayList<String>();
    for (int i = 0; i < parts.size(); i++) {
      names.add("part" + i + ".csv");
    }
    Method m = HttpSource.class.getDeclaredMethod("mergeZipParts", List.class, List.class,
        File.class, boolean.class);
    m.setAccessible(true);
    m.invoke(null, parts, names, out, csvFormat);
    return Files.readAllLines(out.toPath(), StandardCharsets.UTF_8);
  }

  /**
   * The shape of BEA's SAINC.zip: one member covering 1929-1930, another covering 1990-1991.
   * Concatenating them under the first member's header alone puts the 1990 value in the 1929
   * column — the failure this alignment exists to prevent.
   */
  @Test void alignsMembersWithDifferentYearColumnsByName() throws Exception {
    File early = csv("GeoFIPS,GeoName,1929,1930\n"
        + "\"01000\",Alabama,11,12\n");
    File late = csv("GeoFIPS,GeoName,1990,1991\n"
        + "\"01000\",Alabama,9901,9902\n");

    List<String> lines = merge(Arrays.asList(early, late), true);

    assertEquals("GeoFIPS,GeoName,1929,1930,1990,1991", lines.get(0));
    // Early member: values stay under 1929/1930, later columns empty.
    assertEquals("01000,Alabama,11,12,,", lines.get(1));
    // Later member: 9901 belongs to 1990, NOT 1929.
    assertEquals("01000,Alabama,,,9901,9902", lines.get(2));
    assertEquals(3, lines.size());
  }

  /** Identical headers stay a plain header-strip concatenation — one header, all rows. */
  @Test void concatenatesMembersWithIdenticalHeadersUnderOneHeader() throws Exception {
    File a = csv("GeoFIPS,GeoName,1929\n\"01000\",Alabama,11\n");
    File b = csv("GeoFIPS,GeoName,1929\n\"02000\",Alaska,22\n");

    List<String> lines = merge(Arrays.asList(a, b), true);

    assertEquals(3, lines.size());
    assertEquals("GeoFIPS,GeoName,1929", lines.get(0));
    assertTrue(lines.get(1).contains("Alabama"));
    assertTrue(lines.get(2).contains("Alaska"));
  }

  /** A value carrying a comma survives the rewrite as a properly quoted field. */
  @Test void requotesFieldsContainingDelimiters() throws Exception {
    File early = csv("GeoFIPS,Description,1929\n"
        + "\"01000\",\"Per capita income, dollars\",11\n");
    File late = csv("GeoFIPS,Description,1990\n"
        + "\"01000\",\"Per capita income, dollars\",9901\n");

    List<String> lines = merge(Arrays.asList(early, late), true);

    assertEquals("GeoFIPS,Description,1929,1990", lines.get(0));
    assertEquals("01000,\"Per capita income, dollars\",11,", lines.get(1));
    assertEquals("01000,\"Per capita income, dollars\",,9901", lines.get(2));
  }

  /** Non-CSV members concatenate byte-for-byte — no header handling at all. */
  @Test void concatenatesNonCsvMembersVerbatim() throws Exception {
    File a = csv("line-a-1\nline-a-2\n");
    File b = csv("line-b-1\n");

    List<String> lines = merge(Arrays.asList(a, b), false);

    assertEquals(Arrays.asList("line-a-1", "line-a-2", "line-b-1"), lines);
  }

  /** A single CSV member is passed through untouched, header included. */
  @Test void passesSingleMemberThroughUnchanged() throws Exception {
    File only = csv("GeoFIPS,GeoName,1929\n\"01000\",Alabama,11\n");

    List<String> lines = merge(Arrays.asList(only), true);

    assertEquals(Arrays.asList("GeoFIPS,GeoName,1929", "\"01000\",Alabama,11"), lines);
  }
}
