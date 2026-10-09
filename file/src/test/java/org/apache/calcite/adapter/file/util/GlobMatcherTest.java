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
package org.apache.calcite.adapter.file.util;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.regex.PatternSyntaxException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link GlobMatcher}.
 *
 * <p>The Windows rules are exercised through the overload that takes the platform as an
 * argument, so they are checked wherever the tests run.
 */
@Tag("unit")
class GlobMatcherTest {
  private static final boolean WINDOWS = true;
  private static final boolean UNIX = false;

  @Test void aStarStaysWithinOneName() {
    GlobMatcher m = GlobMatcher.forKeys("*.parquet");
    assertTrue(m.matches("data.parquet"));
    assertTrue(m.matches(".parquet"));
    assertFalse(m.matches("year=2024/data.parquet"));
    assertFalse(m.matches("data.parquet.crc"));
  }

  @Test void twoStarsCrossDirectories() {
    GlobMatcher m = GlobMatcher.forKeys("**/*.parquet");
    assertTrue(m.matches("year=2024/data.parquet"));
    assertTrue(m.matches("year=2024/month=01/data.parquet"));
    // as with the file system's matcher, "**/" still asks for one separator
    assertFalse(m.matches("data.parquet"));
    assertTrue(GlobMatcher.forKeys("**.parquet").matches("a/b/c.parquet"));
  }

  @Test void aFixedDepthPatternMatchesOnlyThatDepth() {
    GlobMatcher m = GlobMatcher.forKeys("year=*/month=*/*.parquet");
    assertTrue(m.matches("year=2024/month=01/part-0.parquet"));
    assertFalse(m.matches("year=2024/part-0.parquet"));
    assertFalse(m.matches("year=2024/month=01/day=05/part-0.parquet"));
  }

  @Test void aQuestionMarkIsOneCharacterOfAName() {
    GlobMatcher m = GlobMatcher.forKeys("file?.csv");
    assertTrue(m.matches("file1.csv"));
    assertFalse(m.matches("file.csv"));
    assertFalse(m.matches("file12.csv"));
    assertFalse(GlobMatcher.forKeys("a?b").matches("a/b"));
  }

  @Test void aClassIsOneCharacterOfAName() {
    assertTrue(GlobMatcher.forKeys("part-[0-9].csv").matches("part-7.csv"));
    assertFalse(GlobMatcher.forKeys("part-[0-9].csv").matches("part-x.csv"));
    assertTrue(GlobMatcher.forKeys("[abc].csv").matches("b.csv"));
    assertTrue(GlobMatcher.forKeys("[!abc].csv").matches("d.csv"));
    assertFalse(GlobMatcher.forKeys("[!abc].csv").matches("a.csv"));
    assertFalse(GlobMatcher.forKeys("a[!x]b").matches("a/b"));
    // regular-expression syntax inside a class is taken literally
    assertTrue(GlobMatcher.forKeys("[a^].csv").matches("^.csv"));
    assertTrue(GlobMatcher.forKeys("[a&&b].csv").matches("&.csv"));
    assertTrue(GlobMatcher.forKeys("[a\\].csv").matches("\\.csv"));
  }

  @Test void aGroupMatchesAnyOfItsAlternatives() {
    GlobMatcher m = GlobMatcher.forKeys("*.{csv,tsv,json}");
    assertTrue(m.matches("a.csv"));
    assertTrue(m.matches("a.tsv"));
    assertTrue(m.matches("a.json"));
    assertFalse(m.matches("a.parquet"));
    assertTrue(GlobMatcher.forKeys("{2023,2024}/**").matches("2024/q1/a.csv"));
    // outside a group a comma and a closing brace are themselves
    assertTrue(GlobMatcher.forKeys("a,b}.csv").matches("a,b}.csv"));
  }

  @Test void regularExpressionSyntaxInAGlobIsLiteral() {
    assertTrue(GlobMatcher.forKeys("a.b+c(1)$|^.csv").matches("a.b+c(1)$|^.csv"));
    assertFalse(GlobMatcher.forKeys("a.csv").matches("aXcsv"));
    assertTrue(GlobMatcher.forKeys("a\\*b").matches("a*b"));
    assertFalse(GlobMatcher.forKeys("a\\*b").matches("aXb"));
  }

  @Test void aKeyNeedNotBeAFileName() {
    // Neither of these is a path on Windows
    assertTrue(GlobMatcher.forKeys("**/*.json").matches("ts=2024-01-01T00:00:00Z/a.json"));
    assertTrue(GlobMatcher.forKeys("*.json").matches("what?<now>|\"a\".json"));
    assertTrue(GlobMatcher.forKeys("a*").matches("a\nb"));
  }

  @Test void keysRespectCaseAndKeepTheirBackslashes() {
    assertFalse(GlobMatcher.forKeys("*.CSV").matches("a.csv"));
    assertTrue(GlobMatcher.forKeys("*.csv").matches("dir\\a.csv"));
    assertFalse(GlobMatcher.forKeys("dir/*.csv").matches("dir\\a.csv"));
  }

  @Test void aLocalPathOnWindowsIgnoresCaseAndTakesEitherSeparator() {
    GlobMatcher m = GlobMatcher.forLocalPaths("Sales/**/*.CSV", WINDOWS);
    assertTrue(m.matches("sales\\2024\\q1.csv"));
    assertTrue(m.matches("sales/2024\\q1.csv"));
    assertTrue(m.matches("SALES/2024/Q1.CSV"));
    assertFalse(GlobMatcher.forLocalPaths("*.csv", WINDOWS).matches("sales\\q1.csv"));
  }

  @Test void aLocalPathOffWindowsIsMatchedAsAKeyIs() {
    assertFalse(GlobMatcher.forLocalPaths("*.CSV", UNIX).matches("a.csv"));
    assertTrue(GlobMatcher.forLocalPaths("*.csv", UNIX).matches("dir\\a.csv"));
  }

  @Test void aMalformedGlobIsRefused() {
    for (String glob : new String[] {"[abc", "{a,b", "{a,{b,c}}", "[a/b]", "abc\\"}) {
      assertThrows(PatternSyntaxException.class, () -> GlobMatcher.forKeys(glob), glob);
    }
  }

  /** The matcher this one replaces is the reference wherever both can be asked. */
  @Test void agreesWithTheFileSystemsMatcher() {
    String[] globs = {
        "*.parquet", "**/*.parquet", "**", "*", "year=*/*.parquet", "year=*/month=*/*.parquet",
        "*.{csv,tsv}", "data/**/part-[0-9]*.parquet", "[!.]*", "**/[a-c]?.json", "a\\*b",
        "{2023,2024}/**", "**.csv", "dir/**", "a.b+c", "x,y", "*/", "**/"
    };
    String[] paths = {
        "a.parquet", "year=2024/a.parquet", "year=2024/month=01/a.parquet", "a.csv", "a.tsv",
        "data/x/y/part-0-000.parquet", "data/part-x.parquet", ".hidden", "visible",
        "d/ab.json", "d/e/c1.json", "a*b", "aXb", "2024/q/a.csv", "2025/q/a.csv",
        "dir/a", "dir/a/b", "a.b+c", "aXb+c", "x,y", "dir", "A.PARQUET"
    };
    PathMatcher caseProbe = FileSystems.getDefault().getPathMatcher("glob:a");
    boolean caseSensitive = !caseProbe.matches(Paths.get("A"));
    for (String glob : globs) {
      PathMatcher reference = FileSystems.getDefault().getPathMatcher("glob:" + glob);
      GlobMatcher matcher = caseSensitive
          ? GlobMatcher.forKeys(glob) : GlobMatcher.forLocalPaths(glob);
      for (String path : paths) {
        if (path.indexOf('*') >= 0 && !caseSensitive) {
          continue;   // not a file name there
        }
        assertEquals(reference.matches(Paths.get(path)), matcher.matches(path),
            "glob '" + glob + "' against '" + path + "'");
      }
    }
  }
}
