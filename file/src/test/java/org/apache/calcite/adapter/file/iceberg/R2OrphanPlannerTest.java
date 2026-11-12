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
package org.apache.calcite.adapter.file.iceberg;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Keep-set logic of {@link R2OrphanPlanner}; reads only temp files, never touches R2. */
@Tag("unit")
public class R2OrphanPlannerTest {

  private static final Instant CUTOFF = Instant.parse("2026-11-05T00:00:00Z");
  private static final Instant OLD = Instant.parse("2026-10-01T00:00:00Z");
  private static final Instant NEW = Instant.parse("2026-11-10T00:00:00Z");

  private static R2OrphanPlanner.Entry e(String path, long size, Instant t) {
    return new R2OrphanPlanner.Entry(path, size, t);
  }

  private static Set<String> keep(String... paths) {
    return new HashSet<>(Arrays.asList(paths));
  }

  @Test void keepSetFilesAreNeverCandidates() {
    List<R2OrphanPlanner.Entry> r2 = new ArrayList<>();
    r2.add(e("metadata/v5.metadata.json", 10, OLD));
    r2.add(e("data/a.parquet", 100, OLD));
    r2.add(e("data/b.parquet", 200, OLD));
    R2OrphanPlanner.Plan plan =
        R2OrphanPlanner.plan(keep("metadata/v5.metadata.json", "data/a.parquet"), r2, CUTOFF);
    assertEquals(1, plan.candidates.size());
    assertEquals("data/b.parquet", plan.candidates.get(0).path);
    assertEquals(200, plan.candidateBytes);
    assertTrue(plan.isSafeToDelete());
  }

  @Test void versionHintIsNeverACandidate() {
    List<R2OrphanPlanner.Entry> r2 = new ArrayList<>();
    r2.add(e("metadata/version-hint.text", 2, OLD));
    R2OrphanPlanner.Plan plan = R2OrphanPlanner.plan(keep(), r2, CUTOFF);
    assertTrue(plan.candidates.isEmpty());
  }

  @Test void objectsInsideSafetyWindowAreNotCandidates() {
    List<R2OrphanPlanner.Entry> r2 = new ArrayList<>();
    r2.add(e("data/new.parquet", 5, NEW));
    r2.add(e("data/at-cutoff.parquet", 5, CUTOFF));
    r2.add(e("data/old.parquet", 5, OLD));
    R2OrphanPlanner.Plan plan = R2OrphanPlanner.plan(keep(), r2, CUTOFF);
    assertEquals(1, plan.candidates.size());
    assertEquals("data/old.parquet", plan.candidates.get(0).path);
    assertEquals(2, plan.insideSafetyWindow);
  }

  @Test void objectsOutsideMetadataAndDataAreNeverTouched() {
    List<R2OrphanPlanner.Entry> r2 = new ArrayList<>();
    r2.add(e("notes.txt", 1, OLD));
    r2.add(e("other/x.parquet", 1, OLD));
    r2.add(e("metadatax/y", 1, OLD));
    R2OrphanPlanner.Plan plan = R2OrphanPlanner.plan(keep(), r2, CUTOFF);
    assertTrue(plan.candidates.isEmpty());
    assertEquals(3, plan.outOfScope);
  }

  @Test void keepFileMissingOnR2MakesPlanUnsafe() {
    List<R2OrphanPlanner.Entry> r2 = new ArrayList<>();
    r2.add(e("data/orphan.parquet", 7, OLD));
    R2OrphanPlanner.Plan plan = R2OrphanPlanner.plan(keep("data/needed.parquet"), r2, CUTOFF);
    assertFalse(plan.isSafeToDelete());
    assertEquals(Arrays.asList("data/needed.parquet"), plan.keepMissingOnR2);
  }

  @Test void listingAndKeepFilesParse(@TempDir Path dir) throws Exception {
    Path list = dir.resolve("r2.list");
    Files.write(list, Arrays.asList(
        "data/a.parquet\t12\t2026-10-01 00:00:00",
        "data/b.parquet\t34\t2026-11-10 00:00:00"), StandardCharsets.UTF_8);
    Path keepFile = dir.resolve("keep");
    Files.write(keepFile, Arrays.asList("VERSION 3", "data/a.parquet"), StandardCharsets.UTF_8);

    R2OrphanPlanner.Plan plan = R2OrphanPlanner.plan(R2OrphanPlanner.readKeep(keepFile),
        R2OrphanPlanner.readListing(list), CUTOFF);
    assertEquals(2, plan.listed);
    assertEquals(46, plan.listedBytes);
    assertTrue(plan.candidates.isEmpty());
    assertEquals(1, plan.insideSafetyWindow);
    assertTrue(plan.isSafeToDelete());
  }
}
