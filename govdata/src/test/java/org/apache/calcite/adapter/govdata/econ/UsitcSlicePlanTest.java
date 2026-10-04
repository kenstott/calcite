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
package org.apache.calcite.adapter.govdata.econ;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link UsitcSlicePlan}: cut points, the slices they imply, and the file they are kept in. */
@Tag("unit")
class UsitcSlicePlanTest {

  @TempDir File dir;

  private File planFile() {
    return new File(dir, "usitc-slice-plan.json");
  }

  private static void assertPartitionsAllHeadings(List<int[]> slices) {
    int next = 0;
    for (int[] s : slices) {
      assertEquals(next, s[0], "slices must be contiguous with no gap or overlap");
      assertTrue(s[1] >= s[0], "slice must hold at least one heading");
      next = s[1] + 1;
    }
    assertEquals(UsitcSlicePlan.HEADINGS_PER_CHAPTER, next, "slices must end at heading 99");
  }

  @Test void aChapterWithNoCutsIsOneSliceOfEveryHeading() {
    List<int[]> slices = UsitcSlicePlan.empty().slices("84");
    assertEquals(1, slices.size());
    assertEquals(0, slices.get(0)[0]);
    assertEquals(99, slices.get(0)[1]);
  }

  @Test void cutsSplitAChapterIntoContiguousSlices() {
    UsitcSlicePlan plan = UsitcSlicePlan.empty();
    plan.addCut("84", 50);
    plan.addCut("84", 13);
    plan.addCut("84", 25);
    List<int[]> slices = plan.slices("84");
    assertEquals(4, slices.size());
    assertPartitionsAllHeadings(slices);
    assertEquals(12, slices.get(0)[1]);
    assertEquals(13, slices.get(1)[0]);
    assertEquals(1, UsitcSlicePlan.empty().slices("84").size(), "other plans are unaffected");
    assertEquals(1, plan.slices("85").size(), "cuts are per chapter");
  }

  @Test void everyPossibleCutSetStillPartitionsTheChapter() {
    UsitcSlicePlan plan = UsitcSlicePlan.empty();
    for (int cut = 1; cut < 100; cut++) {
      plan.addCut("29", cut);
      assertPartitionsAllHeadings(plan.slices("29"));
    }
    assertEquals(100, plan.slices("29").size());
  }

  @Test void cutsOutsideTheChapterAreRejected() {
    UsitcSlicePlan plan = UsitcSlicePlan.empty();
    assertThrows(IllegalArgumentException.class, () -> plan.addCut("84", 0));
    assertThrows(IllegalArgumentException.class, () -> plan.addCut("84", 100));
  }

  @Test void aMissingFileIsAColdStart() throws Exception {
    UsitcSlicePlan plan = UsitcSlicePlan.load(planFile());
    assertTrue(plan.isEmpty());
    assertFalse(planFile().exists(), "loading must not create the file");
  }

  @Test void savedCutsAreReadBack() throws Exception {
    UsitcSlicePlan learned = UsitcSlicePlan.empty();
    learned.addCut("84", 25);
    learned.addCut("84", 50);
    learned.addCut("90", 13);
    UsitcSlicePlan.saveMerged(planFile(), learned);

    UsitcSlicePlan loaded = UsitcSlicePlan.load(planFile());
    assertEquals(3, loaded.slices("84").size());
    assertEquals(2, loaded.slices("90").size());
    assertEquals(2, loaded.splitChapters());
    assertFalse(loaded.hasCutsNotIn(learned) || learned.hasCutsNotIn(loaded));
  }

  @Test void savingMergesWithWhatAnotherRunAlreadyWrote() throws Exception {
    UsitcSlicePlan first = UsitcSlicePlan.empty();
    first.addCut("84", 25);
    UsitcSlicePlan.saveMerged(planFile(), first);
    UsitcSlicePlan second = UsitcSlicePlan.empty();
    second.addCut("84", 75);
    second.addCut("85", 50);
    UsitcSlicePlan merged = UsitcSlicePlan.saveMerged(planFile(), second);

    assertEquals(3, merged.slices("84").size(), "both runs' cuts must survive");
    UsitcSlicePlan onDisk = UsitcSlicePlan.load(planFile());
    assertEquals(3, onDisk.slices("84").size());
    assertEquals(2, onDisk.slices("85").size());
  }

  @Test void savingNothingNewDoesNotRewriteTheFile() throws Exception {
    UsitcSlicePlan learned = UsitcSlicePlan.empty();
    learned.addCut("84", 25);
    UsitcSlicePlan.saveMerged(planFile(), learned);
    assertTrue(planFile().setLastModified(1_000_000_000L));

    UsitcSlicePlan.saveMerged(planFile(), learned);
    UsitcSlicePlan.saveMerged(planFile(), UsitcSlicePlan.empty());
    assertEquals(1_000_000_000L, planFile().lastModified(), "no new cut must mean no write");
  }

  @Test void anUnparseableFileFailsLoudAndNamesTheFile() throws Exception {
    Files.write(planFile().toPath(), "{not json".getBytes(StandardCharsets.UTF_8));
    IOException e = assertThrows(IOException.class, () -> UsitcSlicePlan.load(planFile()));
    assertTrue(e.getMessage().contains(planFile().getName()), e.getMessage());
  }

  @Test void aWrongVersionFailsLoud() throws Exception {
    Files.write(planFile().toPath(), "{\"version\":2,\"cuts\":{}}".getBytes(StandardCharsets.UTF_8));
    assertThrows(IOException.class, () -> UsitcSlicePlan.load(planFile()));
  }

  @Test void anInvalidCutOrChapterFailsLoud() throws Exception {
    Files.write(planFile().toPath(),
        "{\"version\":1,\"cuts\":{\"84\":[0]}}".getBytes(StandardCharsets.UTF_8));
    assertThrows(IOException.class, () -> UsitcSlicePlan.load(planFile()));
    Files.write(planFile().toPath(),
        "{\"version\":1,\"cuts\":{\"8x\":[5]}}".getBytes(StandardCharsets.UTF_8));
    assertThrows(IOException.class, () -> UsitcSlicePlan.load(planFile()));
  }

  @Test void aCorruptFileIsNotOverwrittenBySaving() throws Exception {
    Files.write(planFile().toPath(), "garbage".getBytes(StandardCharsets.UTF_8));
    UsitcSlicePlan learned = UsitcSlicePlan.empty();
    learned.addCut("84", 25);
    assertThrows(IOException.class, () -> UsitcSlicePlan.saveMerged(planFile(), learned));
    assertEquals("garbage",
        new String(Files.readAllBytes(planFile().toPath()), StandardCharsets.UTF_8));
  }
}
