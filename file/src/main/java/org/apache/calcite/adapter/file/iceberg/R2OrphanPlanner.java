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
// storage-provider-guard:ignore-file - audited: all filesystem operations here read and write genuinely-local CLI work files (keep list, R2 listing, candidate list), not object-store URIs.

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Decides which objects of one table on R2 are orphans, for {@code r2-closure-cleanup.sh}.
 *
 * <p>The keep set is the closure of the version named by R2's own {@code version-hint.text}
 * (resolved by {@link ClosureResolver} against R2). An R2 object is a candidate only when all of
 * these hold:
 * <ul>
 *   <li>it lies under {@code metadata/} or {@code data/} (anything else is never touched);
 *   <li>it is not in the keep set and is not {@code metadata/version-hint.text};
 *   <li>its modification time is strictly older than the safety cutoff, so files a sync is still
 *       copying ahead of a pointer move are never candidates.
 * </ul>
 *
 * <p>The plan is unsafe, and nothing may be deleted, when any keep-set file is absent from the R2
 * listing: R2's pointer then names files R2 does not hold, and the table needs a catch-up sync
 * rather than a cleanup.
 *
 * <p>The listing format is one object per line: {@code path<TAB>size<TAB>modtime}, where modtime
 * is {@code yyyy-MM-dd HH:mm:ss} in UTC (rclone {@code lsf --format pst} with the UTC time
 * option). This class only reads files and writes the candidate list; it never touches R2.
 */
public final class R2OrphanPlanner {

  static final String VERSION_HINT = "metadata/version-hint.text";

  /** One R2 object. */
  public static final class Entry {
    final String path;
    final long size;
    final Instant modified;

    public Entry(String path, long size, Instant modified) {
      this.path = path;
      this.size = size;
      this.modified = modified;
    }
  }

  /** Result of planning one table. */
  public static final class Plan {
    final List<Entry> candidates = new ArrayList<>();
    long candidateBytes;
    int insideSafetyWindow;
    int outOfScope;
    int listed;
    long listedBytes;
    final List<String> keepMissingOnR2 = new ArrayList<>();

    /** True when nothing referenced by R2's pointer is missing from R2. */
    public boolean isSafeToDelete() {
      return keepMissingOnR2.isEmpty();
    }
  }

  private R2OrphanPlanner() {}

  /** Computes the plan; pure, no I/O. */
  public static Plan plan(Set<String> keep, Collection<Entry> r2Objects, Instant cutoff) {
    Plan plan = new Plan();
    Set<String> present = new HashSet<>();
    for (Entry e : r2Objects) {
      plan.listed++;
      plan.listedBytes += e.size;
      present.add(e.path);
      if (!inScope(e.path)) {
        plan.outOfScope++;
        continue;
      }
      if (keep.contains(e.path) || VERSION_HINT.equals(e.path)) {
        continue;
      }
      if (!e.modified.isBefore(cutoff)) {
        plan.insideSafetyWindow++;
        continue;
      }
      plan.candidates.add(e);
      plan.candidateBytes += e.size;
    }
    for (String k : keep) {
      if (!present.contains(k)) {
        plan.keepMissingOnR2.add(k);
      }
    }
    return plan;
  }

  static boolean inScope(String path) {
    return path.startsWith("metadata/") || path.startsWith("data/");
  }

  static List<Entry> readListing(Path file) throws IOException {
    List<Entry> out = new ArrayList<>();
    try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      String line;
      while ((line = r.readLine()) != null) {
        if (line.isEmpty()) {
          continue;
        }
        String[] f = line.split("\t");
        if (f.length != 3) {
          throw new IllegalArgumentException("Malformed listing line: " + line);
        }
        out.add(new Entry(f[0], Long.parseLong(f[1]), parseUtc(f[2])));
      }
    }
    return out;
  }

  static Instant parseUtc(String s) {
    return Instant.parse(s.trim().replace(' ', 'T') + "Z");
  }

  static Set<String> readKeep(Path file) throws IOException {
    Set<String> keep = new HashSet<>();
    for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
      if (!line.isEmpty() && !line.startsWith("VERSION ")) {
        keep.add(line);
      }
    }
    return keep;
  }

  /**
   * Usage: {@code R2OrphanPlanner --keep F --r2list F --cutoff 'yyyy-MM-dd HH:mm:ss' --out F
   * [--scope metadata|data]}. Writes candidate paths (one per line) to {@code --out} and prints one
   * tab-separated summary line: listed, listedBytes, candidates, candidateBytes, insideWindow,
   * outOfScope, keepMissingOnR2. Exits 3 when the plan is unsafe.
   */
  public static void main(String[] args) throws Exception {
    Path keepFile = null;
    Path listFile = null;
    Path outFile = null;
    String cutoff = null;
    String scope = null;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
      case "--keep":
        keepFile = Paths.get(args[++i]);
        break;
      case "--r2list":
        listFile = Paths.get(args[++i]);
        break;
      case "--out":
        outFile = Paths.get(args[++i]);
        break;
      case "--cutoff":
        cutoff = args[++i];
        break;
      case "--scope":
        scope = args[++i];
        break;
      default:
        System.err.println("Unknown argument: " + args[i]);
        System.exit(1);
      }
    }
    if (keepFile == null || listFile == null || outFile == null || cutoff == null) {
      System.err.println("Usage: R2OrphanPlanner --keep F --r2list F --cutoff TS --out F"
          + " [--scope metadata|data]");
      System.exit(1);
    }
    Set<String> keep = readKeep(keepFile);
    Plan plan = plan(keep, readListing(listFile), parseUtc(cutoff));
    String prefix = scope == null ? null : scope + "/";
    try (BufferedWriter w = Files.newBufferedWriter(outFile, StandardCharsets.UTF_8)) {
      for (Entry e : plan.candidates) {
        if (prefix == null || e.path.startsWith(prefix)) {
          w.write(e.path);
          w.newLine();
        }
      }
    }
    System.out.println(plan.listed + "\t" + plan.listedBytes + "\t" + plan.candidates.size()
        + "\t" + plan.candidateBytes + "\t" + plan.insideSafetyWindow + "\t" + plan.outOfScope
        + "\t" + plan.keepMissingOnR2.size());
    for (String m : plan.keepMissingOnR2) {
      System.err.println("KEEP-MISSING-ON-R2 " + m);
    }
    if (!plan.isSafeToDelete()) {
      System.exit(3);
    }
  }
}
