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
// storage-provider-guard:ignore-file - audited: the plan is a host-local hint file under java.io.tmpdir
// beside the DataWeb API lock; losing it only costs one cold run, so it is deliberately not durable storage
package org.apache.calcite.adapter.govdata.econ;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Where past runs had to split an HTS chapter, so the next run starts from slices DataWeb is known
 * to answer instead of paying a full query deadline to rediscover each split.
 *
 * <p>Per chapter this is a set of <b>cut points</b>: an offset {@code c} in {@code 1..99} means "a
 * slice starts at HTS-4 heading {@code c}", so a cut between two headings was a split point of some
 * earlier bisection. A chapter's slices are the intervals between consecutive cuts, which always
 * partition headings {@code 0..99} exactly once with no gaps or overlaps — whatever the set holds —
 * so a plan can change how a year is fetched but never which rows it contains. A chapter with no
 * cuts is one slice, the bare chapter query.
 *
 * <p>Cuts only ever accumulate (a union), which makes merging order-independent and safe between
 * worker JVMs sharing one file. A cut that turns out to be unnecessary costs one extra fast
 * request; a missing one costs a whole query deadline of dead waiting, so over-splitting is the
 * cheap side. Cuts are facts about slices that timed out, valid whether or not the run that found
 * them went on to succeed.
 *
 * <p>A missing file is a cold start. An unreadable or invalid one throws, naming the file: a plan
 * that cannot be trusted must be deleted on purpose rather than ignored.
 */
final class UsitcSlicePlan {

  /** HTS-4 headings under one chapter: offsets {@code 0..99}. */
  static final int HEADINGS_PER_CHAPTER = 100;
  private static final int VERSION = 1;
  private static final Pattern CHAPTER = Pattern.compile("\\d\\d");
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final SortedMap<String, SortedSet<Integer>> cuts =
      new TreeMap<String, SortedSet<Integer>>();

  static UsitcSlicePlan empty() {
    return new UsitcSlicePlan();
  }

  /** Reads {@code file}; an absent file is an empty plan, an invalid one throws. */
  static UsitcSlicePlan load(File file) throws IOException {
    UsitcSlicePlan plan = new UsitcSlicePlan();
    if (!file.exists()) {
      return plan;
    }
    JsonNode root;
    try {
      root = MAPPER.readTree(file);
    } catch (IOException e) {
      throw new IOException("usitc slice plan " + file + " is unreadable (" + e.getMessage()
          + ") — delete it to start from a cold plan", e);
    }
    if (root == null || !root.isObject() || root.path("version").asInt(-1) != VERSION
        || !root.path("cuts").isObject()) {
      throw new IOException("usitc slice plan " + file + " is not a version-" + VERSION
          + " plan — delete it to start from a cold plan");
    }
    for (Map.Entry<String, JsonNode> entry : root.get("cuts").properties()) {
      if (!CHAPTER.matcher(entry.getKey()).matches() || !entry.getValue().isArray()) {
        throw new IOException("usitc slice plan " + file + " has an invalid chapter entry '"
            + entry.getKey() + "' — delete it to start from a cold plan");
      }
      for (JsonNode cut : entry.getValue()) {
        if (!cut.isInt() || cut.asInt() < 1 || cut.asInt() >= HEADINGS_PER_CHAPTER) {
          throw new IOException("usitc slice plan " + file + " has an invalid cut " + cut
              + " for chapter " + entry.getKey() + " — delete it to start from a cold plan");
        }
        plan.addCut(entry.getKey(), cut.asInt());
      }
    }
    return plan;
  }

  /**
   * The slices of {@code chapter} as inclusive {@code [lo, hi]} heading-offset pairs, in order,
   * partitioning {@code 0..99}.
   */
  List<int[]> slices(String chapter) {
    List<int[]> out = new ArrayList<int[]>();
    int lo = 0;
    SortedSet<Integer> set = cuts.get(chapter);
    if (set != null) {
      for (int cut : set) {
        out.add(new int[] {lo, cut - 1});
        lo = cut;
      }
    }
    out.add(new int[] {lo, HEADINGS_PER_CHAPTER - 1});
    return out;
  }

  /** Records that a slice of {@code chapter} was split so a new slice starts at {@code cut}. */
  void addCut(String chapter, int cut) {
    if (cut < 1 || cut >= HEADINGS_PER_CHAPTER) {
      throw new IllegalArgumentException("cut " + cut + " outside 1.." + (HEADINGS_PER_CHAPTER - 1));
    }
    SortedSet<Integer> set = cuts.get(chapter);
    if (set == null) {
      set = new TreeSet<Integer>();
      cuts.put(chapter, set);
    }
    set.add(cut);
  }

  boolean isEmpty() {
    return cuts.isEmpty();
  }

  /** Number of chapters that start from more than one slice. */
  int splitChapters() {
    return cuts.size();
  }

  /** True if this plan holds a cut that {@code other} does not. */
  boolean hasCutsNotIn(UsitcSlicePlan other) {
    for (Map.Entry<String, SortedSet<Integer>> e : cuts.entrySet()) {
      SortedSet<Integer> theirs = other.cuts.get(e.getKey());
      if (theirs == null || !theirs.containsAll(e.getValue())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Unions {@code learned} into the plan stored in {@code file} and rewrites it atomically, under
   * an OS lock so concurrent worker JVMs do not drop each other's cuts. Writes nothing when
   * {@code learned} adds no cut the file already has.
   *
   * @return the plan now on disk
   */
  static UsitcSlicePlan saveMerged(File file, UsitcSlicePlan learned) throws IOException {
    File lockFile = new File(file.getPath() + ".lock");
    try (RandomAccessFile raf = new RandomAccessFile(lockFile, "rw");
         FileChannel channel = raf.getChannel();
         FileLock lock = channel.lock()) {
      UsitcSlicePlan onDisk = load(file);
      if (!learned.hasCutsNotIn(onDisk)) {
        return onDisk;
      }
      for (Map.Entry<String, SortedSet<Integer>> e : learned.cuts.entrySet()) {
        for (int cut : e.getValue()) {
          onDisk.addCut(e.getKey(), cut);
        }
      }
      File tmp = new File(file.getPath() + ".tmp");
      Files.write(tmp.toPath(), onDisk.toJson().getBytes(StandardCharsets.UTF_8));
      Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE);
      return onDisk;
    }
  }

  private String toJson() throws IOException {
    ObjectNode root = MAPPER.createObjectNode();
    root.put("version", VERSION);
    ObjectNode chapters = root.putObject("cuts");
    for (Map.Entry<String, SortedSet<Integer>> e : cuts.entrySet()) {
      ArrayNode arr = chapters.putArray(e.getKey());
      for (int cut : e.getValue()) {
        arr.add(cut);
      }
    }
    return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root);
  }
}
