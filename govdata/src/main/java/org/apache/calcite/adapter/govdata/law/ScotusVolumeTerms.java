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
package org.apache.calcite.adapter.govdata.law;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Maps a Court term to the United States Reports volumes that hold its decisions.
 *
 * <p>GovInfo gives each volume a {@code courtTerm}. For most volumes it is one year
 * ({@code 2015}); for the earliest reporters it is a span ({@code 1781-1793}). A term is
 * identified here by the first year of that value, so a volume belongs to the term its
 * {@code courtTerm} starts with, and the later years of a span have no volumes of their own.
 * Several volumes can share a term (571 and 572 are both 2013).
 */
final class ScotusVolumeTerms {

  private static final Pattern LEADING_YEAR = Pattern.compile("^\\s*(\\d{4})(?:\\D.*)?$");

  private final TreeMap<Integer, List<Integer>> volumesByTerm;

  /**
   * @param courtTermByVolume GovInfo's {@code courtTerm} string keyed by volume number
   * @throws IllegalArgumentException when a volume's {@code courtTerm} does not start with a year
   */
  ScotusVolumeTerms(Map<Integer, String> courtTermByVolume) {
    TreeMap<Integer, List<Integer>> byTerm = new TreeMap<Integer, List<Integer>>();
    for (Map.Entry<Integer, String> e : courtTermByVolume.entrySet()) {
      int term = termOf(e.getKey(), e.getValue());
      List<Integer> volumes = byTerm.get(term);
      if (volumes == null) {
        volumes = new ArrayList<Integer>();
        byTerm.put(term, volumes);
      }
      volumes.add(e.getKey());
    }
    for (List<Integer> volumes : byTerm.values()) {
      Collections.sort(volumes);
    }
    this.volumesByTerm = byTerm;
  }

  /** The volumes of the term, in volume order; empty when no volume starts in that term. */
  List<Integer> volumesFor(int term) {
    List<Integer> volumes = volumesByTerm.get(term);
    return volumes == null ? Collections.<Integer>emptyList()
        : Collections.unmodifiableList(volumes);
  }

  /** The first and last term that have a volume. */
  int firstTerm() {
    return volumesByTerm.firstKey();
  }

  int lastTerm() {
    return volumesByTerm.lastKey();
  }

  private static int termOf(int volume, String courtTerm) {
    Matcher m = courtTerm == null ? null : LEADING_YEAR.matcher(courtTerm);
    if (m == null || !m.matches()) {
      throw new IllegalArgumentException("United States Reports volume " + volume
          + " has a courtTerm that does not start with a year: " + courtTerm);
    }
    return Integer.parseInt(m.group(1));
  }
}
