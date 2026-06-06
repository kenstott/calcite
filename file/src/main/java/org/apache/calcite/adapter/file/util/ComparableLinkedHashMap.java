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
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * A LinkedHashMap implementation that can be compared for ordering.
 * Comparison is done by converting maps to JSON strings and comparing them lexicographically.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public class ComparableLinkedHashMap<K, V> extends LinkedHashMap<K, V>
    implements Comparable<ComparableLinkedHashMap<K, V>> {
  @Override public int compareTo(ComparableLinkedHashMap<K, V> o) {
    ObjectMapper mapper = new ObjectMapper();

    try {
      // Convert this map to a JSON string
      String thisJson = mapper.writeValueAsString(this);

      // Convert the other map to a JSON string
      String otherJson = mapper.writeValueAsString(o);

      // Return the comparison result
      return thisJson.compareTo(otherJson);

    } catch (IOException e) {
      // Handle the exception
      throw new RuntimeException("Failed to serialize LinkedHashMap to JSON", e);
    }
  }
}
