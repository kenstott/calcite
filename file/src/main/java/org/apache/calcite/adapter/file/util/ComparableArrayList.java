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
import java.util.ArrayList;

/**
 * An ArrayList implementation that can be compared for ordering.
 * Comparison is done by converting lists to JSON strings and comparing them lexicographically.
 *
 * @param <T> the type of elements in this list
 */
public class ComparableArrayList<T> extends ArrayList<T>
    implements Comparable<ComparableArrayList<T>> {
  @Override public int compareTo(ComparableArrayList<T> o) {
    ObjectMapper mapper = new ObjectMapper();

    try {
      // Convert this list to a JSON string
      String thisJson = mapper.writeValueAsString(this);

      // Convert the other list to a JSON string
      String otherJson = mapper.writeValueAsString(o);

      // Return the comparison result
      return thisJson.compareTo(otherJson);
    } catch (IOException e) {
      // Handle the exception
      throw new RuntimeException("Failed to serialize ArrayList to JSON", e);
    }
  }
}
