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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DimensionPartition}.
 */
@Tag("unit")
class DimensionPartitionTest {

  @Test void testBasic() {
    Map<String, String> context = new HashMap<String, String>();
    context.put("state_abbr", "CA");

    List<Map<String, String>> combinations = new ArrayList<Map<String, String>>();
    Map<String, String> combo1 = new HashMap<String, String>();
    combo1.put("state_abbr", "CA");
    combo1.put("year", "2020");
    combo1.put("ori", "CA001");
    combinations.add(combo1);

    Map<String, String> combo2 = new HashMap<String, String>();
    combo2.put("state_abbr", "CA");
    combo2.put("year", "2020");
    combo2.put("ori", "CA002");
    combinations.add(combo2);

    DimensionPartition partition = new DimensionPartition(context, combinations);

    assertEquals("CA", partition.getPartitionContext().get("state_abbr"));
    assertEquals(2, partition.getCombinations().size());
  }

  @Test void testPartitionContextIsImmutable() {
    Map<String, String> context = new HashMap<String, String>();
    context.put("state_abbr", "CA");
    List<Map<String, String>> combinations = new ArrayList<Map<String, String>>();

    DimensionPartition partition = new DimensionPartition(context, combinations);

    assertThrows(UnsupportedOperationException.class, () ->
        partition.getPartitionContext().put("new_key", "value"));
  }

  @Test void testToString() {
    Map<String, String> context = Collections.singletonMap("state", "NY");
    List<Map<String, String>> combinations = new ArrayList<Map<String, String>>();
    combinations.add(Collections.singletonMap("state", "NY"));

    DimensionPartition partition = new DimensionPartition(context, combinations);
    String str = partition.toString();
    assertTrue(str.contains("state"));
    assertTrue(str.contains("NY"));
    assertTrue(str.contains("1"));
  }

  @Test void testEmptyCombinations() {
    Map<String, String> context = Collections.singletonMap("key", "val");
    List<Map<String, String>> combinations = Collections.emptyList();

    DimensionPartition partition = new DimensionPartition(context, combinations);
    assertEquals(0, partition.getCombinations().size());
  }
}
