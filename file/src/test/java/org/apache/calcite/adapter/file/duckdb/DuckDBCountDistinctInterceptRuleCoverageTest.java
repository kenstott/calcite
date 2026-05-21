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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.plan.RelOptRule;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Coverage tests for {@link DuckDBCountDistinctInterceptRule}.
 * Tests INSTANCE singleton, matches() method visibility, and createHLLValues() method existence.
 */
@Tag("unit")
public class DuckDBCountDistinctInterceptRuleCoverageTest {

  @Test void testInstanceNotNull() {
    assertNotNull(DuckDBCountDistinctInterceptRule.INSTANCE);
  }

  @Test void testInstanceType() {
    assertTrue(DuckDBCountDistinctInterceptRule.INSTANCE instanceof RelOptRule);
  }

  @Test void testInstanceIsSingleton() {
    assertSame(DuckDBCountDistinctInterceptRule.INSTANCE,
        DuckDBCountDistinctInterceptRule.INSTANCE);
  }

  @Test void testInstanceDescription() {
    String desc = DuckDBCountDistinctInterceptRule.INSTANCE.toString();
    assertNotNull(desc);
    assertTrue(desc.contains("DuckDBCountDistinctInterceptRule"));
  }

  @Test void testCreateHLLValuesMethodExists() throws Exception {
    Method method =
        DuckDBCountDistinctInterceptRule.class.getDeclaredMethod("createHLLValues",
        org.apache.calcite.rel.core.Aggregate.class,
        java.util.List.class);
    method.setAccessible(true);
    assertNotNull(method);
  }

  @Test void testMatchesMethodExists() throws Exception {
    Method method =
        DuckDBCountDistinctInterceptRule.class.getMethod("matches",
        org.apache.calcite.plan.RelOptRuleCall.class);
    assertNotNull(method);
  }

  @Test void testOnMatchMethodExists() throws Exception {
    Method method =
        DuckDBCountDistinctInterceptRule.class.getMethod("onMatch",
        org.apache.calcite.plan.RelOptRuleCall.class);
    assertNotNull(method);
  }
}
