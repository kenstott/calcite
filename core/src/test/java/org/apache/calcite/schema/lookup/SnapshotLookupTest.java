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
package org.apache.calcite.schema.lookup;

import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test for CachedLookup.
 */
class SnapshotLookupTest {
  private final Lookup<String> testee = new SnapshotLookup<>(new FakeLookup("a", "1"));

  @Test void testNull() {
    assertThat(testee.get("c"), nullValue());
  }

  @Test void test() {
    assertThat(testee.get("a"), equalTo("1"));
  }

  @Test void testIgnoreCase() {
    assertThat(testee.getIgnoreCase("A"), equalTo(new Named<>("a", "1")));
  }

  /** A listed name the delegate then cannot produce (a view whose on-demand CREATE failed)
   * must drop out of the frozen name set, or every later listing keeps naming it. */
  @Test void testNameThatFailsToResolveIsNoLongerListed() {
    Lookup<String> lookup =
        new SnapshotLookup<>(new FakeLookup("a", "1", "b", "2") {
          @Override public @Nullable String get(String name) {
            return "b".equals(name) ? null : super.get(name);
          }
        });
    assertThat(lookup.getNames(LikePattern.any()), equalTo(ImmutableSet.of("a", "b")));
    assertThat(lookup.get("b"), nullValue());
    assertThat(lookup.getNames(LikePattern.any()), equalTo(ImmutableSet.of("a")));
    assertThat(lookup.get("a"), equalTo("1"));
  }

}
