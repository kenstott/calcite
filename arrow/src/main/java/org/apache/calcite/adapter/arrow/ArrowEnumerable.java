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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.Schema;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Enumerable that reads from an Arrow file, either through Gandiva (when available — see
 * {@link GandivaAvailability}) or, as a fallback, by reading vectors directly and — when a
 * conjunctive filter was pushed down — evaluating it in plain Java via
 * {@link ArrowJavaFilterEnumerator}.
 */
class ArrowEnumerable extends AbstractEnumerable<Object> {
  private final ArrowFileReader arrowFileReader;
  private final ImmutableIntList fields;
  private final @Nullable Object gandivaProjector;
  private final @Nullable Object gandivaFilter;
  private final @Nullable Schema javaFilterSchema;
  private final @Nullable List<String> javaFilterConditions;

  /** Gandiva path: exactly one of {@code projector}/{@code filter} is non-null, or both are
   * null for a plain scan. */
  ArrowEnumerable(ArrowFileReader arrowFileReader, ImmutableIntList fields,
      @Nullable Object projector, @Nullable Object filter) {
    this.arrowFileReader = arrowFileReader;
    this.fields = fields;
    this.gandivaProjector = projector;
    this.gandivaFilter = filter;
    this.javaFilterSchema = null;
    this.javaFilterConditions = null;
  }

  /** No-Gandiva path with a pushed-down conjunctive filter to evaluate in plain Java. */
  ArrowEnumerable(ArrowFileReader arrowFileReader, ImmutableIntList fields, Schema schema,
      List<String> conditions) {
    this.arrowFileReader = arrowFileReader;
    this.fields = fields;
    this.gandivaProjector = null;
    this.gandivaFilter = null;
    this.javaFilterSchema = schema;
    this.javaFilterConditions = conditions;
  }

  @Override public Enumerator<Object> enumerator() {
    try {
      if (gandivaProjector != null) {
        return new ArrowProjectEnumerator(arrowFileReader, fields, gandivaProjector);
      } else if (gandivaFilter != null) {
        return new ArrowGandivaFilterEnumerator(arrowFileReader, fields, gandivaFilter);
      } else if (javaFilterConditions != null) {
        return new ArrowJavaFilterEnumerator(arrowFileReader, fields,
            java.util.Objects.requireNonNull(javaFilterSchema, "javaFilterSchema"),
            javaFilterConditions);
      }
      // No projector, no filter, no conditions: Gandiva unavailable and no filter was pushed
      // down. Read the requested vectors directly; Calcite's Enumerable convention applies any
      // filter above the scan.
      return new ArrowScanEnumerator(arrowFileReader, fields);
    } catch (Exception e) {
      throw Util.toUnchecked(e);
    }
  }
}
