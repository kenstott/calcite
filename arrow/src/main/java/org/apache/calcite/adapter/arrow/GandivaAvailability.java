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

import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;

/**
 * Whether Gandiva, the optional LLVM expression compiler, can actually run here.
 *
 * <p>Gandiva accelerates Arrow scans by JIT-compiling projections and conjunctive comparison
 * filters and evaluating them over Arrow buffers, with a selection vector so a filter materializes
 * only surviving rows. It is optional: it ships ~118 MB of platform-specific native libraries, far
 * more than the rest of this adapter combined, so it is a {@code compileOnly} dependency and is not
 * on the runtime classpath unless a deployment adds it deliberately. Without it the adapter reads
 * the same data through {@link ArrowScanEnumerator} and lets Calcite's Enumerable convention apply
 * filters and projections — same results, no vectorised evaluation.
 *
 * <p>Presence of the classes is not enough to answer the question. The native library loads its own
 * LLVM, and on some Linux/LLVM combinations it fails at JIT-construction time with
 * {@code Could not create LLJIT instance: Symbols not found: [llvm_orc_registerEHFrameSectionWrapper]}
 * — the classes resolve, the linkage does not. So this actually builds a trivial projector once and
 * caches whether that worked. Anything thrown, including {@link UnsatisfiedLinkError} and
 * {@link ExceptionInInitializerError}, means unavailable.
 */
final class GandivaAvailability {

  private GandivaAvailability() {
  }

  private static final boolean AVAILABLE = probe();

  /** True when a Gandiva projector can be built and closed on this JVM and platform. */
  static boolean isAvailable() {
    return AVAILABLE;
  }

  @SuppressWarnings("CatchAndPrintStackTrace")
  private static boolean probe() {
    try {
      Field field =
          new Field("probe", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Schema schema = new Schema(Collections.singletonList(field));
      TreeNode node = TreeBuilder.makeField(field);
      ExpressionTree expression = TreeBuilder.makeExpression(node, field);
      Projector projector =
          Projector.make(schema, Collections.singletonList(expression));
      projector.close();
      return true;
    } catch (Throwable t) {
      // NoClassDefFoundError (not deployed), UnsatisfiedLinkError (no native library for this
      // platform), or a RuntimeException from LLJIT construction. All mean the same thing to a
      // caller: take the non-vectorised path.
      return false;
    }
  }
}
