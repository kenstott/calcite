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

import org.apache.calcite.config.CalciteSystemProperty;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit5 extension to handle Arrow tests.
 *
 * <p>Tests will be skipped if the Gandiva library cannot be loaded on the given platform.
 */
class ArrowExtension implements ExecutionCondition {

  /**
   * Whether to run this test.
   *
   * <p>Enabled by default, unless explicitly disabled from command line
   * ({@code -Dcalcite.test.arrow=false}) or if Gandiva library, used to implement arrow
   * filtering/projection, cannot be loaded.
   *
   * @return {@code true} if the test is enabled and can run in the current environment,
   *         {@code false} otherwise
   */
  @Override public ConditionEvaluationResult evaluateExecutionCondition(
      final ExtensionContext context) {

    // Delegate to the same probe the adapter uses at runtime, rather than re-deriving it here.
    // The previous check built a projector over an EMPTY schema and treated any GandivaException
    // as "the JNI library loaded properly" — but a platform whose LLVM cannot start reports
    // exactly that ("Could not create LLJIT instance: Symbols not found:
    // [llvm_orc_registerEHFrameSectionWrapper]"), so these tests were enabled and then failed
    // instead of skipping. GandivaAvailability builds a real projector and treats anything
    // thrown as unavailable, which is the question being asked.
    boolean enabled =
        CalciteSystemProperty.TEST_ARROW.value() && GandivaAvailability.isAvailable();

    if (enabled) {
      return ConditionEvaluationResult.enabled("Arrow tests enabled");
    } else {
      return ConditionEvaluationResult.disabled(
          "Arrow tests disabled: Gandiva is not available on this platform");
    }
  }
}
