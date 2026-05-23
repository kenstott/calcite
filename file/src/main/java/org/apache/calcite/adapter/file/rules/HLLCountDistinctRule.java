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
package org.apache.calcite.adapter.file.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.immutables.value.Value;

/**
 * Rule that replaces COUNT(DISTINCT) operations with pre-computed HLL sketch lookups.
 * This provides instant responses for cardinality queries without scanning data.
 */
@Value.Enclosing
public class HLLCountDistinctRule extends RelRule<HLLCountDistinctRule.Config> {

  public static final HLLCountDistinctRule INSTANCE =
      Config.DEFAULT.toRule();

  private HLLCountDistinctRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    // TODO: Temporarily disabled due to AggregateCall API changes
    // The HLL optimization rule needs to be updated to match the current Calcite API
    return;
  }

  /** Configuration for HLLCountDistinctRule. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableHLLCountDistinctRule.Config.builder()
        .withOperandSupplier(b0 ->
            b0.operand(Aggregate.class)
                .oneInput(b1 ->
                    b1.operand(RelNode.class)
                        .anyInputs()))
        .build();

    @Override default HLLCountDistinctRule toRule() {
      return new HLLCountDistinctRule(this);
    }
  }
}
