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
package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlDialect;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Rule that splits a {@link Filter} whose conjuncts are only partly pushable
 * to a JDBC source into two filters: a lower one holding the conjuncts the
 * source can run ({@link JdbcRules#canPush(RexNode, SqlDialect)}), which
 * {@link JdbcRules.JdbcFilterRule} then pushes, and an upper one holding the
 * rest, which Calcite evaluates.
 *
 * <p>It matches a filter whose input is in the rule's {@link JdbcConvention}
 * ({@link #create}), or is a {@link Project} whose input is
 * ({@link #createOverProject}): a filter the planner has not moved below
 * its project. It is a
 * {@link SubstitutionRule} and prunes the original filter, so the split
 * replaces it rather than competing with it on cost.
 */
@Value.Enclosing
public class JdbcFilterSplitRule extends RelRule<JdbcFilterSplitRule.Config>
    implements SubstitutionRule {

  /** Creates a JdbcFilterSplitRule. */
  protected JdbcFilterSplitRule(Config config) {
    super(config);
  }

  /** Creates a JdbcFilterSplitRule for the given JDBC convention. */
  public static JdbcFilterSplitRule create(JdbcConvention out) {
    return ImmutableJdbcFilterSplitRule.Config.builder()
        .withDialect(out.dialect)
        .withOperandSupplier(b ->
            b.operand(Filter.class)
                .trait(Convention.NONE)
                .predicate(filter ->
                    filter.getInput().getConvention() == out
                        && isPartlyPushable(filter.getCondition(),
                            out.dialect))
                .anyInputs())
        .withDescription("JdbcFilterSplitRule(out:" + out + ")")
        .build()
        .toRule();
  }

  /** Creates a JdbcFilterSplitRule for a filter over a {@link Project} whose
   * input is in the given JDBC convention; the lower filter goes between the
   * filter and the project, and {@link JdbcRules.JdbcFilterRule} pushes it
   * with the project. */
  public static JdbcFilterSplitRule createOverProject(JdbcConvention out) {
    return ImmutableJdbcFilterSplitRule.Config.builder()
        .withDialect(out.dialect)
        .withOperandSupplier(b0 ->
            b0.operand(Filter.class)
                .trait(Convention.NONE)
                .predicate(filter ->
                    isPartlyPushable(filter.getCondition(), out.dialect))
                .oneInput(b1 ->
                    b1.operand(Project.class)
                        .trait(Convention.NONE)
                        .predicate(project ->
                            project.getInput().getConvention() == out)
                        .anyInputs()))
        .withDescription("JdbcFilterSplitRule:project(out:" + out + ")")
        .build()
        .toRule();
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final SqlDialect dialect = config.dialect();
    final List<RexNode> pushable = new ArrayList<>();
    final List<RexNode> rest = new ArrayList<>();
    for (RexNode conjunct : RelOptUtil.conjunctions(filter.getCondition())) {
      (JdbcRules.canPush(conjunct, dialect) ? pushable : rest).add(conjunct);
    }
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    final RelNode lower =
        filter.copy(filter.getTraitSet(), filter.getInput(),
            RexUtil.composeConjunction(rexBuilder, pushable));
    call.transformTo(
        filter.copy(filter.getTraitSet(), lower,
            RexUtil.composeConjunction(rexBuilder, rest)));
    // The split is not a cost-based alternative: the source always gets the
    // conjuncts it can run, as Trino pushes them (REQ-2256). The planner's
    // row-count cost would otherwise prefer the unsplit filter, which has
    // one operator fewer. As a SubstitutionRule this runs before the rules
    // that implement the unsplit filter, so pruning it here stops them.
    call.getPlanner().prune(filter);
  }

  /** Returns whether some but not all conjuncts of a condition can be
   * pushed to a source with the given dialect. */
  private static boolean isPartlyPushable(RexNode condition,
      SqlDialect dialect) {
    boolean anyPushable = false;
    boolean anyNotPushable = false;
    for (RexNode conjunct : RelOptUtil.conjunctions(condition)) {
      if (JdbcRules.canPush(conjunct, dialect)) {
        anyPushable = true;
      } else {
        anyNotPushable = true;
      }
    }
    return anyPushable && anyNotPushable;
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    /** The dialect of the JDBC source the lower filter is pushed to. */
    SqlDialect dialect();

    /** Sets {@link #dialect}. */
    Config withDialect(SqlDialect dialect);

    @Override default JdbcFilterSplitRule toRule() {
      return new JdbcFilterSplitRule(this);
    }
  }
}
