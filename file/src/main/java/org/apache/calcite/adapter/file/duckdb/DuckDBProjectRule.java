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

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Rule to convert a {@link Project} to a {@link JdbcRules.JdbcProject}, registered alongside
 * (not instead of) core Calcite's stock {@code JdbcProjectRule}. The stock rule refuses to push
 * down a Project containing ANY user-defined-function call, unconditionally, without ever
 * consulting the target dialect (see {@code JdbcRules$JdbcProjectRule.create}, which checks a
 * hardcoded visitor rather than {@code out.dialect.supportsFunction}). That meant this project's
 * own DuckDB-pushdown stub UDFs (JSON_EXTRACT, STRING_SPLIT, ...; see
 * {@link DuckDBFunctionMapping}) could never reach DuckDB even though the dialect already knows
 * exactly how to render them ({@link DuckDBFunctionMapping#unparseCall}) -- the query validated
 * but always fell back to Enumerable execution, invoking the stub's intentional throw.
 *
 * <p>This rule allows a Project through when every user-defined function it calls is one
 * {@link DuckDBFunctionMapping} recognizes, and still blocks (returns null / doesn't match) for
 * any other unrecognized UDF, exactly like the stock rule. For a Project with no UDF at all,
 * both rules' predicates are true and either may fire; only this rule fires when a recognized
 * DuckDB stub UDF is present.
 */
public class DuckDBProjectRule extends ConverterRule {
  /** Creates a DuckDBProjectRule. */
  public static DuckDBProjectRule create(JdbcConvention out) {
    return Config.INSTANCE
        .withConversion(Project.class,
            project -> !hasUnsupportedUdf(project),
            Convention.NONE, out, "DuckDBProjectRule")
        .withRuleFactory(DuckDBProjectRule::new)
        .toRule(DuckDBProjectRule.class);
  }

  /** Called from the Config. */
  protected DuckDBProjectRule(Config config) {
    super(config);
  }

  private static boolean hasUnsupportedUdf(Project project) {
    for (RexNode node : project.getProjects()) {
      if (DuckDBFunctionMapping.hasUnsupportedUserDefinedFunction(node)) {
        return true;
      }
    }
    return false;
  }

  @Override public boolean matches(RelOptRuleCall call) {
    Project project = call.rel(0);
    return project.getVariablesSet().isEmpty();
  }

  @Override public @Nullable RelNode convert(RelNode rel) {
    final Project project = (Project) rel;
    return new JdbcRules.JdbcProject(
        rel.getCluster(),
        rel.getTraitSet().replace(out),
        RelOptRule.convert(
            project.getInput(),
            project.getInput().getTraitSet().replace(out)),
        project.getProjects(),
        project.getRowType());
  }
}
