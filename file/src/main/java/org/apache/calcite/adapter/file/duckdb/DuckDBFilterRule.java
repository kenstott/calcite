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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Filter;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Rule to convert a {@link Filter} to a {@link JdbcRules.JdbcFilter}, registered alongside (not
 * instead of) core Calcite's stock {@code JdbcFilterRule}. Same rationale as
 * {@link DuckDBProjectRule}: the stock rule blocks a Filter containing ANY user-defined-function
 * call unconditionally, without consulting the dialect, so a WHERE clause referencing one of
 * this project's DuckDB-pushdown stub UDFs (see {@link DuckDBFunctionMapping}) could never push
 * down. This rule allows it through when every UDF the condition calls is one
 * {@link DuckDBFunctionMapping} recognizes, and still blocks any other unrecognized UDF exactly
 * like the stock rule.
 */
public class DuckDBFilterRule extends ConverterRule {
  /** Creates a DuckDBFilterRule. */
  public static DuckDBFilterRule create(JdbcConvention out) {
    return Config.INSTANCE
        .withConversion(Filter.class,
            filter -> !DuckDBFunctionMapping.hasUnsupportedUserDefinedFunction(
                filter.getCondition()),
            Convention.NONE, out, "DuckDBFilterRule")
        .withRuleFactory(DuckDBFilterRule::new)
        .toRule(DuckDBFilterRule.class);
  }

  /** Called from the Config. */
  protected DuckDBFilterRule(Config config) {
    super(config);
  }

  @Override public @Nullable RelNode convert(RelNode rel) {
    final Filter filter = (Filter) rel;
    return new JdbcRules.JdbcFilter(
        rel.getCluster(),
        rel.getTraitSet().replace(out),
        org.apache.calcite.plan.RelOptRule.convert(
            filter.getInput(),
            filter.getInput().getTraitSet().replace(out)),
        filter.getCondition());
  }
}
