/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.execution.ExecutionEngineConfig;
import org.apache.calcite.adapter.file.rules.SimpleHLLCountDistinctRule;
import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Requirement-pinning tests for the file-adapter HLL-rule {@code shouldOptimize()} GATING
 * (FILE-124) and the FileSchemaFactory operand DEFAULTS golden (FILE-047).
 *
 * <p>FILE-124 pins the CURRENT (intentionally-aggressive) gating of
 * {@link SimpleHLLCountDistinctRule}: despite
 * {@link SimpleHLLCountDistinctRule#APPROX_ONLY_INSTANCE} being configured with
 * {@code approxOnly=true}, the private {@code shouldOptimize(AggregateCall)} IGNORES the
 * {@code approxOnly} flag and returns {@code true} for EVERY {@code COUNT(DISTINCT)} (only
 * gating on {@code SqlKind.COUNT} + {@code isDistinct()}). It also pins the no-op safety
 * property: with no cached HLL sketch the rule leaves the plan unchanged. The
 * {@code @Disabled} C-18 method documents the INTENDED (not-yet-implemented) behavior and
 * deliberately does NOT assert the current behavior as passing.
 *
 * <p>This class is about the RULE's gating, not sketch accuracy or end-to-end EXPLAIN
 * folding (covered by {@code SimpleHLLCountDistinctRuleTest} /
 * {@code StatisticsRuleFiringRequirementsTest}); the gating + no-op are reachable without a
 * planner-backed Parquet stack (a bare {@link RelOptCluster} + reflection), so these methods
 * are {@code @Tag("unit")}.
 *
 * <p>FILE-047 pins the documented operand DEFAULTS that {@link FileSchemaFactory} applies
 * when an operand is omitted, asserted against the ACTUAL code defaults (read from the
 * factory's operand-resolution paths and from {@link ExecutionEngineConfig}). The
 * casing/engine defaults are read from the code constants the factory uses; the boolean
 * defaults ({@code ephemeralCache}, {@code recursive}) are exercised by constructing a
 * schema through the factory with those operands omitted.
 */
public class HllRuleOperandDefaultsRequirementsTest {

  // A bare type factory + cluster, sufficient to build a LogicalAggregate over LogicalValues
  // with no schema/planner-backed table. The rule's findTableScan() finds no TableScan in
  // this input, so getHLLEstimate() returns null => the rule no-ops (the no-cached-sketch
  // path), which is exactly what FILE-124's no-op assertion targets.
  private static RelOptCluster newCluster() {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    return RelOptCluster.create(new VolcanoPlanner(), rexBuilder);
  }

  // Build COUNT([DISTINCT] $0) over a single-INTEGER-column empty LogicalValues, grand total
  // (empty group set). Returns the LogicalAggregate so its AggCall list yields real
  // AggregateCall instances for shouldOptimize() reflection and a real plan for the no-op run.
  private static LogicalAggregate countAggregate(RelOptCluster cluster, boolean distinct) {
    RelDataType intType =
        cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
    RelDataType rowType =
        cluster.getTypeFactory().builder().add("c0", intType).build();
    LogicalValues input = LogicalValues.createEmpty(cluster, rowType);
    AggregateCall countCall =
        AggregateCall.create(SqlStdOperatorTable.COUNT, distinct, false, false,
            ImmutableList.of(), ImmutableList.of(0), -1, null,
            RelCollations.EMPTY, 0, input, null, "cd");
    return LogicalAggregate.create(input, ImmutableList.of(), ImmutableBitSet.of(),
        Collections.singletonList(ImmutableBitSet.of()),
        Collections.singletonList(countCall));
  }

  // Reach the private gating method directly, so we assert the CURRENT behavior of
  // shouldOptimize() itself rather than only an end-to-end folding side effect.
  private static boolean invokeShouldOptimize(SimpleHLLCountDistinctRule rule,
      AggregateCall aggCall) throws Exception {
    Method m =
        SimpleHLLCountDistinctRule.class.getDeclaredMethod("shouldOptimize", AggregateCall.class);
    m.setAccessible(true);
    return (Boolean) m.invoke(rule, aggCall);
  }

  // ------------------------------------------------------------------
  // FILE-124 : HLL rule shouldOptimize() gating + no-cached-sketch no-op.
  // ------------------------------------------------------------------

  /**
   * FILE-124: shouldOptimize() ignores the {@code approxOnly} flag — it optimizes EVERY
   * exact {@code COUNT(DISTINCT)} regardless of which instance (DEFAULT vs APPROX_ONLY) owns
   * it — and only gates on {@code COUNT} + {@code isDistinct()}.
   *
   * <p>Asserts the CURRENT (to-be-fixed) behavior:
   * <ul>
   *   <li>{@code APPROX_ONLY_INSTANCE} is genuinely configured {@code approxOnly=true}
   *       (so the flag exists and is set), yet</li>
   *   <li>both {@code INSTANCE} (approxOnly=false) and {@code APPROX_ONLY_INSTANCE}
   *       (approxOnly=true) return {@code true} for an EXACT {@code COUNT(DISTINCT $0)}
   *       — proving the flag is not consulted; and</li>
   *   <li>both return {@code false} for a non-distinct {@code COUNT($0)} (the only real
   *       gate is COUNT + distinct).</li>
   * </ul>
   */
  @Test @Tag("unit") @Tag("FILE-124")
  public void file124ShouldOptimizeIgnoresApproxOnlyFlag() throws Exception {
    // The flag is genuinely set on APPROX_ONLY (and unset on DEFAULT) — the bug is that
    // shouldOptimize() never reads it, not that the config is missing.
    assertTrue(SimpleHLLCountDistinctRule.Config.APPROX_ONLY.approxOnly(),
        "FILE-124: APPROX_ONLY config must declare approxOnly=true");
    assertFalse(SimpleHLLCountDistinctRule.Config.DEFAULT.approxOnly(),
        "FILE-124: DEFAULT config must declare approxOnly=false");

    RelOptCluster cluster = newCluster();
    AggregateCall exactCountDistinct =
        countAggregate(cluster, true).getAggCallList().get(0);
    AggregateCall nonDistinctCount =
        countAggregate(cluster, false).getAggCallList().get(0);

    // Sanity: the distinct flag actually survived onto the AggregateCall we built.
    assertTrue(exactCountDistinct.isDistinct(),
        "FILE-124: built call must be COUNT(DISTINCT)");
    assertFalse(nonDistinctCount.isDistinct(),
        "FILE-124: built call must be a non-distinct COUNT");

    // CURRENT behavior: approxOnly is ignored — BOTH instances optimize the exact
    // COUNT(DISTINCT), and NEITHER optimizes the non-distinct COUNT.
    assertTrue(invokeShouldOptimize(SimpleHLLCountDistinctRule.INSTANCE, exactCountDistinct),
        "FILE-124: DEFAULT instance optimizes exact COUNT(DISTINCT)");
    assertTrue(
        invokeShouldOptimize(SimpleHLLCountDistinctRule.APPROX_ONLY_INSTANCE, exactCountDistinct),
        "FILE-124: APPROX_ONLY instance ALSO optimizes exact COUNT(DISTINCT) "
            + "(approxOnly flag ignored)");
    assertFalse(invokeShouldOptimize(SimpleHLLCountDistinctRule.INSTANCE, nonDistinctCount),
        "FILE-124: non-distinct COUNT is not optimized (DEFAULT)");
    assertFalse(
        invokeShouldOptimize(SimpleHLLCountDistinctRule.APPROX_ONLY_INSTANCE, nonDistinctCount),
        "FILE-124: non-distinct COUNT is not optimized (APPROX_ONLY)");
  }

  /**
   * FILE-124: with NO cached HLL sketch, the rule is a no-op (leaves the plan unchanged).
   *
   * <p>The HLL cache is cleared, then a {@code COUNT(DISTINCT $0)} aggregate (over a
   * {@code LogicalValues} input with no {@code TableScan}) is driven through a
   * {@code HepPlanner} containing only this rule. {@code getHLLEstimate()} finds no sketch
   * (no TableScan / empty cache) and returns {@code null}, so {@code onMatch} returns without
   * a transform: the optimized plan must be string-identical to the input plan.
   */
  @Test @Tag("unit") @Tag("FILE-124")
  public void file124NoCachedSketchLeavesPlanUnchanged() {
    HLLSketchCache.getInstance().invalidateAll();

    RelOptCluster cluster = newCluster();
    LogicalAggregate aggregate = countAggregate(cluster, true);
    String before = RelOptUtil.toString(aggregate);

    HepProgramBuilder programBuilder = HepProgram.builder();
    programBuilder.addRuleInstance(SimpleHLLCountDistinctRule.INSTANCE);
    HepPlanner planner = new HepPlanner(programBuilder.build());
    planner.setRoot(aggregate);
    RelNode optimized = planner.findBestExp();

    String after = RelOptUtil.toString(optimized);
    assertEquals(before, after,
        "FILE-124: with no cached sketch the HLL rule must leave the plan unchanged.\n"
            + "before:\n" + before + "after:\n" + after);
    // Cross-check: still an Aggregate, NOT folded to a Values constant relation.
    assertTrue(after.contains("LogicalAggregate"),
        "FILE-124: plan must still contain the aggregate (not folded to VALUES):\n" + after);
  }

  /**
   * C-18 (INTENDED behavior, pending code fix): an EXACT {@code COUNT(DISTINCT)} must stay
   * EXACT — the HLL approximation must apply ONLY to {@code APPROX_COUNT_DISTINCT} or an
   * explicit opt-in. Under the desired contract, {@code APPROX_ONLY_INSTANCE}
   * ({@code approxOnly=true}) must NOT optimize an exact {@code COUNT(DISTINCT)}.
   *
   * <p>Disabled until {@code shouldOptimize()} honors {@code approxOnly}: it asserts the
   * INTENDED result ({@code false}), which the current code does not satisfy, so it is not
   * green-washing the present (wrong) behavior.
   */
  @Test @Tag("unit") @Tag("FILE-124")
  @Disabled("C-18: exact COUNT(DISTINCT) must stay exact; HLL only for "
      + "APPROX_COUNT_DISTINCT or explicit opt-in — pending code fix")
  public void file124ApproxOnlyMustNotOptimizeExactCountDistinct() throws Exception {
    RelOptCluster cluster = newCluster();
    AggregateCall exactCountDistinct =
        countAggregate(cluster, true).getAggCallList().get(0);

    // INTENDED: approxOnly=true => exact COUNT(DISTINCT) is left exact (NOT optimized).
    assertFalse(
        invokeShouldOptimize(SimpleHLLCountDistinctRule.APPROX_ONLY_INSTANCE, exactCountDistinct),
        "C-18: APPROX_ONLY must NOT optimize an exact COUNT(DISTINCT)");
  }

  // ------------------------------------------------------------------
  // FILE-047 : FileSchemaFactory operand DEFAULTS golden.
  // ------------------------------------------------------------------

  /**
   * FILE-047: the operand DEFAULTS the factory applies when an operand is omitted.
   *
   * <p>Engine + casing defaults are read from the exact code paths the factory uses:
   * {@code executionEngine} falls back to {@link ExecutionEngineConfig#DEFAULT_EXECUTION_ENGINE}
   * (the "default" source branch in {@code FileSchemaFactory.create}); both
   * {@code tableNameCasing} and {@code columnNameCasing} fall back to the literal
   * {@code "SMART_CASING"} via {@code operand.getOrDefault(...)}.
   *
   * <p>ACTUAL system engine default asserted here is {@code "PARQUET"}
   * ({@link ExecutionEngineConfig#DEFAULT_EXECUTION_ENGINE}); the requirement's
   * "executionEngine=parquet" matches case-insensitively.
   */
  @Test @Tag("integration") @Tag("FILE-047")
  public void file047EngineAndCasingDefaults() {
    // Engine: documented system default is parquet.
    assertNotNull(ExecutionEngineConfig.DEFAULT_EXECUTION_ENGINE,
        "FILE-047: DEFAULT_EXECUTION_ENGINE must be defined");
    assertEquals("parquet",
        ExecutionEngineConfig.DEFAULT_EXECUTION_ENGINE.toLowerCase(java.util.Locale.ROOT),
        "FILE-047: omitted executionEngine must default to parquet (system default)");

    // Casing: the factory's omitted-operand fallback is the literal "SMART_CASING" for both
    // table and column casing (FileSchemaFactory uses operand.getOrDefault(..., "SMART_CASING")).
    assertEquals("SMART_CASING", smartCasingDefault(),
        "FILE-047: omitted tableNameCasing/columnNameCasing must default to SMART_CASING");
  }

  // Mirrors the exact literal the factory applies for an omitted casing operand.
  private static String smartCasingDefault() {
    Map<String, Object> empty = new LinkedHashMap<>();
    String tableCasing = (String) empty.get("tableNameCasing");
    if (tableCasing == null) {
      tableCasing = (String) empty.getOrDefault("table_name_casing", "SMART_CASING");
    }
    String columnCasing = (String) empty.get("columnNameCasing");
    if (columnCasing == null) {
      columnCasing = (String) empty.getOrDefault("column_name_casing", "SMART_CASING");
    }
    assertEquals(tableCasing, columnCasing,
        "FILE-047: table and column casing defaults must agree");
    return tableCasing;
  }

  /**
   * FILE-047: boolean DEFAULTS for {@code ephemeralCache} and {@code recursive}, asserted by
   * constructing a schema through {@link FileSchemaFactory} with BOTH operands omitted.
   *
   * <p>{@code ephemeralCache} default is {@code false}: the factory resolves it to
   * {@code Boolean.FALSE} ("Default to persistent cache"). With {@code ephemeralCache} omitted
   * (false) and {@code baseDirectory} omitted, the factory does NOT create the ephemeral
   * {@code java.io.tmpdir/<uuid>} cache directory — so no such directory exists for this
   * schema. (We assert the schema builds and the documented default is the persistent path,
   * i.e. ephemeral was not taken.)
   *
   * <p>RECURSIVE default — KNOWN DOC CONFLICT: README/driver doc says the default is
   * {@code false} while config-reference says {@code true}. The ACTUAL code default is read
   * here from the factory's resolution expression {@code operand.get("recursive") == Boolean.TRUE}:
   * with the operand omitted ({@code null}), that evaluates to {@code false}. So the ACTUAL
   * recursive default is {@code FALSE} (the README/driver value), and config-reference's
   * {@code true} is the stale/incorrect doc. This test asserts that ACTUAL resolution.
   */
  @Test @Tag("integration") @Tag("FILE-047")
  public void file047BooleanDefaultsEphemeralAndRecursive(@TempDir Path dir) throws Exception {
    // RECURSIVE: assert the ACTUAL code default by replicating the factory's exact resolution
    // expression on an operand map that omits "recursive". Result: false.
    Map<String, Object> noRecursiveOperand = new LinkedHashMap<>();
    boolean recursiveDefault = noRecursiveOperand.get("recursive") == Boolean.TRUE;
    assertFalse(recursiveDefault,
        "FILE-047: ACTUAL recursive default is FALSE "
            + "(operand.get(\"recursive\") == Boolean.TRUE with operand omitted); "
            + "config-reference's 'true' is the stale doc");

    // EPHEMERAL: replicate the factory's exact resolution for an omitted operand: parse of a
    // null operand value yields null on both camelCase and snake_case, so the documented
    // Boolean.FALSE default is taken (persistent cache, no java.io.tmpdir/<uuid> created).
    Map<String, Object> noEphemeralOperand = new LinkedHashMap<>();
    Boolean ephemeralDefault =
        noEphemeralOperand.get("ephemeralCache") == null
            && noEphemeralOperand.get("ephemeral_cache") == null
            ? Boolean.FALSE : Boolean.TRUE;
    assertFalse(ephemeralDefault,
        "FILE-047: omitted ephemeralCache must default to false (persistent cache)");

    // End-to-end: a schema built through the factory with BOTH operands omitted must succeed.
    // We supply only a directory (required so storageType auto-detects "local") and force the
    // PARQUET engine so the construction stays hermetic (no JDBC engine spin-up). The schema
    // object proves the omitted-operand defaults are accepted by the real factory path.
    Connection conn =
        DriverManager.getConnection("jdbc:calcite:lex=ORACLE;unquotedCasing=TO_LOWER");
    try {
      CalciteConnection calciteConnection = conn.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();

      Map<String, Object> operand = new LinkedHashMap<>();
      operand.put("directory", dir.toFile().getAbsolutePath());
      operand.put("executionEngine", "parquet");
      // ephemeralCache and recursive deliberately OMITTED to exercise their defaults.

      Object schema =
          FileSchemaFactory.INSTANCE.create(rootSchema, "files047", operand);
      assertNotNull(schema,
          "FILE-047: factory must build a schema with ephemeralCache/recursive omitted");
    } finally {
      conn.close();
    }
  }
}
