---
description: How to dump relational algebra plans, enable planner tracing, interpret Volcano search space, and diagnose rule application failures in this project
---

# Calcite Debugging Guide

Apply these techniques to diagnose the issue described in $ARGUMENTS.

## 1. Dump the RelNode Plan

The single most useful debugging action. Shows what the planner actually produced.

```java
// Import
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExplainLevel;

// Quick dump (attributes only)
System.out.println(RelOptUtil.toString(relNode));

// Full dump with types (use this when debugging type mismatches)
System.out.println(RelOptUtil.toString(relNode, SqlExplainLevel.ALL_ATTRIBUTES));

// Dump with description prefix
System.out.println(RelOptUtil.dumpPlan("After optimization", relNode,
    SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
```

**Where to insert**: Place `RelOptUtil.toString()` calls:
- After `SqlToRelConverter.convertQuery()` — see what SQL-to-rel produced
- After `planner.findBestExpr()` — see the optimized plan
- Inside rule `onMatch()` methods — see what the rule is matching against

## 2. Enable Planner Tracing via CalciteTrace

All tracers are defined in `core/src/main/java/org/apache/calcite/util/trace/CalciteTrace.java`.

### Logger names to enable (set to DEBUG or TRACE in logback-test.xml):

| Logger | What it shows |
|--------|--------------|
| `org.apache.calcite.plan.RelOptPlanner` | Rule firing, plan registration. **Start here.** |
| `org.apache.calcite.plan.volcano.task` | Volcano task scheduling (which rules fire in what order) |
| `org.apache.calcite.sql.parser` | Parse events |
| `org.apache.calcite.sql2rel` | SQL-to-RelNode conversion |
| `org.apache.calcite.sql.timing` | Timing for each processing stage |
| `org.apache.calcite.plan.AbstractRelOptPlanner.rule_execution_summary` | Rule attempt counts and elapsed time per rule |

### Enable in logback-test.xml:
```xml
<!-- Add to src/test/resources/logback-test.xml -->
<logger name="org.apache.calcite.plan.RelOptPlanner" level="DEBUG"/>

<!-- For verbose Volcano internals (noisy but complete): -->
<logger name="org.apache.calcite.plan.RelOptPlanner" level="TRACE"/>
<logger name="org.apache.calcite.plan.volcano.task" level="DEBUG"/>
```

### Enable programmatically for one-off debugging:
```java
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

((Logger) LoggerFactory.getLogger("org.apache.calcite.plan.RelOptPlanner"))
    .setLevel(Level.DEBUG);
```

## 3. Interpret Volcano Planner Search Space

### Provenance tracking (enabled automatically when planner logger is DEBUG):

The `VolcanoPlanner` maintains a provenance map (`Map<RelNode, Provenance>`) that records which rule produced each RelNode. When DEBUG is enabled:

```
Provenance:
  rel#42:EnumerableProject ← rule [FilterProjectTransposeRule] on rel#15:LogicalFilter
  rel#15:LogicalFilter ← rule [FilterMergeRule] on rel#8:LogicalFilter, rel#3:LogicalFilter
```

Read bottom-up: each line shows what rule produced a node and from what inputs.

### Dump the full search space:

```java
// In VolcanoPlanner (or via reflection in tests):
import org.apache.calcite.plan.volcano.Dumpers;

// Dump all RelSets with best costs
StringWriter sw = new StringWriter();
Dumpers.dumpSets(planner, new PrintWriter(sw));
System.out.println(sw.toString());

// Generate Graphviz visualization (open in browser)
StringWriter gv = new StringWriter();
Dumpers.dumpGraphviz(planner, new PrintWriter(gv));
Files.write(Paths.get("/tmp/volcano.dot"), gv.toString().getBytes());
// Then: dot -Tsvg /tmp/volcano.dot > /tmp/volcano.svg
```

### Interactive visualization (best for complex debugging):

```java
import org.apache.calcite.plan.visualizer.RuleMatchVisualizer;

// Attach before optimization
RuleMatchVisualizer viz = new RuleMatchVisualizer("/tmp/calcite-viz", "debug");
viz.attachTo(planner);

planner.findBestExpr();

// For HepPlanner only:
viz.writeToFile();

// Open /tmp/calcite-viz/debug.html in browser
```

### Cost interpretation:

When you see `LOGGER.trace("Subset cost changed: subset [{}] cost was {} now {}", ...)`:
- Cost = (rowCount, cpu, io) triple
- Lower is better
- `inf` means no valid physical implementation yet
- Convention.NONE subsets start at infinite cost until a converter rule fires

## 4. Diagnose Rule Application Failures

### Rule not firing at all?

**Check 1: Operand mismatch**
The rule's operand tree must match the RelNode class hierarchy exactly.
```java
// If your rule expects LogicalFilter but the plan has EnumerableFilter,
// the rule will never match. Check with:
System.out.println("Node class: " + relNode.getClass().getName());
System.out.println("Convention: " + relNode.getConvention());
```

**Check 2: Predicate rejection**
Rules with predicates can silently reject matches:
```java
// Example: FilterProjectTransposeRule rejects correlated filters
Config DEFAULT = ImmutableFilterProjectTransposeRule.Config.of()
    .withOperandFor(filterClass,
        f -> !RexUtil.containsCorrelation(f.getCondition()),  // <-- predicate
        projectClass, project -> true);
```
Add logging inside the predicate to confirm.

**Check 3: TransformationRule vs physical nodes**
If your rule implements `TransformationRule`, VolcanoPlanner will NOT fire it on physical nodes (nodes implementing `PhysicalNode`). This is intentional — transformation rules are for logical rewrites only.

**Check 4: Rule not registered**
```java
// Verify rule is in the planner
planner.getRules().forEach(r -> System.out.println(r.getClass().getSimpleName()));

// For convention-based registration, check Convention.register():
// See file/src/.../duckdb/DuckDBConvention.java for the pattern
```

### Rule fires but produces nothing?

**Check 5: `transformTo()` not called in `onMatch()`**
The rule matched but didn't produce an alternative. Add logging at the top and before each `return` in `onMatch()`.

**Check 6: Equivalent node already exists**
VolcanoPlanner deduplicates by digest. If the produced node has the same digest as an existing node, it's silently ignored. Check with:
```java
System.out.println("Produced node digest: " + newNode.getDigest());
```

### Rule fires but result is not chosen?

**Check 7: Cost comparison**
The produced node may have higher cost than alternatives:
```java
RelMetadataQuery mq = call.getMetadataQuery();
System.out.println("Original cost: " + mq.getCumulativeCost(originalRel));
System.out.println("New cost: " + mq.getCumulativeCost(newRel));
```

### Rule causes infinite loop?

See `/project:calcite:rules` for prevention patterns. Quick diagnosis:
```java
// Enable rule attempt counting
// Logger: org.apache.calcite.plan.AbstractRelOptPlanner.rule_execution_summary
// At DEBUG level, prints attempt count + elapsed time per rule after optimization
```

## 5. Common Debug Scenarios in This Project

### DuckDB adapter query not pushing down:
1. Dump plan after optimization — look for `EnumerableXxx` nodes that should be `DuckDBXxx`
2. Check `DuckDBConvention.register()` — is the pushdown rule registered?
3. Verify the table's convention: `table.getConvention()` should return `DuckDBConvention`

### File adapter filter not pruning partitions:
1. Check `SimpleFileFilterPushdownRule` matched — enable planner tracing
2. Verify table implements `StatisticsProvider` and returns valid min/max statistics
3. Dump the filter's `RexNode`: `System.out.println(filter.getCondition())`

### Iceberg table scan reading too many files:
1. Check partition pruning in `IcebergTable` — add logging to `planFiles()`
2. Verify time-travel snapshot resolution
3. Check `getStatistic()` returns correct row counts

## 6. System Properties for Debug Control

```bash
# Enable comprehensive debug mode
-Dcalcite.debug=true

# Control Volcano dumps (both default true when logger is TRACE)
-Dcalcite.volcano.dump.graphviz=true
-Dcalcite.volcano.dump.sets=true

# Top-down optimization tracing
-Dcalcite.planner.topdown.opt=true

# Metadata cache sizing (reduce for debugging, increase for production)
-Dcalcite.metadata.handler.cache.maximum.size=1000
```

## 7. Quick Debug Checklist

When a query produces wrong results or fails:

1. [ ] Dump plan before and after optimization
2. [ ] Enable `RelOptPlanner` logger at DEBUG
3. [ ] Check rule attempt summary (are expected rules firing?)
4. [ ] Verify conventions match (no Convention.NONE in final plan)
5. [ ] Check type coercions in plan (look for CAST nodes)
6. [ ] For adapter issues: verify `Convention.register()` adds your rules
7. [ ] For cost issues: print `RelMetadataQuery.getCumulativeCost()` on key nodes
