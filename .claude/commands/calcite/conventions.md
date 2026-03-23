---
description: Conventions and trait propagation in this project — EnumerableConvention, DuckDB, JDBC, and custom conventions
---

# Calcite Conventions Guide

Reference when working on convention/trait code for $ARGUMENTS.

## What is a Convention?

A convention defines how a RelNode is executed. The planner converts nodes between conventions using `ConverterRule`.

```
Convention.NONE (logical)
  → EnumerableConvention (Linq4j in-memory execution)
  → JdbcConvention (push to JDBC database)
  → DuckDBConvention (push to DuckDB)
  → Custom convention (your adapter)
```

## Conventions in This Project

### EnumerableConvention (Default execution)
- Location: `core/src/.../adapter/enumerable/EnumerableConvention.java`
- Singleton: `EnumerableConvention.INSTANCE`
- Execution: Generates Java code via Linq4j, runs in-process
- Every adapter eventually converts to Enumerable for final execution

### JdbcConvention (JDBC pushdown)
- Location: `core/src/.../adapter/jdbc/JdbcConvention.java`
- Instance per JDBC connection (not singleton)
- Execution: Generates SQL, pushes to remote database

### DuckDBConvention (This project's custom convention)
- Location: `file/src/.../duckdb/DuckDBConvention.java`
- Extends `JdbcConvention`
- Registers aggressive pushdown rules (HLL, COUNT(*), COUNT DISTINCT)
- Used when file adapter engine is DuckDB

## Trait System

Conventions are one type of `RelTrait`. The full trait set:

```java
RelTraitSet traits = relNode.getTraitSet();

// Convention (execution method)
Convention conv = traits.getTrait(ConventionTraitDef.INSTANCE);

// Collation (sort order)
RelCollation coll = traits.getTrait(RelCollationTraitDef.INSTANCE);

// Distribution (partitioning)
RelDistribution dist = traits.getTrait(RelDistributionTraitDef.INSTANCE);
```

## Convention Lifecycle

### 1. Register rules in Convention

```java
// DuckDBConvention.java pattern
@Override public void register(RelOptPlanner planner) {
  // Register converter (Logical → DuckDB)
  // Implicitly done by JdbcConvention parent

  // Register pushdown rules
  planner.addRule(DuckDBHLLCountDistinctRule.INSTANCE);
  planner.addRule(DuckDBIcebergCountStarRule.INSTANCE);

  // Register enumerable rules (for nodes that can't push down)
  for (RelOptRule rule : EnumerableRules.rules()) {
    planner.addRule(rule);
  }
}
```

### 2. Table advertises convention

```java
// TranslatableTable.toRel() should return a node with the right convention
@Override public RelNode toRel(ToRelContext context, RelOptTable table) {
  return new MyTableScan(
      context.getCluster(),
      context.getCluster().traitSetOf(myConvention),  // Convention here
      table, this);
}
```

### 3. ConverterRule converts between conventions

```java
// Convention.NONE → EnumerableConvention
public class MyToEnumerableConverterRule extends ConverterRule {
  Config DEFAULT = Config.INSTANCE
      .withConversion(LogicalTableScan.class, Convention.NONE,
          EnumerableConvention.INSTANCE, "MyToEnumerable")
      .withRuleFactory(MyToEnumerableConverterRule::new);
}
```

## Trait Propagation

When a rule produces a new node, it must handle traits correctly:

```java
@Override public void onMatch(RelOptRuleCall call) {
  RelNode input = call.rel(0);

  // CORRECT: Preserve the input's trait set
  RelNode newNode = new LogicalFilter(
      input.getCluster(),
      input.getTraitSet(),     // Same traits as input
      input, condition);

  // WRONG: Hardcoding traits
  RelNode bad = new LogicalFilter(
      input.getCluster(),
      cluster.traitSetOf(Convention.NONE),  // May lose collation/distribution
      input, condition);

  call.transformTo(newNode);
}
```

## Convention.NONE → Infinite Cost

```java
// VolcanoPlanner marks Convention.NONE nodes as infinite cost by default
// This forces conversion to a physical convention (Enumerable, JDBC, etc.)
planner.setNoneConventionHasInfiniteCost(true);  // Default

// A final plan with Convention.NONE nodes means a required ConverterRule is missing
```

## Debugging Convention Issues

```java
// Check a node's convention
System.out.println("Convention: " + relNode.getConvention());
System.out.println("TraitSet: " + relNode.getTraitSet());

// If final plan has NONE nodes: missing converter rule
// If final plan has wrong convention: rule produces wrong traits
// If cost is infinite: no physical implementation for that convention
```

## Common Mistakes

1. **Not registering EnumerableRules**: Your convention must register enumerable rules as fallback, or non-pushable operations fail.
2. **ConverterRule wrong direction**: Convention conversion is one-way. NONE→Enumerable, not Enumerable→NONE.
3. **Losing traits in rule output**: Always copy `traitSet` from matched node, don't construct fresh.
4. **Singleton vs instance**: EnumerableConvention is singleton; JdbcConvention is per-connection. DuckDB extends JDBC, so it's per-connection too.
