---
description: How to write and register planner rules in this project — RelRule patterns, operand matching, Volcano/HepPlanner, avoiding infinite loops
model: sonnet
effort: medium
---

# Calcite Planner Rules Guide

Use this guide when working on planner rules related to $ARGUMENTS.

## 1. Rule Class Hierarchy — Which Base Class to Use

| Base Class | When to Use | Example in This Project |
|------------|-------------|------------------------|
| `RelRule<Config>` | **Default choice.** Modern config-driven rules. | `SimpleFileFilterPushdownRule`, `HLLCountDistinctRule` |
| `ConverterRule` | Converting between conventions (e.g., Logical → DuckDB) | `DuckDBToEnumerableConverterRule` (implicit in DuckDB adapter) |
| `TransformationRule` | Marker interface. Logical-only rewrites that must NOT match physical nodes. | `SubstitutionRule` (extends this) |
| `SubstitutionRule` | The new node is always better — planner can auto-prune the old. | Rarely used directly in this project |

**Never extend `RelOptRule` directly for new rules.** Use `RelRule<Config>` — it's the modern framework and what this project uses.

## 2. Writing a New Rule (Step by Step)

### Step 1: Define the Config interface

```java
@Value.Enclosing  // Required for Immutables code generation
public class MyNewRule extends RelRule<MyNewRule.Config> {

  protected MyNewRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    // Step 4
  }

  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableMyNewRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(LogicalFilter.class)        // Match a Filter...
                .oneInput(b1 ->                     // ...with one input that is...
                    b1.operand(LogicalProject.class) // ...a Project...
                        .anyInputs()));              // ...with any inputs

    @Override default MyNewRule toRule() {
      return new MyNewRule(this);
    }
  }
}
```

### Step 2: Choose operand matching pattern

```java
// Match single node (any inputs)
b -> b.operand(LogicalFilter.class).anyInputs()

// Match parent-child pair
b0 -> b0.operand(LogicalFilter.class)
    .oneInput(b1 -> b1.operand(LogicalProject.class).anyInputs())

// Match with predicate (filter which nodes match)
b -> b.operand(LogicalFilter.class)
    .predicate(f -> !RexUtil.containsCorrelation(f.getCondition()))
    .anyInputs()

// Match with specific convention
b -> b.operand(LogicalFilter.class)
    .trait(Convention.NONE)   // Only match logical nodes
    .anyInputs()

// Match leaf node (no inputs)
b -> b.operand(LogicalValues.class).none()

// Match three levels deep
b0 -> b0.operand(Aggregate.class)
    .oneInput(b1 -> b1.operand(Project.class)
        .oneInput(b2 -> b2.operand(TableScan.class).anyInputs()))
```

### Step 3: Access matched nodes in onMatch

```java
@Override public void onMatch(RelOptRuleCall call) {
  // Nodes are indexed in operand order (depth-first)
  final LogicalFilter filter = call.rel(0);    // First operand
  final LogicalProject project = call.rel(1);  // Second operand (child)

  // Access metadata
  final RelMetadataQuery mq = call.getMetadataQuery();
  final Double rowCount = mq.getRowCount(filter);
}
```

### Step 4: Produce the transformed node

```java
@Override public void onMatch(RelOptRuleCall call) {
  final LogicalFilter filter = call.rel(0);
  final LogicalProject project = call.rel(1);

  // Option A: Build new node directly
  final RelNode newNode = filter.copy(
      filter.getTraitSet(),
      ImmutableList.of(newInput));

  // Option B: Use RelBuilder (preferred for complex transforms)
  final RelBuilder builder = call.builder();
  builder.push(project.getInput())
      .filter(adjustedCondition)
      .project(project.getProjects(), project.getRowType().getFieldNames());
  final RelNode newNode = builder.build();

  // Register the transformation
  call.transformTo(newNode);
}
```

**CRITICAL**: Always call `call.transformTo()`. If you `return` without calling it, the rule matched but did nothing — this is a silent bug.

## 3. Registering Rules

### Convention-based registration (adapter rules):

This is how this project's DuckDB and file adapter rules are registered.

```java
// In your Convention class (see file/.../duckdb/DuckDBConvention.java)
@Override public void register(RelOptPlanner planner) {
  // Register your custom rules
  planner.addRule(MyNewRule.Config.DEFAULT.toRule());

  // Also register required enumerable rules
  for (RelOptRule rule : EnumerableRules.rules()) {
    planner.addRule(rule);
  }

  // Add specific rules
  planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
}
```

### CoreRules constant (for rules in core):

```java
// In CoreRules.java (core module only)
public static final MyNewRule MY_NEW_RULE = MyNewRule.Config.DEFAULT.toRule();
```

### Program-based registration (for HepPlanner sequences):

```java
HepProgramBuilder builder = new HepProgramBuilder();
builder.addRuleInstance(MyNewRule.Config.DEFAULT.toRule());
builder.addRuleInstance(CoreRules.FILTER_MERGE);
HepPlanner planner = new HepPlanner(builder.build());
```

## 4. Avoiding Infinite Rule Loops

This is the most common pitfall. A rule that produces output matching its own input pattern will fire forever.

### Pattern 1: Guard with a predicate

```java
// BAD: This filter-push rule will re-match its own output
b -> b.operand(LogicalFilter.class)
    .oneInput(b1 -> b1.operand(TableScan.class).anyInputs())

// GOOD: Only match filters that haven't been pushed yet
b -> b.operand(LogicalFilter.class)
    .predicate(f -> !isPushed(f))  // Custom check
    .oneInput(b1 -> b1.operand(TableScan.class).anyInputs())
```

### Pattern 2: Change the node type or convention

```java
// Converting LogicalFilter → EnumerableFilter won't re-match
// because the operand specifies LogicalFilter.class
@Override public void onMatch(RelOptRuleCall call) {
  final LogicalFilter filter = call.rel(0);
  call.transformTo(
      EnumerableFilter.create(filter.getInput(), filter.getCondition()));
}
```

### Pattern 3: SubstitutionRule with auto-prune

```java
// Mark as SubstitutionRule — planner may prune old node
public class MyRule extends RelRule<Config> implements SubstitutionRule {
  @Override public boolean autoPruneOld() {
    return true;  // Remove old node after substitution
  }
}
```

### Pattern 4: HepPlanner match limit

```java
// For HepPlanner: limit iterations instead of running to fixpoint
HepProgramBuilder builder = new HepProgramBuilder();
builder.addMatchLimit(100);  // Stop after 100 rule applications
builder.addRuleInstance(riskyRule);
```

### Pattern 5: VolcanoPlanner digest dedup

VolcanoPlanner naturally prevents infinite loops through digest canonicalization — if a rule produces a node with the same digest as an existing node, it's silently ignored. But **beware**: if your rule changes something that doesn't affect the digest (like a trait), you can still loop.

### Diagnosing a loop:

```java
// Enable the rule attempt summary logger
// org.apache.calcite.plan.AbstractRelOptPlanner.rule_execution_summary → DEBUG
// After optimization, it prints:
//   MyRule: 5000 attempts, 2340000 µs elapsed  ← suspiciously high
```

## 5. Volcano vs HepPlanner — When to Use Which

| Aspect | VolcanoPlanner | HepPlanner |
|--------|---------------|------------|
| Strategy | Cost-based, explores alternatives | Heuristic, applies rules in sequence |
| Use when | Multiple valid plans, need cheapest | Fixed transformation order, preprocessing |
| Loop risk | Lower (digest dedup + cost pruning) | Higher (must guard manually) |
| Convention conversion | Built-in (ConverterRule) | Not supported |
| This project uses | Main optimization (adapter queries) | Pre-processing steps (sub-query expansion) |

### Standard two-phase pattern in this project:

```java
// Phase 1: HepPlanner for deterministic rewrites
HepProgramBuilder hep = new HepProgramBuilder();
hep.addRuleInstance(CoreRules.SUB_QUERY_REMOVE_RULE);
hep.addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE);
HepPlanner hepPlanner = new HepPlanner(hep.build());
hepPlanner.setRoot(logicalPlan);
RelNode preprocessed = hepPlanner.findBestExpr();

// Phase 2: VolcanoPlanner for cost-based optimization
VolcanoPlanner volcano = new VolcanoPlanner();
// Rules registered via Convention.register()
volcano.setRoot(preprocessed);
RelNode optimized = volcano.findBestExpr();
```

## 6. Project-Specific Rule Patterns

### File adapter pushdown rules (`file/src/.../rules/`):

These rules use `StatisticsProvider` to make data-driven decisions:
```java
// SimpleFileFilterPushdownRule pattern:
// 1. Match Filter on ParquetTranslatableTable scan
// 2. Get table statistics (min/max per column)
// 3. If filter condition eliminates all rows based on stats, replace with empty
// 4. Otherwise, push filter to table scan level
```

### DuckDB rules (`file/src/.../duckdb/`):

```java
// DuckDBHLLCountDistinctRule — rewrites COUNT(DISTINCT x) to HLL approximation
// DuckDBIcebergCountStarRule — rewrites COUNT(*) using Iceberg metadata
// DuckDBCountDistinctInterceptRule — intercepts count distinct for DuckDB-native execution
```

### Key files to study:
- `file/src/.../rules/SimpleFileFilterPushdownRule.java` — filter pushdown with statistics
- `file/src/.../rules/HLLCountDistinctRule.java` — aggregate rewrite
- `file/src/.../rules/CountStarStatisticsRule.java` — metadata-only COUNT(*)
- `file/src/.../duckdb/DuckDBConvention.java` — rule registration
- `splunk/src/.../SplunkPushDownRule.java` — multi-pattern rule (3 configs in one class)

## 7. Rule Testing Pattern

```java
// Use RelOptFixture from testkit
@Test void testMyRule() {
  RelOptFixture fixture = Fixtures.forRules()
      .withPlanner(planner -> {
        planner.addRule(MyNewRule.Config.DEFAULT.toRule());
      });

  fixture.sql("SELECT * FROM t WHERE x > 5")
      .check();  // Compares against DiffRepository expected output
}
```

Or test directly:
```java
@Test void testMyRuleDirectly() {
  HepProgramBuilder builder = new HepProgramBuilder();
  builder.addRuleInstance(MyNewRule.Config.DEFAULT.toRule());
  HepPlanner planner = new HepPlanner(builder.build());
  planner.setRoot(inputRel);
  RelNode result = planner.findBestExpr();

  String plan = RelOptUtil.toString(result);
  assertThat(plan, containsString("expected pattern"));
}
```

## 8. Common Mistakes Checklist

- [ ] **Forgot `@Value.Enclosing`** on rule class → Immutables won't generate Config
- [ ] **Wrong operand order** → `call.rel(0)` returns unexpected type, ClassCastException
- [ ] **Operand matches physical nodes** when you meant logical → add `.trait(Convention.NONE)` or implement `TransformationRule`
- [ ] **No `call.transformTo()`** on some code path → rule silently does nothing
- [ ] **Produced node has wrong convention** → VolcanoPlanner rejects it
- [ ] **Rule not registered** → check `Convention.register()` or explicit `planner.addRule()`
- [ ] **Using `RelOptRule` instead of `RelRule`** → won't compile with config pattern
- [ ] **Java 8 violation** → no `var`, no records, no text blocks in rule code
