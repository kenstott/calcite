---
description: How to construct relational algebra with RelBuilder — common patterns, trait sets, conventions, and pitfalls
---

# Calcite RelBuilder Guide

Use RelBuilder patterns when working on $ARGUMENTS.

## Getting a RelBuilder

```java
// In a planner rule's onMatch():
RelBuilder builder = call.builder();

// From a cluster:
RelBuilder builder = RelBuilder.proto(cluster).create();

// From Frameworks (for tests):
RelBuilder builder = RelBuilder.create(
    Frameworks.newConfigBuilder()
        .defaultSchema(schema)
        .build());
```

## Core Operations

```java
builder
  .scan("employees")                          // TableScan
  .filter(                                    // Filter
      builder.equals(
          builder.field("dept"),
          builder.literal("ENG")))
  .project(                                   // Project
      builder.field("name"),
      builder.field("salary"))
  .aggregate(                                 // Aggregate
      builder.groupKey("dept"),
      builder.count(false, "cnt"),
      builder.sum(false, "total_sal", builder.field("salary")))
  .sort(builder.desc(builder.field("cnt")))   // Sort
  .limit(0, 10)                               // Limit
  .build();                                   // Returns RelNode
```

## Join Patterns

```java
// Inner join
builder
  .scan("orders")
  .scan("customers")
  .join(JoinRelType.INNER,
      builder.equals(
          builder.field(2, 0, "customer_id"),  // (inputCount, inputOrdinal, fieldName)
          builder.field(2, 1, "id")));

// Left join
builder
  .scan("orders")
  .scan("customers")
  .join(JoinRelType.LEFT, condition);
```

## Field References — The #1 Pitfall

```java
// By name (safest)
builder.field("column_name")

// By index (fragile — breaks when projections change)
builder.field(0)

// Multi-input field reference: field(inputCount, inputOrdinal, fieldIndex)
builder.field(2, 0, "left_col")   // Left input's column
builder.field(2, 1, "right_col")  // Right input's column

// PITFALL: After a project(), field indices reset to the projected columns
builder.scan("t")
  .project(builder.field("b"), builder.field("a"))  // Now index 0=b, 1=a
  .filter(builder.equals(builder.field(0), ...))     // This is field "b", not "a"
```

## Trait Sets and Conventions

```java
// PITFALL: RelBuilder.build() creates nodes with Convention.NONE by default.
// If you need a specific convention:
RelNode node = builder.build();
RelNode converted = planner.changeTraits(node,
    node.getTraitSet().replace(EnumerableConvention.INSTANCE));

// In a rule, the output should typically have the same convention as the input:
@Override public void onMatch(RelOptRuleCall call) {
  RelNode input = call.rel(0);
  RelBuilder builder = call.builder();
  builder.push(input.getInput(0))
      .filter(condition)
      .build();
  // build() inherits traits from the stack
}
```

## Using RelBuilder in Rules

```java
@Override public void onMatch(RelOptRuleCall call) {
  final LogicalFilter filter = call.rel(0);
  final LogicalProject project = call.rel(1);
  final RelBuilder builder = call.builder();

  // Push the project's input onto the stack
  builder.push(project.getInput());

  // Apply transformed filter
  RexNode newCondition = adjustCondition(filter.getCondition(), project);
  builder.filter(newCondition);

  // Re-apply the project
  builder.project(project.getProjects(), project.getRowType().getFieldNames());

  call.transformTo(builder.build());
}
```

## Common Mistakes

1. **Forgetting to `push()` before building**: RelBuilder needs something on its stack
2. **Field index drift after project/join**: Always verify indices after transformations
3. **Building with wrong convention**: Use `call.builder()` in rules — it handles conventions
4. **Not preserving field names**: Pass field names to `project()` to maintain column aliases
5. **Mixing logical/physical nodes**: Don't push EnumerableRel into a builder making LogicalRel
