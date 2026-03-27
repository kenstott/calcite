---
description: RexNode construction, RexBuilder patterns, simplification, and common RexCall vs RexInputRef mistakes
model: haiku
effort: low
---

# Calcite RexNode Guide

Reference when working with row expressions for $ARGUMENTS.

## RexNode Hierarchy

```
RexNode (abstract)
├── RexInputRef      — Column reference by index (e.g., $3)
├── RexLiteral       — Constant value (e.g., 42, 'hello', NULL)
├── RexCall          — Function/operator call (e.g., >(x, 5), CAST(x AS INT))
├── RexFieldAccess   — Field access on struct (e.g., $0.name)
├── RexCorrelVariable — Correlated variable reference
├── RexSubQuery      — Subquery (IN, EXISTS, scalar)
├── RexOver          — Window function
└── RexDynamicParam  — Prepared statement parameter (?)
```

## RexBuilder — Constructing Expressions

```java
RexBuilder rexBuilder = cluster.getRexBuilder();
// Or from RelBuilder: builder.getRexBuilder()

// Literal values
RexNode lit42 = rexBuilder.makeLiteral(42, intType, false);
RexNode litStr = rexBuilder.makeLiteral("hello");
RexNode litNull = rexBuilder.makeNullLiteral(intType);
RexNode litTrue = rexBuilder.makeLiteral(true);

// Column references
RexNode col0 = rexBuilder.makeInputRef(intType, 0);  // $0

// Function calls
RexNode gt = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, col0, lit42);
RexNode and = rexBuilder.makeCall(SqlStdOperatorTable.AND, cond1, cond2);
RexNode cast = rexBuilder.makeCast(targetType, expr);

// Via RelBuilder (simpler)
builder.equals(builder.field("x"), builder.literal(5))
builder.and(cond1, cond2)
builder.or(cond1, cond2)
builder.not(cond)
builder.isNull(builder.field("x"))
builder.call(SqlStdOperatorTable.LIKE, builder.field("name"), builder.literal("%foo%"))
```

## Analyzing RexNodes — Type Checking is Critical

```java
// WRONG — ClassCastException if not RexInputRef
RexInputRef ref = (RexInputRef) node;

// CORRECT — Always check type first
if (node instanceof RexInputRef) {
  RexInputRef ref = (RexInputRef) node;
  int index = ref.getIndex();
} else if (node instanceof RexCall) {
  RexCall call = (RexCall) node;
  SqlOperator op = call.getOperator();
  List<RexNode> operands = call.getOperands();
} else if (node instanceof RexLiteral) {
  RexLiteral lit = (RexLiteral) node;
  Comparable value = RexLiteral.value(lit);
}
```

This is a **critical pattern** in this project — see `splunk/src/.../SplunkPushDownRule.java` for the canonical example of safe RexNode type checking.

## RexNode in Filter Pushdown Rules

```java
// Common pattern: walk a filter condition to extract pushable parts
@Override public void onMatch(RelOptRuleCall call) {
  Filter filter = call.rel(0);
  RexNode condition = filter.getCondition();

  // Decompose AND conditions
  List<RexNode> conjunctions = RelOptUtil.conjunctions(condition);

  for (RexNode conj : conjunctions) {
    if (conj instanceof RexCall) {
      RexCall rexCall = (RexCall) conj;
      SqlKind kind = rexCall.getKind();

      switch (kind) {
        case EQUALS:
        case GREATER_THAN:
        case LESS_THAN:
          // Check if one side is column ref, other is literal
          RexNode left = rexCall.getOperands().get(0);
          RexNode right = rexCall.getOperands().get(1);
          if (left instanceof RexInputRef && right instanceof RexLiteral) {
            // Pushable predicate
          }
          break;
        case AND:
          // Recurse
          break;
        default:
          // Not pushable — leave for Calcite to handle
      }
    }
  }
}
```

## RexNode Simplification

```java
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;

// Simplify a condition
RexSimplify simplify = new RexSimplify(rexBuilder,
    RelOptPredicateList.EMPTY, RexUtil.EXECUTOR);
RexNode simplified = simplify.simplify(condition);

// Check if always true/false
if (condition.isAlwaysTrue()) { ... }
if (condition.isAlwaysFalse()) { ... }

// Decompose
List<RexNode> ands = RelOptUtil.conjunctions(condition);  // AND parts
List<RexNode> ors = RelOptUtil.disjunctions(condition);   // OR parts
```

## Adjusting RexNodes After Project Rewrite

When you push a filter below a project, column indices change:
```java
// Map filter condition through project
RexNode adjusted = RelOptUtil.pushPastProject(
    filter.getCondition(), project);
// Or manually with RexPermuteInputsShuttle
```

## Common Mistakes

1. **Assuming RexInputRef**: Not all projections are column refs — they can be RexCall (computed columns), RexLiteral, etc.
2. **Wrong column index after join**: In a join, right-side columns are offset by left-side field count.
3. **Ignoring nullability in comparisons**: `x = 5` is false when x is NULL, but `x IS NULL` is true.
4. **Using `==` instead of `equals()`**: RexNode objects should be compared with `.equals()` or `.toString()`.
5. **Forgetting CAST nodes**: Calcite inserts implicit CASTs — your condition walker must handle them.