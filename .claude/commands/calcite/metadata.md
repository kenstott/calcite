---
description: RelMetadataProvider implementation, row count estimation, cost model, and metadata caching in this project
---

# Calcite Metadata System Guide

Reference when working on metadata/statistics for $ARGUMENTS.

## Metadata Architecture

```
SQL Query → RelNode tree
  → RelMetadataQuery (facade)
    → RelMetadataProvider (dispatch)
      → JaninoRelMetadataProvider (compiled cache)
        → Handler methods (actual computation)
```

## RelMetadataQuery — The Facade

```java
// Get metadata for a RelNode
RelMetadataQuery mq = relNode.getCluster().getMetadataQuery();

// Row count estimation
Double rowCount = mq.getRowCount(relNode);

// Cumulative cost (for planner decisions)
RelOptCost cost = mq.getCumulativeCost(relNode);

// Column uniqueness
Boolean unique = mq.areColumnsUnique(relNode, columnSet);

// Distinct row count
Double distinct = mq.getDistinctRowCount(relNode, columnSet, predicate);

// Selectivity of a predicate
Double sel = mq.getSelectivity(relNode, predicate);

// Distribution (parallelism info)
RelDistribution dist = mq.distribution(relNode);

// Memory usage estimate
Double memory = mq.memory(relNode);
```

## Custom Metadata Provider

```java
public class MyMetadataHandler
    implements MetadataHandler<BuiltInMetadata.RowCount> {

  // Must implement the handler interface method
  public Double getRowCount(RelNode rel, RelMetadataQuery mq) {
    if (rel instanceof MyTableScan) {
      MyTable table = ((MyTableScan) rel).getTable();
      return table.getStatistics().getRowCount();
    }
    return null;  // Fallback to default estimation
  }

  @Override public MetadataDef<BuiltInMetadata.RowCount> getDef() {
    return BuiltInMetadata.RowCount.DEF;
  }
}
```

## Statistics Provider Pattern (File Adapter)

From `file/src/.../statistics/StatisticsProvider.java` and `ParquetTranslatableTable.java`:

```java
// Tables can provide statistics for optimization
public interface StatisticsProvider {
  TableStatistics getTableStatistics();
}

// Used by rules like SimpleFileFilterPushdownRule:
// 1. Get column min/max from statistics
// 2. Compare against filter predicate
// 3. If filter eliminates all rows based on stats, replace with empty
```

## JaninoRelMetadataProvider — The Cache

All metadata handlers are compiled by Janino into generated classes and cached:

```java
// Cache configuration via system properties
-Dcalcite.metadata.handler.cache.maximum.size=1000  // Max cache entries

// The cache key is (handlerClass, provider), so different providers
// create different cache entries
```

**Debugging metadata cache issues:**
```java
// Clear metadata cache (useful in tests)
relNode.getCluster().invalidateMetadataQuery();

// Or create a fresh query
RelMetadataQuery mq = RelMetadataQuery.instance();
```

## Cost Model

```java
// RelOptCost has three components: (rows, cpu, io)
RelOptCost cost = mq.getCumulativeCost(relNode);
double rows = cost.getRows();
double cpu = cost.getCpu();
double io = cost.getIo();

// Infinite cost = no valid implementation
boolean isInfinite = cost.isInfinite();

// Cost comparison (used by VolcanoPlanner)
boolean cheaper = cost1.isLt(cost2);
```

## Registering Custom Metadata

```java
// In your planner setup
RelMetadataProvider provider = ChainedRelMetadataProvider.of(
    ImmutableList.of(
        myCustomProvider,
        DefaultRelMetadataProvider.INSTANCE  // Fallback
    ));
relNode.getCluster().setMetadataProvider(
    JaninoRelMetadataProvider.of(provider));
```

## System Properties

| Property | Default | Purpose |
|----------|---------|---------|
| `calcite.metadata.handler.cache.maximum.size` | 1000 | Max compiled handler cache entries |
| `calcite.bindable.cache.maxSize` | 0 | Bindable class cache (0=disabled) |
| `calcite.function.cache.maxSize` | 1000 | Function-level cache (RLIKE, etc.) |

## Common Mistakes

1. **Returning 0.0 for row count**: Use `null` to fall through to default estimation. Returning 0 makes the planner think the table is empty.
2. **Not invalidating metadata after plan changes**: Call `cluster.invalidateMetadataQuery()` if you modify the plan outside normal rule application.
3. **Metadata handler for wrong interface**: Ensure your handler's `getDef()` matches the metadata type you're providing.
