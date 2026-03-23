---
description: SLF4J/Logback conventions, CalciteTrace component loggers, log levels, and structured logging for this project
---

# Java Logging Conventions

Apply these logging patterns when working on $ARGUMENTS.

## Logger Setup

```java
// Standard pattern (every class that logs)
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

private static final Logger LOGGER = LoggerFactory.getLogger(MyClass.class);
```

## CalciteTrace Component Loggers

Defined in `core/src/.../util/trace/CalciteTrace.java`. Use these for component-specific tracing:

| Method | Logger Name | Use For |
|--------|------------|---------|
| `getPlannerTracer()` | `RelOptPlanner` | Rule firing, optimization |
| `getPlannerTaskTracer()` | `o.a.c.plan.volcano.task` | Volcano task scheduling |
| `getStatementTracer()` | `Prepare` | Generated program output |
| `getParserTracer()` | `o.a.c.sql.parser` | Parse events |
| `getSqlToRelTracer()` | `o.a.c.sql2rel` | SQL-to-rel conversion |
| `getSqlTimingTracer()` | `o.a.c.sql.timing` | Processing stage timing |
| `getRuleAttemptsTracer()` | `AbstractRelOptPlanner.rule_execution_summary` | Rule attempt counts |

## Log Levels — What Goes Where

| Level | When to Use | Example |
|-------|------------|---------|
| `ERROR` | Unrecoverable failures, data loss risk | `LOGGER.error("Failed to write Iceberg commit", e)` |
| `WARN` | Recoverable but unexpected | `LOGGER.warn("Retrying S3 upload after timeout")` |
| `INFO` | Operational milestones | `LOGGER.info("Processing batch {} (partition {}/{})", batch, part, total)` |
| `DEBUG` | Developer diagnostics | `LOGGER.debug("Entering {} with params: {}", method, param)` |
| `TRACE` | Verbose internals | `LOGGER.trace("Subset cost changed: {} was {} now {}", id, old, new_)` |

## Performance Guards

```java
// REQUIRED: Guard expensive string operations
if (LOGGER.isDebugEnabled()) {
  LOGGER.debug("Plan:\n{}", RelOptUtil.toString(relNode));
}

// SLF4J {} placeholders are lazy — no guard needed for simple args
LOGGER.debug("Row count: {}", rowCount);  // OK without guard

// Guard needed when arg computation is expensive
if (LOGGER.isDebugEnabled()) {
  LOGGER.debug("Schema: {}", schema.getTableMap().keySet());  // getTableMap() may be expensive
}
```

## Govdata Adapter Logging (Mandatory Pattern)

All govdata methods MUST have four debug statements (see CLAUDE-ADAPTERS.md):

```java
private static final Logger GOVDATA_TRACER =
    LoggerFactory.getLogger("org.apache.calcite.adapter.govdata." + MyClass.class.getSimpleName());

public Result myMethod(Input input) {
  // 1. Entry
  if (GOVDATA_TRACER.isDebugEnabled()) {
    GOVDATA_TRACER.debug("Entering {} with params: {}", "myMethod", input);
  }
  try {
    Result result = compute();
    // 2. Success exit
    if (GOVDATA_TRACER.isDebugEnabled()) {
      GOVDATA_TRACER.debug("Successfully completed {}, returning: {}", "myMethod", result);
    }
    return result;
  } catch (Exception e) {
    // 3. Error
    GOVDATA_TRACER.debug("Error in {}: {}", "myMethod", e.getMessage(), e);
    throw e;
  }
  // 4. Fail exit (for validation failures)
}
```

## Logback Configuration

Test logging: `src/test/resources/logback-test.xml`

```xml
<!-- Enable planner tracing for debugging -->
<logger name="org.apache.calcite.plan.RelOptPlanner" level="DEBUG"/>

<!-- Enable rule attempt summary -->
<logger name="org.apache.calcite.plan.AbstractRelOptPlanner.rule_execution_summary" level="DEBUG"/>
```

## Anti-Patterns

```java
// NEVER: System.out/System.err in committed code
System.out.println("DEBUG: " + value);  // Remove before commit

// NEVER: String concatenation in log calls
LOGGER.debug("Value is " + expensiveToString());  // Use {} instead

// NEVER: Catching and not logging
catch (Exception e) { /* swallowed */ }  // At minimum log at debug

// NEVER: Logging and rethrowing without context
catch (Exception e) {
  LOGGER.error("Error", e);
  throw e;  // Prefer: throw new CalciteException("context", e);
}
```
