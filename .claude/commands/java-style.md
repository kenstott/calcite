---
description: Java coding conventions for this project — naming, packages, exceptions, annotations, nullness
---

# Java Style Conventions

Apply these conventions when working on $ARGUMENTS.

## Naming

- **Packages**: `org.apache.calcite.adapter.<module>` for adapters, `org.apache.calcite.<component>` for core
- **Classes**: PascalCase. Rules end with `Rule`, tables with `Table`, schemas with `Schema`
- **Constants**: `UPPER_SNAKE_CASE`, prefer `static final` fields
- **Logger fields**: `private static final Logger LOGGER = LoggerFactory.getLogger(ClassName.class);`
- **Test classes**: Same package as source, suffix with `Test` (not prefix)

## Package Structure (Adapters)

```
org.apache.calcite.adapter.<name>/
  <Name>SchemaFactory.java      # Entry point, implements SchemaFactory
  <Name>Schema.java             # extends AbstractSchema
  <Name>Table.java              # implements TranslatableTable/ScannableTable
  <Name>Convention.java          # if custom convention needed
  rules/                         # Planner rules for this adapter
  metadata/                      # Information schema, pg_catalog
```

## Exception Handling

- **Unchecked preferred**: `CalciteException extends RuntimeException` is the project standard
- **Checked exceptions**: Only at API boundaries (`SqlParseException`, `ValidationException`)
- **Wrapping**: Wrap checked exceptions in unchecked at adapter boundaries
- **Context**: Use `CalciteContextException` when position info (line/col) is available
- **Never**: Empty catch blocks. Log or rethrow.

## Null Safety

```java
// REQUIRED: Use Checker Framework annotations
import org.checkerframework.checker.nullness.qual.Nullable;

// Parameter validation
import static java.util.Objects.requireNonNull;

public Foo(Bar bar) {
  this.bar = requireNonNull(bar, "bar");  // Not Guava checkNotNull
}

// Nullable return types
public @Nullable String findName(int id) { ... }

// Suppress false positives
@SuppressWarnings("nullness")
```

## Immutability Pattern

```java
// This project uses @Value.Immutable (Immutables library 2.8.8)
import org.apache.calcite.CalciteImmutable;
import org.immutables.value.Value;

@Value.Immutable
@CalciteImmutable
public interface MyConfig {
  String name();
  @Value.Default default int limit() { return 100; }
}
// Generates ImmutableMyConfig with builder
```

## Java 8 Constraints — ZERO TOLERANCE

```java
// PROHIBITED (Java 10+)
var x = getSomething();

// PROHIBITED (Java 14+)
record Point(int x, int y) {}

// PROHIBITED (Java 13+)
String s = """
    text block
    """;

// PROHIBITED (Java 14+)
if (obj instanceof String s) { ... }

// USE INSTEAD
final String x = getSomething();
// Regular class with fields for records
// String concatenation or String.format for text blocks
// Explicit cast after instanceof check
```

## Formatting

- Autostyle plugin enforces formatting — run `./gradlew autostyleApply` if needed
- License headers required on all source files
- Checkstyle rules enforced via `./gradlew checkstyleAll`
