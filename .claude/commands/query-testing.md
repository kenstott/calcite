---
description: How to write integration tests that run SQL through the full Calcite stack and assert on plans
---

# Query Testing Guide

Reference when writing or debugging query tests for $ARGUMENTS.

## Full-Stack SQL Test

```java
@Tag("integration")
@Test void testQueryEndToEnd() {
  Properties props = new Properties();
  props.put("model", modelPath);  // JSON model file

  try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props);
       Statement stmt = conn.createStatement()) {
    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM myschema.mytable");
    assertThat(rs.next(), is(true));
    assertThat(rs.getInt("cnt"), greaterThan(0));
  }
}
```

## Assert on Relational Algebra (Plan Testing)

### Using RelOptFixture (preferred)

```java
@Test void testFilterPushdown() {
  RelOptFixture fixture = Fixtures.forRules()
      .withPlanner(planner -> {
        planner.addRule(SimpleFileFilterPushdownRule.Config.DEFAULT.toRule());
      });

  // DiffRepository compares plan output against expected in .xml file
  fixture.sql("SELECT * FROM t WHERE x > 5")
      .check();
}
```

### Direct plan comparison

```java
@Test void testPlanShape() {
  // Parse and optimize
  RelNode optimized = optimize(sql);
  String plan = RelOptUtil.toString(optimized);

  // Assert on plan structure
  assertThat(plan, containsString("EnumerableFilter"));
  assertThat(plan, not(containsString("LogicalFilter")));  // Should be converted
  assertThat(plan, containsString("condition=[$0 > 5]"));
}
```

### Using DiffRepository (regression testing)

```java
// Test class
class MyPlanTest {
  final DiffRepository diffRepos = DiffRepository.lookup(MyPlanTest.class);

  @Test void testJoinOrder() {
    String sql = diffRepos.expand("sql", "${sql}");
    RelNode plan = optimize(sql);
    diffRepos.assertEquals("plan", "${plan}",
        RelOptUtil.toString(plan));
  }
}

// Companion file: src/test/resources/org/apache/.../MyPlanTest.xml
// Contains expected SQL and plan output
```

## Testing with HepPlanner (Deterministic)

```java
@Test void testRuleProducesExpectedPlan() {
  HepProgramBuilder builder = new HepProgramBuilder();
  builder.addRuleInstance(MyRule.Config.DEFAULT.toRule());
  HepPlanner planner = new HepPlanner(builder.build());

  planner.setRoot(logicalPlan);
  RelNode result = planner.findBestExpr();

  // Deterministic — same input always gives same output
  String plan = RelOptUtil.toString(result);
  assertThat(plan, containsString("expected pattern"));
}
```

## Testing SQL Validation

```java
@Test void testValidationRejectsAmbiguousColumn() {
  Fixtures.forValidator()
      .sql("SELECT ^x^ FROM t1, t2")  // ^ marks expected error position
      .fails("Column 'x' is ambiguous");
}

@Test void testValidationAccepts() {
  Fixtures.forValidator()
      .sql("SELECT x FROM t1")
      .ok();
}
```

## Testing SQL Parsing

```java
@Test void testParseWindowFunction() {
  Fixtures.forParser()
      .sql("SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY sal) FROM emp")
      .ok("expected parse tree output");
}
```

## Environment Setup for Integration Tests

```bash
# Source credentials before running
cd /root/calcite/govdata && source .env.sh

# Run specific test
./gradlew :govdata:test -PincludeTags=integration --tests "*MyQueryTest*"

# With debug output
./gradlew :govdata:test -PincludeTags=integration --tests "*MyQueryTest*" --console=plain
```

## Common Test Mistakes

1. **Missing `-PincludeTags=integration`** → 0 tests executed
2. **Not sourcing `.env.sh`** → API key / S3 credential failures
3. **Asserting on plan string too tightly** → Plans change with rule additions. Assert on structure, not exact string.
4. **Using VolcanoPlanner for deterministic tests** → Use HepPlanner when you need reproducible plan output
