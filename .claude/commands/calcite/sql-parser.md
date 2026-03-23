---
description: SQL parser extension patterns, SqlNode visitors, custom DDL/functions in this project
---

# Calcite SQL Parser Guide

Reference when working on SQL parsing for $ARGUMENTS.

## Parser Architecture

```
SQL String
  → SqlParser (configurable via SqlParser.Config)
    → SqlNode tree (AST)
      → SqlValidator (type resolution, scope)
        → SqlToRelConverter
          → RelNode tree (relational algebra)
```

## SqlNode Hierarchy

```
SqlNode (abstract)
├── SqlCall           — Operator invocation (SELECT, WHERE, function calls)
│   ├── SqlSelect     — SELECT statement
│   ├── SqlInsert     — INSERT statement
│   ├── SqlUpdate     — UPDATE
│   ├── SqlDelete     — DELETE
│   ├── SqlJoin       — JOIN clause
│   ├── SqlOrderBy    — ORDER BY
│   └── SqlBasicCall  — Generic function/operator call
├── SqlIdentifier     — Table/column name
├── SqlLiteral        — Constant value
├── SqlNodeList       — List of SqlNodes
├── SqlDataTypeSpec   — Type specification
└── SqlDynamicParam   — ? parameter
```

## SqlNode Visitors

```java
// Read-only visitor (returns a value)
public class MyVisitor extends SqlBasicVisitor<Void> {
  @Override public Void visit(SqlCall call) {
    // Process call
    return call.getOperator().acceptCall(this, call);
  }
  @Override public Void visit(SqlIdentifier id) {
    // Process identifier
    return null;
  }
  @Override public Void visit(SqlLiteral literal) {
    // Process literal
    return null;
  }
}

// Transforming visitor (returns modified SqlNode)
public class MyShuttle extends SqlShuttle {
  @Override public SqlNode visit(SqlCall call) {
    // Transform and return new SqlNode
    return super.visit(call);  // Recurse into children
  }
}
```

## Custom DDL (This Project)

SharePoint adapter implements custom DDL execution:
`sharepoint-list/src/.../SharePointDdlExecutor.java`

```java
public class SharePointDdlExecutor extends DdlExecutorImpl {
  @Override public void execute(SqlCreateTable create, CalcitePrepare.Context context) {
    // Extract column defs from SqlCreateTable
    // Map SqlTypeName → SharePoint field types
    // Create SharePoint list via REST API
  }

  @Override public void execute(SqlDropTable drop, CalcitePrepare.Context context) {
    // Drop SharePoint list
  }
}
```

## Parser Configuration

```java
// Standard config for this project (from CLAUDE-CORE.md)
SqlParser.Config parserConfig = SqlParser.config()
    .withLex(Lex.ORACLE)                          // Oracle-style lexing
    .withUnquotedCasing(Casing.TO_LOWER)          // Unquoted identifiers → lowercase
    .withCaseSensitive(false);                    // Case-insensitive

// For extended SQL (babel module):
SqlParser.Config babelConfig = SqlParser.config()
    .withParserFactory(SqlBabelParserImpl.FACTORY);
```

## SQL Conventions (This Project)

From CLAUDE-CORE.md:
- **Default**: `lex=ORACLE, unquotedCasing=TO_LOWER, nameGeneration=SMART_CASING`
- **Quote when**: Reserved words as identifiers, mixed/upper case identifiers
- **Don't quote**: Lowercase identifiers

## Testing SQL Parsing

```java
// Using SqlParserFixture
Fixtures.forParser()
    .sql("SELECT * FROM t WHERE x > 5")
    .check();  // Validates parse tree against expected

// Direct parsing
SqlParser parser = SqlParser.create(sql, parserConfig);
SqlNode sqlNode = parser.parseStmt();
System.out.println(sqlNode.toSqlString(SqlDialect.DatabaseProduct.CALCITE.getDialect()));
```

## No Custom Parser Factory in Adapters

This project's adapters use the standard Calcite parser (or Babel for extended SQL). Custom DDL is handled via `DdlExecutorImpl`, not parser extensions. If you need custom syntax, prefer the Babel module over a custom parser factory.
