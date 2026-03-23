# Claude Code Custom Commands for Calcite

This directory contains custom commands for Claude Code to help with Calcite adapter development and testing.

## Available Commands

### Testing Commands

| Command | Description | Example Usage |
|---------|-------------|---------------|
| `/project:regression` | Run full regression tests for an adapter | `/project:regression file` |
| `/project:test:adapter` | Test specific adapter with options | `/project:test:adapter splunk IntegrationTest` |
| `/project:test:quick` | Quick test run for development | `/project:test:quick file` |
| `/project:test:all` | Run all adapter tests comprehensively | `/project:test:all` |

### Java Code Quality Skills

| Command | Description |
|---------|-------------|
| `/project:java-style` | Naming, packages, exceptions, annotations, null safety |
| `/project:java-testing` | JUnit 5, fixtures, tags, assertions |
| `/project:java-logging` | SLF4J/CalciteTrace conventions, log levels |
| `/project:java-build` | Gradle KTS, dependencies, modules, build profiles |

### Calcite Skills

| Command | Description |
|---------|-------------|
| `/project:calcite:debug` | Plan dumps, planner tracing, Volcano search space, rule diagnosis |
| `/project:calcite:rules` | Write/register rules, operand matching, avoid infinite loops |
| `/project:calcite:schema` | SchemaFactory, Table, TableScan implementation |
| `/project:calcite:types` | RelDataType, type inference, nullable handling |
| `/project:calcite:rex` | RexNode construction, RexBuilder, type checking |
| `/project:calcite:relbuilder` | RelBuilder patterns, field refs, trait sets |
| `/project:calcite:conventions` | Conventions, trait propagation, DuckDB/JDBC |
| `/project:calcite:metadata` | RelMetadataProvider, cost model, statistics |
| `/project:calcite:sql-parser` | SqlNode visitors, custom DDL, parser config |
| `/project:calcite:extensions` | Custom operators, functions, aggregates |

### SQL & Query Skills

| Command | Description |
|---------|-------------|
| `/project:sql-patterns` | Supported SQL, edge cases, dialect handling |
| `/project:query-testing` | Full-stack SQL tests, plan assertions |

### Debugging & Diagnostics Skills

| Command | Description |
|---------|-------------|
| `/project:java-heap-debug` | JVM tuning, heap dumps, memory patterns |

### Architecture Skills

| Command | Description |
|---------|-------------|
| `/project:project-modules` | Module ownership, dependencies, where code goes |

## Usage Examples

### Run regression tests for file adapter
```
/project:regression file
```

### Test Splunk adapter with specific pattern
```
/project:test:adapter splunk "*IntegrationTest"
```

### Quick test during development
```
/project:test:quick sharepoint-list
```

### Run complete test suite
```
/project:test:all
```

## Command Arguments

Most commands accept arguments in the format:
```
/project:command adapter [engine] [test-pattern]
```

- `adapter`: The adapter to test (file, splunk, sharepoint-list, etc.)
- `engine`: For file adapter only (PARQUET, DUCKDB, LINQ4J, ARROW)
- `test-pattern`: Optional test class or method pattern to run

## Adding New Commands

To add a new command:

1. Create a new `.md` file in `.claude/commands/` or a subdirectory
2. Name it according to the command you want (e.g., `deploy.md` for `/project:deploy`)
3. Use `$ARGUMENTS` placeholder for dynamic arguments
4. Include clear instructions for Claude to follow

## Best Practices

1. Keep commands focused on a single task
2. Include error handling instructions
3. Specify timeout values for long-running operations
4. Document expected outcomes
5. Use subdirectories to organize related commands

## Environment Setup

Some adapters require environment variables:

### Splunk Adapter
- `CALCITE_TEST_SPLUNK=true`
- Optional: `SPLUNK_URL`, `SPLUNK_USER`, `SPLUNK_PASSWORD`

### SharePoint Adapter
- `SHAREPOINT_INTEGRATION_TESTS=true`
- `SHAREPOINT_TENANT_ID`
- `SHAREPOINT_CLIENT_ID`
- `SHAREPOINT_CLIENT_SECRET`
- `SHAREPOINT_SITE_URL`

### File Adapter
- `CALCITE_FILE_ENGINE_TYPE` (PARQUET|DUCKDB|LINQ4J|ARROW)
