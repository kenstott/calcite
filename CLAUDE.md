# Claude Development Guidelines

## Token Management
- CRITICAL: Run /compact after every 5 tool calls or 10 messages.
- CRITICAL: Use /context every 15 minutes to verify 'Messages' < 40k tokens.
- If 'Messages' > 50k, stop work and run /compact immediately.

## Token Cost
**Do not re-read files you have already modified in this session unless I explicitly ask.** Trust your internal state of the file from the last edit.
**When messaging teammates, only send file paths and line numbers.** Do not include code blocks.

## Critical Rules (Zero Tolerance)
1. **Java 8 Compatibility** - No var, text blocks, switch expressions, records, pattern matching, sealed classes
2. **Implementation Honesty** - Never claim "implemented" for stubs. Use `wip:` prefix for stubs, `feat:` only when working
3. **Test Execution** - Check @Tag first, use `-PincludeTags=integration` for integration tests
4. **Task Completion** - No abandonment. Provide verification evidence. No "this should work" claims.
5. **No Memory Answers** - Before answering any question about what existing code does, run a Grep or Read tool call first. No exceptions. Do not answer from memory.
6. **No Fallback Values / Silent Error Handling** - Never add fallback values or silent error handling. Caused repeated production issues. When a value might be missing, find the design guarantee or fix the upstream source — never patch around it.

## Common Commands
```bash
./gradlew :module:test                                              # unit tests
./gradlew :module:test -PincludeTags=integration                    # integration tests
./gradlew :module:test -PincludeTags=integration --tests "*Name*"   # specific test
./gradlew :module:test -PincludeTags=integration --console=plain    # debug output
duckdb -c "DESCRIBE SELECT * FROM read_parquet('/path/file.parquet')" # check DuckDB
```

## Reference Files (read on demand)
- @CLAUDE-CORE.ref - Coding standards, verification checklists, decision trees
- @CLAUDE-TESTING.ref - Test execution patterns, tag system, debugging workflows, anti-patterns
- @CLAUDE-ADAPTERS.ref - Adapter env vars, engine commands, error patterns (File, GovData, Splunk, SharePoint)
- @CLAUDE-TROUBLESHOOTING.ref - Systematic debug workflows, error pattern matching, cleanup protocols
- @CLAUDE-TEAMS.ref - Team definitions, agent-to-team mapping, coordination workflows

## Commit Checklist
- [ ] Builds without errors
- [ ] Tests pass (provide command + output)
- [ ] No debug artifacts (System.out/err)
- [ ] Commit message reflects actual changes

## Legacy Commands
- **Cleanup debug code**: Remove System.out/err, temp tests, dead code, temp markdown
- **Regression testing**: `CALCITE_FILE_ENGINE_TYPE=[engine] gtimeout 1800 ./gradlew :file:test --continue --console=plain`
- **Splunk**: `CALCITE_TEST_SPLUNK=true gtimeout 1800 ./gradlew :splunk:test --continue --console=plain`
- **SharePoint**: `SHAREPOINT_INTEGRATION_TESTS=true gtimeout 1800 ./gradlew :sharepoint-list:test --continue --console=plain`