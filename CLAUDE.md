# Claude Development Guidelines - Main Index

## 📁 MODULAR GUIDELINE STRUCTURE

This project uses a modular guideline system for better organization and effectiveness:

- **CLAUDE-CORE.md** - Essential rules, quick reference cards, and decision trees
- **CLAUDE-TESTING.md** - Comprehensive testing guidelines and command patterns
- **CLAUDE-ADAPTERS.md** - Adapter-specific knowledge and debugging
- **CLAUDE-TROUBLESHOOTING.md** - Systematic debug workflows and error patterns

All `CLAUDE*.md` files are automatically loaded by Claude Code.

## 🚨 EMERGENCY QUICK REFERENCE

### Most Critical Rules
1. **Java 8 Compatibility** - No newer language features (ZERO TOLERANCE)
2. **Implementation Honesty** - Never claim "implemented" for stubs (ZERO TOLERANCE)
3. **Test Execution** - Check @Tag first, use -PincludeTags=integration (CRITICAL)
4. **Task Completion** - No abandonment, provide verification evidence (NON-NEGOTIABLE)

### Most Common Commands
```bash
# Unit tests (default)
./gradlew :module:test

# Integration tests
./gradlew :module:test -PincludeTags=integration

# Specific test
./gradlew :module:test -PincludeTags=integration --tests "*TestName*"
```

### Emergency Debugging
```bash
# Get clean error output
./gradlew :module:test -PincludeTags=integration --tests "*FailingTest*" --console=plain

# Check DuckDB issues
duckdb -c "DESCRIBE SELECT * FROM read_parquet('/path/to/file.parquet')"
```

## 🎯 NAVIGATION BY TASK TYPE

### When Working on Tests
→ See **CLAUDE-TESTING.md** for:
- Test execution patterns
- Tag-based system explanation
- Debugging workflows
- Common anti-patterns

### When Working on Adapters
→ See **CLAUDE-ADAPTERS.md** for:
- Environment variable setup
- Engine-specific commands
- Common error patterns
- Cross-adapter patterns

### When Debugging Issues
→ See **CLAUDE-TROUBLESHOOTING.md** for:
- Systematic investigation workflows
- Error pattern matching
- Debug output strategies
- Cleanup protocols

### For Core Development Rules
→ See **CLAUDE-CORE.md** for:
- Essential coding standards
- Verification requirements
- Decision trees
- Communication standards

## 🔧 LEGACY COMMANDS (PRESERVED)

### Cleanup Debug Code
When asked to "cleanup debug code":
1. Identify `System.out`/`System.err` usage - remove or convert to logger debug
2. Identify temp/debug tests - remove OR tag as unit/integration/performance
3. Identify dead code - report and ask for approval to remove
4. Remove temp markdown files

### Regression Testing
When asked to "run full regression tests":
- **File adapter**: `CALCITE_FILE_ENGINE_TYPE=[engine] gtimeout 1800 ./gradlew :file:test --continue --console=plain`
- **Splunk adapter**: `CALCITE_TEST_SPLUNK=true gtimeout 1800 ./gradlew :splunk:test --continue --console=plain`
- **SharePoint adapter**: `SHAREPOINT_INTEGRATION_TESTS=true gtimeout 1800 ./gradlew :sharepoint-list:test --continue --console=plain`

### Stacktrace Generation
To determine how a value was computed: generate stacktrace output

## 📋 ACCOUNTABILITY STANDARDS

### Before Any Commit
- [ ] All modified code builds without errors
- [ ] All related tests pass (provide command + output)
- [ ] No debugging artifacts left in code
- [ ] Commit message accurately describes actual changes
- [ ] If claiming "fix", original issue scenario tested and resolved

### Communication Requirements
- **PROHIBITED**: "This should work" → Use: "This works, verified by [command]"
- **PROHIBITED**: "I think..." → Use: "The issue is [root cause], traced via [method]"
- **REQUIRED**: Evidence-based claims with verification

### Quality Gates - NON-NEGOTIABLE
A task is ONLY complete when:
1. **Functionality verified**: Real execution with expected inputs/outputs
2. **Tests passing**: All unit and integration tests green
3. **Code quality**: No warnings, proper error handling, no debug artifacts
4. **Documentation current**: Comments and docs reflect actual behavior
5. **Regression tested**: Related functionality still works
