# Claude Development Guidelines - Core Rules

## 🚨 CRITICAL PRIORITIES (Zero Tolerance)

### Java 8 Compatibility - MANDATORY
- **PROHIBITED**: Text blocks, var keyword, switch expressions, records, pattern matching, sealed classes
- **REQUIRED**: Use java.sql.LocalTime instead of deprecated java.sql.Time
- **REQUIRED**: Avoid timezone offset functions when computing day offsets from epoch

### Implementation Honesty - ZERO TOLERANCE
- **NEVER** claim "implemented" when only stubs exist
- **NEVER** write commit messages claiming non-existent functionality
- **REQUIRED**: All stubs must throw `UnsupportedOperationException` with descriptive message
- **REQUIRED**: Use commit prefixes: `wip:` for stubs, `feat:` only when working

### Task Completion - NON-NEGOTIABLE
- **PROHIBITED**: Abandoning current task for new work
- **PROHIBITED**: Claiming "constraints" to avoid completion
- **REQUIRED**: Explicit status reporting (In Progress/Blocked/Complete)

## 📋 QUICK REFERENCE CARDS

### Test Execution Commands
```bash
# Unit tests only (default)
./gradlew :module:test

# Integration tests only
./gradlew :module:test -PincludeTags=integration

# Specific integration test
./gradlew :module:test -PincludeTags=integration --tests "*ClassName*"

# All tests
./gradlew :module:test -PrunAllTests
```

### Verification Checklist Before Claiming "Complete"
- [ ] Test execution output showing success
- [ ] Sample data demonstrating functionality
- [ ] Command used for verification documented
- [ ] Expected vs actual results compared
- [ ] All related tests pass
- [ ] No debugging artifacts in code

### SQL Conventions
- **Default**: lex=ORACLE, unquotedCasing=TO_LOWER, nameGeneration=SMART_CASING
- **Quote**: Reserved words used as identifiers, mixed/upper case identifiers
- **Don't Quote**: Lower case identifiers

## 🔧 CORE PRACTICES

### Code Quality
- Breaking changes preferred over backward compatibility hacks
- Fix forbidden API issues immediately
- Never unilaterally remove features
- Always verify changes with tests

### Communication Standards
- **PROHIBITED**: "This should work" → Use: "This works, verified by [command]"
- **PROHIBITED**: "I think..." → Use: "The issue is [root cause], traced via [method]"
- **REQUIRED**: Evidence-based claims with verification

## 🎯 DECISION TREES

### When Test Fails:
```
Test Failing?
├─ Check @Tag annotation first
│  ├─ @Tag("integration")? → Use -PincludeTags=integration
│  └─ @Tag("unit")? → Default command should work
├─ Error "0 tests executed"?
│  └─ Missing -PincludeTags parameter
└─ Still failing? → Follow debugging workflow (see CLAUDE-TROUBLESHOOTING.md)
```

### When Claiming Task Complete:
```
Ready to claim "Complete"?
├─ Does it process real data? (not empty lists/nulls)
├─ Have you run the verification checklist?
├─ Do all related tests pass?
└─ Is functionality end-to-end tested?
    ├─ Yes to all → Safe to claim complete
    └─ No to any → Status is "In Progress"
```

## 📚 CROSS-REFERENCES
- Testing details → See CLAUDE-TESTING.md
- Adapter specifics → See CLAUDE-ADAPTERS.md
- Debug workflows → See CLAUDE-TROUBLESHOOTING.md
