---
description: Optimize Java imports by removing qualified names and adding proper imports, handling conflicts
---

# Java Imports Optimization

Optimize Java imports in the specified file or directory by removing qualified class names and adding proper import statements.

**Usage:**
- `/java-imports path/to/File.java` - Optimize a specific file
- `/java-imports path/to/directory` - Optimize all .java files in directory recursively
- `/java-imports` - Optimize all .java files in the current working directory

**Target:** $ARGUMENTS (or current working directory if not specified)

---

## Process

For each Java file, perform the following steps:

### 1. Identify Qualified Class Names
Find all fully qualified class references in the code body (not in import statements):
- `java.util.List`, `java.util.Collections`, `java.nio.charset.StandardCharsets`
- `org.apache.calcite.sql.SqlNode`
- `com.fasterxml.jackson.databind.JsonNode`

### 2. Check for Name Conflicts
- If multiple classes share the same simple name (e.g., `java.util.Date` and `java.sql.Date`):
  - Import the **most frequently used** class
  - Keep others fully qualified with a comment
  - Document the conflict above the qualified usage

### 3. Add Missing Imports
Add import statements for all classes that will be unqualified:
- Organize in standard Java order:
  - `java.*` and `javax.*` (alphabetically)
  - Third-party packages (alphabetically)
  - `org.apache.calcite.*` (alphabetically)
- Insert in alphabetical order within each section
- Remove any unused imports

### 4. Replace Qualified Names with Simple Names
- Replace all qualified class names in the code body with their simple names
- EXCEPT where name conflicts require qualified names
- Examples:
  - `java.util.Collections.singletonList(x)` → `Collections.singletonList(x)`
  - `java.nio.charset.StandardCharsets.UTF_8` → `StandardCharsets.UTF_8`
  - `com.fasterxml.jackson.databind.JsonNode node` → `JsonNode node`

### 5. Verification
- Run `./gradlew :module:compileJava` to ensure the code still compiles
- Report any compilation errors

---

## Conflict Resolution Strategy

When name conflicts exist:
- Import the **most frequently used** class
- Keep less frequently used classes fully qualified
- Add a comment explaining the conflict:

```java
// Using qualified name to avoid conflict with imported ClassName
some.other.package.ClassName variable = ...;
```

---

## Example Transformation

**Before:**
```java
package org.example;

public class MyClass {
    private java.util.List<String> items;
    private java.util.Map<String, Object> data;
    private org.apache.calcite.sql.SqlNode node;

    public void process() {
        List<String> single = java.util.Collections.singletonList("test");
        byte[] bytes = text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
```

**After:**
```java
package org.example;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlNode;

public class MyClass {
    private List<String> items;
    private Map<String, Object> data;
    private SqlNode node;

    public void process() {
        List<String> single = Collections.singletonList("test");
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    }
}
```

---

## Handling Name Conflicts

**Before:**
```java
package org.example;

public class DateHandler {
    private java.util.Date utilDate;
    private java.sql.Date sqlDate;
    private java.util.Date anotherUtilDate;
}
```

**After:**
```java
package org.example;

import java.util.Date;

public class DateHandler {
    private Date utilDate;
    // Using qualified name to avoid conflict with imported java.util.Date
    private java.sql.Date sqlDate;
    private Date anotherUtilDate;
}
```

---

## Notes

- Process all `.java` files recursively when operating on a directory
- Preserve existing import organization style (blank lines between sections)
- Do not modify import static statements
- Verify compilation after all changes