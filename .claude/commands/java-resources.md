---
description: Extract constants to JSON resource file and refactor code to use it
---

# Java Resources Extraction

**Target:** $ARGUMENTS (file path required)

This command identifies ALL constants within a Java file, creates a JSON resource file for those constants, and refactors the code to use the JSON resource instead of hardcoded values.

## What This Command Does

1. **Identifies Constants**: Finds all constant declarations and hardcoded values:
   - `public static final` constants
   - `private static final` constants
   - Hardcoded string literals (URLs, messages, patterns)
   - Hardcoded numeric values used as configuration
   - Enum values that represent configuration

2. **Creates JSON Resource**: Generates a JSON file in `src/main/resources/` with:
   - Hierarchical structure matching the constant categories
   - Clear naming conventions (snake_case or camelCase)
   - Comments preserved as JSON properties where applicable

3. **Refactors Code**: Updates the Java file to:
   - Remove constant declarations (or mark as deprecated)
   - Add resource loading logic (using Jackson ObjectMapper)
   - Replace constant references with resource lookups
   - Add proper error handling for missing resources

## Usage Patterns

### Specific File
```bash
/java-resources path/to/File.java
```

### Current Open File
```bash
/java-resources .
```

## Example Transformations

### Before: Hardcoded Constants
```java
public class FredCatalogDownloader {
    private static final String BASE_URL = "https://api.stlouisfed.org/fred";
    private static final int MAX_RETRIES = 3;
    private static final long TIMEOUT_MS = 30000;

    public void download() {
        String url = BASE_URL + "/series";
        httpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(TIMEOUT_MS))
            .build();
    }
}
```

### After: Resource-Based Configuration
```java
public class FredCatalogDownloader {
    private static final Map<String, Object> CONFIG = loadConfig();

    private static Map<String, Object> loadConfig() {
        try (InputStream is = FredCatalogDownloader.class
                .getResourceAsStream("/fred_catalog_config.json")) {
            return new ObjectMapper().readValue(is,
                new TypeReference<Map<String, Object>>() {});
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config", e);
        }
    }

    public void download() {
        String baseUrl = (String) CONFIG.get("base_url");
        String url = baseUrl + "/series";
        int timeoutMs = (Integer) CONFIG.get("timeout_ms");
        httpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(timeoutMs))
            .build();
    }
}
```

### Generated Resource File
**Location:** `src/main/resources/fred_catalog_config.json`
```json
{
  "base_url": "https://api.stlouisfed.org/fred",
  "max_retries": 3,
  "timeout_ms": 30000,
  "api": {
    "series_endpoint": "/series",
    "observations_endpoint": "/series/observations"
  }
}
```

## Categories of Constants to Extract

1. **API Configuration**
   - Base URLs
   - Endpoints
   - API keys/tokens (with security note)
   - Rate limits

2. **Timeouts and Retry Logic**
   - Connection timeouts
   - Read timeouts
   - Max retry counts
   - Backoff intervals

3. **File Paths and Names**
   - Cache directories
   - Output file patterns
   - Resource paths

4. **Business Logic Constants**
   - Default values
   - Thresholds
   - Validation patterns
   - Format strings

5. **SQL/Query Templates**
   - Table names
   - Column names
   - Query patterns

## Implementation Steps

1. **Analyze the file** using Read tool to identify all constants and hardcoded values
2. **Categorize constants** by their purpose (API, timeouts, paths, etc.)
3. **Design JSON structure** with logical grouping and naming
4. **Create resource file** in `src/main/resources/` with appropriate name
5. **Add resource loading code** at the top of the Java class
6. **Replace constant usages** throughout the file with resource lookups
7. **Handle type conversions** (String to int, long, boolean, etc.)
8. **Add error handling** for missing or invalid resource values
9. **Verify compilation** with `./gradlew :module:compileJava`
10. **Update tests** if constants were used in test files

## Naming Conventions

### Resource File Name
- Pattern: `{class_name}_config.json`
- Example: `fred_catalog_downloader_config.json`
- Location: `src/main/resources/`

### JSON Property Names
- Use snake_case for consistency with JSON conventions
- Group related properties under nested objects
- Example: `api.base_url`, `retry.max_attempts`, `timeout.connect_ms`

## Security Considerations

- **API Keys**: Add comment in JSON: `"// TODO: Move to environment variable or secrets manager"`
- **Sensitive URLs**: Consider if they should be in config vs. environment
- **Passwords**: NEVER put in resource files - use environment variables

## Error Handling Pattern

```java
private static <T> T getConfigValue(String key, Class<T> type, T defaultValue) {
    try {
        Object value = CONFIG.get(key);
        if (value == null) {
            LOGGER.warn("Config key '{}' not found, using default: {}", key, defaultValue);
            return defaultValue;
        }
        return type.cast(value);
    } catch (ClassCastException e) {
        LOGGER.error("Config key '{}' has wrong type, using default: {}", key, defaultValue, e);
        return defaultValue;
    }
}
```

## Verification

After refactoring:
1. Run `./gradlew :module:compileJava` to ensure code compiles
2. Run `./gradlew :module:test` to ensure tests pass
3. Verify resource file is in correct location
4. Check that all constant references are replaced
5. Ensure no hardcoded values remain (use grep for common patterns)

## Example Usage

```bash
# Extract constants from FRED downloader
/java-resources govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/FredCatalogDownloader.java

# Extract from BLS downloader
/java-resources govdata/src/main/java/org/apache/calcite/adapter/govdata/econ/BlsDataDownloader.java
```

## Notes

- This command performs significant refactoring - review changes carefully
- May require updating dependent classes if constants were public
- Consider backward compatibility if constants are part of public API
- Resource files can be overridden at runtime for different environments
- JSON resources are easier to modify without recompilation
