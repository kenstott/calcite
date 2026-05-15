# Connecting to GovData from Python

GovData is a Calcite-based JDBC adapter that exposes US government data (SEC filings, geographic, economic, education, health, and other datasets) as SQL tables. Python connects via [JPype](https://jpype.readthedocs.io/), which embeds the JVM in the Python process and gives direct access to Java objects.

**Do not use JayDeBeAPI.** GovData requires calling `GovDataDriver.connect()` directly rather than routing through `java.sql.DriverManager`. The JVM-global DriverManager causes cross-contamination when other JDBC drivers (Hive, Trino, ClickHouse) are registered in the same process.

---

## Prerequisites

- Python 3.13+
- JPype: `pip install jpype1`
- The GovData fat JAR (see below)

---

## Building the JAR

From the repository root:

```bash
./gradlew :govdata:shadowJar
```

The fat JAR lands at:

```
build/libs/calcite-govdata-<version>-all.jar
```

Set `JAR` in your script to this path. The JAR bundles all dependencies, including Calcite, Avatica, and the GovData adapter.

---

## Starting the JVM

```python
import jpype

JAR = "build/libs/calcite-govdata-1.42.0-SNAPSHOT-all.jar"
jpype.startJVM(classpath=[JAR])
```

The JVM can only start once per process. If another library has already started it, `startJVM()` raises an exception. Guard against that:

```python
if not jpype.isJVMStarted():
    jpype.startJVM(classpath=[JAR])
```

### Silencing log output

The adapter uses SLF4J backed by Logback. Without configuration it prints INFO and DEBUG messages to stdout. Suppress them at startup:

```python
try:
    ILoggerFactory = jpype.JClass("org.slf4j.LoggerFactory").getILoggerFactory()
    Level = jpype.JClass("ch.qos.logback.classic.Level")
    ILoggerFactory.getLogger("ROOT").setLevel(Level.ERROR)
except Exception:
    pass
```

The `except` here guards against environments where the Logback implementation is absent. It does not suppress errors from GovData itself.

---

## Opening a Connection

Instantiate the driver once and reuse it across connections:

```python
GovDataDriver = jpype.JClass("org.apache.calcite.adapter.govdata.GovDataDriver")
driver = GovDataDriver()

def connect(url):
    props = jpype.JClass("java.util.Properties")()
    conn = driver.connect(url, props)
    assert conn is not None, f"driver.connect() returned null for: {url}"
    return conn
```

### URL format

| Use case | URL |
|---|---|
| Read-only, multiple schemas | `jdbc:govdata:source=sec,geo` |
| Read-only, single schema | `jdbc:govdata:source=sec` |
| Single schema with download | `jdbc:govdata:source=sec&ciks=AAPL` |

Comma-separate source names for multi-schema read-only access. Adding `ciks=` (or other download parameters) triggers ingestion and only makes sense with a single source. Schema names in SQL are uppercase (`SEC`, `GEO`, `EDU`); table names are lowercase as ingested.

SQL identifiers are case-insensitive — the driver sets `lex=ORACLE`, `unquotedCasing=TO_LOWER`, and `caseSensitive=false` automatically. You do not pass these as connection properties.

---

## Schema and Table Discovery

The JDBC metadata API works for discovery. Pass `None` for catalog (GovData has no catalog concept) and uppercase schema names where a schema filter is needed.

```python
conn = connect("jdbc:govdata:source=sec,geo")
meta = conn.getMetaData()

# List all schemas
rs = meta.getSchemas()
while rs.next():
    print(rs.getString("TABLE_SCHEM"))  # GEO, SEC, metadata

# List tables in SEC
rs = meta.getTables(None, "SEC", "%", None)
while rs.next():
    print(rs.getString("TABLE_NAME"), rs.getString("TABLE_TYPE"))

# List columns for a specific table
# Table name pattern in getColumns() is case-sensitive — use exact lowercase or "%"
rs = meta.getColumns(None, "SEC", "filing_metadata", "%")
while rs.next():
    print(rs.getString("COLUMN_NAME"), rs.getString("TYPE_NAME"))
```

The `getColumns()` table name pattern uses Calcite's `LikePattern`, which is always case-sensitive regardless of the connection's `caseSensitive` setting. Pass the exact lowercase table name (e.g. `"filing_metadata"`) or `"%"` to match all tables.

---

## Querying via SQL

Standard JDBC `Statement` and `ResultSet` work as expected:

```python
stmt = conn.createStatement()
rs = stmt.executeQuery("SELECT cik, company_name FROM sec.filing_metadata FETCH FIRST 5 ROWS ONLY")
while rs.next():
    print(str(rs.getString(1)), str(rs.getString(2)))
```

Call `str()` on JPype string objects before using Python format specs. JPype returns a Java `String` proxy, not a Python `str`, and format specifiers like `f"{val:30s}"` raise `TypeError` on Java objects.

### Fetch limit syntax

GovData uses Calcite's FETCH/OFFSET syntax (not MySQL `LIMIT`):

```sql
FETCH FIRST 10 ROWS ONLY
OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY
```

---

## System Tables

The metadata schema is `metadata`, not `INFORMATION_SCHEMA`. It exposes two tables: `TABLES` and `COLUMNS`. Column names follow Java field naming conventions and must be quoted in SQL.

| SQL name | Column names |
|---|---|
| `metadata."TABLES"` | `tableSchem`, `tableName`, `tableType`, `tableCat` |
| `metadata."COLUMNS"` | `tableSchem`, `tableName`, `columnName`, `dataType`, `typeName` |

Querying the system tables directly:

```python
stmt = conn.createStatement()
rs = stmt.executeQuery(
    'SELECT "tableSchem", "tableName", "tableType" '
    'FROM metadata."TABLES" '
    "WHERE \"tableSchem\" = 'SEC' "
    'ORDER BY "tableName" '
    'FETCH FIRST 10 ROWS ONLY'
)
while rs.next():
    print(str(rs.getString(1)), str(rs.getString(2)), str(rs.getString(3)))
```

The JDBC metadata API (`getSchemas()`, `getTables()`, `getColumns()`) reads from these same tables. Use whichever is more convenient.

---

## Complete Example

```python
import jpype

JAR = "build/libs/calcite-govdata-1.42.0-SNAPSHOT-all.jar"
jpype.startJVM(classpath=[JAR])

try:
    ILoggerFactory = jpype.JClass("org.slf4j.LoggerFactory").getILoggerFactory()
    Level = jpype.JClass("ch.qos.logback.classic.Level")
    ILoggerFactory.getLogger("ROOT").setLevel(Level.ERROR)
except Exception:
    pass

GovDataDriver = jpype.JClass("org.apache.calcite.adapter.govdata.GovDataDriver")
driver = GovDataDriver()

def connect(url):
    props = jpype.JClass("java.util.Properties")()
    conn = driver.connect(url, props)
    assert conn is not None, f"driver.connect() returned null for: {url}"
    return conn

conn = connect("jdbc:govdata:source=sec,geo")

# Schemas
meta = conn.getMetaData()
rs = meta.getSchemas()
while rs.next():
    print(rs.getString("TABLE_SCHEM"))

# Tables in SEC
rs = meta.getTables(None, "SEC", "%", None)
while rs.next():
    print(rs.getString("TABLE_NAME"), rs.getString("TABLE_TYPE"))

# Columns for a table — exact lowercase name required
rs = meta.getColumns(None, "SEC", "filing_metadata", "%")
while rs.next():
    print(rs.getString("COLUMN_NAME"), rs.getString("TYPE_NAME"))

# SQL query
stmt = conn.createStatement()
rs = stmt.executeQuery(
    'SELECT "tableSchem", "tableName", "tableType" '
    'FROM metadata."TABLES" '
    "WHERE \"tableSchem\" = 'SEC' "
    'ORDER BY "tableName" '
    'FETCH FIRST 10 ROWS ONLY'
)
while rs.next():
    print(str(rs.getString(1)), str(rs.getString(2)), str(rs.getString(3)))

conn.close()
```

---

## Caveats

**`getColumns()` pattern is case-sensitive.** The `tableNamePattern` argument uses Calcite `LikePattern` matching, which ignores the connection-level `caseSensitive=false` setting. Always pass exact lowercase names or `"%"`.

**No `INFORMATION_SCHEMA`.** Use `metadata."TABLES"` and `metadata."COLUMNS"`. Querying `INFORMATION_SCHEMA` will raise an error.

**camelCase column names must be quoted.** `tableSchem`, `tableName`, `tableType` in SQL require double-quote wrapping. Unquoted, the parser lowercases them and they still resolve (case-insensitive identifiers), but mixing conventions is error-prone. Quote them.

**JPype strings need `str()` for Python format specs.** `rs.getString()` returns a Java `String` proxy. `print()` accepts it, but `f"{val:30s}"` raises `TypeError`. Call `str(val)` first.

**`source=sec` is read-only.** Adding `ciks=` parameters triggers data download. Do not add download parameters to multi-source URLs.

**One JVM per process.** JPype cannot start, stop, and restart the JVM. Structure long-running scripts to open and close connections, not to restart the JVM.
