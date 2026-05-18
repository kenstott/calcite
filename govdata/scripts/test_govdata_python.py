#!/usr/bin/env python3
"""
PG-like behaviour verification for the govdata JDBC driver.

Tests that identifiers behave like PostgreSQL:
  - Metadata (getSchemas/getTables/getColumns) returns lowercase names
  - Unquoted lowercase SQL resolves correctly
  - Unquoted UPPERCASE SQL resolves correctly (case-insensitive)
  - information_schema queries work in both lower and upper case

Usage:
  python scripts/test_govdata_python.py [path/to/askamerica-engine*.jar]
"""

import sys
import os
import glob

# ── JAR resolution ─────────────────────────────────────────────────────────────

def find_jar():
    here = os.path.dirname(os.path.abspath(__file__))
    calcite_root = os.path.dirname(os.path.dirname(here))

    engine = os.path.join(calcite_root, "askamerica-engine", "build", "libs", "askamerica-engine*.jar")
    matches = [m for m in glob.glob(engine) if "-sources" not in m and "-javadoc" not in m]
    if matches:
        return sorted(matches)[-1]

    govdata = os.path.join(calcite_root, "govdata", "build", "libs", "calcite-govdata-*-all.jar")
    matches = glob.glob(govdata)
    if matches:
        print("  (using full govdata JAR — run ./gradlew :askamerica-engine:shadowJar for the engine JAR)")
        return sorted(matches)[-1]

    raise FileNotFoundError("No JAR found. Run: ./gradlew :askamerica-engine:shadowJar")

jar_path = sys.argv[1] if len(sys.argv) > 1 else find_jar()
print(f"JAR: {jar_path}\n")

# ── JVM startup ────────────────────────────────────────────────────────────────

try:
    import jpype
except ImportError:
    sys.exit("jpype1 not installed. Run: pip install jpype1")

if not jpype.isJVMStarted():
    jpype.startJVM(classpath=[jar_path])

try:
    ILoggerFactory = jpype.JClass("org.slf4j.LoggerFactory").getILoggerFactory()
    Level = jpype.JClass("ch.qos.logback.classic.Level")
    ILoggerFactory.getLogger("ROOT").setLevel(Level.ERROR)
except Exception:
    pass

# ── Helpers ────────────────────────────────────────────────────────────────────

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
failures = []

def check(label, condition, detail=""):
    if condition:
        print(f"  {PASS}  {label}")
    else:
        print(f"  {FAIL}  {label}" + (f" — {detail}" if detail else ""))
        failures.append(label)

def connect(url="jdbc:govdata:source=sec,geo"):
    GovDataDriver = jpype.JClass("org.apache.calcite.adapter.govdata.GovDataDriver")
    props = jpype.JClass("java.util.Properties")()
    conn = GovDataDriver().connect(url, props)
    assert conn is not None, f"driver.connect() returned null for: {url}"
    return conn

def query_rows(conn, sql):
    stmt = conn.createStatement()
    rs = stmt.executeQuery(sql)
    meta = rs.getMetaData()
    cols = [str(meta.getColumnName(i+1)).lower() for i in range(meta.getColumnCount())]
    rows = []
    while rs.next():
        rows.append({c: (str(rs.getString(i+1)) if not rs.wasNull() else None)
                     for i, c in enumerate(cols)})
    rs.close()
    stmt.close()
    return rows

# ── Connection ─────────────────────────────────────────────────────────────────

conn = connect("jdbc:govdata:source=geo")
meta = conn.getMetaData()

# ══════════════════════════════════════════════════════════════════════════════
print("── 1. Schema metadata is lowercase ──────────────────────────────────────")

rs = meta.getSchemas()
schemas = []
while rs.next():
    schemas.append(str(rs.getString("TABLE_SCHEM")))
rs.close()

print(f"  schemas: {schemas}")
check("GEO schema name is lowercase 'geo'",   "geo" in schemas,   f"got {schemas}")
check("No uppercase schema names",
      all(s == s.lower() for s in schemas),   f"uppercase found: {[s for s in schemas if s != s.lower()]}")

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 2. Table metadata is lowercase ───────────────────────────────────────")

rs = meta.getTables(None, "geo", "%", None)
tables = []
while rs.next():
    tables.append(str(rs.getString("TABLE_NAME")))
rs.close()

print(f"  geo tables (first 5): {tables[:5]}")
check("Tables present in geo",         len(tables) > 0)
check("Table names are lowercase",
      all(t == t.lower() for t in tables), f"uppercase: {[t for t in tables if t != t.lower()]}")
check("states present",                "states" in tables)

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 3. Column metadata is lowercase ──────────────────────────────────────")

rs = meta.getColumns(None, "geo", "states", "%")
cols = []
while rs.next():
    cols.append(str(rs.getString("COLUMN_NAME")))
rs.close()

print(f"  states columns (first 5): {cols[:5]}")
check("Columns present",           len(cols) > 0)
check("Column names are lowercase",
      all(c == c.lower() for c in cols), f"uppercase: {[c for c in cols if c != c.lower()]}")

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 4. Lowercase SQL resolves correctly ──────────────────────────────────")

try:
    rows = query_rows(conn,
        "select state_name, state_abbr, state_fips from geo.states "
        "order by state_name fetch first 3 rows only")
    check("lowercase geo.states query returns rows", len(rows) > 0)
    check("result columns are lowercase",
          all(k == k.lower() for row in rows for k in row),
          str(rows[0].keys()) if rows else "")
    for r in rows:
        print(f"    {r.get('state_fips',''):<6} {r.get('state_abbr',''):<4} {r.get('state_name','')}")
except Exception as e:
    check("lowercase geo.states query returns rows", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 5. UPPERCASE SQL resolves correctly (case-insensitive) ───────────────")

try:
    rows_upper = query_rows(conn,
        "SELECT STATE_NAME, STATE_ABBR, STATE_FIPS FROM GEO.STATES "
        "ORDER BY STATE_NAME FETCH FIRST 3 ROWS ONLY")
    check("UPPERCASE GEO.STATES query returns rows", len(rows_upper) > 0)
    for r in rows_upper:
        print(f"    {r.get('state_fips',''):<6} {r.get('state_abbr',''):<4} {r.get('state_name','')}")
except Exception as e:
    check("UPPERCASE GEO.STATES query returns rows", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 6. Mixed-case SQL resolves correctly ─────────────────────────────────")

try:
    rows_mixed = query_rows(conn,
        "Select State_Name, State_Abbr From Geo.States "
        "Order By State_Name Fetch First 3 Rows Only")
    check("Mixed-case Geo.States query returns rows", len(rows_mixed) > 0)
except Exception as e:
    check("Mixed-case Geo.States query returns rows", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 7. information_schema.tables — lowercase ─────────────────────────────")

try:
    rows_is = query_rows(conn,
        "select table_schema, table_name from information_schema.tables "
        "where table_schema = 'geo' order by table_name fetch first 5 rows only")
    check("information_schema.tables (lowercase) returns rows", len(rows_is) > 0)
    check("table_schema values are lowercase",
          all(r.get('table_schema','').islower() for r in rows_is),
          str([r.get('table_schema') for r in rows_is]))
    for r in rows_is:
        print(f"    {r.get('table_schema','')}.{r.get('table_name','')}")
except Exception as e:
    check("information_schema.tables (lowercase) returns rows", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 8. INFORMATION_SCHEMA.TABLES — uppercase ─────────────────────────────")

try:
    rows_IS = query_rows(conn,
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = 'geo' ORDER BY TABLE_NAME FETCH FIRST 5 ROWS ONLY")
    check("INFORMATION_SCHEMA.TABLES (uppercase) returns rows", len(rows_IS) > 0)
except Exception as e:
    check("INFORMATION_SCHEMA.TABLES (uppercase) returns rows", False, str(e))

conn.close()

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 9. Cross-schema join (geo + counties within geo) ─────────────────────")

conn2 = connect("jdbc:govdata:source=geo")
try:
    rows_join = query_rows(conn2,
        "select s.state_name, c.county_name, c.state_fips "
        "from geo.states s "
        "join geo.counties c on s.state_fips = c.state_fips "
        "where s.state_abbr = 'CA' "
        "order by c.county_name fetch first 3 rows only")
    check("Intra-schema geo join returns rows", len(rows_join) > 0)
    for r in rows_join:
        print(f"    {r.get('state_name',''):<15} {r.get('county_name','')}")
except Exception as e:
    check("Intra-schema geo join returns rows", False, str(e))
finally:
    conn2.close()

print()
if failures:
    print(f"\033[31m{len(failures)} test(s) FAILED:\033[0m")
    for f in failures:
        print(f"  ✗ {f}")
    sys.exit(1)
else:
    print(f"\033[32mAll tests passed — PG-like identifier behaviour confirmed.\033[0m")
