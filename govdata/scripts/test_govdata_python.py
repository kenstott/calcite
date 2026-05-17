#!/usr/bin/env python3
"""
Validates the GovData fat JAR via JPype:
  - JDBC metadata: schemas, tables, columns, PKs, FKs
  - SQL query through INFORMATION_SCHEMA.TABLES
  - SQL query returning real data from sec.filing_metadata

Usage:
  python scripts/test_govdata_python.py [path/to/calcite-govdata-*-all.jar]

If no JAR path is given the script searches govdata/build/libs for the fat JAR.
AWS credentials must be available via env vars (AWS_ACCESS_KEY_ID, etc.) or
~/.aws/credentials for the data query test to succeed.
"""

import sys
import os
import glob

# --- JAR resolution -----------------------------------------------------------

def find_jar():
    here = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(here)
    pattern = os.path.join(project_root, "build", "libs", "calcite-govdata-*-all.jar")
    matches = glob.glob(pattern)
    if matches:
        return sorted(matches)[-1]
    raise FileNotFoundError(
        f"No fat JAR found at {pattern}. Run: ./gradlew :govdata:shadowJar"
    )

jar_path = sys.argv[1] if len(sys.argv) > 1 else find_jar()
print(f"Using JAR: {jar_path}")

# --- JVM startup --------------------------------------------------------------

try:
    import jpype
except ImportError:
    sys.exit("jpype1 is not installed. Run: pip install jpype1")

if not jpype.isJVMStarted():
    jpype.startJVM(classpath=[jar_path])

# Silence INFO/DEBUG noise from the adapter
try:
    ILoggerFactory = jpype.JClass("org.slf4j.LoggerFactory").getILoggerFactory()
    Level = jpype.JClass("ch.qos.logback.classic.Level")
    ILoggerFactory.getLogger("ROOT").setLevel(Level.ERROR)
except Exception:
    pass

# --- Single connection for all tests ------------------------------------------
# A single long-lived connection lets the async schema initialization complete
# before the data query in Test 6 executes.

GovDataDriver = jpype.JClass("org.apache.calcite.adapter.govdata.GovDataDriver")
driver = GovDataDriver()

def connect(url):
    props = jpype.JClass("java.util.Properties")()
    conn = driver.connect(url, props)
    assert conn is not None, f"driver.connect() returned null for: {url}"
    return conn

conn = connect("jdbc:govdata:source=sec,geo")
meta = conn.getMetaData()

# ==============================================================================
# Test 1 — schemas present
# ==============================================================================

print("\nTest 1: schemas ...")
rs = meta.getSchemas()
schemas = []
while rs.next():
    schemas.append(str(rs.getString("TABLE_SCHEM")))
rs.close()

print(f"  Schemas: {schemas}")
assert "SEC" in schemas, f"Expected SEC in schemas, got: {schemas}"
assert "GEO" in schemas, f"Expected GEO in schemas, got: {schemas}"
print("  PASS")

# ==============================================================================
# Test 2 — tables in SEC schema
# ==============================================================================

print("\nTest 2: tables in SEC ...")
rs = meta.getTables(None, "SEC", "%", None)
tables = []
while rs.next():
    tables.append(str(rs.getString("TABLE_NAME")))
rs.close()

print(f"  Table count: {len(tables)}")
assert len(tables) > 0, "Expected at least one table in SEC schema"
assert "filing_metadata" in tables, f"Expected filing_metadata in tables, got: {tables[:10]}"
print("  PASS")

# ==============================================================================
# Test 3 — columns for filing_metadata
# ==============================================================================

print("\nTest 3: columns for filing_metadata ...")
rs = meta.getColumns(None, "SEC", "filing_metadata", "%")
cols = []
while rs.next():
    cols.append(str(rs.getString("COLUMN_NAME")))
rs.close()

print(f"  Column count: {len(cols)}, first 5: {cols[:5]}")
assert len(cols) > 0, "Expected columns for filing_metadata"
print("  PASS")

# ==============================================================================
# Test 4 — getPrimaryKeys (may return empty — just must not throw)
# ==============================================================================

print("\nTest 4: getPrimaryKeys (must not throw) ...")
rs = meta.getPrimaryKeys(None, "SEC", "filing_metadata")
pk_cols = []
while rs.next():
    pk_cols.append(str(rs.getString("COLUMN_NAME")))
rs.close()
print(f"  PKs: {pk_cols}")
print("  PASS")

# ==============================================================================
# Test 5 — SQL query via INFORMATION_SCHEMA.TABLES
# ==============================================================================

print("\nTest 5: SQL query via INFORMATION_SCHEMA.TABLES ...")
stmt = conn.createStatement()
rs = stmt.executeQuery(
    "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE "
    "FROM information_schema.TABLES "
    "WHERE TABLE_SCHEMA = 'SEC' "
    "ORDER BY TABLE_NAME "
    "FETCH FIRST 10 ROWS ONLY"
)
count = 0
while rs.next():
    print(f"  {str(rs.getString('TABLE_SCHEMA'))}.{str(rs.getString('TABLE_NAME')):35s} {str(rs.getString('TABLE_TYPE'))}")
    count += 1
rs.close()
stmt.close()

assert count > 0, "Expected rows from INFORMATION_SCHEMA.TABLES for SEC schema"
print("  PASS")

# ==============================================================================
# Test 6 — query real data from sec.filing_metadata
# ==============================================================================

print("\nTest 6: query real data from sec.filing_metadata ...")
stmt = conn.createStatement()
rs = stmt.executeQuery(
    "SELECT cik, accession_number, filing_type, filing_date "
    "FROM sec.filing_metadata "
    "ORDER BY filing_date DESC "
    "FETCH FIRST 5 ROWS ONLY"
)
rows = []
while rs.next():
    rows.append((
        str(rs.getString("cik")),
        str(rs.getString("accession_number")),
        str(rs.getString("filing_type")),
        str(rs.getString("filing_date")),
    ))
rs.close()
stmt.close()

print(f"  {'CIK':<12} {'ACCESSION':<25} {'TYPE':<10} {'DATE'}")
print(f"  {'-'*12} {'-'*25} {'-'*10} {'-'*10}")
for row in rows:
    print(f"  {row[0]:<12} {row[1]:<25} {row[2]:<10} {row[3]}")

assert len(rows) > 0, "Expected rows from sec.filing_metadata"
print("  PASS")

# ==============================================================================

conn.close()
print("\nAll tests passed.")
