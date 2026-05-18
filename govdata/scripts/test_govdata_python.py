#!/usr/bin/env python3
"""
Full JDBC DatabaseMetaData + PG-like behaviour verification for the govdata driver.

Covers every major DatabaseMetaData call a client (BI tool, LLM, ORM) would make:
  getCatalogs, getSchemas, getTableTypes, getTables, getColumns,
  getPrimaryKeys, getImportedKeys, getExportedKeys, getIndexInfo,
  getTypeInfo, getDatabaseProductName, getDriverName/Version,
  supportsTransactions, getMaxConnections, and SQL execution in all cases.

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
SKIP = "\033[33mSKIP\033[0m"
failures = []

def check(label, condition, detail=""):
    if condition:
        print(f"  {PASS}  {label}")
    else:
        print(f"  {FAIL}  {label}" + (f" — {detail}" if detail else ""))
        failures.append(label)

def skip(label, reason=""):
    print(f"  {SKIP}  {label}" + (f" — {reason}" if reason else ""))

def rs_to_list(rs, col):
    rows = []
    while rs.next():
        v = rs.getString(col)
        rows.append(str(v) if v is not None else None)
    rs.close()
    return rows

def connect(url="jdbc:askamerica:source=geo"):
    AskAmericaDriver = jpype.JClass("org.apache.calcite.adapter.askamerica.AskAmericaDriver")
    props = jpype.JClass("java.util.Properties")()
    conn = AskAmericaDriver().connect(url, props)
    assert conn is not None, f"AskAmericaDriver.connect() returned null for: {url}"
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

conn = connect("jdbc:askamerica:source=geo")
meta = conn.getMetaData()

# ══════════════════════════════════════════════════════════════════════════════
print("── 1. Driver identity (getDatabaseProductName / getDriverName) ───────────")

try:
    product = str(meta.getDatabaseProductName())
    driver  = str(meta.getDriverName())
    version = str(meta.getDriverVersion())
    print(f"  product={product}  driver={driver}  version={version}")
    check("getDatabaseProductName() == 'AskAmerica'", product == "AskAmerica", f"got {product}")
    check("getDriverName() == 'AskAmerica JDBC Driver'", driver == "AskAmerica JDBC Driver", f"got {driver}")
    check("getDriverVersion() returns a non-empty string",       len(version) > 0)
except Exception as e:
    check("getDatabaseProductName/DriverName/Version", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 2. getCatalogs() ─────────────────────────────────────────────────────")

try:
    cats = rs_to_list(meta.getCatalogs(), "TABLE_CAT")
    print(f"  catalogs: {cats}")
    check("getCatalogs() does not throw", True)
    # Calcite returns one empty-string catalog or none — both are valid
    check("getCatalogs() returns a list (empty or populated)", isinstance(cats, list))
except Exception as e:
    check("getCatalogs() does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 3. getSchemas() — names are lowercase ────────────────────────────────")

rs = meta.getSchemas()
schemas = []
while rs.next():
    schemas.append(str(rs.getString("TABLE_SCHEM")))
rs.close()

print(f"  schemas: {schemas}")
check("geo schema is lowercase 'geo'",    "geo" in schemas,  f"got {schemas}")
check("No uppercase schema names",
      all(s == s.lower() for s in schemas),
      f"uppercase: {[s for s in schemas if s != s.lower()]}")
check("information_schema present",       "information_schema" in schemas)
check("pg_catalog present",               "pg_catalog" in schemas)

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 4. getSchemas(catalog, schemaPattern) — filtered ─────────────────────")

try:
    rs = meta.getSchemas(None, "geo")
    filtered = []
    while rs.next():
        filtered.append(str(rs.getString("TABLE_SCHEM")))
    rs.close()
    print(f"  getSchemas(null, 'geo'): {filtered}")
    check("getSchemas(null, 'geo') returns ['geo']", filtered == ["geo"], f"got {filtered}")
except Exception as e:
    check("getSchemas(null, 'geo') does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 5. getTableTypes() ───────────────────────────────────────────────────")

try:
    types = rs_to_list(meta.getTableTypes(), "TABLE_TYPE")
    print(f"  table types: {types}")
    check("getTableTypes() returns at least one type", len(types) > 0)
    check("TABLE type present", "TABLE" in types or any("TABLE" in (t or "") for t in types))
except Exception as e:
    check("getTableTypes() does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 6. getTables() — names are lowercase ─────────────────────────────────")

rs = meta.getTables(None, "geo", "%", None)
tables = []
while rs.next():
    tables.append(str(rs.getString("TABLE_NAME")))
rs.close()

print(f"  geo tables (first 5): {tables[:5]}")
check("getTables() returns rows",          len(tables) > 0)
check("Table names are lowercase",
      all(t == t.lower() for t in tables),
      f"uppercase: {[t for t in tables if t != t.lower()]}")
check("'states' table present",            "states" in tables)
check("'counties' table present",          "counties" in tables)

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 7. getTables() — pattern matching ────────────────────────────────────")

try:
    rs = meta.getTables(None, "geo", "state%", None)
    pattern_tables = []
    while rs.next():
        pattern_tables.append(str(rs.getString("TABLE_NAME")))
    rs.close()
    print(f"  getTables(geo, 'state%'): {pattern_tables}")
    check("getTables with 'state%' pattern returns matches", len(pattern_tables) > 0)
    check("All matches start with 'state'", all(t.startswith("state") for t in pattern_tables),
          str(pattern_tables))
except Exception as e:
    check("getTables() pattern matching does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 8. getColumns() — names are lowercase ────────────────────────────────")

rs = meta.getColumns(None, "geo", "states", "%")
cols = []
while rs.next():
    cols.append(str(rs.getString("COLUMN_NAME")))
rs.close()

print(f"  states columns: {cols}")
check("getColumns() returns rows",         len(cols) > 0)
check("Column names are lowercase",
      all(c == c.lower() for c in cols),
      f"uppercase: {[c for c in cols if c != c.lower()]}")
check("'state_name' column present",       "state_name" in cols)
check("'state_abbr' column present",       "state_abbr" in cols)
check("'state_fips' column present",       "state_fips" in cols)

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 9. getColumns() — TYPE_NAME populated ────────────────────────────────")

try:
    rs = meta.getColumns(None, "geo", "states", "%")
    col_types = {}
    while rs.next():
        col_types[str(rs.getString("COLUMN_NAME"))] = str(rs.getString("TYPE_NAME"))
    rs.close()
    print(f"  column types (first 5): {dict(list(col_types.items())[:5])}")
    check("TYPE_NAME is non-null for all columns",
          all(v not in (None, "null", "None") for v in col_types.values()),
          str({k: v for k, v in col_types.items() if v in (None, "null", "None")}))
except Exception as e:
    check("getColumns() TYPE_NAME populated", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 10. getPrimaryKeys() ─────────────────────────────────────────────────")

try:
    rs = meta.getPrimaryKeys(None, "geo", "states")
    pks = []
    while rs.next():
        pks.append(str(rs.getString("COLUMN_NAME")))
    rs.close()
    print(f"  states primary keys: {pks}")
    # Calcite may return empty for views/parquet tables — not an error
    check("getPrimaryKeys() does not throw", True)
    check("getPrimaryKeys() returns a list", isinstance(pks, list))
except Exception as e:
    check("getPrimaryKeys() does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 11. getImportedKeys() ────────────────────────────────────────────────")

try:
    rs = meta.getImportedKeys(None, "geo", "states")
    fks = []
    while rs.next():
        fks.append(str(rs.getString("FK_NAME")))
    rs.close()
    print(f"  states imported keys (FK): {fks}")
    check("getImportedKeys() does not throw", True)
except Exception as e:
    check("getImportedKeys() does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 12. getExportedKeys() ────────────────────────────────────────────────")

try:
    rs = meta.getExportedKeys(None, "geo", "states")
    eks = []
    while rs.next():
        eks.append(str(rs.getString("FK_NAME")))
    rs.close()
    print(f"  states exported keys: {eks}")
    check("getExportedKeys() does not throw", True)
except Exception as e:
    check("getExportedKeys() does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 13. getIndexInfo() ───────────────────────────────────────────────────")

try:
    rs = meta.getIndexInfo(None, "geo", "states", False, True)
    idxs = []
    while rs.next():
        idx_name = rs.getString("INDEX_NAME")
        idxs.append(str(idx_name) if idx_name is not None else None)
    rs.close()
    print(f"  states indexes: {idxs}")
    check("getIndexInfo() does not throw", True)
except Exception as e:
    check("getIndexInfo() does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 14. getTypeInfo() ────────────────────────────────────────────────────")

try:
    rs = meta.getTypeInfo()
    type_names = []
    while rs.next():
        type_names.append(str(rs.getString("TYPE_NAME")))
    rs.close()
    print(f"  type info ({len(type_names)} types, first 5): {type_names[:5]}")
    check("getTypeInfo() returns at least one type", len(type_names) > 0)
except Exception as e:
    check("getTypeInfo() does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 15. Driver capability flags ──────────────────────────────────────────")

try:
    check("supportsTransactions() does not throw",
          meta.supportsTransactions() in (True, False))
    check("isReadOnly() does not throw",
          conn.isReadOnly() in (True, False))
    max_conn = int(meta.getMaxConnections())
    print(f"  getMaxConnections(): {max_conn}")
    check("getMaxConnections() returns non-negative int", max_conn >= 0)
    url = str(meta.getURL())
    print(f"  getURL(): {url}")
    check("getURL() starts with 'jdbc:askamerica:'", url.startswith("jdbc:askamerica:"))
except Exception as e:
    check("Driver capability flags do not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 16. Lowercase SQL ────────────────────────────────────────────────────")

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
print("\n── 17. UPPERCASE SQL (case-insensitive) ─────────────────────────────────")

try:
    rows = query_rows(conn,
        "SELECT STATE_NAME, STATE_ABBR, STATE_FIPS FROM GEO.STATES "
        "ORDER BY STATE_NAME FETCH FIRST 3 ROWS ONLY")
    check("UPPERCASE GEO.STATES query returns rows", len(rows) > 0)
except Exception as e:
    check("UPPERCASE GEO.STATES query returns rows", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 18. Mixed-case SQL ───────────────────────────────────────────────────")

try:
    rows = query_rows(conn,
        "Select State_Name, State_Abbr From Geo.States "
        "Order By State_Name Fetch First 3 Rows Only")
    check("Mixed-case Geo.States query returns rows", len(rows) > 0)
except Exception as e:
    check("Mixed-case Geo.States query returns rows", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 19. PreparedStatement with parameters ────────────────────────────────")

try:
    ps = conn.prepareStatement(
        "select state_name, state_abbr from geo.states where state_abbr = ? fetch first 1 row only")
    ps.setString(1, "CA")
    rs = ps.executeQuery()
    ps_rows = []
    while rs.next():
        ps_rows.append(str(rs.getString("state_name")))
    rs.close()
    ps.close()
    print(f"  PreparedStatement(state_abbr='CA'): {ps_rows}")
    check("PreparedStatement returns rows",      len(ps_rows) > 0)
    check("PreparedStatement result is California",
          any("California" in r for r in ps_rows), str(ps_rows))
except Exception as e:
    check("PreparedStatement does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 20. ResultSetMetaData — column labels and types ──────────────────────")

try:
    stmt = conn.createStatement()
    rs = stmt.executeQuery(
        "select state_name, state_abbr, state_fips from geo.states fetch first 1 row only")
    rsmeta = rs.getMetaData()
    col_count = int(rsmeta.getColumnCount())
    labels = [str(rsmeta.getColumnLabel(i+1)) for i in range(col_count)]
    types  = [str(rsmeta.getColumnTypeName(i+1)) for i in range(col_count)]
    rs.close()
    stmt.close()
    print(f"  labels: {labels}")
    print(f"  types:  {types}")
    check("getColumnCount() == 3",              col_count == 3, f"got {col_count}")
    check("Column labels are lowercase",
          all(l == l.lower() for l in labels),  str(labels))
    check("getColumnTypeName() non-empty",
          all(len(t) > 0 for t in types),       str(types))
except Exception as e:
    check("ResultSetMetaData does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 21. information_schema.tables — lowercase ────────────────────────────")

try:
    rows = query_rows(conn,
        "select table_schema, table_name from information_schema.tables "
        "where table_schema = 'geo' order by table_name fetch first 5 rows only")
    check("information_schema.tables (lowercase) returns rows", len(rows) > 0)
    check("table_schema values are lowercase",
          all(r.get('table_schema','').islower() for r in rows),
          str([r.get('table_schema') for r in rows]))
    for r in rows:
        print(f"    {r.get('table_schema','')}.{r.get('table_name','')}")
except Exception as e:
    check("information_schema.tables (lowercase) returns rows", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 22. INFORMATION_SCHEMA.TABLES — uppercase ────────────────────────────")

try:
    rows = query_rows(conn,
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = 'geo' ORDER BY TABLE_NAME FETCH FIRST 5 ROWS ONLY")
    check("INFORMATION_SCHEMA.TABLES (uppercase) returns rows", len(rows) > 0)
except Exception as e:
    check("INFORMATION_SCHEMA.TABLES (uppercase) returns rows", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 23. information_schema.columns ───────────────────────────────────────")

try:
    rows = query_rows(conn,
        "select column_name, data_type from information_schema.columns "
        "where table_schema = 'geo' and table_name = 'states' "
        "order by column_name fetch first 5 rows only")
    check("information_schema.columns returns rows", len(rows) > 0)
    check("column_name values are lowercase",
          all(r.get('column_name','').islower() for r in rows),
          str([r.get('column_name') for r in rows]))
    for r in rows:
        print(f"    {r.get('column_name',''):<20} {r.get('data_type','')}")
except Exception as e:
    check("information_schema.columns returns rows", False, str(e))

conn.close()

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 24. Cross-schema join ────────────────────────────────────────────────")

conn2 = connect("jdbc:askamerica:source=geo")
try:
    rows = query_rows(conn2,
        "select s.state_name, c.county_name "
        "from geo.states s "
        "join geo.counties c on s.state_fips = c.state_fips "
        "where s.state_abbr = 'CA' "
        "order by c.county_name fetch first 3 rows only")
    check("Intra-schema geo join returns rows", len(rows) > 0)
    for r in rows:
        print(f"    {r.get('state_name',''):<15} {r.get('county_name','')}")
except Exception as e:
    check("Intra-schema geo join returns rows", False, str(e))
finally:
    conn2.close()

# ── DBeaver / DataGrip introspection patterns ─────────────────────────────────

conn3 = connect("jdbc:askamerica:source=geo")
meta3 = conn3.getMetaData()

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 25. getTables(null, null, '%', null) — all schemas at once ───────────")

try:
    rs = meta3.getTables(None, None, "%", None)
    all_tables = []
    while rs.next():
        schem = rs.getString("TABLE_SCHEM")
        tname = rs.getString("TABLE_NAME")
        all_tables.append((str(schem) if schem is not None else None, str(tname)))
    rs.close()
    print(f"  total tables across all schemas: {len(all_tables)}")
    check("getTables(null,null,'%',null) returns rows", len(all_tables) > 0)
    check("geo tables included in bulk fetch",
          any(s == "geo" for s, t in all_tables), str(all_tables[:3]))
except Exception as e:
    check("getTables(null,null,'%',null) does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 26. getColumns(null, null, '%', '%') — bulk column fetch ─────────────")

try:
    rs = meta3.getColumns(None, None, "%", "%")
    bulk_cols = 0
    while rs.next():
        bulk_cols += 1
    rs.close()
    print(f"  total columns across all schemas: {bulk_cols}")
    check("getColumns(null,null,'%','%') returns rows", bulk_cols > 0)
except Exception as e:
    check("getColumns(null,null,'%','%') does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 27. getBestRowIdentifier() ───────────────────────────────────────────")

try:
    DatabaseMetaData = jpype.JClass("java.sql.DatabaseMetaData")
    rs = meta3.getBestRowIdentifier(None, "geo", "states",
                                    DatabaseMetaData.bestRowSession, True)
    bri = []
    while rs.next():
        col = rs.getString("COLUMN_NAME")
        bri.append(str(col) if col is not None else None)
    rs.close()
    print(f"  bestRowIdentifier for geo.states: {bri}")
    check("getBestRowIdentifier() does not throw", True)
except Exception as e:
    check("getBestRowIdentifier() does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 28. getVersionColumns() ──────────────────────────────────────────────")

try:
    rs = meta3.getVersionColumns(None, "geo", "states")
    vc = []
    while rs.next():
        col = rs.getString("COLUMN_NAME")
        vc.append(str(col) if col is not None else None)
    rs.close()
    print(f"  versionColumns for geo.states: {vc}")
    check("getVersionColumns() does not throw", True)
except Exception as e:
    check("getVersionColumns() does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 29. getProcedures() / getFunctions() ─────────────────────────────────")

try:
    rs = meta3.getProcedures(None, None, "%")
    procs = 0
    while rs.next():
        procs += 1
    rs.close()
    print(f"  procedures: {procs}")
    check("getProcedures() does not throw", True)
except Exception as e:
    check("getProcedures() does not throw", False, str(e))

try:
    rs = meta3.getFunctions(None, None, "%")
    funcs = 0
    while rs.next():
        funcs += 1
    rs.close()
    print(f"  functions: {funcs}")
    check("getFunctions() does not throw", True)
except Exception as e:
    check("getFunctions() does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 30. getSuperTypes() / getSuperTables() ───────────────────────────────")

try:
    rs = meta3.getSuperTypes(None, "geo", "%")
    while rs.next():
        pass
    rs.close()
    check("getSuperTypes() does not throw", True)
except Exception as e:
    check("getSuperTypes() does not throw", False, str(e))

try:
    rs = meta3.getSuperTables(None, "geo", "%")
    while rs.next():
        pass
    rs.close()
    check("getSuperTables() does not throw", True)
except Exception as e:
    check("getSuperTables() does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 31. meta.getConnection() returns the connection ──────────────────────")

try:
    meta_conn = meta3.getConnection()
    check("meta.getConnection() returns non-null", meta_conn is not None)
except Exception as e:
    check("meta.getConnection() does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 32. connection.isValid() / getSchema() ───────────────────────────────")

try:
    valid = bool(conn3.isValid(5))
    print(f"  isValid(5): {valid}")
    check("connection.isValid(5) returns True", valid)
except Exception as e:
    check("connection.isValid() does not throw", False, str(e))

try:
    schema = conn3.getSchema()
    print(f"  getSchema(): {schema}")
    check("connection.getSchema() does not throw", True)
    check("getSchema() returns lowercase value",
          schema is None or str(schema) == str(schema).lower(),
          f"got {schema}")
except Exception as e:
    check("connection.getSchema() does not throw", False, str(e))

# ══════════════════════════════════════════════════════════════════════════════
print("\n── 33. supportsResultSetConcurrency() / supportsBatchUpdates() ──────────")

try:
    ResultSet = jpype.JClass("java.sql.ResultSet")
    rsc = bool(meta3.supportsResultSetConcurrency(
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
    print(f"  supportsResultSetConcurrency(FORWARD_ONLY, READ_ONLY): {rsc}")
    check("supportsResultSetConcurrency() does not throw", True)
except Exception as e:
    check("supportsResultSetConcurrency() does not throw", False, str(e))

try:
    batch = bool(meta3.supportsBatchUpdates())
    print(f"  supportsBatchUpdates(): {batch}")
    check("supportsBatchUpdates() does not throw", True)
except Exception as e:
    check("supportsBatchUpdates() does not throw", False, str(e))

conn3.close()

# ══════════════════════════════════════════════════════════════════════════════
TOTAL_GROUPS = 33
print()
if failures:
    print(f"\033[31m{len(failures)} test(s) FAILED:\033[0m")
    for f in failures:
        print(f"  ✗ {f}")
    sys.exit(1)
else:
    print(f"\033[32mAll {TOTAL_GROUPS} test groups passed — JDBC metadata + DBeaver/DataGrip compatibility confirmed.\033[0m")
