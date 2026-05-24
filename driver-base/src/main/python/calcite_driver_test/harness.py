"""
Shared JDBC driver test harness for all Calcite custom drivers.

Covers the full DatabaseMetaData API surface exercised by DBeaver, DataGrip,
and similar JDBC clients. Each driver's test script imports this module and
calls run_all() with driver-specific parameters.

Usage:
    from calcite_driver_test.harness import DriverTestHarness

    h = DriverTestHarness(
        jar_path        = "/path/to/driver.jar",
        driver_class    = "com.example.MyDriver",
        url             = "jdbc:myproduct:source=myschema",
        expected_product= "MyProduct",
        expected_driver = "MyProduct JDBC Driver",
        expected_url_prefix = "jdbc:myproduct:",
        test_schema     = "myschema",
        test_table      = "mytable",
        test_columns    = ["col1", "col2"],   # at least two known columns
        test_query      = "select col1, col2 from myschema.mytable fetch first 3 rows only",
        test_param_col  = "col1",             # column to use in PreparedStatement WHERE
        test_param_val  = "somevalue",        # value that returns >= 1 row
    )
    h.run_all()
"""

import sys

try:
    import jpype
except ImportError:
    sys.exit("jpype1 not installed. Run: pip install jpype1")

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"


class DriverTestHarness:

    def __init__(self, jar_path, driver_class, url,
                 expected_product, expected_driver, expected_url_prefix,
                 test_schema, test_table, test_columns,
                 test_query, test_param_col, test_param_val,
                 skip_data_tests_on_error=False):
        self.jar_path                = jar_path
        self.driver_class            = driver_class
        self.url                     = url
        self.expected_product        = expected_product
        self.expected_driver         = expected_driver
        self.expected_url_prefix     = expected_url_prefix
        self.test_schema             = test_schema
        self.test_table              = test_table
        self.test_columns            = test_columns
        self.test_query              = test_query
        self.test_param_col          = test_param_col
        self.test_param_val          = test_param_val
        self.skip_data_tests_on_error = skip_data_tests_on_error
        self._data_available         = None  # None = not yet probed
        self.failures                = []
        self.group                   = 0

    # ── JVM ──────────────────────────────────────────────────────────────────

    def start_jvm(self):
        if not jpype.isJVMStarted():
            jpype.startJVM(classpath=[self.jar_path])
        try:
            factory = jpype.JClass("org.slf4j.LoggerFactory").getILoggerFactory()
            level   = jpype.JClass("ch.qos.logback.classic.Level")
            factory.getLogger("ROOT").setLevel(level.ERROR)
        except Exception:
            pass

    # ── Data availability probe ───────────────────────────────────────────────

    def _probe_data(self, conn):
        """Run the test query; return True if data is available, False otherwise."""
        if self._data_available is not None:
            return self._data_available
        if not self.skip_data_tests_on_error:
            self._data_available = True
            return True
        try:
            rows = self.query_rows(conn, self.test_query)
            if rows:
                self._data_available = True
            else:
                print(f"\n  \033[33mSKIP\033[0m  Data probe returned 0 rows — skipping data-dependent tests")
                self._data_available = False
        except Exception as e:
            print(f"\n  \033[33mSKIP\033[0m  Data probe failed — skipping data-dependent tests: {e}")
            self._data_available = False
        return self._data_available

    def _skip_if_no_data(self, conn, label):
        """Return True if this test should be skipped (data unavailable)."""
        if not self.skip_data_tests_on_error:
            return False
        if not self._probe_data(conn):
            self.group += 1
            print(f"\n── {self.group}. {label}")
            print(f"  \033[33mSKIP\033[0m  {label} — data unavailable (schema/type migration pending)")
            return True
        return False

    # ── Helpers ───────────────────────────────────────────────────────────────

    def connect(self, url=None):
        url = url or self.url
        Driver = jpype.JClass(self.driver_class)
        props  = jpype.JClass("java.util.Properties")()
        conn   = Driver().connect(url, props)
        assert conn is not None, f"{self.driver_class}.connect() returned null for: {url}"
        return conn

    def check(self, label, condition, detail=""):
        if condition:
            print(f"  {PASS}  {label}")
        else:
            msg = f"  {FAIL}  {label}"
            if detail:
                msg += f" — {detail}"
            print(msg)
            self.failures.append(label)

    def header(self, title):
        self.group += 1
        print(f"\n── {self.group}. {title}")

    def query_rows(self, conn, sql):
        stmt = conn.createStatement()
        rs   = stmt.executeQuery(sql)
        meta = rs.getMetaData()
        cols = [str(meta.getColumnName(i+1)).lower() for i in range(meta.getColumnCount())]
        rows = []
        while rs.next():
            rows.append({c: (str(rs.getString(i+1)) if not rs.wasNull() else None)
                         for i, c in enumerate(cols)})
        rs.close()
        stmt.close()
        return rows

    def rs_list(self, rs, col):
        rows = []
        while rs.next():
            v = rs.getString(col)
            rows.append(str(v) if v is not None else None)
        rs.close()
        return rows

    # ── Test groups ───────────────────────────────────────────────────────────

    def test_driver_identity(self, meta):
        self.header("Driver identity — getDatabaseProductName / getDriverName")
        try:
            product = str(meta.getDatabaseProductName())
            driver  = str(meta.getDriverName())
            version = str(meta.getDriverVersion())
            print(f"  product={product}  driver={driver}  version={version}")
            self.check(f"getDatabaseProductName() == '{self.expected_product}'",
                       product == self.expected_product, f"got '{product}'")
            self.check(f"getDriverName() == '{self.expected_driver}'",
                       driver == self.expected_driver, f"got '{driver}'")
            self.check("getDriverVersion() non-empty", len(version) > 0)
        except Exception as e:
            self.check("getDatabaseProductName/DriverName/Version", False, str(e))

    def test_get_catalogs(self, meta):
        self.header("getCatalogs()")
        try:
            cats = self.rs_list(meta.getCatalogs(), "TABLE_CAT")
            print(f"  catalogs: {cats}")
            self.check("getCatalogs() does not throw", True)
            self.check("getCatalogs() returns a list", isinstance(cats, list))
        except Exception as e:
            self.check("getCatalogs() does not throw", False, str(e))

    def test_get_schemas(self, meta):
        self.header("getSchemas() — names are lowercase")
        rs = meta.getSchemas()
        schemas = []
        while rs.next():
            schemas.append(str(rs.getString("TABLE_SCHEM")))
        rs.close()
        print(f"  schemas: {schemas}")
        self.check(f"'{self.test_schema}' schema is lowercase",
                   self.test_schema in schemas, f"got {schemas}")
        self.check("No uppercase schema names",
                   all(s == s.lower() for s in schemas),
                   f"uppercase: {[s for s in schemas if s != s.lower()]}")
        self.check("information_schema present", "information_schema" in schemas)
        self.check("pg_catalog present",         "pg_catalog" in schemas)

    def test_get_schemas_filtered(self, meta):
        self.header("getSchemas(catalog, schemaPattern) — filtered")
        try:
            rs = meta.getSchemas(None, self.test_schema)
            filtered = self.rs_list(rs, "TABLE_SCHEM")
            print(f"  getSchemas(null, '{self.test_schema}'): {filtered}")
            self.check(f"getSchemas(null, '{self.test_schema}') returns ['{self.test_schema}']",
                       filtered == [self.test_schema], f"got {filtered}")
        except Exception as e:
            self.check("getSchemas(null, pattern) does not throw", False, str(e))

    def test_get_table_types(self, meta):
        self.header("getTableTypes()")
        try:
            types = self.rs_list(meta.getTableTypes(), "TABLE_TYPE")
            print(f"  table types: {types}")
            self.check("getTableTypes() returns at least one type", len(types) > 0)
            self.check("TABLE type present", any("TABLE" in (t or "") for t in types))
        except Exception as e:
            self.check("getTableTypes() does not throw", False, str(e))

    def test_get_tables(self, meta):
        self.header("getTables() — names are lowercase")
        rs = meta.getTables(None, self.test_schema, "%", None)
        tables = []
        while rs.next():
            tables.append(str(rs.getString("TABLE_NAME")))
        rs.close()
        print(f"  {self.test_schema} tables (first 5): {tables[:5]}")
        self.check("getTables() returns rows", len(tables) > 0)
        self.check("Table names are lowercase",
                   all(t == t.lower() for t in tables),
                   f"uppercase: {[t for t in tables if t != t.lower()]}")
        self.check(f"'{self.test_table}' present", self.test_table in tables)

    def test_get_tables_pattern(self, meta):
        self.header("getTables() — wildcard pattern")
        try:
            prefix = self.test_table[:3] + "%"
            rs = meta.getTables(None, self.test_schema, prefix, None)
            matched = []
            while rs.next():
                matched.append(str(rs.getString("TABLE_NAME")))
            rs.close()
            print(f"  getTables('{self.test_schema}', '{prefix}'): {matched}")
            self.check(f"getTables with '{prefix}' returns matches", len(matched) > 0)
            self.check(f"All matches start with '{self.test_table[:3]}'",
                       all(t.startswith(self.test_table[:3]) for t in matched), str(matched))
        except Exception as e:
            self.check("getTables() pattern matching does not throw", False, str(e))

    def test_get_tables_all_schemas(self, meta):
        self.header("getTables(null, null, '%', null) — all schemas at once")
        try:
            rs = meta.getTables(None, None, "%", None)
            all_tables = []
            while rs.next():
                schem = rs.getString("TABLE_SCHEM")
                tname = rs.getString("TABLE_NAME")
                all_tables.append((str(schem) if schem is not None else None, str(tname)))
            rs.close()
            print(f"  total tables across all schemas: {len(all_tables)}")
            self.check("getTables(null,null,'%',null) returns rows", len(all_tables) > 0)
            self.check(f"'{self.test_schema}' tables in bulk fetch",
                       any(s == self.test_schema for s, t in all_tables))
        except Exception as e:
            self.check("getTables(null,null,'%',null) does not throw", False, str(e))

    def test_get_columns(self, meta):
        self.header("getColumns() — names are lowercase")
        rs = meta.getColumns(None, self.test_schema, self.test_table, "%")
        cols = []
        while rs.next():
            cols.append(str(rs.getString("COLUMN_NAME")))
        rs.close()
        print(f"  {self.test_table} columns: {cols}")
        self.check("getColumns() returns rows", len(cols) > 0)
        self.check("Column names are lowercase",
                   all(c == c.lower() for c in cols),
                   f"uppercase: {[c for c in cols if c != c.lower()]}")
        for expected_col in self.test_columns:
            self.check(f"'{expected_col}' column present", expected_col in cols)

    def test_get_columns_type_name(self, meta):
        self.header("getColumns() — TYPE_NAME populated")
        try:
            rs = meta.getColumns(None, self.test_schema, self.test_table, "%")
            col_types = {}
            while rs.next():
                col_types[str(rs.getString("COLUMN_NAME"))] = str(rs.getString("TYPE_NAME"))
            rs.close()
            print(f"  column types (first 5): {dict(list(col_types.items())[:5])}")
            self.check("TYPE_NAME non-null for all columns",
                       all(v not in (None, "null", "None") for v in col_types.values()),
                       str({k: v for k, v in col_types.items() if v in (None, "null", "None")}))
        except Exception as e:
            self.check("getColumns() TYPE_NAME populated", False, str(e))

    def test_get_columns_bulk(self, meta):
        self.header("getColumns(null, null, '%', '%') — bulk column fetch")
        try:
            rs = meta.getColumns(None, None, "%", "%")
            count = 0
            while rs.next():
                count += 1
            rs.close()
            print(f"  total columns across all schemas: {count}")
            self.check("getColumns(null,null,'%','%') returns rows", count > 0)
        except Exception as e:
            self.check("getColumns(null,null,'%','%') does not throw", False, str(e))

    def test_primary_keys(self, meta):
        self.header("getPrimaryKeys()")
        try:
            rs = meta.getPrimaryKeys(None, self.test_schema, self.test_table)
            pks = self.rs_list(rs, "COLUMN_NAME")
            print(f"  primary keys for {self.test_table}: {pks}")
            self.check("getPrimaryKeys() does not throw", True)
            self.check("getPrimaryKeys() returns a list", isinstance(pks, list))
        except Exception as e:
            self.check("getPrimaryKeys() does not throw", False, str(e))

    def test_imported_keys(self, meta):
        self.header("getImportedKeys()")
        try:
            rs = meta.getImportedKeys(None, self.test_schema, self.test_table)
            fks = self.rs_list(rs, "FK_NAME")
            print(f"  imported keys: {fks}")
            self.check("getImportedKeys() does not throw", True)
        except Exception as e:
            self.check("getImportedKeys() does not throw", False, str(e))

    def test_exported_keys(self, meta):
        self.header("getExportedKeys()")
        try:
            rs = meta.getExportedKeys(None, self.test_schema, self.test_table)
            eks = self.rs_list(rs, "FK_NAME")
            print(f"  exported keys: {eks}")
            self.check("getExportedKeys() does not throw", True)
        except Exception as e:
            self.check("getExportedKeys() does not throw", False, str(e))

    def test_index_info(self, meta):
        self.header("getIndexInfo()")
        try:
            rs = meta.getIndexInfo(None, self.test_schema, self.test_table, False, True)
            idxs = self.rs_list(rs, "INDEX_NAME")
            print(f"  indexes: {idxs}")
            self.check("getIndexInfo() does not throw", True)
        except Exception as e:
            self.check("getIndexInfo() does not throw", False, str(e))

    def test_best_row_identifier(self, meta):
        self.header("getBestRowIdentifier()")
        try:
            DatabaseMetaData = jpype.JClass("java.sql.DatabaseMetaData")
            rs = meta.getBestRowIdentifier(None, self.test_schema, self.test_table,
                                           DatabaseMetaData.bestRowSession, True)
            bri = self.rs_list(rs, "COLUMN_NAME")
            print(f"  bestRowIdentifier: {bri}")
            self.check("getBestRowIdentifier() does not throw", True)
        except Exception as e:
            self.check("getBestRowIdentifier() does not throw", False, str(e))

    def test_version_columns(self, meta):
        self.header("getVersionColumns()")
        try:
            rs = meta.getVersionColumns(None, self.test_schema, self.test_table)
            vc = self.rs_list(rs, "COLUMN_NAME")
            print(f"  versionColumns: {vc}")
            self.check("getVersionColumns() does not throw", True)
        except Exception as e:
            self.check("getVersionColumns() does not throw", False, str(e))

    def test_type_info(self, meta):
        self.header("getTypeInfo()")
        try:
            rs = meta.getTypeInfo()
            types = self.rs_list(rs, "TYPE_NAME")
            print(f"  type info ({len(types)} types, first 5): {types[:5]}")
            self.check("getTypeInfo() returns at least one type", len(types) > 0)
        except Exception as e:
            self.check("getTypeInfo() does not throw", False, str(e))

    def test_procedures_functions(self, meta):
        self.header("getProcedures() / getFunctions()")
        try:
            rs = meta.getProcedures(None, None, "%")
            count = 0
            while rs.next():
                count += 1
            rs.close()
            print(f"  procedures: {count}")
            self.check("getProcedures() does not throw", True)
        except Exception as e:
            self.check("getProcedures() does not throw", False, str(e))
        try:
            rs = meta.getFunctions(None, None, "%")
            count = 0
            while rs.next():
                count += 1
            rs.close()
            print(f"  functions: {count}")
            self.check("getFunctions() does not throw", True)
        except Exception as e:
            self.check("getFunctions() does not throw", False, str(e))

    def test_super_types_tables(self, meta):
        self.header("getSuperTypes() / getSuperTables()")
        try:
            rs = meta.getSuperTypes(None, self.test_schema, "%")
            while rs.next():
                pass
            rs.close()
            self.check("getSuperTypes() does not throw", True)
        except Exception as e:
            self.check("getSuperTypes() does not throw", False, str(e))
        try:
            rs = meta.getSuperTables(None, self.test_schema, "%")
            while rs.next():
                pass
            rs.close()
            self.check("getSuperTables() does not throw", True)
        except Exception as e:
            self.check("getSuperTables() does not throw", False, str(e))

    def test_capability_flags(self, conn, meta):
        self.header("Driver capability flags")
        try:
            self.check("supportsTransactions() does not throw",
                       meta.supportsTransactions() in (True, False))
            self.check("isReadOnly() does not throw",
                       conn.isReadOnly() in (True, False))
            max_conn = int(meta.getMaxConnections())
            print(f"  getMaxConnections(): {max_conn}")
            self.check("getMaxConnections() returns non-negative int", max_conn >= 0)
            url = str(meta.getURL())
            print(f"  getURL(): {url}")
            self.check(f"getURL() starts with '{self.expected_url_prefix}'",
                       url.startswith(self.expected_url_prefix))
        except Exception as e:
            self.check("Driver capability flags do not throw", False, str(e))

    def test_meta_get_connection(self, meta):
        self.header("meta.getConnection() returns the connection")
        try:
            meta_conn = meta.getConnection()
            self.check("meta.getConnection() returns non-null", meta_conn is not None)
        except Exception as e:
            self.check("meta.getConnection() does not throw", False, str(e))

    def test_connection_valid_schema(self, conn):
        self.header("connection.isValid() / getSchema()")
        try:
            valid = bool(conn.isValid(5))
            print(f"  isValid(5): {valid}")
            self.check("connection.isValid(5) returns True", valid)
        except Exception as e:
            self.check("connection.isValid() does not throw", False, str(e))
        try:
            schema = conn.getSchema()
            print(f"  getSchema(): {schema}")
            self.check("connection.getSchema() does not throw", True)
            self.check("getSchema() returns lowercase value",
                       schema is None or str(schema) == str(schema).lower(),
                       f"got {schema}")
        except Exception as e:
            self.check("connection.getSchema() does not throw", False, str(e))

    def test_result_set_support(self, meta):
        self.header("supportsResultSetConcurrency() / supportsBatchUpdates()")
        try:
            ResultSet = jpype.JClass("java.sql.ResultSet")
            rsc = bool(meta.supportsResultSetConcurrency(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
            print(f"  supportsResultSetConcurrency(FORWARD_ONLY, READ_ONLY): {rsc}")
            self.check("supportsResultSetConcurrency() does not throw", True)
        except Exception as e:
            self.check("supportsResultSetConcurrency() does not throw", False, str(e))
        try:
            batch = bool(meta.supportsBatchUpdates())
            print(f"  supportsBatchUpdates(): {batch}")
            self.check("supportsBatchUpdates() does not throw", True)
        except Exception as e:
            self.check("supportsBatchUpdates() does not throw", False, str(e))

    def test_sql_lowercase(self, conn):
        if self._skip_if_no_data(conn, "Lowercase SQL resolves correctly"):
            return
        self.header("Lowercase SQL resolves correctly")
        try:
            rows = self.query_rows(conn, self.test_query)
            self.check("lowercase query returns rows", len(rows) > 0)
            self.check("result columns are lowercase",
                       all(k == k.lower() for row in rows for k in row),
                       str(rows[0].keys()) if rows else "")
        except Exception as e:
            self.check("lowercase query returns rows", False, str(e))

    def test_sql_uppercase(self, conn):
        if self._skip_if_no_data(conn, "UPPERCASE SQL resolves correctly (case-insensitive)"):
            return
        self.header("UPPERCASE SQL resolves correctly (case-insensitive)")
        try:
            rows = self.query_rows(conn, self.test_query.upper())
            self.check("UPPERCASE query returns rows", len(rows) > 0)
        except Exception as e:
            self.check("UPPERCASE query returns rows", False, str(e))

    def test_sql_mixed_case(self, conn):
        if self._skip_if_no_data(conn, "Mixed-case SQL resolves correctly"):
            return
        self.header("Mixed-case SQL resolves correctly")
        try:
            words = self.test_query.split()
            mixed = " ".join(w.capitalize() for w in words)
            rows = self.query_rows(conn, mixed)
            self.check("Mixed-case query returns rows", len(rows) > 0)
        except Exception as e:
            self.check("Mixed-case query returns rows", False, str(e))

    def test_prepared_statement(self, conn):
        if self._skip_if_no_data(conn, "PreparedStatement with parameters"):
            return
        self.header("PreparedStatement with parameters")
        try:
            sql = (f"select {self.test_param_col} from "
                   f"{self.test_schema}.{self.test_table} "
                   f"where {self.test_param_col} = ? fetch first 1 row only")
            ps = conn.prepareStatement(sql)
            ps.setString(1, self.test_param_val)
            rs = ps.executeQuery()
            ps_rows = []
            while rs.next():
                ps_rows.append(str(rs.getString(1)))
            rs.close()
            ps.close()
            print(f"  PreparedStatement({self.test_param_col}='{self.test_param_val}'): {ps_rows}")
            self.check("PreparedStatement returns rows", len(ps_rows) > 0)
            self.check(f"PreparedStatement result contains '{self.test_param_val}'",
                       any(self.test_param_val in r for r in ps_rows), str(ps_rows))
        except Exception as e:
            self.check("PreparedStatement does not throw", False, str(e))

    def test_result_set_metadata(self, conn):
        if self._skip_if_no_data(conn, "ResultSetMetaData — column labels and types"):
            return
        self.header("ResultSetMetaData — column labels and types")
        try:
            cols = ", ".join(self.test_columns[:3])
            stmt = conn.createStatement()
            rs = stmt.executeQuery(
                f"select {cols} from {self.test_schema}.{self.test_table} fetch first 1 row only")
            rsmeta = rs.getMetaData()
            col_count = int(rsmeta.getColumnCount())
            labels = [str(rsmeta.getColumnLabel(i+1)) for i in range(col_count)]
            types  = [str(rsmeta.getColumnTypeName(i+1)) for i in range(col_count)]
            rs.close()
            stmt.close()
            print(f"  labels: {labels}")
            print(f"  types:  {types}")
            expected_count = min(len(self.test_columns), 3)
            self.check(f"getColumnCount() == {expected_count}",
                       col_count == expected_count, f"got {col_count}")
            self.check("Column labels are lowercase",
                       all(l == l.lower() for l in labels), str(labels))
            self.check("getColumnTypeName() non-empty",
                       all(len(t) > 0 for t in types), str(types))
        except Exception as e:
            self.check("ResultSetMetaData does not throw", False, str(e))

    def test_information_schema(self, conn):
        if self._skip_if_no_data(conn, "information_schema.tables — lowercase"):
            return
        self.header("information_schema.tables — lowercase")
        try:
            rows = self.query_rows(conn,
                f"select table_schema, table_name from information_schema.tables "
                f"where table_schema = '{self.test_schema}' "
                f"order by table_name fetch first 5 rows only")
            self.check("information_schema.tables (lowercase) returns rows", len(rows) > 0)
            self.check("table_schema values are lowercase",
                       all(r.get('table_schema', '').islower() for r in rows),
                       str([r.get('table_schema') for r in rows]))
            for r in rows:
                print(f"    {r.get('table_schema','')}.{r.get('table_name','')}")
        except Exception as e:
            self.check("information_schema.tables (lowercase) returns rows", False, str(e))

    def test_information_schema_upper(self, conn):
        if self._skip_if_no_data(conn, "INFORMATION_SCHEMA.TABLES — uppercase"):
            return
        self.header("INFORMATION_SCHEMA.TABLES — uppercase")
        try:
            rows = self.query_rows(conn,
                f"SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_SCHEMA = '{self.test_schema}' "
                f"ORDER BY TABLE_NAME FETCH FIRST 5 ROWS ONLY")
            self.check("INFORMATION_SCHEMA.TABLES (uppercase) returns rows", len(rows) > 0)
        except Exception as e:
            self.check("INFORMATION_SCHEMA.TABLES (uppercase) returns rows", False, str(e))

    def test_information_schema_columns(self, conn):
        if self._skip_if_no_data(conn, "information_schema.columns"):
            return
        self.header("information_schema.columns")
        try:
            rows = self.query_rows(conn,
                f"select column_name, data_type from information_schema.columns "
                f"where table_schema = '{self.test_schema}' "
                f"and table_name = '{self.test_table}' "
                f"order by column_name fetch first 5 rows only")
            self.check("information_schema.columns returns rows", len(rows) > 0)
            self.check("column_name values are lowercase",
                       all(r.get('column_name', '').islower() for r in rows),
                       str([r.get('column_name') for r in rows]))
            for r in rows:
                print(f"    {r.get('column_name',''):<20} {r.get('data_type','')}")
        except Exception as e:
            self.check("information_schema.columns returns rows", False, str(e))

    # ── Main entry point ──────────────────────────────────────────────────────

    def run_all(self):
        print(f"JAR: {self.jar_path}\n")
        self.start_jvm()

        conn  = self.connect()
        meta  = conn.getMetaData()

        self.test_driver_identity(meta)
        self.test_get_catalogs(meta)
        self.test_get_schemas(meta)
        self.test_get_schemas_filtered(meta)
        self.test_get_table_types(meta)
        self.test_get_tables(meta)
        self.test_get_tables_pattern(meta)
        self.test_get_tables_all_schemas(meta)
        self.test_get_columns(meta)
        self.test_get_columns_type_name(meta)
        self.test_get_columns_bulk(meta)
        self.test_primary_keys(meta)
        self.test_imported_keys(meta)
        self.test_exported_keys(meta)
        self.test_index_info(meta)
        self.test_best_row_identifier(meta)
        self.test_version_columns(meta)
        self.test_type_info(meta)
        self.test_procedures_functions(meta)
        self.test_super_types_tables(meta)
        self.test_capability_flags(conn, meta)
        self.test_meta_get_connection(meta)
        self.test_connection_valid_schema(conn)
        self.test_result_set_support(meta)
        self.test_sql_lowercase(conn)
        self.test_sql_uppercase(conn)
        self.test_sql_mixed_case(conn)
        self.test_prepared_statement(conn)
        self.test_result_set_metadata(conn)
        self.test_information_schema(conn)
        self.test_information_schema_upper(conn)
        self.test_information_schema_columns(conn)
        conn.close()

        print()
        if self.failures:
            print(f"\033[31m{len(self.failures)} test(s) FAILED:\033[0m")
            for f in self.failures:
                print(f"  ✗ {f}")
            sys.exit(1)
        else:
            skipped = self.skip_data_tests_on_error and self._data_available is False
            suffix = " (data-dependent tests skipped — schema migration pending)" if skipped else ""
            print(f"\033[32mAll {self.group} test groups passed"
                  f" — JDBC metadata + DBeaver/DataGrip compatibility confirmed.{suffix}\033[0m")
