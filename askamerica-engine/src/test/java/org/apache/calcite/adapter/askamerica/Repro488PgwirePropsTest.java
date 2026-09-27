package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * D-488: reproduces the pgwire-govdata server's actual connection setup —
 * {@code CalciteBackend._connect()} against the full 26-schema
 * {@code pgwire-govdata/model.json} (every schema sharing one DuckDB catalog file), the same
 * shape {@code PgwireGovDataConnector.spawnIfPossible} produces — rather than the single-schema
 * {@code jdbc:govdata:source=health} connection GovDataDriver opens. The reported AVG/CAST
 * validator crashes traced to {@link org.apache.calcite.adapter.file.duckdb.DuckDBJdbcSchemaFactory}
 * racing concurrent {@code createSchemaInSharedDatabase} calls across the 26 schemas on first
 * connect, intermittently leaving one schema's DuckDB registration in a corrupt state; which
 * schema loses the race is nondeterministic; a losing health.cdc_mortality is presumed to be
 * why AVG/CAST validation only failed for some sessions.
 */
@Tag("integration")
class Repro488PgwirePropsTest {

    private static Connection openPgwireStyleConnection() throws Exception {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties props = new Properties();
        props.setProperty("model", "/home/kstott/calcite/pgwire-govdata/model.json");
        props.setProperty("schema", "health");
        props.setProperty("lex", "ORACLE");
        props.setProperty("fun", "standard,postgresql,spatial,mssql,bigquery");
        props.setProperty("caseSensitive", "false");
        props.setProperty("parserFactory",
            "org.apache.calcite.sql.parser.babel.SqlBabelParserImpl#FACTORY");
        return DriverManager.getConnection("jdbc:calcite:", props);
    }

    @Test void avgGroupByYearQualified() throws Exception {
        try (Connection conn = openPgwireStyleConnection();
             Statement st = conn.createStatement()) {
            String sql = "SELECT year, AVG(age_adjusted_rate) FROM health.cdc_mortality "
                + "WHERE source_type='annual' GROUP BY year";
            try (ResultSet rs = st.executeQuery(sql)) {
                assertTrue(rs.next(), "expected at least one grouped row");
            }
        }
    }

    @Test void castToDoubleQualified() throws Exception {
        try (Connection conn = openPgwireStyleConnection();
             Statement st = conn.createStatement()) {
            String sql = "SELECT CAST(age_adjusted_rate AS DOUBLE) FROM health.cdc_mortality "
                + "WHERE source_type='annual' LIMIT 5";
            try (ResultSet rs = st.executeQuery(sql)) {
                assertTrue(rs.next(), "expected at least one row");
            }
        }
    }

    @Test void avgOfCaseWhenOverVarcharQualified() throws Exception {
        try (Connection conn = openPgwireStyleConnection();
             Statement st = conn.createStatement()) {
            String sql = "SELECT AVG(CASE WHEN state IN ('California', 'Texas') "
                + "THEN age_adjusted_rate END) "
                + "FROM health.cdc_mortality WHERE source_type='annual'";
            try (ResultSet rs = st.executeQuery(sql)) {
                assertTrue(rs.next(), "expected one aggregate row");
            }
        }
    }

    @Test void schemaQualifiedSelectAgainstFullModel() throws Exception {
        try (Connection conn = openPgwireStyleConnection();
             Statement st = conn.createStatement()) {
            String sql = "SELECT age_adjusted_rate FROM health.cdc_mortality "
                + "WHERE source_type='annual' LIMIT 5";
            try (ResultSet rs = st.executeQuery(sql)) {
                assertTrue(rs.next(), "expected at least one row");
            }
        }
    }
}
