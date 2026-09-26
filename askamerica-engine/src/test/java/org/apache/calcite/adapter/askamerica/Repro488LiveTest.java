package org.apache.calcite.adapter.askamerica;

import org.apache.calcite.adapter.govdata.GovDataDriver;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

@Tag("integration")
class Repro488LiveTest {

    private static Connection openHealthConnection() throws Exception {
        GovDataDriver driver = new GovDataDriver();
        Properties connProps = new Properties();
        connProps.setProperty("fun", "standard,postgresql,spatial,mssql,bigquery");
        connProps.setProperty("parserFactory",
            "org.apache.calcite.sql.parser.babel.SqlBabelParserImpl#FACTORY");
        return driver.connect("jdbc:govdata:source=health", connProps);
    }

    @Test void avgGroupByYear() throws Exception {
        try (Connection conn = openHealthConnection();
             Statement st = conn.createStatement()) {
            String sql = "SELECT year, AVG(age_adjusted_rate) FROM cdc_mortality "
                + "WHERE source_type = 'annual' GROUP BY year";
            try (ResultSet rs = st.executeQuery(sql)) {
                int n = 0;
                while (rs.next() && n < 5) {
                    System.out.println(rs.getString(1) + " -> " + rs.getDouble(2));
                    n++;
                }
            }
        }
    }

    @Test void castToDouble() throws Exception {
        try (Connection conn = openHealthConnection();
             Statement st = conn.createStatement()) {
            String sql = "SELECT CAST(age_adjusted_rate AS DOUBLE) FROM cdc_mortality "
                + "WHERE source_type = 'annual' LIMIT 5";
            try (ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    System.out.println(rs.getDouble(1));
                }
            }
        }
    }

    @Test void avgOfCaseWhenOverVarchar() throws Exception {
        try (Connection conn = openHealthConnection();
             Statement st = conn.createStatement()) {
            String sql = "SELECT AVG(CASE WHEN state IN ('California', 'Texas') THEN age_adjusted_rate END) "
                + "FROM cdc_mortality WHERE source_type = 'annual'";
            try (ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    System.out.println("case_avg -> " + rs.getDouble(1));
                }
            }
        }
    }
}
