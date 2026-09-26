package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

@Tag("unit")
class Repro488Test {
    private static Connection openBareCalciteConnection() throws Exception {
        return DriverManager.getConnection("jdbc:calcite:", new Properties());
    }

    @Test void avgOfCastVarcharToDoubleInAggregate() throws Exception {
        try (Connection conn = openBareCalciteConnection();
             Statement st = conn.createStatement()) {
            String sql = "SELECT grp, AVG(CAST(val AS DOUBLE)) FROM "
                + "(VALUES ('a', CAST('1' AS VARCHAR)), ('a', CAST('2' AS VARCHAR)), "
                + "('b', CAST('10' AS VARCHAR))) AS t(grp, val) GROUP BY grp";
            try (ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    System.out.println(rs.getString(1) + " -> " + rs.getDouble(2));
                }
            }
        }
    }

    @Test void avgDirectlyOnVarcharColumn() throws Exception {
        try (Connection conn = openBareCalciteConnection();
             Statement st = conn.createStatement()) {
            String sql = "SELECT grp, AVG(val) FROM "
                + "(VALUES ('a', CAST('1' AS VARCHAR)), ('a', CAST('2' AS VARCHAR)), "
                + "('b', CAST('10' AS VARCHAR))) AS t(grp, val) GROUP BY grp";
            try (ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) {
                    System.out.println(rs.getString(1) + " -> " + rs.getDouble(2));
                }
            }
        }
    }
}
