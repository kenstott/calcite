package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to see what actually works
 */
@Tag("integration")
public class ActualConstraintTest {

    @Test
    public void testWhatActuallyWorks() throws Exception {
        // Test with GEO
        testSchema("geo");

        // Test with ECON
        testSchema("econ");
    }

    private void testSchema(String schemaName) throws Exception {
        System.out.println("\n========== TESTING " + schemaName.toUpperCase() + " ==========");

        String model = String.format(
            "inline:{\"version\":\"1.0\",\"defaultSchema\":\"%s\"," +
            "\"schemas\":[{\"name\":\"%s\",\"type\":\"custom\"," +
            "\"factory\":\"%s\",\"operand\":{" +
            "\"dataSource\":\"%s\"," +
            "\"autoDownload\":true," +
            "\"startYear\":2024,\"endYear\":2024," +
            "\"cacheDir\":\"/Volumes/T9/govdata-cache/%s\"," +
            "\"parquetDir\":\"/Volumes/T9/govdata-parquet/source=%s\"," +
            "\"fileEngineType\":\"DUCKDB\"" +
            "}}]}",
            schemaName, schemaName,
            GovDataSchemaFactory.class.getName(),
            schemaName, schemaName, schemaName
        );

        try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=" + model)) {
            try (Statement stmt = conn.createStatement()) {

                // First, see what's in information_schema.TABLE_CONSTRAINTS
                System.out.println("\n1. Testing information_schema.TABLE_CONSTRAINTS structure:");
                try {
                    ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM information_schema.\"TABLE_CONSTRAINTS\" WHERE 1=0");
                    ResultSetMetaData md = rs.getMetaData();
                    System.out.println("   Columns in TABLE_CONSTRAINTS:");
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        System.out.println("   - " + md.getColumnName(i) + " (" + md.getColumnTypeName(i) + ")");
                    }
                    rs.close();
                } catch (Exception e) {
                    System.out.println("   ERROR: " + e.getMessage());
                }

                // Try a simple count
                System.out.println("\n2. Counting constraints for schema '" + schemaName + "':");
                try {
                    ResultSet rs = stmt.executeQuery(
                        "SELECT COUNT(*) as cnt FROM information_schema.\"TABLE_CONSTRAINTS\"");
                    if (rs.next()) {
                        System.out.println("   Total constraints in information_schema: " + rs.getInt("cnt"));
                    }
                    rs.close();
                } catch (Exception e) {
                    System.out.println("   ERROR: " + e.getMessage());
                }

                // Try without WHERE clause
                System.out.println("\n3. First 5 constraints (no WHERE):");
                try {
                    ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM information_schema.\"TABLE_CONSTRAINTS\" LIMIT 5");
                    ResultSetMetaData md = rs.getMetaData();
                    int rowCount = 0;
                    while (rs.next()) {
                        rowCount++;
                        System.out.print("   Row " + rowCount + ": ");
                        for (int i = 1; i <= md.getColumnCount(); i++) {
                            System.out.print(md.getColumnName(i) + "=" + rs.getString(i) + " ");
                        }
                        System.out.println();
                    }
                    if (rowCount == 0) {
                        System.out.println("   No rows found");
                    }
                    rs.close();
                } catch (Exception e) {
                    System.out.println("   ERROR: " + e.getMessage());
                }
            }
        }
    }
}