package org.apache.calcite.adapter.govdata;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Debug test to investigate information_schema availability
 */
@Tag("integration")
public class InformationSchemaDebugTest {

    @Test
    public void debugInformationSchemaForGeo() throws Exception {
        String model = String.format(
            "inline:{\"version\":\"1.0\",\"defaultSchema\":\"GEO\"," +
            "\"schemas\":[{\"name\":\"GEO\",\"type\":\"custom\"," +
            "\"factory\":\"%s\",\"operand\":{" +
            "\"dataSource\":\"geo\"," +
            "\"autoDownload\":true," +
            "\"startYear\":2024,\"endYear\":2024," +
            "\"cacheDir\":\"/Volumes/T9/govdata-cache/geo\"," +
            "\"parquetDir\":\"/Volumes/T9/govdata-parquet/source=geo\"," +
            "\"fileEngineType\":\"DUCKDB\"" +
            "}}]}",
            GovDataSchemaFactory.class.getName()
        );

        try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=" + model)) {
            DatabaseMetaData metadata = conn.getMetaData();

            System.out.println("\n=== DATABASE METADATA ===");
            System.out.println("Database Product: " + metadata.getDatabaseProductName());
            System.out.println("Database Version: " + metadata.getDatabaseProductVersion());

            // List all schemas
            System.out.println("\n=== ALL SCHEMAS ===");
            try (ResultSet schemas = metadata.getSchemas()) {
                while (schemas.next()) {
                    System.out.println("Schema: " + schemas.getString("TABLE_SCHEM"));
                }
            }

            // Check if information_schema exists
            System.out.println("\n=== CHECKING INFORMATION_SCHEMA ===");
            try (Statement stmt = conn.createStatement()) {
                // Try to list tables in information_schema
                try {
                    String query = "SELECT * FROM information_schema.tables WHERE 1=0";
                    stmt.executeQuery(query);
                    System.out.println("✓ information_schema.tables exists");
                } catch (Exception e) {
                    System.out.println("✗ information_schema.tables failed: " + e.getMessage());
                }

                // Try TABLE_CONSTRAINTS
                try {
                    String query = "SELECT * FROM information_schema.TABLE_CONSTRAINTS WHERE 1=0";
                    stmt.executeQuery(query);
                    System.out.println("✓ information_schema.TABLE_CONSTRAINTS exists");
                } catch (Exception e) {
                    System.out.println("✗ information_schema.TABLE_CONSTRAINTS failed: " + e.getMessage());
                }

                // Try with quotes
                try {
                    String query = "SELECT * FROM information_schema.\"TABLE_CONSTRAINTS\" WHERE 1=0";
                    ResultSet rs = stmt.executeQuery(query);
                    ResultSetMetaData rsmd = rs.getMetaData();
                    System.out.println("✓ information_schema.\"TABLE_CONSTRAINTS\" exists");
                    System.out.println("  Column count: " + rsmd.getColumnCount());
                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                        System.out.println("  Column " + i + ": " + rsmd.getColumnName(i));
                    }
                } catch (Exception e) {
                    System.out.println("✗ information_schema.\"TABLE_CONSTRAINTS\" failed: " + e.getMessage());
                }
            }

            // Use JDBC metadata to get constraints
            System.out.println("\n=== JDBC METADATA CONSTRAINTS ===");

            // Get primary keys for states table
            try (ResultSet pks = metadata.getPrimaryKeys(null, "GEO", "states")) {
                System.out.println("Primary keys for GEO.states:");
                while (pks.next()) {
                    System.out.println("  Column: " + pks.getString("COLUMN_NAME") +
                                     ", Key Name: " + pks.getString("PK_NAME"));
                }
            }

            // Get foreign keys
            try (ResultSet fks = metadata.getImportedKeys(null, "GEO", "counties")) {
                System.out.println("Foreign keys for GEO.counties:");
                while (fks.next()) {
                    System.out.println("  FK Column: " + fks.getString("FKCOLUMN_NAME") +
                                     " -> " + fks.getString("PKTABLE_NAME") + "." +
                                     fks.getString("PKCOLUMN_NAME"));
                }
            }
        }
    }
}