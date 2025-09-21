import org.apache.calcite.adapter.file.FileSchemaFactory;
import java.sql.*;
import java.util.Properties;

public class TestEconDebug {
    public static void main(String[] args) throws Exception {
        String modelJson =
            "{" +
            "  \"version\": \"1.0\"," +
            "  \"schemas\": [" +
            "    {" +
            "      \"name\": \"econ\"," +
            "      \"type\": \"custom\"," +
            "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\"," +
            "      \"operand\": {" +
            "        \"dataSource\": \"econ\"," +
            "        \"executionEngine\": \"DUCKDB\"," +
            "        \"autoDownload\": false" +
            "      }" +
            "    }" +
            "  ]" +
            "}";

        java.nio.file.Path modelFile = java.nio.file.Files.createTempFile("econ-debug", ".json");
        java.nio.file.Files.write(modelFile, modelJson.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        Properties props = new Properties();
        props.setProperty("lex", "ORACLE");
        props.setProperty("unquotedCasing", "TO_LOWER");

        try (Connection conn = DriverManager.getConnection("jdbc:calcite:model=" + modelFile, props)) {
            try (Statement stmt = conn.createStatement()) {
                // List all tables
                ResultSet tables = stmt.executeQuery(
                    "SELECT \"TABLE_NAME\" FROM information_schema.\"TABLES\" " +
                    "WHERE \"TABLE_SCHEMA\" = 'econ' " +
                    "ORDER BY \"TABLE_NAME\"");
                System.out.println("Tables found:");
                while (tables.next()) {
                    System.out.println("  - " + tables.getString("TABLE_NAME"));
                }
                tables.close();
            }
        }
    }
}