import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.govdata.econ.EconSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import java.util.Map;
import java.util.HashMap;

public class test_ita_discovery {
    public static void main(String[] args) throws Exception {
        // Set up operand map for EconSchemaFactory
        Map<String, Object> operandMap = new HashMap<>();
        operandMap.put("source", "econ");
        operandMap.put("cache", "/Volumes/T9/govdata-cache/econ");
        operandMap.put("parquet", "/Volumes/T9/govdata-parquet/source=econ");
        operandMap.put("storageProviderClass", "org.apache.calcite.adapter.file.storage.LocalFileStorageProvider");
        
        // Create the schema
        EconSchemaFactory factory = new EconSchemaFactory();
        Schema schema = factory.create(null, "econ", operandMap);
        
        // Try to get ita_data table
        Table itaTable = schema.getTable("ita_data");
        if (itaTable != null) {
            System.out.println("ita_data table FOUND!");
            System.out.println("Table type: " + itaTable.getClass().getName());
        } else {
            System.out.println("ita_data table NOT FOUND");
            
            // List all tables
            System.out.println("\nAll available tables:");
            for (String tableName : schema.getTableNames()) {
                System.out.println("  - " + tableName);
            }
        }
    }
}
