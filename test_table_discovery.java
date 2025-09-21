import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class test_table_discovery {
    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        
        try (InputStream is = test_table_discovery.class.getResourceAsStream("/econ-schema.json")) {
            Map<String, Object> schema = mapper.readValue(is, Map.class);
            List<Map<String, Object>> tables = (List<Map<String, Object>>) schema.get("partitionedTables");
            
            System.out.println("Total tables defined: " + tables.size());
            
            for (Map<String, Object> table : tables) {
                String name = (String) table.get("name");
                String pattern = (String) table.get("pattern");
                if ("ita_data".equals(name)) {
                    System.out.println("\nFound ita_data table:");
                    System.out.println("  Pattern: " + pattern);
                    System.out.println("  Full config: " + table);
                }
            }
        }
    }
}
