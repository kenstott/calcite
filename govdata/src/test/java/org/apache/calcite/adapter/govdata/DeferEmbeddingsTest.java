package org.apache.calcite.adapter.govdata;

import org.apache.calcite.adapter.file.etl.PostProcessConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for deferEmbeddings and PostProcessConfig functionality.
 */
@Tag("unit")
public class DeferEmbeddingsTest {

    @Test
    void testPostProcessConfigFromMap() {
        Map<String, Object> config = new HashMap<>();
        config.put("name", "gpu_embeddings");
        config.put("script", "scripts/vss-gpu-runner.sh");
        config.put("args", java.util.Arrays.asList("--years", "${year}"));
        config.put("onFailure", "error");

        PostProcessConfig ppConfig = PostProcessConfig.fromMap(config);

        assertNotNull(ppConfig);
        assertEquals("gpu_embeddings", ppConfig.getName());
        assertEquals("scripts/vss-gpu-runner.sh", ppConfig.getScript());
        assertEquals(2, ppConfig.getArgs().size());
        assertEquals("--years", ppConfig.getArgs().get(0));
        assertEquals("${year}", ppConfig.getArgs().get(1));
        assertEquals(PostProcessConfig.OnFailure.ERROR, ppConfig.getOnFailure());
    }

    @Test
    void testPostProcessConfigBuilder() {
        PostProcessConfig config = PostProcessConfig.builder()
            .name("test_process")
            .script("/path/to/script.sh")
            .addArg("--table")
            .addArg("${table}")
            .onFailure(PostProcessConfig.OnFailure.WARN)
            .runAfter(PostProcessConfig.RunAfter.ALL_PARTITIONS)
            .build();

        assertEquals("test_process", config.getName());
        assertEquals("/path/to/script.sh", config.getScript());
        assertEquals(2, config.getArgs().size());
        assertEquals(PostProcessConfig.OnFailure.WARN, config.getOnFailure());
        assertEquals(PostProcessConfig.RunAfter.ALL_PARTITIONS, config.getRunAfter());
    }

    @Test
    void testPostProcessConfigRequiresScript() {
        assertThrows(IllegalStateException.class, () -> {
            PostProcessConfig.builder()
                .name("no_script")
                .build();
        });
    }

    @Test
    void testComputedColumnsFilteringForEmbeddings() {
        // Simulate the deferEmbeddings logic from SecSchemaFactory
        Map<String, String> computedColumns = new java.util.LinkedHashMap<>();
        computedColumns.put("embedding", "embed(enriched_text)::FLOAT[384]");
        computedColumns.put("full_name", "first_name || ' ' || last_name");
        computedColumns.put("jina_embedding", "embed_jina(text)::FLOAT[768]");

        boolean deferEmbeddings = true;
        
        if (deferEmbeddings && !computedColumns.isEmpty()) {
            Map<String, String> filteredColumns = new java.util.LinkedHashMap<>();
            for (Map.Entry<String, String> entry : computedColumns.entrySet()) {
                if (!entry.getValue().contains("embed(") && !entry.getValue().contains("embed_jina")) {
                    filteredColumns.put(entry.getKey(), entry.getValue());
                }
            }
            computedColumns = filteredColumns;
        }

        // Should only keep non-embedding columns
        assertEquals(1, computedColumns.size());
        assertTrue(computedColumns.containsKey("full_name"));
        assertFalse(computedColumns.containsKey("embedding"));
        assertFalse(computedColumns.containsKey("jina_embedding"));
    }
}
