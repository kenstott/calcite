/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 *
 * NOTICE: Use of this software for training artificial intelligence or
 * machine learning models is strictly prohibited without explicit written
 * permission from the copyright holder.
 */
package org.apache.calcite.adapter.file.etl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deep tests for {@link HooksConfig}.
 */
@Tag("unit")
class HooksConfigDeepTest {

  @Test void testHooksConfigBuilder() {
    HooksConfig config = HooksConfig.builder()
        .dimensionResolverClass("org.example.MyDimensionResolver")
        .responseTransformerClass("org.example.MyResponseTransformer")
        .variableNormalizerClass("org.example.MyNormalizer")
        .build();

    assertEquals("org.example.MyDimensionResolver", config.getDimensionResolverClass());
    assertEquals("org.example.MyResponseTransformer", config.getResponseTransformerClass());
    assertEquals("org.example.MyNormalizer", config.getVariableNormalizerClass());
  }

  @Test void testHooksConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("dimensionResolver", "org.example.DimResolver");
    map.put("responseTransformer", "org.example.RespTransformer");
    map.put("variableNormalizer", "org.example.VarNormalizer");

    Map<String, Object> normalizerConfig = new HashMap<String, Object>();
    normalizerConfig.put("mappingFile", "census-mappings.json");
    normalizerConfig.put("defaultType", "acs");
    map.put("variableNormalizerConfig", normalizerConfig);

    HooksConfig config = HooksConfig.fromMap(map);

    assertNotNull(config);
    assertEquals("org.example.DimResolver", config.getDimensionResolverClass());
    assertEquals("org.example.RespTransformer", config.getResponseTransformerClass());
    assertEquals("org.example.VarNormalizer", config.getVariableNormalizerClass());
    assertNotNull(config.getVariableNormalizerConfig());
    assertEquals("census-mappings.json", config.getVariableNormalizerConfig().get("mappingFile"));
  }

  @Test void testHooksConfigFromMapNull() {
    HooksConfig config = HooksConfig.fromMap(null);
    assertNotNull(config);
    assertNull(config.getDimensionResolverClass());
  }

  @Test void testHooksConfigFromEmptyMap() {
    HooksConfig config = HooksConfig.fromMap(new HashMap<String, Object>());
    assertNotNull(config);
    assertNull(config.getDimensionResolverClass());
    assertNull(config.getResponseTransformerClass());
  }

  @Test void testHooksConfigDefaults() {
    HooksConfig config = HooksConfig.builder().build();
    assertNull(config.getDimensionResolverClass());
    assertNull(config.getResponseTransformerClass());
    assertNull(config.getVariableNormalizerClass());
    assertNotNull(config.getRowTransformers());
    assertTrue(config.getRowTransformers().isEmpty());
    assertNotNull(config.getValidators());
    assertTrue(config.getValidators().isEmpty());
    assertNotNull(config.getPostProcess());
    assertTrue(config.getPostProcess().isEmpty());
  }

  @Test void testHooksConfigWithRowTransformers() {
    Map<String, Object> transformerMap = new HashMap<String, Object>();
    transformerMap.put("type", "class");
    transformerMap.put("class", "org.example.MyTransformer");

    Map<String, Object> map = new HashMap<String, Object>();
    List<Map<String, Object>> transformers = new ArrayList<Map<String, Object>>();
    transformers.add(transformerMap);
    map.put("rowTransformers", transformers);

    HooksConfig config = HooksConfig.fromMap(map);
    assertNotNull(config);
    assertFalse(config.getRowTransformers().isEmpty());
  }

  @Test void testHooksConfigWithValidators() {
    Map<String, Object> validatorMap = new HashMap<String, Object>();
    validatorMap.put("type", "expression");
    validatorMap.put("condition", "value > 0");
    validatorMap.put("action", "drop");

    Map<String, Object> map = new HashMap<String, Object>();
    List<Map<String, Object>> validators = new ArrayList<Map<String, Object>>();
    validators.add(validatorMap);
    map.put("validators", validators);

    HooksConfig config = HooksConfig.fromMap(map);
    assertNotNull(config);
    assertFalse(config.getValidators().isEmpty());
  }

  @Test void testHooksConfigWithPostProcesses() {
    Map<String, Object> postProcess = new HashMap<String, Object>();
    postProcess.put("name", "gpu_embeddings");
    postProcess.put("script", "scripts/vss-gpu-runner.sh");
    postProcess.put("args", Arrays.asList("--table", "${table}"));
    postProcess.put("runAfter", "all_partitions");
    postProcess.put("onFailure", "warn");

    Map<String, Object> map = new HashMap<String, Object>();
    List<Map<String, Object>> postProcesses = new ArrayList<Map<String, Object>>();
    postProcesses.add(postProcess);
    map.put("postProcess", postProcesses);

    HooksConfig config = HooksConfig.fromMap(map);
    assertNotNull(config);
    assertFalse(config.getPostProcess().isEmpty());
  }

  @Test void testHooksConfigWithTableLifecycleListener() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("tableLifecycleListener", "org.example.MyTableListener");

    HooksConfig config = HooksConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("org.example.MyTableListener", config.getTableLifecycleListenerClass());
  }

  // --- PostProcessConfig tests ---

  @Test void testPostProcessConfigBuilder() {
    PostProcessConfig config = PostProcessConfig.builder()
        .name("test_post")
        .script("scripts/test.sh")
        .args(Arrays.asList("--year", "${year}"))
        .onFailure(PostProcessConfig.OnFailure.WARN)
        .runAfter(PostProcessConfig.RunAfter.ALL_PARTITIONS)
        .async(false)
        .build();

    assertEquals("test_post", config.getName());
    assertEquals("scripts/test.sh", config.getScript());
    assertEquals(2, config.getArgs().size());
    assertEquals(PostProcessConfig.OnFailure.WARN, config.getOnFailure());
    assertEquals(PostProcessConfig.RunAfter.ALL_PARTITIONS, config.getRunAfter());
    assertFalse(config.isAsync());
  }

  @Test void testPostProcessConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "my_process");
    map.put("script", "run.sh");
    map.put("args", Arrays.asList("arg1", "arg2"));
    map.put("onFailure", "error");
    map.put("runAfter", "each_partition");
    map.put("async", true);

    PostProcessConfig config = PostProcessConfig.fromMap(map);

    assertEquals("my_process", config.getName());
    assertEquals("run.sh", config.getScript());
    assertEquals(PostProcessConfig.OnFailure.ERROR, config.getOnFailure());
    assertEquals(PostProcessConfig.RunAfter.EACH_PARTITION, config.getRunAfter());
    assertTrue(config.isAsync());
  }

  @Test void testPostProcessConfigDefaults() {
    PostProcessConfig config = PostProcessConfig.builder()
        .name("minimal")
        .script("test.sh")
        .build();

    assertEquals(PostProcessConfig.OnFailure.ERROR, config.getOnFailure());
    assertEquals(PostProcessConfig.RunAfter.ALL_PARTITIONS, config.getRunAfter());
    assertFalse(config.isAsync());
    assertNotNull(config.getArgs());
    assertTrue(config.getArgs().isEmpty());
    assertNotNull(config.getDependsOn());
    assertTrue(config.getDependsOn().isEmpty());
  }

  @Test void testPostProcessConfigWithDependsOn() {
    PostProcessConfig config = PostProcessConfig.builder()
        .name("dependent")
        .script("test.sh")
        .dependsOn(Arrays.asList("task1", "task2"))
        .build();

    assertEquals(2, config.getDependsOn().size());
    assertTrue(config.getDependsOn().contains("task1"));
    assertTrue(config.getDependsOn().contains("task2"));
  }

  @Test void testPostProcessConfigWithCondition() {
    PostProcessConfig config = PostProcessConfig.builder()
        .name("conditional")
        .script("test.sh")
        .condition("${ENABLE_POSTPROCESS:false}")
        .build();

    assertEquals("${ENABLE_POSTPROCESS:false}", config.getCondition());
  }

  @Test void testPostProcessOnFailureEnum() {
    assertEquals(PostProcessConfig.OnFailure.WARN,
        PostProcessConfig.OnFailure.valueOf("WARN"));
    assertEquals(PostProcessConfig.OnFailure.ERROR,
        PostProcessConfig.OnFailure.valueOf("ERROR"));
  }

  @Test void testPostProcessRunAfterEnum() {
    assertEquals(PostProcessConfig.RunAfter.EACH_PARTITION,
        PostProcessConfig.RunAfter.valueOf("EACH_PARTITION"));
    assertEquals(PostProcessConfig.RunAfter.ALL_PARTITIONS,
        PostProcessConfig.RunAfter.valueOf("ALL_PARTITIONS"));
  }

  // --- FileSourceConfig tests ---

  @Test void testFileSourceConfigBuilder() {
    FileSourceConfig config = FileSourceConfig.builder()
        .path("data/${year}/report.xlsx")
        .format("xlsx")
        .sheet("Sheet1")
        .build();

    assertEquals("data/${year}/report.xlsx", config.getPath());
    assertEquals("xlsx", config.getFormat());
    assertEquals("Sheet1", config.getSheet());
  }

  @Test void testFileSourceConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("path", "s3://bucket/data.csv");
    map.put("format", "csv");

    FileSourceConfig config = FileSourceConfig.fromMap(map);

    assertEquals("s3://bucket/data.csv", config.getPath());
    assertEquals("csv", config.getFormat());
  }

  @Test void testFileSourceConfigDefaults() {
    FileSourceConfig config = FileSourceConfig.builder()
        .path("/data/file.parquet")
        .build();

    assertNotNull(config.getPath());
    assertNull(config.getFormat());
    assertNull(config.getSheet());
  }
}
