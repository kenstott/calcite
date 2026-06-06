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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link PostProcessConfig}.
 */
@Tag("unit")
class PostProcessConfigTest {

  @Test void testBuilderBasic() {
    PostProcessConfig config = PostProcessConfig.builder()
        .name("gpu_embeddings")
        .script("scripts/runner.sh")
        .addArg("--table")
        .addArg("test")
        .build();

    assertEquals("gpu_embeddings", config.getName());
    assertEquals("scripts/runner.sh", config.getScript());
    assertEquals(2, config.getArgs().size());
    assertEquals("--table", config.getArgs().get(0));
    assertEquals("test", config.getArgs().get(1));
  }

  @Test void testBuilderDefaults() {
    PostProcessConfig config = PostProcessConfig.builder()
        .script("scripts/test.sh")
        .build();

    assertEquals("post_process", config.getName());
    assertEquals(PostProcessConfig.OnFailure.ERROR, config.getOnFailure());
    assertEquals(PostProcessConfig.RunAfter.ALL_PARTITIONS, config.getRunAfter());
    assertTrue(config.getArgs().isEmpty());
    assertTrue(config.getDependsOn().isEmpty());
    assertNull(config.getCondition());
    assertFalse(config.isAsync());
  }

  @Test void testBuilderAllFields() {
    PostProcessConfig config = PostProcessConfig.builder()
        .name("vss_refresh")
        .script("scripts/vss.sh")
        .args(Arrays.asList("refresh", "--force"))
        .onFailure(PostProcessConfig.OnFailure.WARN)
        .runAfter(PostProcessConfig.RunAfter.EACH_PARTITION)
        .dependsOn(Arrays.asList("vectorized_chunks"))
        .addDependsOn("embeddings")
        .condition("${ENABLE_VSS:-true}")
        .async(true)
        .build();

    assertEquals("vss_refresh", config.getName());
    assertEquals("scripts/vss.sh", config.getScript());
    assertEquals(2, config.getArgs().size());
    assertEquals(PostProcessConfig.OnFailure.WARN, config.getOnFailure());
    assertEquals(PostProcessConfig.RunAfter.EACH_PARTITION, config.getRunAfter());
    assertEquals(2, config.getDependsOn().size());
    assertEquals("${ENABLE_VSS:-true}", config.getCondition());
    assertTrue(config.isAsync());
  }

  @Test void testBuilderMissingScriptThrows() {
    assertThrows(IllegalStateException.class, () ->
        PostProcessConfig.builder()
            .name("test")
            .build());
  }

  @Test void testBuilderEmptyScriptThrows() {
    assertThrows(IllegalStateException.class, () ->
        PostProcessConfig.builder()
            .name("test")
            .script("")
            .build());
  }

  @Test void testFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("name", "gpu_embeddings");
    map.put("script", "scripts/runner.sh");
    map.put("args", Arrays.asList("--years", "${year}"));
    map.put("onFailure", "warn");
    map.put("runAfter", "each_partition");
    map.put("dependsOn", Arrays.asList("vectorized_chunks"));
    map.put("condition", "${ENABLE_GPU}");
    map.put("async", true);

    PostProcessConfig config = PostProcessConfig.fromMap(map);
    assertNotNull(config);
    assertEquals("gpu_embeddings", config.getName());
    assertEquals("scripts/runner.sh", config.getScript());
    assertEquals(2, config.getArgs().size());
    assertEquals(PostProcessConfig.OnFailure.WARN, config.getOnFailure());
    assertEquals(PostProcessConfig.RunAfter.EACH_PARTITION, config.getRunAfter());
    assertEquals(1, config.getDependsOn().size());
    assertEquals("${ENABLE_GPU}", config.getCondition());
    assertTrue(config.isAsync());
  }

  @Test void testFromMapOnFailureError() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("script", "scripts/test.sh");
    map.put("onFailure", "error");

    PostProcessConfig config = PostProcessConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(PostProcessConfig.OnFailure.ERROR, config.getOnFailure());
  }

  @Test void testFromMapRunAfterAllPartitions() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("script", "scripts/test.sh");
    map.put("runAfter", "all_partitions");

    PostProcessConfig config = PostProcessConfig.fromMap(map);
    assertNotNull(config);
    assertEquals(PostProcessConfig.RunAfter.ALL_PARTITIONS, config.getRunAfter());
  }

  @Test void testFromMapAsyncString() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("script", "scripts/test.sh");
    map.put("async", "true");

    PostProcessConfig config = PostProcessConfig.fromMap(map);
    assertNotNull(config);
    assertTrue(config.isAsync());
  }

  @Test void testFromMapNull() {
    assertNull(PostProcessConfig.fromMap(null));
  }

  @Test void testToString() {
    PostProcessConfig config = PostProcessConfig.builder()
        .name("test")
        .script("scripts/test.sh")
        .addArg("--force")
        .async(true)
        .build();

    String str = config.toString();
    assertTrue(str.contains("test"));
    assertTrue(str.contains("scripts/test.sh"));
    assertTrue(str.contains("async=true"));
  }

  @Test void testArgsAreImmutable() {
    PostProcessConfig config = PostProcessConfig.builder()
        .script("scripts/test.sh")
        .addArg("arg1")
        .build();

    try {
      config.getArgs().add("arg2");
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }

  @Test void testDependsOnAreImmutable() {
    PostProcessConfig config = PostProcessConfig.builder()
        .script("scripts/test.sh")
        .addDependsOn("dep1")
        .build();

    try {
      config.getDependsOn().add("dep2");
      assertTrue(false, "Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
  }
}
