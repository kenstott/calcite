/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * Tests for HooksConfig and related classes.
 */
@Tag("unit")
public class HooksConfigTest {

  @Test void testEmptyHooksConfig() {
    HooksConfig config = HooksConfig.empty();

    assertNull(config.getResponseTransformerClass());
    assertTrue(config.getRowTransformers().isEmpty());
    assertTrue(config.getValidators().isEmpty());
    assertNull(config.getDimensionResolverClass());
    assertFalse(config.hasHooks());
    assertNotNull(config.getErrorHandling());
  }

  @Test void testHooksConfigBuilder() {
    List<HooksConfig.TransformerConfig> transformers = new ArrayList<HooksConfig.TransformerConfig>();
    transformers.add(HooksConfig.TransformerConfig.ofClass("com.example.MyTransformer"));

    List<HooksConfig.ValidatorConfig> validators = new ArrayList<HooksConfig.ValidatorConfig>();
    validators.add(HooksConfig.ValidatorConfig.ofClass("com.example.MyValidator"));

    HooksConfig config = HooksConfig.builder()
        .responseTransformerClass("com.example.ResponseTransformer")
        .rowTransformers(transformers)
        .validators(validators)
        .dimensionResolverClass("com.example.DimensionResolver")
        .build();

    assertEquals("com.example.ResponseTransformer", config.getResponseTransformerClass());
    assertEquals(1, config.getRowTransformers().size());
    assertEquals(1, config.getValidators().size());
    assertEquals("com.example.DimensionResolver", config.getDimensionResolverClass());
    assertTrue(config.hasHooks());
  }

  @Test void testHooksConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("responseTransformer", "com.example.ResponseTransformer");
    map.put("dimensionResolver", "com.example.DimensionResolver");

    List<Map<String, Object>> rowTransformers = new ArrayList<Map<String, Object>>();
    Map<String, Object> transformerMap = new HashMap<String, Object>();
    transformerMap.put("type", "class");
    transformerMap.put("class", "com.example.RowTransformer");
    rowTransformers.add(transformerMap);
    map.put("rowTransformers", rowTransformers);

    List<Map<String, Object>> validators = new ArrayList<Map<String, Object>>();
    Map<String, Object> validatorMap = new HashMap<String, Object>();
    validatorMap.put("type", "expression");
    validatorMap.put("condition", "id IS NOT NULL");
    validatorMap.put("action", "drop");
    validators.add(validatorMap);
    map.put("validators", validators);

    HooksConfig config = HooksConfig.fromMap(map);

    assertEquals("com.example.ResponseTransformer", config.getResponseTransformerClass());
    assertEquals("com.example.DimensionResolver", config.getDimensionResolverClass());
    assertEquals(1, config.getRowTransformers().size());
    assertTrue(config.getRowTransformers().get(0).isClassBased());
    assertEquals("com.example.RowTransformer", config.getRowTransformers().get(0).getClassName());
    assertEquals(1, config.getValidators().size());
    assertTrue(config.getValidators().get(0).isExpressionBased());
    assertEquals("id IS NOT NULL", config.getValidators().get(0).getCondition());
    assertEquals("drop", config.getValidators().get(0).getAction());
  }

  @Test void testHooksConfigFromNullMap() {
    HooksConfig config = HooksConfig.fromMap(null);
    assertFalse(config.hasHooks());
  }

  @Test void testTransformerConfigOfClass() {
    HooksConfig.TransformerConfig config =
        HooksConfig.TransformerConfig.ofClass("com.example.MyTransformer");

    assertTrue(config.isClassBased());
    assertFalse(config.isExpressionBased());
    assertEquals("class", config.getType());
    assertEquals("com.example.MyTransformer", config.getClassName());
    assertNull(config.getColumn());
    assertNull(config.getExpression());
  }

  @Test void testTransformerConfigOfExpression() {
    HooksConfig.TransformerConfig config =
        HooksConfig.TransformerConfig.ofExpression("data_value", "UPPER(data_value)");

    assertFalse(config.isClassBased());
    assertTrue(config.isExpressionBased());
    assertEquals("expression", config.getType());
    assertNull(config.getClassName());
    assertEquals("data_value", config.getColumn());
    assertEquals("UPPER(data_value)", config.getExpression());
  }

  @Test void testTransformerConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "expression");
    map.put("column", "value");
    map.put("expression", "TRIM(value)");

    HooksConfig.TransformerConfig config = HooksConfig.TransformerConfig.fromMap(map);

    assertEquals("expression", config.getType());
    assertEquals("value", config.getColumn());
    assertEquals("TRIM(value)", config.getExpression());
  }

  @Test void testValidatorConfigOfClass() {
    HooksConfig.ValidatorConfig config =
        HooksConfig.ValidatorConfig.ofClass("com.example.MyValidator");

    assertTrue(config.isClassBased());
    assertFalse(config.isExpressionBased());
    assertEquals("class", config.getType());
    assertEquals("com.example.MyValidator", config.getClassName());
    assertNull(config.getCondition());
    assertNull(config.getAction());
  }

  @Test void testValidatorConfigOfExpression() {
    HooksConfig.ValidatorConfig config =
        HooksConfig.ValidatorConfig.ofExpression("geo_fips IS NOT NULL", "fail");

    assertFalse(config.isClassBased());
    assertTrue(config.isExpressionBased());
    assertEquals("expression", config.getType());
    assertNull(config.getClassName());
    assertEquals("geo_fips IS NOT NULL", config.getCondition());
    assertEquals("fail", config.getAction());
  }

  @Test void testValidatorConfigFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("type", "class");
    map.put("class", "com.example.DataValidator");

    HooksConfig.ValidatorConfig config = HooksConfig.ValidatorConfig.fromMap(map);

    assertEquals("class", config.getType());
    assertEquals("com.example.DataValidator", config.getClassName());
  }

  @Test void testHookErrorHandlingDefaults() {
    HooksConfig.HookErrorHandling handling = HooksConfig.HookErrorHandling.defaults();

    assertEquals(HooksConfig.HookErrorHandling.ErrorAction.FAIL,
        handling.getResponseTransformerAction());
    assertEquals(HooksConfig.HookErrorHandling.ErrorAction.SKIP_ROW,
        handling.getRowTransformerAction());
    assertEquals(HooksConfig.HookErrorHandling.ErrorAction.CONTINUE,
        handling.getValidatorAction());
  }

  @Test void testHookErrorHandlingFromMap() {
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("responseTransformer", "skip_row");
    map.put("rowTransformer", "fail");
    map.put("validator", "skip_row");

    HooksConfig.HookErrorHandling handling = HooksConfig.HookErrorHandling.fromMap(map);

    assertEquals(HooksConfig.HookErrorHandling.ErrorAction.SKIP_ROW,
        handling.getResponseTransformerAction());
    assertEquals(HooksConfig.HookErrorHandling.ErrorAction.FAIL,
        handling.getRowTransformerAction());
    assertEquals(HooksConfig.HookErrorHandling.ErrorAction.SKIP_ROW,
        handling.getValidatorAction());
  }

  @Test void testHooksConfigWithErrorHandling() {
    Map<String, Object> errorHandlingMap = new HashMap<String, Object>();
    errorHandlingMap.put("responseTransformer", "continue");
    errorHandlingMap.put("rowTransformer", "continue");
    errorHandlingMap.put("validator", "fail");

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("responseTransformer", "com.example.Transformer");
    map.put("errorHandling", errorHandlingMap);

    HooksConfig config = HooksConfig.fromMap(map);

    assertEquals(HooksConfig.HookErrorHandling.ErrorAction.CONTINUE,
        config.getErrorHandling().getResponseTransformerAction());
    assertEquals(HooksConfig.HookErrorHandling.ErrorAction.CONTINUE,
        config.getErrorHandling().getRowTransformerAction());
    assertEquals(HooksConfig.HookErrorHandling.ErrorAction.FAIL,
        config.getErrorHandling().getValidatorAction());
  }

  @Test void testHooksConfigToString() {
    HooksConfig config = HooksConfig.builder()
        .responseTransformerClass("com.example.ResponseTransformer")
        .dimensionResolverClass("com.example.DimensionResolver")
        .build();

    String toString = config.toString();
    assertTrue(toString.contains("ResponseTransformer"));
    assertTrue(toString.contains("DimensionResolver"));
  }

  @Test void testTransformerConfigToString() {
    HooksConfig.TransformerConfig classConfig =
        HooksConfig.TransformerConfig.ofClass("com.example.Transformer");
    assertTrue(classConfig.toString().contains("com.example.Transformer"));

    HooksConfig.TransformerConfig exprConfig =
        HooksConfig.TransformerConfig.ofExpression("col", "UPPER(col)");
    assertTrue(exprConfig.toString().contains("col"));
    assertTrue(exprConfig.toString().contains("UPPER(col)"));
  }

  @Test void testValidatorConfigToString() {
    HooksConfig.ValidatorConfig classConfig =
        HooksConfig.ValidatorConfig.ofClass("com.example.Validator");
    assertTrue(classConfig.toString().contains("com.example.Validator"));

    HooksConfig.ValidatorConfig exprConfig =
        HooksConfig.ValidatorConfig.ofExpression("id > 0", "warn");
    assertTrue(exprConfig.toString().contains("id > 0"));
    assertTrue(exprConfig.toString().contains("warn"));
  }

  @Test void testHasHooksWithOnlyResponseTransformer() {
    HooksConfig config = HooksConfig.builder()
        .responseTransformerClass("com.example.Transformer")
        .build();
    assertTrue(config.hasHooks());
  }

  @Test void testHasHooksWithOnlyRowTransformers() {
    List<HooksConfig.TransformerConfig> transformers = new ArrayList<HooksConfig.TransformerConfig>();
    transformers.add(HooksConfig.TransformerConfig.ofClass("com.example.Transformer"));

    HooksConfig config = HooksConfig.builder()
        .rowTransformers(transformers)
        .build();
    assertTrue(config.hasHooks());
  }

  @Test void testHasHooksWithOnlyValidators() {
    List<HooksConfig.ValidatorConfig> validators = new ArrayList<HooksConfig.ValidatorConfig>();
    validators.add(HooksConfig.ValidatorConfig.ofClass("com.example.Validator"));

    HooksConfig config = HooksConfig.builder()
        .validators(validators)
        .build();
    assertTrue(config.hasHooks());
  }

  @Test void testHasHooksWithOnlyDimensionResolver() {
    HooksConfig config = HooksConfig.builder()
        .dimensionResolverClass("com.example.Resolver")
        .build();
    assertTrue(config.hasHooks());
  }
}
