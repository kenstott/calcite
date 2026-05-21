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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for {@link VariableResolver}.
 */
@Tag("unit")
class VariableResolverTest {

  @Test void testSubstituteSimpleVariable() {
    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");
    String result = VariableResolver.substitute("data/{year}/file.csv", variables);
    assertEquals("data/2024/file.csv", result);
  }

  @Test void testSubstituteMultipleVariables() {
    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");
    variables.put("region", "NORTH");
    String result =
        VariableResolver.substitute("https://api.example.com/{year}/{region}/data", variables);
    assertEquals("https://api.example.com/2024/NORTH/data", result);
  }

  @Test void testSubstituteNullTemplate() {
    String result = VariableResolver.substitute(null, Collections.<String, String>emptyMap());
    assertNull(result);
  }

  @Test void testSubstituteEmptyTemplate() {
    String result = VariableResolver.substitute("", Collections.<String, String>emptyMap());
    assertEquals("", result);
  }

  @Test void testSubstituteNoVariables() {
    String result = VariableResolver.substitute("plain text", null);
    assertEquals("plain text", result);
  }

  @Test void testSubstituteMissingVariable() {
    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");
    String result = VariableResolver.substitute("{year}/{missing}", variables);
    assertEquals("2024/{missing}", result);
  }

  @Test void testSubstituteYearSuffixDerived() {
    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");
    String result = VariableResolver.substitute("file_{yearSuffix}.csv", variables);
    assertEquals("file_24.csv", result);
  }

  @Test void testSubstituteYearSuffixNotOverridden() {
    Map<String, String> variables = new HashMap<String, String>();
    variables.put("year", "2024");
    variables.put("yearSuffix", "XX");
    String result = VariableResolver.substitute("file_{yearSuffix}.csv", variables);
    assertEquals("file_XX.csv", result);
  }

  @Test void testSubstituteEnvVarPattern() {
    // Test {env:NAME:default} pattern
    Map<String, String> variables = new HashMap<String, String>();
    String result =
        VariableResolver.substitute("{env:UNLIKELY_VARIABLE_NAME_12345:fallback}", variables);
    assertEquals("fallback", result);
  }

  @Test void testResolveEnvVarsNullTemplate() {
    String result = VariableResolver.resolveEnvVars(null);
    assertNull(result);
  }

  @Test void testResolveEnvVarsEmptyTemplate() {
    String result = VariableResolver.resolveEnvVars("");
    assertEquals("", result);
  }

  @Test void testResolveEnvVarsWithDefault() {
    String result =
        VariableResolver.resolveEnvVars("${UNLIKELY_VARIABLE_XYZ123:-defaultVal}");
    assertEquals("defaultVal", result);
  }

  @Test void testResolveEnvVarsWithColonDefault() {
    String result =
        VariableResolver.resolveEnvVars("${UNLIKELY_VARIABLE_ABC456:colonDefault}");
    assertEquals("colonDefault", result);
  }

  @Test void testResolveEnvVarsNoDefault() {
    // Unresolved variable with no default keeps placeholder
    String result =
        VariableResolver.resolveEnvVars("${UNLIKELY_VARIABLE_NO_DEFAULT_999}");
    assertEquals("${UNLIKELY_VARIABLE_NO_DEFAULT_999}", result);
  }

  @Test void testResolveEnvVarsPlainText() {
    String result = VariableResolver.resolveEnvVars("plain text no vars");
    assertEquals("plain text no vars", result);
  }

  @Test void testResolveIntegerPlainNumber() {
    Integer result = VariableResolver.resolveInteger("42");
    assertNotNull(result);
    assertEquals(42, result.intValue());
  }

  @Test void testResolveIntegerNull() {
    assertNull(VariableResolver.resolveInteger(null));
  }

  @Test void testResolveIntegerEmpty() {
    assertNull(VariableResolver.resolveInteger(""));
  }

  @Test void testResolveIntegerInvalid() {
    assertNull(VariableResolver.resolveInteger("not_a_number"));
  }

  @Test void testResolveIntegerWithWhitespace() {
    Integer result = VariableResolver.resolveInteger("  99  ");
    assertNotNull(result);
    assertEquals(99, result.intValue());
  }

  @Test void testResolveIntegerFromEnvDefault() {
    Integer result =
        VariableResolver.resolveInteger("{env:UNLIKELY_INTEGER_VAR_789:2020}");
    assertNotNull(result);
    assertEquals(2020, result.intValue());
  }

  @Test void testSubstituteWithNullVariablesMap() {
    String result = VariableResolver.substitute("{novar}", null);
    assertEquals("{novar}", result);
  }

  @Test void testSubstituteWithEmptyVariablesMap() {
    String result = VariableResolver.substitute("{novar}", Collections.<String, String>emptyMap());
    assertEquals("{novar}", result);
  }

  @Test void testResolveEnvVarsIterativeResolution() {
    // Test that nested variables are resolved iteratively
    String result =
        VariableResolver.resolveEnvVars("${UNLIKELY_OUTER_VAR:-inner_${UNLIKELY_INNER_VAR:-resolved}}");
    assertEquals("inner_resolved", result);
  }
}
