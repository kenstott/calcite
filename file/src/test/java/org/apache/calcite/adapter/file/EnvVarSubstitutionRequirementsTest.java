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
package org.apache.calcite.adapter.file;

import org.apache.calcite.util.EnvironmentVariableSubstitutor;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * FILE-049 — operand {@code ${VAR}} / {@code ${VAR:default}} substitution, exact-assertion golden
 * (recode of the weak {@code config/EnvironmentVariableTest}, which used contains() for the JSON
 * type-conversion cases). The file-adapter operand path resolves variables via the core
 * {@link EnvironmentVariableSubstitutor} (used by ModelHandler); this pins that contract.
 *
 * <p>CONTRADICTION C-33: the requirement text states the resolution order "env -&gt; system property",
 * but the production {@code substitute(String)} overlays {@code System.getProperties()} ON TOP of
 * {@code System.getenv()} — so a SYSTEM PROPERTY OVERRIDES an env var of the same name (intentional;
 * the source comments "System properties take precedence"). This golden asserts the ACTUAL behaviour
 * and the requirement was reworded to "system property -&gt; env -&gt; default -&gt; error". The env
 * layer itself cannot be set in-process, so it is exercised indirectly via the override proof below.
 */
@Tag("unit")
public class EnvVarSubstitutionRequirementsTest {

  private static Map<String, String> env(String... kv) {
    Map<String, String> m = new HashMap<String, String>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put(kv[i], kv[i + 1]);
    }
    return m;
  }

  @Test @Tag("FILE-049") void mapSubstituteResolvesDefaultsAndThrows() {
    // ${VAR} resolves to the mapped value.
    assertEquals("Value: test_value",
        EnvironmentVariableSubstitutor.substitute("Value: ${TEST_VAR}", env("TEST_VAR", "test_value")));
    // ${VAR:default} uses the default when the var is absent...
    assertEquals("Value: default_value",
        EnvironmentVariableSubstitutor.substitute("Value: ${MISSING:default_value}", env()));
    // ...and the var value when present (value beats default).
    assertEquals("v", EnvironmentVariableSubstitutor.substitute("${A:d}", env("A", "v")));
    // Empty default yields empty string.
    assertEquals("[]", EnvironmentVariableSubstitutor.substitute("[${EMPTY:}]", env()));
    // Multiple variables + a default in one string.
    assertEquals("First: value1, Second: value2, Default: default3",
        EnvironmentVariableSubstitutor.substitute(
            "First: ${VAR1}, Second: ${VAR2}, Default: ${VAR3:default3}",
            env("VAR1", "value1", "VAR2", "value2")));
    // Undefined with no default raises.
    assertThrows(IllegalArgumentException.class,
        () -> EnvironmentVariableSubstitutor.substitute("Value: ${MISSING}", env()));
  }

  @Test @Tag("FILE-049") void jsonSubstituteUnquotesNumbersAndBooleansKeepsStrings() {
    // A standalone "${VAR}" JSON value that is numeric/boolean is emitted UNQUOTED; a string stays quoted.
    assertEquals("{\"n\": 1000}",
        EnvironmentVariableSubstitutor.substituteInJson("{\"n\": \"${N}\"}", env("N", "1000")));
    assertEquals("{\"b\": true}",
        EnvironmentVariableSubstitutor.substituteInJson("{\"b\": \"${B}\"}", env("B", "true")));
    assertEquals("{\"b\": false}",
        EnvironmentVariableSubstitutor.substituteInJson("{\"b\": \"${B}\"}", env("B", "false")));
    assertEquals("{\"s\": \"hello\"}",
        EnvironmentVariableSubstitutor.substituteInJson("{\"s\": \"${S}\"}", env("S", "hello")));
  }

  @Test @Tag("FILE-049") void systemPropertyOverridesEnvAndUndefinedThrows() {
    // The production substitute(String) combines getenv() then overlays getProperties() — so a
    // system property of the same name as an existing env var WINS (C-33). PATH is reliably present
    // in env on the (Linux/WSL) test runtime.
    assumeTrue(System.getenv("PATH") != null, "needs an env var named PATH to prove the override");
    String original = System.getProperty("PATH");
    try {
      System.setProperty("PATH", "SYSPROP_WINS_SENTINEL");
      assertEquals("SYSPROP_WINS_SENTINEL",
          EnvironmentVariableSubstitutor.substitute("${PATH}"),
          "a system property overrides the same-named environment variable");
    } finally {
      if (original == null) {
        System.clearProperty("PATH");
      } else {
        System.setProperty("PATH", original);
      }
    }
    // An undefined name (absent from both env and system properties), no default, raises.
    assertThrows(IllegalArgumentException.class,
        () -> EnvironmentVariableSubstitutor.substitute("${__file049_definitely_absent__}"));
  }
}
