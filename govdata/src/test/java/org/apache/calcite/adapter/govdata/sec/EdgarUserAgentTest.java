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
package org.apache.calcite.adapter.govdata.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The SEC User-Agent comes from the schema YAML when the model does not name one. */
@Tag("unit")
class EdgarUserAgentTest {

  @Test void aModelWithoutEdgarUserAgentGetsTheSchemaYamlValue() {
    String ua = EdgarRateLimiter.userAgent();
    assertFalse(ua.isEmpty());
    assertFalse(ua.contains("${"), ua);
    assertTrue(ua.contains("@"), "SEC requires a contact: " + ua);
  }

  @Test void noSecSourceFileNamesThePlaceholderAgent() throws Exception {
    java.nio.file.Path yamlOnClasspath = java.nio.file.Paths.get(
        EdgarUserAgentTest.class.getResource("/sec/sec-schema.yaml").toURI());
    // build/resources/main/sec/sec-schema.yaml -> <module>/src/main
    java.nio.file.Path root = yamlOnClasspath.getParent().getParent().getParent().getParent()
        .getParent().resolve("src/main");
    java.util.List<String> offenders = new java.util.ArrayList<String>();
    try (java.util.stream.Stream<java.nio.file.Path> files = java.nio.file.Files.walk(root)) {
      for (java.nio.file.Path p : (Iterable<java.nio.file.Path>) files::iterator) {
        String n = p.toString();
        if (!java.nio.file.Files.isRegularFile(p) || !n.contains("/sec/")
            || !(n.endsWith(".java") || n.endsWith(".yaml"))) {
          continue;
        }
        String text = new String(java.nio.file.Files.readAllBytes(p),
            java.nio.charset.StandardCharsets.UTF_8);
        if (text.contains("apache-calcite@apache.org")) {
          offenders.add(n);
        }
      }
    }
    assertTrue(offenders.isEmpty(), "placeholder agent still named in " + offenders);
  }

  @Test void theDocumentSourceHeaderIsTheDeclaredAgent() throws Exception {
    java.util.Map<?, ?> yaml;
    try (java.io.InputStream in =
        EdgarUserAgentTest.class.getResourceAsStream("/sec/sec-schema.yaml")) {
      yaml = (java.util.Map<?, ?>) new org.yaml.snakeyaml.Yaml(loaderOptions()).load(in);
    }
    java.util.Map<?, ?> defaults = (java.util.Map<?, ?>) yaml.get("document_source_defaults");
    java.util.Map<?, ?> headers = (java.util.Map<?, ?>) defaults.get("headers");
    assertTrue(headers.get("User-Agent").equals(yaml.get("edgarUserAgent")),
        "document source User-Agent must alias edgarUserAgent: " + headers.get("User-Agent"));
  }

  private static org.yaml.snakeyaml.LoaderOptions loaderOptions() {
    org.yaml.snakeyaml.LoaderOptions o = new org.yaml.snakeyaml.LoaderOptions();
    o.setMaxAliasesForCollections(10000);
    return o;
  }
}
