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
package org.apache.calcite.adapter.govdata.etl;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The schema name lists must not drift from the {@code *-schema.yaml} resources.
 *
 * <p>Three places name the schemas: the YAML resources themselves, {@code SCHEMA_YAML} in
 * {@link GovDataModelVerificationRunner}, and {@code ALL_SCHEMAS} in
 * {@code govdata/scripts/model-verify.sh}. The last two are hand-maintained, and adding a schema
 * YAML does not add it to either. {@code fiscal} was omitted from both from the day it was added,
 * with three consequences that all present as something else: it was never verified, it never
 * reached the JAR-bundled DuckDB seed (so it alone rebuilt every view from Iceberg metadata on a
 * user's first query), and the seed shipped covering 24 of 25 schemas while looking complete.
 *
 * <p>An omission like that is invisible at runtime — the schema still mounts and queries fine — so
 * this is where it has to be caught.
 */
@Tag("unit")
class SchemaListDriftTest {

  /**
   * Schema names derived from the {@code *-schema.yaml} resources, which are the ground truth:
   * a schema exists because its YAML does. Underscores in a name are hyphens in the filename
   * ({@code econ-reference-schema.yaml} -> {@code econ_reference}).
   */
  private static TreeSet<String> schemasFromYamlResources() throws IOException {
    Path resourceRoot = resourceRoot();
    TreeSet<String> names = new TreeSet<>();
    try (Stream<Path> walk = Files.walk(resourceRoot)) {
      for (Path p : (Iterable<Path>) walk.filter(Files::isRegularFile)::iterator) {
        String file = p.getFileName().toString();
        if (file.endsWith("-schema.yaml")) {
          names.add(file.substring(0, file.length() - "-schema.yaml".length())
              .replace('-', '_').toLowerCase(Locale.ROOT));
        }
      }
    }
    assertTrue(names.size() >= 20,
        "expected the schema YAML resources to be discoverable; found " + names.size()
        + " under " + resourceRoot + " — if this fired, the discovery below is broken and the "
        + "test would otherwise pass vacuously");
    return names;
  }

  /**
   * Directory holding the schema YAML resources, derived from a registered entry that resolves.
   *
   * <p>Not {@code getResource("/")}: that returns the test-classes directory, which contains no
   * YAML, so walking it would find nothing. Anchoring on a real resource lands in the exploded
   * {@code build/resources/main} where the YAML files actually live. Any registered entry will do,
   * so this does not hard-code a schema name.
   */
  private static Path resourceRoot() {
    for (String resource : GovDataModelVerificationRunner.SCHEMA_YAML.values()) {
      URL url = SchemaListDriftTest.class.getResource(resource);
      if (url != null && "file".equals(url.getProtocol())) {
        // /<root>/<dir>/<name>-schema.yaml -> up two -> /<root>
        return new File(url.getPath()).toPath().getParent().getParent();
      }
    }
    return fail("no SCHEMA_YAML resource resolved to a file: URL, so the *-schema.yaml resources "
        + "cannot be walked. Failing rather than passing without having checked anything.");
  }

  /** {@code ALL_SCHEMAS} as declared in the shell script, parsed from source. */
  private static TreeSet<String> schemasFromModelVerifyScript() throws IOException {
    // <module>/build/resources/main -> build/resources -> build -> <module>
    Path module = resourceRoot().getParent().getParent().getParent();
    Path script = module.resolve("scripts").resolve("model-verify.sh");
    assertTrue(Files.isRegularFile(script),
        "could not locate model-verify.sh at " + script + " — resolve the path rather than "
        + "letting this check quietly not run");
    Matcher m = Pattern.compile("^ALL_SCHEMAS=\"([^\"]*)\"", Pattern.MULTILINE)
        .matcher(new String(Files.readAllBytes(script), StandardCharsets.UTF_8));
    assertTrue(m.find(), "no ALL_SCHEMAS=\"...\" assignment found in " + script);
    TreeSet<String> names = new TreeSet<>();
    for (String s : m.group(1).trim().split("\\s+")) {
      if (!s.isEmpty()) {
        names.add(s.toLowerCase(Locale.ROOT));
      }
    }
    return names;
  }

  @Test @DisplayName("every *-schema.yaml has a SCHEMA_YAML entry")
  void runnerCoversEveryDeclaredSchema() throws Exception {
    TreeSet<String> declared = schemasFromYamlResources();
    TreeSet<String> registered = new TreeSet<>(GovDataModelVerificationRunner.SCHEMA_YAML.keySet());
    List<String> missing = new ArrayList<>(declared);
    missing.removeAll(registered);
    assertTrue(missing.isEmpty(),
        "schemas with a YAML but no SCHEMA_YAML entry: " + missing
        + " — they cannot be verified, and never reach the bundled seed, so they rebuild every "
        + "view on a user's first query");
  }

  @Test @DisplayName("SCHEMA_YAML has no entry without a YAML")
  void runnerHasNoPhantomSchemas() throws Exception {
    TreeSet<String> declared = schemasFromYamlResources();
    List<String> phantom = new ArrayList<>(GovDataModelVerificationRunner.SCHEMA_YAML.keySet());
    phantom.removeAll(declared);
    assertTrue(phantom.isEmpty(),
        "SCHEMA_YAML names schemas with no *-schema.yaml resource: " + phantom);
  }

  @Test @DisplayName("model-verify.sh ALL_SCHEMAS matches SCHEMA_YAML exactly")
  void shellListMatchesRunner() throws Exception {
    assertEquals(new TreeSet<>(GovDataModelVerificationRunner.SCHEMA_YAML.keySet()),
        schemasFromModelVerifyScript(),
        "model-verify.sh ALL_SCHEMAS and GovDataModelVerificationRunner.SCHEMA_YAML disagree; "
        + "a schema in one but not the other is silently skipped by whichever entry point "
        + "reads the shorter list");
  }

  @Test @DisplayName("every registered SCHEMA_YAML path resolves to a resource")
  void everyRegisteredPathResolves() {
    List<String> broken = new ArrayList<>();
    for (java.util.Map.Entry<String, String> e
        : GovDataModelVerificationRunner.SCHEMA_YAML.entrySet()) {
      if (SchemaListDriftTest.class.getResource(e.getValue()) == null) {
        broken.add(e.getKey() + " -> " + e.getValue());
      }
    }
    assertTrue(broken.isEmpty(), "SCHEMA_YAML paths that resolve to nothing: " + broken);
  }
}
