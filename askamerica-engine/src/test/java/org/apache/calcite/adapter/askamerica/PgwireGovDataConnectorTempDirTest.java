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
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ops#736: a durable {@code pgwire-govdata} server must never be spawned with a catalog path
 * built from an ephemeral {@code ASKAMERICA_DATA_DIR}, such as a JUnit {@code @TempDir} leaked
 * through a system property into a real process spawn.
 */
@Tag("unit")
class PgwireGovDataConnectorTempDirTest {

  @Test void junitTempDirIsDetected(@TempDir Path tmpDir) {
    assertTrue(PgwireGovDataConnector.isUnderTempDir(tmpDir.toString()));
  }

  @Test void nestedPathUnderTempDirIsDetected(@TempDir Path tmpDir) {
    File nested = new File(tmpDir.toFile(), ".duckdb");
    assertTrue(PgwireGovDataConnector.isUnderTempDir(nested.getAbsolutePath()));
  }

  @Test void realHomeDirectoryIsNotDetected() {
    String home = System.getProperty("user.home");
    assertFalse(PgwireGovDataConnector.isUnderTempDir(home + "/.mcp_askamerica"));
  }
}
