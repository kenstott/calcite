/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The MCP server reports the engine's release version, not a constant. */
@Tag("unit")
class McpServerVersionTest {

  @Test void stampedJarReportsTheEngineVersion() {
    assertEquals("0.124.5", McpServer.serverVersion("0.124.5"));
  }

  @Test void unstampedJarReportsALocalBuild() {
    assertEquals(McpServer.LOCAL_BUILD_VERSION, McpServer.serverVersion(null));
    assertEquals(McpServer.LOCAL_BUILD_VERSION, McpServer.serverVersion(""));
  }

  @Test void initializeCarriesTheSameVersionTheInstallerReads() {
    JsonNode info = McpServer.handleInitialize(null).path("result").path("serverInfo");
    assertEquals("AskAmerica", info.path("name").asText());
    assertEquals(McpServer.serverVersion(PgwireGovDataInstaller.ownEngineVersion()),
        info.path("version").asText());
  }
}
