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
package org.apache.calcite.adapter.govdata.cyber;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for cyber schema factory construction and resource routing.
 */
@Tag("unit")
class CyberSchemaFactoryTest {

  @Test void testCyberVulnFactorySelectsCorrectResource() {
    CyberSchemaFactory factory = new CyberSchemaFactory("cyber_vuln");
    assertEquals("/cyber/cyber-vuln-schema.yaml", factory.getSchemaResourceName());
  }

  @Test void testCyberThreatFactorySelectsCorrectResource() {
    CyberSchemaFactory factory = new CyberSchemaFactory("cyber_threat");
    assertEquals("/cyber/cyber-threat-schema.yaml", factory.getSchemaResourceName());
  }

  @Test void testAliasesResolveToVuln() {
    CyberSchemaFactory factory = new CyberSchemaFactory("cybervuln");
    assertEquals("/cyber/cyber-vuln-schema.yaml", factory.getSchemaResourceName());
  }

  @Test void testNullDefaultsToVuln() {
    CyberSchemaFactory factory = new CyberSchemaFactory(null);
    assertEquals("/cyber/cyber-vuln-schema.yaml", factory.getSchemaResourceName());
  }

  @Test void testCyberThreatAlias() {
    CyberSchemaFactory factory = new CyberSchemaFactory("cyberthreat");
    assertEquals("/cyber/cyber-threat-schema.yaml", factory.getSchemaResourceName());
  }

  @Test void testVulnManifestHasNoDependencies() {
    CyberSchemaFactory factory = new CyberSchemaFactory("cyber_vuln");
    assertTrue(factory.getDependencies().isEmpty());
  }
}
