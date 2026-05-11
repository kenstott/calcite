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

import org.apache.calcite.adapter.govdata.cyber.threat.CyberThreatCacheManifest;
import org.apache.calcite.adapter.govdata.cyber.vuln.CyberVulnCacheManifest;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Phase 0 cyber schema infrastructure.
 *
 * <p>These tests verify factory construction, schema resource routing,
 * and TTL manifest wiring without hitting any live APIs.
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

  @Test void testVulnManifestCreated() {
    CyberSchemaFactory factory = new CyberSchemaFactory("cyber_vuln");
    AbstractCyberCacheManifest manifest = factory.createCacheManifest();
    assertInstanceOf(CyberVulnCacheManifest.class, manifest);
  }

  @Test void testThreatManifestCreated() {
    CyberSchemaFactory factory = new CyberSchemaFactory("cyber_threat");
    AbstractCyberCacheManifest manifest = factory.createCacheManifest();
    assertInstanceOf(CyberThreatCacheManifest.class, manifest);
  }

  @Test void testVulnManifestHasNoDependencies() {
    CyberSchemaFactory factory = new CyberSchemaFactory("cyber_vuln");
    assertTrue(factory.getDependencies().isEmpty());
  }

  @Test void testThreatManifestDefaultIocTtl() {
    CyberThreatCacheManifest manifest = new CyberThreatCacheManifest();
    // Table not in YAML (schema file is a stub) so should fall through to default
    int ttl = manifest.getIocTtlDays("ioc_urls");
    assertTrue(ttl > 0, "Expected positive IOC TTL, got: " + ttl);
  }

  @Test void testVulnManifestUnknownTableReturnsDefault() {
    CyberVulnCacheManifest manifest = new CyberVulnCacheManifest();
    assertEquals(-1, manifest.getTtlDays("nonexistent_table"));
    assertEquals(7, manifest.getTtlDays("nonexistent_table", 7));
  }
}
