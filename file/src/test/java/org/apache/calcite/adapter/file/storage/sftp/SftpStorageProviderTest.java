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
package org.apache.calcite.adapter.file.storage.sftp;

import org.apache.calcite.adapter.file.storage.SftpStorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.adapter.file.storage.StorageProviderFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for SFTP storage provider.
 *
 * These tests require network access to public SFTP test servers.
 * They may fail if the servers are down or change their configuration.
 */
@Tag("integration")
public class SftpStorageProviderTest {

  @BeforeEach
  public void checkSftpTestRequirements() {
    // Check for SFTP test server configuration
    String sftpTestServer = System.getProperty("sftp.test.server");
    if (sftpTestServer == null || sftpTestServer.isEmpty()) {
      System.out.println("WARNING: No SFTP test server configured. "
          + "Set -Dsftp.test.server=<host> to enable full SFTP testing.");
    }
  }

  /**
   * Test factory creation with configuration.
   */
  @Test void testFactoryCreation() {
    Map<String, Object> config = new HashMap<>();
    config.put("username", "testuser");
    config.put("password", "testpass");
    config.put("strictHostKeyChecking", false);

    StorageProvider provider = StorageProviderFactory.createFromType("sftp", config);
    assertNotNull(provider);
    assertEquals("sftp", provider.getStorageType());
  }

  /**
   * Test URL parsing.
   */
  @Test void testUrlParsing() {
    SftpStorageProvider provider = new SftpStorageProvider();

    // Test basic URL
    String resolved = provider.resolvePath("sftp://user@host.com/base/", "file.txt");
    assertEquals("sftp://user@host.com/base/file.txt", resolved);

    // Test with port
    resolved = provider.resolvePath("sftp://user@host.com:2222/base/", "file.txt");
    assertEquals("sftp://user@host.com:2222/base/file.txt", resolved);

    // Test absolute path
    resolved = provider.resolvePath("sftp://user@host.com/base/", "sftp://other@host2.com/file.txt");
    assertEquals("sftp://other@host2.com/file.txt", resolved);
  }
}
