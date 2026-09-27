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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The shared pgwire-govdata server reads its R2 credentials from its environment once, so the
 * connector must recognise when the credentials it was spawned with have expired.
 */
@Tag("unit")
@Isolated("mutates the user.home system property")
class PgwireGovDataConnectorCredentialExpiryTest {
  private String originalHome;
  private Path serverDir;

  @BeforeEach
  void redirectHome(@TempDir Path home) throws Exception {
    originalHome = System.getProperty("user.home");
    System.setProperty("user.home", home.toString());
    serverDir = Files.createDirectories(home.resolve(".askamerica").resolve("pgwire-govdata"));
  }

  @AfterEach
  void restoreHome() {
    System.setProperty("user.home", originalHome);
  }

  private void writeExpiry(String content) throws Exception {
    Files.write(serverDir.resolve("pgwire.creds-expiry"), content.getBytes("UTF-8"));
  }

  @Test void expiredStampMeansServerCredentialsExpired() throws Exception {
    writeExpiry(String.valueOf(System.currentTimeMillis() - 1_000L));
    assertTrue(PgwireGovDataConnector.serverCredentialsExpired());
  }

  @Test void stampInsideRefreshMarginMeansExpired() throws Exception {
    writeExpiry(String.valueOf(System.currentTimeMillis() + 30_000L));
    assertTrue(PgwireGovDataConnector.serverCredentialsExpired());
  }

  @Test void stampFarInFutureMeansUsable() throws Exception {
    writeExpiry(String.valueOf(System.currentTimeMillis() + 3_600_000L) + "\n");
    assertFalse(PgwireGovDataConnector.serverCredentialsExpired());
  }

  @Test void noExpiryFileMeansCredentialsCarriedNoExpiry() {
    assertFalse(PgwireGovDataConnector.serverCredentialsExpired());
  }

  @Test void corruptStampIsTreatedAsExpired() throws Exception {
    writeExpiry("not-a-number");
    assertTrue(PgwireGovDataConnector.serverCredentialsExpired());
  }
}
