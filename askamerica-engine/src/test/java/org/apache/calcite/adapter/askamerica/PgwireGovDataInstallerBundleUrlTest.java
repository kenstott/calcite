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

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The bundle is fetched from its fixed release address, with no GitHub API lookup. */
@Tag("unit")
class PgwireGovDataInstallerBundleUrlTest {

  @Test void bundleAddressIsTheReleaseDownloadForTheVersionAndOs() {
    assertEquals("https://github.com/kenstott/calcite/releases/download/engine-v0.112.0/"
        + "pgwire-govdata-0.112.0-windows-x86_64.tar.gz",
        PgwireGovDataInstaller.bundleUrl("0.112.0", "windows-x86_64"));
    assertEquals("https://github.com/kenstott/calcite/releases/download/engine-v0.112.0/"
        + "pgwire-govdata-0.112.0-macos-arm64.tar.gz",
        PgwireGovDataInstaller.bundleUrl("0.112.0", "macos-arm64"));
  }

  @Test void aMissingChecksumFileMeansTheReleaseHasNoBundleForThisOs() {
    String url = PgwireGovDataInstaller.bundleUrl("0.112.0", "linux-x86_64") + ".sha256";
    PgwireGovDataInstaller.InstallFailedException e = assertThrows(
        PgwireGovDataInstaller.InstallFailedException.class,
        () -> PgwireGovDataInstaller.requiredChecksum(url, u -> {
          throw new PgwireGovDataInstaller.HttpStatusException(404, u);
        }));
    assertTrue(PgwireGovDataInstaller.isAssetMissing(e), e.getMessage());
  }

  @Test void otherFailuresAreNotReadAsMissing() {
    String url = PgwireGovDataInstaller.bundleUrl("0.112.0", "linux-x86_64") + ".sha256";
    PgwireGovDataInstaller.InstallFailedException forbidden = assertThrows(
        PgwireGovDataInstaller.InstallFailedException.class,
        () -> PgwireGovDataInstaller.requiredChecksum(url, u -> {
          throw new PgwireGovDataInstaller.HttpStatusException(403, u);
        }));
    assertFalse(PgwireGovDataInstaller.isAssetMissing(forbidden));
    PgwireGovDataInstaller.InstallFailedException timeout = assertThrows(
        PgwireGovDataInstaller.InstallFailedException.class,
        () -> PgwireGovDataInstaller.requiredChecksum(url, u -> {
          throw new IOException("request timed out");
        }));
    assertFalse(PgwireGovDataInstaller.isAssetMissing(timeout));
  }
}
