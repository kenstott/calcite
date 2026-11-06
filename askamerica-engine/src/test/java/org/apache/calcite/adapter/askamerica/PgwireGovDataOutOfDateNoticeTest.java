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
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** An installed pgwire-govdata older than the engine is reported in one line, never blocked. */
@Tag("unit")
class PgwireGovDataOutOfDateNoticeTest {

  private static void install(Path dir, String stamp) throws Exception {
    File launcher = PgwireGovDataInstaller.launcherPath(dir);
    Files.createDirectories(launcher.toPath().getParent());
    Files.write(launcher.toPath(), "x".getBytes("UTF-8"));
    if (stamp != null) {
      Files.write(dir.resolve(PgwireGovDataInstaller.MARKER), stamp.getBytes("UTF-8"));
    }
  }

  @Test void olderBundleNamesItsVersion(@TempDir Path dir) throws Exception {
    install(dir, "0.102.0");
    assertEquals("AskAmerica 0.102.0 is out of date.",
        PgwireGovDataInstaller.outOfDateNotice(dir, "0.110.1"));
  }

  @Test void unstampedBundleIsOutOfDate(@TempDir Path dir) throws Exception {
    install(dir, null);
    assertEquals("AskAmerica (installed version unknown) is out of date.",
        PgwireGovDataInstaller.outOfDateNotice(dir, "0.110.1"));
  }

  @Test void currentAndNewerBundlesGetNoNotice(@TempDir Path dir) throws Exception {
    install(dir, "0.110.1");
    assertNull(PgwireGovDataInstaller.outOfDateNotice(dir, "0.110.1"));
    assertNull(PgwireGovDataInstaller.outOfDateNotice(dir, "0.109.0"));
  }

  @Test void localBuildAndMissingBundleGetNoNotice(@TempDir Path dir) throws Exception {
    assertNull(PgwireGovDataInstaller.outOfDateNotice(dir, "0.110.1"));
    install(dir, "0.102.0");
    assertNull(PgwireGovDataInstaller.outOfDateNotice(dir, null));
  }
}
