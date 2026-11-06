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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The "still starting" answer states what the server's own log shows (stage, cache hit or
 * miss, liveness, whether the log is advancing) instead of a fixed claim about how long a first
 * start takes.
 */
@Tag("unit")
@ResourceLock("askamerica.pgwire.startState")
class PgwireGovDataConnectorStartStateTest {
  @TempDir File dir;

  @AfterEach void untrack() {
    PgwireGovDataConnector.trackStart(null, -1, null);
  }

  private static final class FakeProcess extends Process {
    private final boolean alive;

    FakeProcess(boolean alive) {
      this.alive = alive;
    }

    @Override public boolean isAlive() {
      return alive;
    }

    @Override public OutputStream getOutputStream() {
      return null;
    }

    @Override public InputStream getInputStream() {
      return null;
    }

    @Override public InputStream getErrorStream() {
      return null;
    }

    @Override public int waitFor() {
      return 0;
    }

    @Override public int exitValue() {
      return 0;
    }

    @Override public void destroy() {
    }
  }

  private String message(String log, long ageSeconds, boolean alive) throws Exception {
    File f = new File(dir, "spawn.log");
    Files.write(f.toPath(), log.getBytes(StandardCharsets.UTF_8));
    assertTrue(f.setLastModified(System.currentTimeMillis() - ageSeconds * 1000));
    PgwireGovDataConnector.trackStart(f, 0, new FakeProcess(alive));
    return PgwireGovDataConnector.stillStartingMessage(65);
  }

  private static final String MOUNTING =
      "2026-10-17 10:00:00,000 INFO pgwire_calcite.launcher: STAGE socket-claimed host=127.0.0.1 "
      + "port=45433\n"
      + "2026-10-17 10:00:00,100 INFO pgwire_calcite.launcher: STAGE backend-building "
      + "model=/m/model.json\n"
      + "2026-10-17 10:00:01,000 INFO pgwire_calcite.calcite_backend: [CALCITE] JVM started with "
      + "261 classpath entries\n"
      + "10:00:02.579 [main] INFO  org.apache.calcite.adapter.govdata.GovDataSchemaFactory - "
      + "Creating government data schema 'sec' for source: sec\n"
      + "10:00:07.579 [main] INFO  org.apache.calcite.adapter.govdata.GovDataSchemaFactory - "
      + "Creating government data schema 'geo' for source: geo\n";

  @Test void progressingMountNamesStageAndSchemaCount() throws Exception {
    String m = message(MOUNTING, 3, true);
    assertTrue(m.contains("after 65s"), m);
    assertTrue(m.contains("opening the Calcite connection to the model"), m);
    assertTrue(m.contains("2 schemas created so far, latest 'geo'"), m);
    assertTrue(m.contains("last advanced 3s ago, so it is progressing"), m);
    assertFalse(m.contains("several minutes"), m);
    assertFalse(m.contains("may be wedged"), m);
  }

  @Test void silentLogIsReportedAsPossiblyWedged() throws Exception {
    String m = message(MOUNTING, 200, true);
    assertTrue(m.contains("no output for 200s, so it may be wedged"), m);
    assertFalse(m.contains("progressing"), m);
  }

  @Test void deadProcessIsSaid() throws Exception {
    assertTrue(message(MOUNTING, 3, false).contains("The server process has exited."));
  }

  @Test void cacheMissIsALoudDefectNamingPathAndHash() throws Exception {
    String m = message(MOUNTING
        + "2026-10-17 10:01:00,000 WARNING pgwire_calcite.catalog_populate: STAGE "
        + "catalog-cache-miss hash=661fbeeb8f49070b looked=/s/catalog-cache-661fbeeb8f49070b.pkl,"
        + "/m/catalog-cache-661fbeeb8f49070b.pkl unloadable=none -- walking the catalog live\n",
        3, true);
    assertTrue(m.contains("DEFECT - CATALOG CACHE MISS"), m);
    assertTrue(m.contains("catalog-cache-661fbeeb8f49070b.pkl (model hash 661fbeeb8f49070b)"), m);
    assertTrue(m.contains("/s/catalog-cache-661fbeeb8f49070b.pkl,/m/"), m);
  }

  @Test void cacheHitIsReportedWithItsPath() throws Exception {
    String m = message(MOUNTING
        + "2026-10-17 10:01:00,000 INFO x: STAGE catalog-cache-hit path=/m/c.pkl tables=673\n",
        3, true);
    assertTrue(m.contains("catalog loaded from cache"), m);
    assertTrue(m.contains("path=/m/c.pkl tables=673"), m);
    assertFalse(m.contains("CACHE MISS"), m);
  }

  @Test void earlierStartsInTheLogAreIgnored() throws Exception {
    String old = "2026-10-16 STAGE socket-claimed host=h port=1\n"
        + "2026-10-16 STAGE catalog-cache-miss hash=old looked=/old unloadable=none\n";
    String m = message(old + MOUNTING, 3, true);
    assertFalse(m.contains("CACHE MISS"), m);
  }

  @Test void noStageYetSaysSo() throws Exception {
    String m = message("WARNING: something\n", 2, true);
    assertTrue(m.contains("shows no start-up stage yet"), m);
  }
}
