/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The shared pgwire server must not live in the spawning connector's process group: Claude
 * Desktop kills that group when it tears the connector down, which killed the server mid cold
 * start whenever the conversation that spawned it went away.
 */
@Tag("unit")
class PgwireGovDataConnectorDetachTest {

  @Test @DisabledOnOs(OS.WINDOWS)
  void launcherRunsAsLeaderOfItsOwnProcessGroupWithTheSpawnedPid(@TempDir Path bundle)
      throws Exception {
    Path python = findOnPath("python3");
    assertNotNull(python, "python3 must be on PATH to stand in for the bundle's cpython");
    Files.createDirectories(bundle.resolve("cpython/bin"));
    Files.createSymbolicLink(bundle.resolve("cpython/bin/python"), python);

    Path out = bundle.resolve("ids.txt");
    Path launcher = Files.createDirectories(bundle.resolve("bin")).resolve("pgwire-govdata");
    Files.write(launcher,
        ("#!/bin/sh\nps -o pid= -o pgid= -p $$ > \"" + out + "\"\n")
            .getBytes(StandardCharsets.UTF_8));
    assertTrue(launcher.toFile().setExecutable(true));

    List<String> command = PgwireGovDataConnector.launchCommand(launcher.toFile(), false);
    Process p = new ProcessBuilder(command).start();
    assertTrue(p.waitFor(30, TimeUnit.SECONDS), "launcher did not finish");
    assertEquals(0, p.exitValue());

    String[] ids = new String(Files.readAllBytes(out), StandardCharsets.UTF_8).trim()
        .split("\\s+");
    assertEquals(String.valueOf(p.pid()), ids[0],
        "the launcher must replace the wrapper in place so the pid file stays accurate");
    assertEquals(ids[0], ids[1], "the launcher must lead its own process group");
  }

  private static Path findOnPath(String name) {
    for (String dir : System.getenv("PATH").split(File.pathSeparator)) {
      Path candidate = new File(dir, name).toPath();
      if (Files.isExecutable(candidate)) {
        return candidate;
      }
    }
    return null;
  }
}
