/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * In {@code --mcp} mode a cached engine must start the server at once, with any update run in
 * the background: Claude Desktop kills a server whose {@code initialize} outlasts 60 seconds,
 * so an update fetched before startup never finished and kept the connector down.
 */
@Tag("unit")
class EngineInstallerServerModeTest {

    @Test void serverModeReturnsCachedJarAndDefersUpdate(@TempDir Path home) throws Exception {
        Path cache = home.resolve(".askamerica/engine/askamerica-engine.jar");
        Files.createDirectories(cache.getParent());
        Files.write(cache, "cached".getBytes(StandardCharsets.UTF_8));
        Path launcherDir = Files.createDirectories(home.resolve("launcher"));

        String savedHome = System.getProperty("user.home");
        System.setProperty("user.home", home.toString());
        try {
            List<Runnable> scheduled = new ArrayList<>();
            Path resolved = EngineInstaller.ensure(launcherDir.toFile(), true, scheduled::add);
            assertEquals(cache, resolved);
            assertEquals(1, scheduled.size(), "the update check must be handed to the executor");
        } finally {
            System.setProperty("user.home", savedHome);
        }
    }

    @Test void orphanedPartialDownloadsAreRemoved(@TempDir Path dir) throws Exception {
        Path jar = Files.write(dir.resolve("askamerica-engine.jar"), new byte[]{1});
        Path orphanA = Files.write(dir.resolve("engine-123.part"), new byte[]{2});
        Path orphanB = Files.write(dir.resolve("engine-456.part"), new byte[]{3});

        EngineInstaller.deleteOrphanedParts(dir);

        assertFalse(Files.exists(orphanA));
        assertFalse(Files.exists(orphanB));
        assertTrue(Files.exists(jar), "only partial downloads may be removed");
    }
}
