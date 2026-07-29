/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.awt.GraphicsEnvironment;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How the engine download reports itself.
 *
 * <p>The download runs before the MCP server exists, so there is no session to send
 * notifications through; the only choice is a window or stderr. Choosing on server-mode
 * alone put a multi-minute download behind stderr that nothing displays, which is what
 * made a first run inside Claude Desktop look like a hang. The choice is now made on
 * whether anything can be drawn.
 */
@Tag("unit")
public class EngineInstallerProgressTest {

    private static Object progressFor(boolean serverMode, long total) throws Exception {
        Method m = EngineInstaller.class
            .getDeclaredMethod("progressFor", boolean.class, long.class);
        m.setAccessible(true);
        return m.invoke(null, serverMode, total);
    }

    /**
     * With no display, both modes must reach stderr. The interactive branch previously
     * returned the dialog regardless, and the dialog degraded to silence when it could
     * not open — so a headless interactive run reported nothing at all.
     */
    @Test void headlessUsesConsoleInBothModes() throws Exception {
        if (!GraphicsEnvironment.isHeadless()) {
            return;
        }
        for (boolean serverMode : new boolean[]{true, false}) {
            Object p = progressFor(serverMode, 450L * 1024 * 1024);
            assertNotNull(p, "progressFor must always return a reporter");
            assertEquals("ConsoleProgress", p.getClass().getSimpleName(),
                "headless must report on stderr, serverMode=" + serverMode);
        }
    }

    /**
     * With a display, server mode gets the window too — that is the fix. Claude Desktop
     * spawns the server in the user's GUI session, so a window is exactly what the user
     * needs to see during a 450 MB download.
     */
    @Test void displayUsesDialogInServerMode() throws Exception {
        if (GraphicsEnvironment.isHeadless()) {
            return;
        }
        Object p = progressFor(true, 450L * 1024 * 1024);
        assertEquals("DialogProgress", p.getClass().getSimpleName(),
            "server mode with a display must still show a progress window");
    }

    /** A reporter is always returned; no path may leave the download unreported. */
    @Test void neverReturnsNull() throws Exception {
        for (boolean serverMode : new boolean[]{true, false}) {
            for (long total : new long[]{-1L, 0L, 450L * 1024 * 1024}) {
                assertNotNull(progressFor(serverMode, total),
                    "serverMode=" + serverMode + " total=" + total);
            }
        }
    }

    /**
     * The dialog owns its own fallback, so a toolkit failure degrades to stderr rather
     * than to silence. Asserted structurally: the field must exist and be a
     * ConsoleProgress, since the failure it covers cannot be provoked in a test JVM.
     */
    @Test void dialogCarriesAConsoleFallback() throws Exception {
        Class<?> dialog = Class.forName(
            "org.apache.calcite.adapter.askamerica.EngineInstaller$DialogProgress");
        java.lang.reflect.Field f = dialog.getDeclaredField("fallback");
        assertEquals("ConsoleProgress", f.getType().getSimpleName(),
            "DialogProgress must degrade to stderr, not to silence");
        assertTrue(java.util.Arrays.stream(dialog.getDeclaredMethods())
                .anyMatch(m -> m.getName().equals("start")),
            "DialogProgress must implement start()");
    }
}
