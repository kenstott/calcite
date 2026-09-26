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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage for ops#490 / ops#638: a cached or freshly-downloaded engine jar that
 * is not a readable jar (truncated download, corrupted-on-disk cache) must be detected and
 * discarded rather than installed or trusted, since every feature that needs a class not yet
 * loaded then fails deep inside the JVM with a bare class name (e.g. {@code
 * ReportPage$Section}) instead of a diagnosable error.
 */
@Tag("unit")
class EngineInstallerJarIntegrityTest {

    private static boolean isStale(Path jar) throws Exception {
        Method m = EngineInstaller.class.getDeclaredMethod("isStale", Path.class);
        m.setAccessible(true);
        return (boolean) m.invoke(null, jar);
    }

    private static void verifyDownloadedJar(Path tmp, String url) throws Throwable {
        Method m = EngineInstaller.class.getDeclaredMethod(
            "verifyDownloadedJar", Path.class, String.class);
        m.setAccessible(true);
        try {
            m.invoke(null, tmp, url);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static Path writeValidJar(Path dir) throws IOException {
        Path jar = dir.resolve("valid.jar");
        Manifest mf = new Manifest();
        mf.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar), mf)) {
            // empty otherwise -- only the manifest matters here
        }
        return jar;
    }

    @Test void corruptCachedJarIsTreatedAsStale(@TempDir Path dir) throws Exception {
        Path jar = dir.resolve("cached.jar");
        Files.write(jar, "not a jar file".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertTrue(isStale(jar), "an unreadable cached jar must be treated as stale");
    }

    @Test void corruptDownloadIsRejectedAndDiscarded(@TempDir Path dir) throws Throwable {
        Path tmp = dir.resolve("engine-download.part");
        Files.write(tmp, "<html>error page</html>"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThrows(IOException.class,
            () -> verifyDownloadedJar(tmp, "https://example.invalid/engine.jar"));
        assertFalse(Files.exists(tmp), "a bad download must be deleted, never cached");
    }

    @Test void validJarPassesVerification(@TempDir Path dir) throws Throwable {
        Path jar = writeValidJar(dir);
        verifyDownloadedJar(jar, "https://example.invalid/engine.jar");
        assertTrue(Files.exists(jar), "a genuine jar must survive verification");
    }
}
