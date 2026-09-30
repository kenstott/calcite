/*
 * Copyright (c) 2026 Kenneth Stott
 *
 * This source code is licensed under the Business Source License 1.1
 * found in the LICENSE-BSL.txt file in the root directory of this source tree.
 */
package org.apache.calcite.adapter.askamerica;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * engine-v0.100.1 was published with no assets while its workflow was still building the jar;
 * every launcher with a cached 0.100.0 saw the newer tag, downloaded
 * releases/latest/download/askamerica-engine.jar, got 404, and failed to start. A release only
 * supersedes the cached jar once the jar is attached.
 */
@Tag("unit")
class EngineInstallerLatestReleaseTest {

    private static final String PUBLISHED_WITHOUT_ASSETS =
        "{\"tag_name\":\"engine-v0.100.1\",\"draft\":false,\"assets\":[]}";

    private static final String PUBLISHED_WITH_JAR =
        "{\"tag_name\": \"engine-v0.100.1\", \"assets\": [\n"
        + "  {\"name\": \"pgwire-govdata-0.100.1-macos-arm64.tar.gz\", \"size\": 1},\n"
        + "  {\"name\": \"askamerica-engine.jar\", \"size\": 435577075}\n"
        + "]}";

    @Test void versionComesFromTheEngineTag() {
        assertEquals("0.100.1", EngineInstaller.releaseVersion(PUBLISHED_WITH_JAR));
        assertNull(EngineInstaller.releaseVersion("{\"tag_name\":\"pgwire-v1.0.0\"}"));
    }

    @Test void releaseWithoutTheJarIsNotYetAvailable() {
        assertFalse(EngineInstaller.hasEngineJar(PUBLISHED_WITHOUT_ASSETS));
    }

    /** Another asset whose name merely contains the jar's name must not count. */
    @Test void onlyTheExactAssetNameCounts() {
        assertTrue(EngineInstaller.hasEngineJar(PUBLISHED_WITH_JAR));
        assertFalse(EngineInstaller.hasEngineJar(
            "{\"tag_name\":\"engine-v0.100.1\",\"assets\":[{\"name\":\"askamerica-engine.jar.sha256\"}]}"));
    }
}
