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

// Generic Trino connector over the Calcite JDBC driver (jdbc:calcite:).
//
// IMPORTANT: Trino's SPI requires a modern JDK, so this module overrides the repo-wide Java 8
// convention with its own toolchain. The Calcite driver jars it wraps are Java 8 bytecode and
// run unchanged on the Trino JVM. A Trino plugin must be built against the exact SPI version of
// the server it deploys into; the version is pinned by `trino.version` in gradle.properties.

val trinoVersion = providers.gradleProperty("trino.version").get()

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}

// `--release 25` makes javac ignore the inherited source/target 1.8 set by the root convention.
tasks.withType<JavaCompile>().configureEach {
    options.release.set(25)
}

// ─── Maven publishing (GitHub Packages + Maven Central) ──────────────────────
// Published as io.simpleishard:trino-calcite so JVM consumers can depend on the
// connector via Maven. The deployable Trino plugin zip (trinoPlugin task) is a
// separate GitHub-release asset — see .github/workflows/publish-trino.yml.
publishing {
    publications {
        create<MavenPublication>("trinoCalcite") {
            groupId    = "io.simpleishard"
            artifactId = "trino-calcite"
            version    = (project.findProperty("releaseVersion") as String?
                ?: project.version.toString().replace("-SNAPSHOT", ""))
                .let { if (it.isBlank() || it == "unspecified") "0.0.1" else it }

            from(components["java"])

            pom {
                name.set("Trino Calcite Connector")
                description.set("Generic Trino connector over the Apache Calcite JDBC driver (jdbc:calcite:)")
                url.set("https://github.com/kenstott/calcite")
                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
                developers {
                    developer {
                        id.set("kenstott")
                        name.set("Ken Stott")
                        email.set("kennethstott@gmail.com")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/kenstott/calcite.git")
                    developerConnection.set("scm:git:ssh://github.com/kenstott/calcite.git")
                    url.set("https://github.com/kenstott/calcite")
                }
            }
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url  = uri("https://maven.pkg.github.com/kenstott/calcite")
            credentials {
                username = System.getenv("GITHUB_ACTOR") ?: project.findProperty("gpr.user") as String?
                password = System.getenv("GITHUB_TOKEN") ?: project.findProperty("gpr.token") as String?
            }
        }
    }
}

// Opt out of the legacy code-quality gates the root build applies to every Java module. Their
// bytecode tooling (forbiddenapis/jandex use an older ASM) cannot parse Java 25 class files, and
// the autostyle ruleset targets the Calcite source conventions, not Trino-style code.
tasks.matching {
    val n = it.name.lowercase()
    n.contains("forbidden") || n.contains("jandex") || n.contains("autostyle")
}.configureEach { enabled = false }

// Jetty 12.1.x offers a Brotli compression provider that requires a platform-native library not
// present in this test environment; drop it so the in-process HTTP server falls back to gzip.
configurations.testRuntimeClasspath {
    exclude(group = "org.eclipse.jetty.compression", module = "jetty-compression-brotli")
}

// The in-process Trino server (trino-testing) needs the same JVM flags as a real Trino launch:
// the incubating Vector API, several add-opens, native access and unsafe memory access on JDK 25.
tasks.withType<Test>().configureEach {
    maxHeapSize = "2g"
    jvmArgs(
        "--add-modules=jdk.incubator.vector",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--enable-native-access=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow",
        "-Djdk.attach.allowAttachSelf=true"
    )
}

dependencies {
    // Provided by the Trino server at runtime via the plugin's parent classloader - must NOT be bundled.
    compileOnly("io.trino:trino-spi:$trinoVersion")

    // Shared connector library: CalciteClient/CalciteClientModule/AutoCommitConnectionFactory, plus
    // (transitively) trino-base-jdbc, the Calcite core driver and the Jetty version pin. `api` so the
    // bundled-runtime classpath (and therefore the plugin zip) picks up the driver and framework.
    api(project(":trino-calcite-base"))

    testImplementation("io.trino:trino-spi:$trinoVersion")
    testImplementation("io.trino:trino-testing:$trinoVersion")
    testImplementation("io.trino:trino-main:$trinoVersion")
    // trino-testing pulls JUnit Platform 6.x; supply a matching launcher so Gradle 8.7 uses it
    // instead of its older bundled launcher (otherwise TagFilter hits a NoSuchMethodError).
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// Assemble a Trino plugin directory (directory-of-jars, not a fat jar). Trino loads plugins from
// `<trino>/plugin/<name>/`; unzip the archive there. trino-spi is compileOnly so it is already
// absent from runtimeClasspath; the names below are the other artifacts Trino exposes to plugins
// parent-first and which therefore must NOT be bundled. NOTE: verify the provided set against a
// real Trino server for the pinned version before shipping.
val trinoProvidedPrefixes = listOf("slice-", "jackson-annotations-", "opentelemetry-api-",
        "opentelemetry-context-", "jol-core-")

tasks.register<Zip>("trinoPlugin") {
    group = "distribution"
    description = "Builds the Trino plugin directory archive for the calcite connector."
    archiveBaseName.set("trino-calcite-plugin")
    into("trino-calcite") {
        from(tasks.named("jar"))
        from(configurations["runtimeClasspath"].filter { file ->
            trinoProvidedPrefixes.none { file.name.startsWith(it) }
        })
    }
}
