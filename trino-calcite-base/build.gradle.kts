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

// Shared library for the Trino/Calcite connectors. Holds the reusable JDBC-framework glue
// (CalciteClient type mapping, CalciteClientModule wiring, AutoCommitConnectionFactory) that the
// generic `trino-calcite` connector and every adapter connector (sharepoint, splunk, file) build on.
//
// IMPORTANT: this is a LIBRARY, not a deployable Trino plugin. It deliberately carries NO
// META-INF/services/io.trino.spi.Plugin and assembles no plugin zip. Each deployable plugin lives in
// its own module (trino-calcite, trino-sharepoint, ...) and registers exactly one connector name.
// Keeping the SPI file out of this shared jar is what prevents the `calcite` connector from being
// re-registered by every adapter plugin that bundles this jar (duplicate-connector startup failure).
//
// Like the other Trino modules, this overrides the repo-wide Java 8 convention with a Java 25
// toolchain: Trino's SPI requires a modern JDK. The Calcite driver jars it wraps are Java 8 bytecode
// and run unchanged on the Trino JVM. Built against the SPI version pinned by `trino.version`.

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
// Published as io.simpleishard:trino-calcite-base so the connector modules — and
// any downstream JVM consumer — can depend on this shared library via Maven.
publishing {
    publications {
        create<MavenPublication>("trinoCalciteBase") {
            groupId    = "io.simpleishard"
            artifactId = "trino-calcite-base"
            version    = (project.findProperty("releaseVersion") as String?
                ?: project.version.toString().replace("-SNAPSHOT", ""))
                .let { if (it.isBlank() || it == "unspecified") "0.0.1" else it }

            from(components["java"])

            pom {
                name.set("Trino Calcite Connector Base")
                description.set("Shared JDBC-framework library for Trino connectors backed by the Apache Calcite driver")
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

dependencies {
    // trino-base-jdbc pulls io.airlift:http-client -> Jetty 12.1.8 onto the compile classpath,
    // but the in-process Trino server (test runtime of the dependent connector modules) is compiled
    // against Jetty 12.1.9. Because the root build pins runtime versions to the compile classpath,
    // bump the compile-side Jetty so the pin lands on 12.1.9 (otherwise the server hits a jetty-http
    // ComplianceUtils NoSuchMethodError). Declared `api` so dependents inherit the pin.
    constraints {
        listOf(
            "org.eclipse.jetty:jetty-http",
            "org.eclipse.jetty:jetty-client",
            "org.eclipse.jetty:jetty-io",
            "org.eclipse.jetty:jetty-util",
            "org.eclipse.jetty.http2:jetty-http2-common",
            "org.eclipse.jetty.http2:jetty-http2-hpack",
            "org.eclipse.jetty.http2:jetty-http2-client",
            "org.eclipse.jetty.http2:jetty-http2-client-transport",
            "org.eclipse.jetty.compression:jetty-compression-common",
            "org.eclipse.jetty.compression:jetty-compression-gzip"
        ).forEach { api(it) { version { require("12.1.9") } } }
    }

    // Provided by the Trino server at runtime via the plugin's parent classloader - must NOT be bundled.
    compileOnly("io.trino:trino-spi:$trinoVersion")

    // The JDBC connector framework. `api` so dependent connector modules compile against (and bundle)
    // it. Brings Guice, Airlift configuration, OpenTelemetry and Jackson onto the classpath transitively.
    api("io.trino:trino-base-jdbc:$trinoVersion")

    // The backing driver: org.apache.calcite.jdbc.Driver (jdbc:calcite:model=...). On the runtime
    // classpath (transitively) so dependent plugin zips bundle it.
    implementation(project(":core"))
}
