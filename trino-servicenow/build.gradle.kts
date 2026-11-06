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

// Trino connector for ServiceNow. A thin wrapper over the generic trino-calcite connector: it reuses
// CalciteClient for type mapping and only swaps in the ServiceNow JDBC driver plus friendly catalog
// properties (instance-url, username, password, ...). See trino-calcite for the toolchain rationale.

plugins {
    id("com.github.johnrengelman.shadow")
}

// Publish a self-contained shadow jar as the Maven artifact: the connector depends on the fork's
// :core (available only as 1.42.0-SNAPSHOT), so a thin POM cannot be resolved by consumers. A fat
// jar yields a dependency-less POM — the pattern govdata/askamerica already use. The deployable
// Trino plugin zip (trinoPlugin) is unaffected and stays the directory-of-jars format.
tasks.shadowJar {
    archiveBaseName.set("trino-servicenow")
    archiveClassifier.set("")
    mergeServiceFiles()
    isZip64 = true
    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")
}

val trinoVersion = providers.gradleProperty("trino.version").get()

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(25)
}

// ─── Maven publishing (GitHub Packages + Maven Central) ──────────────────────
// Published as io.simpleishard:trino-servicenow so JVM consumers can depend on the
// connector via Maven. The deployable Trino plugin zip (trinoPlugin task) is a
// separate GitHub-release asset — see .github/workflows/publish-trino.yml.
publishing {
    publications {
        create<MavenPublication>("trinoServicenow") {
            groupId    = "io.simpleishard"
            artifactId = "trino-servicenow"
            version    = (project.findProperty("releaseVersion") as String?
                ?: project.version.toString().replace("-SNAPSHOT", ""))
                .let { if (it.isBlank() || it == "unspecified") "0.0.1" else it }

            artifact(tasks["shadowJar"])

            pom {
                name.set("Trino ServiceNow Connector")
                description.set("Trino connector for the Apache Calcite ServiceNow adapter")
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

tasks.matching {
    val n = it.name.lowercase()
    n.contains("forbidden") || n.contains("jandex") || n.contains("autostyle")
}.configureEach { enabled = false }

dependencies {
    compileOnly("io.trino:trino-spi:$trinoVersion")

    // Shared connector library (CalciteClient/AutoCommitConnectionFactory) + the JDBC framework
    // (transitively, via its api dep). NOT the trino-calcite plugin: depending on the SPI-less base
    // keeps the `calcite` connector out of this plugin's zip so only `servicenow` is registered.
    implementation(project(":trino-calcite-base"))

    // The backing driver: org.apache.calcite.adapter.servicenow.ServiceNowDriver. Bundled.
    implementation(project(":servicenow"))

    testImplementation("io.trino:trino-spi:$trinoVersion")
    testImplementation("io.trino:trino-testing:$trinoVersion")
    testImplementation("io.trino:trino-main:$trinoVersion")
    // See trino-calcite: supply a JUnit Platform launcher matching trino-testing's JUnit 6.x.
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// See trino-calcite/build.gradle.kts for the plugin-directory packaging rationale.
val trinoProvidedPrefixes = listOf("slice-", "jackson-annotations-", "opentelemetry-api-",
        "opentelemetry-context-", "jol-core-")

tasks.register<Zip>("trinoPlugin") {
    group = "distribution"
    description = "Builds the Trino plugin directory archive for the servicenow connector."
    archiveBaseName.set("trino-servicenow-plugin")
    into("trino-servicenow") {
        from(tasks.named("jar"))
        from(configurations["runtimeClasspath"].filter { file ->
            trinoProvidedPrefixes.none { file.name.startsWith(it) }
        })
    }
}

// See trino-calcite: drop Jetty's Brotli provider (needs a native library) from the test server.
configurations.testRuntimeClasspath {
    exclude(group = "org.eclipse.jetty.compression", module = "jetty-compression-brotli")
}

// The in-process Trino server (trino-testing) needs the same JVM flags as a real Trino launch.
// Integration tests hit a live ServiceNow instance; run them with -PincludeTags=integration.
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
    useJUnitPlatform {
        if (project.hasProperty("includeTags")) {
            includeTags(*project.property("includeTags").toString().split(",").toTypedArray())
        } else {
            excludeTags("integration")
        }
    }
}
