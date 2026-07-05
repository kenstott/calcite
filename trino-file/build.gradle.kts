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

// Trino connector for the Calcite file adapter. A thin wrapper over the generic trino-calcite
// connector: it reuses CalciteClient for type mapping and builds an inline Calcite model that
// instantiates one FileSchemaFactory schema over a single local directory or s3:// glob. See
// trino-calcite for the toolchain rationale.

plugins {
    id("com.github.johnrengelman.shadow")
}

// Publish a self-contained shadow jar as the Maven artifact: the connector depends on the fork's
// :core/:file (available only as 1.42.0-SNAPSHOT), so a thin POM cannot be resolved by consumers. A
// fat jar yields a dependency-less POM — the pattern govdata/askamerica already use. The deployable
// Trino plugin zip (trinoPlugin) is unaffected and stays the directory-of-jars format.
tasks.shadowJar {
    archiveBaseName.set("trino-file")
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
// Published as io.simpleishard:trino-file so JVM consumers can depend on the
// connector via Maven. The deployable Trino plugin zip (trinoPlugin task) is a
// separate GitHub-release asset — see .github/workflows/publish-trino.yml.
publishing {
    publications {
        create<MavenPublication>("trinoFile") {
            groupId    = "io.simpleishard"
            artifactId = "trino-file"
            version    = (project.findProperty("releaseVersion") as String?
                ?: project.version.toString().replace("-SNAPSHOT", ""))
                .let { if (it.isBlank() || it == "unspecified") "0.0.1" else it }

            artifact(tasks["shadowJar"])

            pom {
                name.set("Trino File Connector")
                description.set("Trino connector for the Apache Calcite file adapter (CSV/Parquet/JSON via DuckDB)")
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

tasks.withType<Test>().configureEach {
    // Trino's block-encoding SIMD support requires the incubator vector module at launch
    // (io.trino.block.BlockEncodingSimdSupport -> jdk.incubator.vector). The in-process
    // TestingTrinoServer used by the E2E test fails to bootstrap without it.
    jvmArgs("--add-modules=jdk.incubator.vector")
}

dependencies {
    compileOnly("io.trino:trino-spi:$trinoVersion")

    // Shared connector library (CalciteClient/AutoCommitConnectionFactory) + the JDBC framework
    // (transitively, via its api dep). NOT the trino-calcite plugin: depending on the SPI-less base
    // keeps the `calcite` connector out of this plugin's zip so only `file` is registered.
    implementation(project(":trino-calcite-base"))

    // The file adapter (FileSchemaFactory, S3StorageProvider) + the Calcite JDBC driver. Bundled.
    implementation(project(":file"))
    implementation(project(":core"))

    // DuckDB JDBC driver — the connector's default execution engine. The :file module declares it
    // compileOnly, so the plugin must bundle it at runtime (DuckDBJdbcSchemaFactory loads
    // org.duckdb.DuckDBDriver by name). DuckDB reads CSV/Parquet natively, avoiding the Hadoop
    // dependency that breaks the PARQUET engine on JDK 25.
    implementation("org.duckdb:duckdb_jdbc:1.4.4.0")

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
    description = "Builds the Trino plugin directory archive for the file connector."
    archiveBaseName.set("trino-file-plugin")
    into("trino-file") {
        from(tasks.named("jar"))
        from(configurations["runtimeClasspath"].filter { file ->
            trinoProvidedPrefixes.none { file.name.startsWith(it) }
        })
    }
}
