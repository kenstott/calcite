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

val trinoVersion = providers.gradleProperty("trino.version").get()

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(25))
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(25)
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
