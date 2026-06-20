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

// Trino connector for the Cloud Ops adapter (Azure/AWS/GCP resource inventory). A thin wrapper over
// the shared trino-calcite-base library: it reuses CalciteClient for type mapping and only swaps in
// the CloudOps JDBC driver plus friendly catalog properties (azure.*, aws.*, gcp.*, cache.*). See
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

dependencies {
    compileOnly("io.trino:trino-spi:$trinoVersion")

    // Shared connector library (CalciteClient/AutoCommitConnectionFactory) + the JDBC framework
    // (transitively, via its api dep). NOT the trino-calcite plugin: depending on the SPI-less base
    // keeps the `calcite` connector out of this plugin's zip so only `cloudops` is registered.
    implementation(project(":trino-calcite-base"))

    // The backing driver: org.apache.calcite.adapter.ops.CloudOpsDriver, plus the Azure/AWS/GCP
    // SDKs it queries. All on the runtime classpath (transitively) so they are bundled. Bundled.
    implementation(project(":cloud-ops"))

    // The Calcite JDBC driver. cloud-ops exposes :core only as `implementation`, so its superclass
    // org.apache.calcite.jdbc.Driver (referenced via CloudOpsDriver) is not on our compile classpath
    // transitively. Declare it directly (mirrors trino-file). Already on runtime, so no extra bundling.
    implementation(project(":core"))

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
    description = "Builds the Trino plugin directory archive for the cloudops connector."
    archiveBaseName.set("trino-cloudops-plugin")
    into("trino-cloudops") {
        from(tasks.named("jar"))
        from(configurations["runtimeClasspath"].filter { file ->
            trinoProvidedPrefixes.none { file.name.startsWith(it) }
        })
    }
}
