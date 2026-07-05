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

// Trino connector for SharePoint Lists. A thin wrapper over the generic trino-calcite connector:
// it reuses CalciteClient for type mapping and only swaps in the SharePoint JDBC driver plus
// friendly catalog properties (site-url, auth-type, client-id, ...). See trino-calcite for the
// toolchain rationale.

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
// Published as io.simpleishard:trino-sharepoint so JVM consumers can depend on the
// connector via Maven. The deployable Trino plugin zip (trinoPlugin task) is a
// separate GitHub-release asset — see .github/workflows/publish-trino.yml.
publishing {
    publications {
        create<MavenPublication>("trinoSharepoint") {
            groupId    = "io.simpleishard"
            artifactId = "trino-sharepoint"
            version    = (project.findProperty("releaseVersion") as String?
                ?: project.version.toString().replace("-SNAPSHOT", ""))
                .let { if (it.isBlank() || it == "unspecified") "0.0.1" else it }

            from(components["java"])

            pom {
                name.set("Trino SharePoint Connector")
                description.set("Trino connector for the Apache Calcite SharePoint List adapter")
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
    // keeps the `calcite` connector out of this plugin's zip so only `sharepoint` is registered.
    implementation(project(":trino-calcite-base"))

    // The backing driver: org.apache.calcite.adapter.sharepoint.SharePointListDriver. Bundled.
    implementation(project(":sharepoint-list"))

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
    description = "Builds the Trino plugin directory archive for the sharepoint connector."
    archiveBaseName.set("trino-sharepoint-plugin")
    into("trino-sharepoint") {
        from(tasks.named("jar"))
        from(configurations["runtimeClasspath"].filter { file ->
            trinoProvidedPrefixes.none { file.name.startsWith(it) }
        })
    }
}
