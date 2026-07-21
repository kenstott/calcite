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
plugins {
    id("com.github.johnrengelman.shadow")
    `maven-publish`
}

description = "AskAmerica MCP server — query US government data from Claude Desktop"

// askamerica-engine depends on govdata which requires Java 11+. Skip on JDK 8 so the upstream
// Calcite JDK 8 CI jobs don't fail on missing govdata classes.
if (JavaVersion.current() < JavaVersion.VERSION_11) {
    afterEvaluate {
        tasks.withType<JavaCompile>().configureEach { enabled = false }
        tasks.withType<Test>().configureEach { enabled = false }
    }
}

// BSL module uses Google Java Format (4-space) and has legitimate System.exit() in the launcher.
// Exempt from Apache Calcite style checks (checkstyle, forbiddenApis).
afterEvaluate {
    tasks.withType<de.thetaphi.forbiddenapis.gradle.CheckForbiddenApis>().configureEach {
        enabled = false
    }
    tasks.withType<Checkstyle>().configureEach {
        enabled = false
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":driver-base"))
    implementation(project(":govdata"))
    implementation("com.formdev:flatlaf:3.3")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.2")
    testImplementation("org.duckdb:duckdb_jdbc:1.4.4.0")  // in-memory DB for surface tests
}

tasks.test {
    useJUnitPlatform {
        val tags = project.findProperty("includeTags") as String?
        if (tags != null) {
            includeTags(tags)
        }
    }
    // Tests spawn the shadow JAR as a subprocess — ensure it's built first
    dependsOn(tasks.shadowJar)
    // Working directory must be the module root so build/libs is resolvable
    workingDir = projectDir
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = true
    }
}

// ─── Shadow JAR ──────────────────────────────────────────────────────────────
// Produces a read-only fat JAR by excluding ETL-only classes and dependencies.
// DuckDB handles all S3/Parquet reading so Hadoop is not needed at query time.

tasks.shadowJar {
    archiveBaseName.set("askamerica-engine")
    archiveClassifier.set("")
    isZip64 = true
    mergeServiceFiles()

    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")

    // ── ETL-only govdata packages ──────────────────────────────────────────
    // govdata/etl is pure ETL orchestration — safe to exclude
    exclude("org/apache/calcite/adapter/govdata/etl/**")
    // file/etl and file/refresh contain classes referenced by schema factories
    // at connection time — keep them to avoid NoClassDefFoundError at query time

    // ── ETL-only third-party libraries ─────────────────────────────────────

    // PDF parsing (SEC document extraction)
    exclude("org/apache/pdfbox/**")
    exclude("org/apache/fontbox/**")
    exclude("org/apache/xmpbox/**")

    // Excel parsing (some ETL sources)
    exclude("org/apache/poi/**")

    // HTML scraping
    exclude("org/jsoup/**")

    // Vector embeddings / ML (not used at query time)
    exclude("com/microsoft/onnxruntime/**")
    exclude("ai/djl/**")
    exclude("ai/onnxruntime/**")

    // Hadoop — DuckDB handles S3 natively; hadoop-common Configuration
    // class is imported but never instantiated in the read path.
    exclude("org/apache/hadoop/mapreduce/**")
    exclude("org/apache/hadoop/mapred/**")
    exclude("org/apache/hadoop/hdfs/**")
    exclude("org/apache/hadoop/yarn/**")
    exclude("org/apache/hadoop/fs/viewfs/**")
    exclude("org/apache/hadoop/io/compress/bzip2/**")
    exclude("org/apache/hadoop/ipc/**")
    exclude("org/apache/hadoop/security/kerberos/**")

    // Parquet write path (only reading is needed)
    exclude("org/apache/parquet/avro/**")
    exclude("org/apache/parquet/hadoop/codec/**")

    // HTTP client — keep; needed at query time for schema initialization

    // No main class — this is a pure JDBC driver JAR loaded via JPype or classpath
}

// ─── Maven publishing (GitHub Packages) ──────────────────────────────────────
// Publishes the shadow JAR to https://maven.pkg.github.com/kenstott/calcite
// so Maven/Gradle users can pull it without downloading from GitHub Releases.
//
//   <dependency>
//     <groupId>ai.askamerica</groupId>
//     <artifactId>askamerica-engine</artifactId>
//     <version>VERSION</version>
//   </dependency>

// Version is set by -Pversion=X.Y.Z from CI (tag engine-vX.Y.Z → X.Y.Z).
// Falls back to project.version with SNAPSHOT stripped for local builds.
val publishVersion: String =
    (project.findProperty("releaseVersion") as String?
        ?: project.version.toString().replace("-SNAPSHOT", ""))
        .let { if (it.isBlank() || it == "unspecified") "0.0.1" else it }

publishing {
    publications {
        create<MavenPublication>("askamericaEngine") {
            groupId = "ai.askamerica"
            artifactId = "askamerica-engine"
            version = publishVersion

            artifact(tasks.shadowJar) {
                classifier = ""
            }

            pom {
                name.set("AskAmerica Engine")
                description.set("JDBC driver for querying US government datasets — SEC, BLS, Census, NOAA, FBI, FEC, and more")
                url.set("https://github.com/kenstott/calcite")
                licenses {
                    license {
                        name.set("Business Source License 1.1")
                        url.set("https://github.com/kenstott/calcite/blob/main/LICENSE-BSL.txt")
                    }
                }
                developers {
                    developer {
                        id.set("kenstott")
                        name.set("Kenneth Stott")
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
            url = uri("https://maven.pkg.github.com/kenstott/calcite")
            credentials {
                username = System.getenv("GITHUB_ACTOR") ?: project.findProperty("gpr.user") as String?
                password = System.getenv("GITHUB_TOKEN") ?: project.findProperty("gpr.token") as String?
            }
        }
    }
}

// ─── jpackage task ───────────────────────────────────────────────────────────
// Creates a native installer with a bundled JRE — no Java installation required.
// Run: ./gradlew :askamerica-engine:jpackage
//
// Requires JDK 17+ (jpackage ships with JDK 14+).
// On macOS produces an app-image; on Linux a .deb; on Windows a .msi.
//
// ExFAT + macOS: ExFAT has no native xattr support, so macOS stores code-signing xattrs as
// AppleDouble (._*) sidecar files. jpackage calls `codesign --remove-signature` on every file
// it writes, including the non-Mach-O ._* sidecars, which makes codesign exit 1.
// Fix: detect ExFAT via diskutil and redirect output to ~/.gradle/askamerica-engine-build/
// (APFS, native xattrs). On macOS CI runners (APFS) and Linux CI, use layout.buildDirectory.
val isMacOs = System.getProperty("os.name").lowercase().contains("mac")

fun isExFat(path: File): Boolean {
    if (!isMacOs) return false
    return try {
        val output = ProcessBuilder("diskutil", "info", path.absolutePath)
            .redirectErrorStream(true).start()
            .inputStream.bufferedReader().readText()
        output.contains("ExFAT", ignoreCase = true)
    } catch (e: Exception) { false }
}

val jpackageBuildBase: File = if (isMacOs && isExFat(projectDir)) {
    File(System.getProperty("user.home"), ".gradle/askamerica-engine-build")
} else {
    layout.buildDirectory.asFile.get()
}
val jpackageDirFile = File(jpackageBuildBase, "jpackage")
val jpackageInputDirFile = File(jpackageBuildBase, "jpackage-input")
val jlinkRuntimeDirFile = File(jpackageBuildBase, "jlink-runtime")

// Thin launcher JAR — only McpServerLauncher; the fat engine JAR is downloaded by postinstall
val launcherJar by tasks.registering(Jar::class) {
    archiveBaseName.set("askamerica-launcher")
    archiveClassifier.set("")
    from(sourceSets.main.get().output) {
        include("**/McpServerLauncher.class")
    }
    manifest {
        attributes["Main-Class"] = "org.apache.calcite.adapter.askamerica.McpServerLauncher"
    }
    dependsOn(tasks.compileJava)
}

tasks.register<Copy>("prepareJpackageInput") {
    dependsOn(launcherJar)
    from(launcherJar.get().archiveFile)
    into(jpackageInputDirFile)
}

tasks.register<Exec>("jlinkRuntime") {
    val jlinkTool = "${System.getProperty("java.home")}/bin/jlink"
    commandLine(
        jlinkTool,
        "--module-path", "${System.getProperty("java.home")}/jmods",
        "--add-modules", "java.base,java.logging,java.net.http,java.sql,java.xml",
        "--strip-debug",
        "--no-header-files",
        "--no-man-pages",
        "--compress=2",
        "--output", jlinkRuntimeDirFile.absolutePath
    )
    doFirst {
        jlinkRuntimeDirFile.deleteRecursively()
    }
    doLast {
        // dot_clean on ExFAT just deletes ._* files (nothing to merge into).
        // Removes any AppleDouble sidecars jlink created during the output write.
        project.exec { commandLine("dot_clean", "-m", jlinkRuntimeDirFile.absolutePath) }
        jlinkRuntimeDirFile.walkTopDown()
            .filter { it.name.startsWith("._") }
            .forEach { it.delete() }
    }
}

tasks.register<Exec>("jpackage") {
    group = "distribution"
    description = "Package the query engine with a bundled JRE for distribution"
    dependsOn("prepareJpackageInput")

    val jpackageTool = "${System.getProperty("java.home")}/bin/jpackage"
    val os = System.getProperty("os.name").lowercase()
    val isMac = os.contains("mac")
    val packageType = when {
        isMac -> "app-image"
        os.contains("win") -> "msi"
        else -> "deb"
    }
    val version = project.version.toString().replace("-SNAPSHOT", "")
        .replace("[^0-9.]".toRegex(), "")
        .ifEmpty { "1.0.0" }
    val macResourceDir = project.file("src/packaging/mac").absolutePath

    if (isMac) {
        dependsOn("jlinkRuntime")
    }

    commandLine(
        jpackageTool,
        "--type", packageType,
        "--name", "AskAmerica MCP",
        "--app-version", version,
        "--vendor", "AskAmerica",
        "--description", "AskAmerica MCP — query US government data from Claude",
        "--input", jpackageInputDirFile.absolutePath,
        "--main-jar", launcherJar.get().archiveFileName.get(),
        "--main-class", "org.apache.calcite.adapter.askamerica.McpServerLauncher",
        "--dest", jpackageDirFile.absolutePath,
        "--java-options", "-Xms256m -Xmx2g",
        "--java-options", "-Dfile.encoding=UTF-8",
        *(if (isMac) arrayOf("--runtime-image", jlinkRuntimeDirFile.absolutePath) else emptyArray()),
        *(if (isMac) arrayOf("--resource-dir", macResourceDir) else emptyArray()),
        *(if (isMac && !System.getenv("ASKAMERICA_SIGN_IDENTITY").isNullOrEmpty())
            arrayOf("--mac-sign",
                    "--mac-signing-key-user-name", System.getenv("ASKAMERICA_SIGN_IDENTITY"))
        else emptyArray()),
        *(if (isMac && !System.getenv("ASKAMERICA_SIGN_KEYCHAIN").isNullOrEmpty())
            arrayOf("--mac-signing-keychain", System.getenv("ASKAMERICA_SIGN_KEYCHAIN"))
        else emptyArray())
    )

    doFirst {
        jpackageDirFile.mkdirs()
        for (dir in listOf(jpackageInputDirFile, jpackageDirFile)) {
            dir.walkTopDown().filter { it.name.startsWith("._") }.forEach { it.delete() }
        }
    }
}
