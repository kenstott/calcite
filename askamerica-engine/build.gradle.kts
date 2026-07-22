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

    // Orphaned ML resources — the DJL/ONNX code that loads these is excluded above,
    // so they are dead weight in the read-only engine (Tier-1 slimming).
    exclude("models/all-MiniLM-L6-v2/**")   // ~79 MB ONNX embedding model
    exclude("native/lib/**")                 // ~16 MB HuggingFace tokenizer natives

    // Arrow Gandiva (~118 MB, almost all native libs) — the LLVM expression compiler
    // is reached only through FileSchema.createArrowTable(), which (a) fires only for
    // .arrow file sources and (b) loads the arrow adapter by reflection ("to avoid hard
    // dependency"). govdata datasets are parquet/iceberg read via DuckDB and never
    // declare .arrow sources, so this path is never taken — no NoClassDefFoundError on
    // the read path, at most a caught ClassNotFoundException inside that .arrow-only
    // method. (Tier-3 slimming.)
    exclude("gandiva_jni/**")                 // native libs (linux/osx x86_64+aarch64)
    exclude("org/apache/arrow/gandiva/**")    // gandiva Java classes

    // AWS SDK v1 codegen artifacts — the intermediate/model JSON under models/ are
    // build-time service descriptors, never loaded at runtime (the SDK reads compiled
    // classes). ~68 MB on disk. Flat models/*.json only — do NOT use **/*-model.json,
    // which would wrongly catch config/djia-wiki-model.json (a govdata resource).
    // The AWS v1 *classes* stay: the Iceberg read path still needs s3a (com.amazonaws)
    // until IcebergTable/IcebergCatalogManager move to S3FileIO (v2). (Tier-2a slimming.)
    exclude("models/*-intermediate.json")
    exclude("models/*-model.json")

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

// ─── Unshaded runtime jar-set (Python wheel packaging) ───────────────────────
// distribution.md: the pip / MCP path runs its own dedicated JVM via JPype, so it
// needs no shaded fat jar — it loads the plain runtime jars side by side on the
// classpath (verified: no relocation / mergeServiceFiles reliance). This stages that
// unshaded jar-set (engine jar + runtime deps) minus the ETL/ML-only jars the
// read-only engine never loads, mirroring the shadowJar excludes at the jar level.
//   Run: ./gradlew :askamerica-engine:stageEngineRuntime
//   Output: build/engine-runtime/*.jar  → bundled into the wheel as engine_jars/
//
// KNOWN LIMITATION (follow-up): govdata → :file resolves to file's OWN shadow jar
// (`sih-aperio`, ~641 MB, itself shaded with Gandiva etc.), so the staged set is
// currently [shaded sih-aperio + duplicated deps] ~1.15 GB, not a clean unshaded set.
// The dependency-level dropPrefixes below cannot slim what is baked inside sih-aperio.
// A truly slim/deduped set requires depending on :file's plain `jar` instead of its
// shadow jar. The set still BOOTS (sih-aperio is self-contained), so this validates the
// side-by-side-jars thesis; the <100 MB wheel partition needs the plain-jar change first.
// Unshaded classpath for the wheel jar-set. Plain runtimeClasspath resolves :file to
// its shadow jar (sih-aperio, ~641 MB) because :file targets Java 11 while this module
// inherits Calcite's Java 8 target, so Gradle rejects file's plain (Java 11) variant and
// falls back to the shadowed one (which carries no Java-11 constraint). Requesting
// jvm.version=11 + bundling=external here makes the plain calcite-file jar and its
// transitive dependency jars resolve instead — a genuinely unshaded, slimmable set.
val engineWheelClasspath by configurations.creating {
    isCanBeResolved = true
    isCanBeConsumed = false
    extendsFrom(configurations.runtimeClasspath.get())
    attributes {
        attribute(org.gradle.api.attributes.Usage.USAGE_ATTRIBUTE,
            objects.named(org.gradle.api.attributes.Usage::class.java, org.gradle.api.attributes.Usage.JAVA_RUNTIME))
        attribute(org.gradle.api.attributes.Category.CATEGORY_ATTRIBUTE,
            objects.named(org.gradle.api.attributes.Category::class.java, org.gradle.api.attributes.Category.LIBRARY))
        attribute(org.gradle.api.attributes.LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
            objects.named(org.gradle.api.attributes.LibraryElements::class.java, org.gradle.api.attributes.LibraryElements.JAR))
        attribute(org.gradle.api.attributes.Bundling.BUNDLING_ATTRIBUTE,
            objects.named(org.gradle.api.attributes.Bundling::class.java, org.gradle.api.attributes.Bundling.EXTERNAL))
        attribute(org.gradle.api.attributes.java.TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, 11)
    }
}

val stageEngineRuntime by tasks.registering(Sync::class) {
    group = "distribution"
    description = "Stage the unshaded runtime jar-set for the Python wheel"
    dependsOn(tasks.named("jar"))

    // ETL/ML-only dependency jars — the read path never loads these (loaders are
    // excluded from the fat jar too). Keeps AWS SDK v1: the Iceberg s3a read path
    // still needs com.amazonaws until IcebergTable moves to S3FileIO (v2).
    val dropPrefixes = listOf(
        "onnxruntime", "pdfbox", "fontbox", "xmpbox", "poi", "jsoup",
        "tokenizers",   // ai.djl.huggingface tokenizers — djl classes excluded from fat jar too
        "arrow-gandiva", // LLVM expr compiler — reached only via reflective .arrow path (govdata has none)
        // NOTE: hadoop-aws + the 297 MB aws-java-sdk-bundle CANNOT be dropped yet. The
        // Iceberg read loaders are migrated to S3FileIO (AWS v2), but a full-driver MinIO
        // query proved the partitioned-table discovery/globbing path in FileSchema still
        // instantiates hadoop's S3AFileSystem for S3 file listing. Dropping the bundle
        // needs those S3 filesystem operations migrated off s3a (onto S3StorageProvider)
        // first — tracked in distribution.md.
    )

    from(tasks.named("jar"))   // askamerica-engine's own classes
    from(engineWheelClasspath.filter { f ->
        val n = f.name.lowercase()
        val isDjl = f.absolutePath.replace('\\', '/').contains("/ai.djl/")
        !isDjl && dropPrefixes.none { n.startsWith(it) }
    })
    into(layout.buildDirectory.dir("engine-runtime"))

    doLast {
        val dir = layout.buildDirectory.dir("engine-runtime").get().asFile
        val jars = dir.listFiles { f -> f.name.endsWith(".jar") }?.size ?: 0
        val mb = (dir.listFiles()?.filter { it.isFile }?.sumOf { it.length() } ?: 0L) / 1048576
        logger.lifecycle("Staged engine jar-set: $jars jars, $mb MB → $dir")
    }
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
