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

// askamerica-engine depends on govdata (Java 11+) and on smile-base 4.x, which ships Java 21
// bytecode (class file 65.0). Skip the module on anything older so the upstream Calcite CI jobs
// running an older JDK do not fail on classes they cannot read.
if (JavaVersion.current() < JavaVersion.VERSION_21) {
    afterEvaluate {
        tasks.withType<JavaCompile>().configureEach { enabled = false }
        tasks.withType<Test>().configureEach { enabled = false }
    }
} else {
    // Override the root's --release 11 convention: at 11 javac refuses to read
    // smile's Java 21 class files.
    tasks.withType<JavaCompile>().configureEach { options.release.set(21) }
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

// SLF4J 1.7 resolves its binding through org/slf4j/impl/StaticLoggerBinder, and Hadoop drags in
// slf4j-reload4j, which therefore wins. Nothing configures reload4j, so every adapter log
// statement was discarded and reload4j printed "log4j:WARN No appenders could be found" in its
// place — which is why a cold start that spends minutes rebuilding Iceberg views looks like a hung
// server: the entire diagnostic channel is dead. govdata declares log4j-slf4j2-impl, but that only
// binds under SLF4J 2.x and can never activate while :bom pins slf4j-api to strictly 1.7.25, so
// the bundled log4j2.xml has been inert. Keep the reload4j binding off the classpath so the
// log4j2 provider below is the only one StaticLoggerBinder can resolve to.
configurations.all {
    exclude(group = "org.slf4j", module = "slf4j-reload4j")
}

dependencies {
    implementation(project(":driver-base"))
    implementation(project(":govdata"))
    implementation("com.formdev:flatlaf:3.3")
    implementation("org.knowm.xchart:xchart:4.0.4")
    // PDF-to-text for fetch_pdf_as_text (McpServer). Not previously a real dependency here —
    // the shadowJar excludes for org/apache/pdfbox/** predate this and were dead code (no
    // pdfbox anywhere in the resolved runtime classpath) until this line.
    implementation("org.apache.pdfbox:pdfbox:2.0.31")
    // Excel/Word parsing for fetch_xlsx_as_json/fetch_docx_as_text (McpServer). govdata
    // already declares these for ETL, but Gradle's `implementation` config is not transitive
    // to a consuming module's compile classpath — this module needs its own declaration to
    // reference POI classes directly, even though the resolved jar was already on the
    // runtime classpath via govdata.
    implementation("org.apache.poi:poi:5.3.0")
    implementation("org.apache.poi:poi-ooxml:5.3.0")
    // Multivariate regression (OLS, 2SLS) and hypothesis tests for ols_regression,
    // iv_2sls, diff_in_diff, and hypothesis_test — real matrix algebra DuckDB's
    // single-pass SQL aggregates (corr, regr_slope, ...) can't express. Frozen/stable
    // API (last release 2016); no transitive dependencies of its own.
    implementation("org.apache.commons:commons-math3:3.6.1")
    // Random forest / gradient boosting for flexible_regression, feature_importance, and
    // as the nuisance-function learner in double_ml_ate — nonlinear ML that neither DuckDB
    // SQL nor Commons Math's linear-model classes can do. Pure-JVM (no Python/native
    // runtime), matching this server's existing bundled-JRE distribution model. Pinned to
    // 4.3.0, not the newest 6.x line: 6.2.5's smile-base jar is compiled to Java 25 class
    // files (major version 69), which this module's Java 21 compiler chain cannot read
    // ("bad class file") — 4.3.0 targets Java 21 bytecode (major version 65) and is
    // confirmed compatible. Re-check bytecode version before bumping past this.
    implementation("com.github.haifengl:smile-core:4.3.0") {
        // smile-base strictly pins duckdb_jdbc:1.2.0 (its own optional DataFrame-from-DuckDB
        // reader, unused here — StatsMlEngine builds DataFrames directly from extracted
        // columns) which conflicts with this module's actual duckdb_jdbc:1.4.4.0 and fails
        // Gradle's consistent-resolution check on the test classpath. Excluding it leaves
        // the project's own duckdb_jdbc version in charge everywhere.
        exclude(group = "org.duckdb", module = "duckdb_jdbc")
    }

    // log4j-slf4j-impl (not -slf4j2-impl) is the log4j2 binding for SLF4J 1.7, which is the API
    // version this jar actually ships. Pairs with govdata's bundled log4j2.xml.
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.23.1")
    runtimeOnly("org.apache.logging.log4j:log4j-core:2.23.1")

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
    // hadoop-common brings a dnsjava that registers a JDK InetAddressResolverProvider whose
    // implementation ships only under META-INF/versions/18. This jar is not marked
    // Multi-Release, so the JVM cannot load that class while ServiceLoader still reads the
    // merged provider file -- every InetAddress lookup then dies with
    // ServiceConfigurationError. In the MCP server that surfaces as list_tables and
    // describe_table failing outright, because opening a connection resolves a host.
    // Dropping the registration leaves the JVM's own resolver in charge; dnsjava itself
    // stays on the classpath for callers that use it directly. Same fix as the govdata
    // shadow jar, and it must be excluded in both places: mergeServiceFiles() rebuilds
    // META-INF/services from the inputs, so a task-level exclude alone is overwritten,
    // while the transformer alone leaves the copy that arrives as an ordinary resource.
    mergeServiceFiles {
        exclude("META-INF/services/java.net.spi.InetAddressResolverProvider")
    }
    exclude("META-INF/services/java.net.spi.InetAddressResolverProvider")

    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")

    // slf4j-reload4j and log4j-slf4j-impl both ship org/slf4j/impl/StaticLoggerBinder.class, so
    // which backend SLF4J 1.7 binds to would be settled by whichever jar shadow merges first. Bound
    // to reload4j, which nothing here configures, every adapter log line is discarded — the silent
    // cold start this exists to prevent. The configurations-wide exclude above keeps the module off
    // runtimeClasspath; this drops any copy that still reaches the merge, so exactly one binding
    // survives and it is the log4j2 one that log4j2.xml configures.
    dependencies {
        exclude(dependency("org.slf4j:slf4j-reload4j"))
    }

    // ── ETL-only govdata packages ──────────────────────────────────────────
    // govdata/etl is pure ETL orchestration — safe to exclude
    exclude("org/apache/calcite/adapter/govdata/etl/**")
    // file/etl and file/refresh contain classes referenced by schema factories
    // at connection time — keep them to avoid NoClassDefFoundError at query time

    // ── ETL-only third-party libraries ─────────────────────────────────────

    // PDF parsing (pdfbox/fontbox/xmpbox) and Excel/Word parsing (poi) are now query-time
    // dependencies too — McpServer's fetch_pdf_as_text/fetch_docx_as_text/fetch_xlsx_as_json
    // use them directly. No longer excluded; kept in the shaded jar.

    // HTML scraping
    exclude("org/jsoup/**")

    // DJL stays out: the query-time tokenizer is BertWordPieceTokenizer, in-tree and pure Java,
    // specifically so no per-platform tokenizer binary is needed.
    exclude("ai/djl/**")

    // ONNX Runtime is REQUIRED at query time and must NOT be excluded. SEMANTIC_SEARCH and
    // EMBED embed the query through OnnxClsEmbedder; without these classes both fail with
    // "no embedder configured", which is exactly the state this jar shipped in until now.
    // The earlier "not used at query time" assumption held only while embeddings were an
    // ETL-only concern.

    // Orphaned ML resources — nothing loads these any more (the arctic-embed-xs int8 model
    // under models/snowflake-arctic-embed-xs/ is the live one and is deliberately kept).
    exclude("models/all-MiniLM-L6-v2/**")   // ~79 MB, superseded, mean-pooled
    exclude("native/lib/**")                 // ~16 MB HuggingFace tokenizer natives (DJL)

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
        // "onnxruntime" removed: it backs OnnxClsEmbedder, the query-time embedder for
        // SEMANTIC_SEARCH/EMBED. Dropping the jar left those functions permanently broken.
        // "pdfbox"/"fontbox"/"xmpbox"/"poi" removed: fetch_pdf_as_text/fetch_docx_as_text/
        // fetch_xlsx_as_json (McpServer) need them at query time now.
        "jsoup",
        "tokenizers",   // ai.djl.huggingface tokenizers — djl classes excluded from fat jar too
        "arrow-gandiva", // LLVM expr compiler — reached only via reflective .arrow path (govdata has none)
        // hadoop-aws (S3AFileSystem) + its 297 MB aws-java-sdk-bundle. The whole read path
        // is now off hadoop s3a: Iceberg loaders use S3FileIO (AWS v2), and partitioned-
        // table discovery checks existence via S3StorageProvider (AWS v2). The small split
        // aws-java-sdk-s3/core/kms stays for the still-v1 S3HivePipelineTracker (ETL).
        "hadoop-aws", "aws-java-sdk-bundle",
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

// Stamp the engine release version into the fat jar. EngineInstaller reads it back out of
// the cached jar and compares it against the newest GitHub release; without it a jar cached
// once is used forever.
//
// Deliberately not Implementation-Version: the root build already sets that to the Apache
// Calcite version (1.42.0), which is unrelated to the engine-vX.Y.Z release line. Comparing
// it against the latest tag would mark every jar stale and re-download ~460MB on every
// launch. A dedicated attribute, set only when CI passes -PreleaseVersion, also leaves a
// local build unstamped — the launcher then reports the check inconclusive and keeps the
// jar it has, rather than fighting the developer's own build.
val engineReleaseVersion: String? = project.findProperty("releaseVersion") as String?

tasks.shadowJar {
    if (!engineReleaseVersion.isNullOrBlank()) {
        manifest {
            attributes["AskAmerica-Engine-Version"] = engineReleaseVersion
        }
    }
}

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
// Persist jpackage's working files (incl. the generated config/main.wxs) so CI can
// dump main.wxs to learn where jpackage splices overrides.wxi.
val jpackageTempFile = File(jpackageBuildBase, "jpackage-temp")

// Thin launcher JAR — only McpServerLauncher; the fat engine JAR is downloaded by postinstall
val launcherJar by tasks.registering(Jar::class) {
    archiveBaseName.set("askamerica-launcher")
    archiveClassifier.set("")
    from(sourceSets.main.get().output) {
        include("**/McpServerLauncher.class")
        include("**/EngineInstaller.class")
        include("**/EngineInstaller\$*.class")
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
        // Module set is the output of
        //   jdeps --multi-release 17 --print-module-deps --ignore-missing-deps <shadow jar>
        // plus jdk.crypto.ec, which jdeps cannot see (loaded as a security provider).
        // Re-run that jdeps command whenever a dependency is added: a module missing
        // here fails only at runtime, as a NoClassDefFoundError deep inside a library
        // (e.g. java.management → AwsSdkMetrics → every S3StorageProvider construction).
        // java.desktop → Swing (setup wizard + download progress window);
        // java.management → AWS SDK v1 JMX metrics registration (AmazonS3Client <clinit>);
        // jdk.unsupported → sun.misc.Unsafe (Arrow, Netty, Guava);
        // jdk.crypto.ec → TLS ciphers for the HTTPS engine download.
        "--add-modules",
        "java.base,java.compiler,java.desktop,java.instrument,java.logging,java.management,"
            + "java.naming,java.net.http,java.rmi,java.scripting,java.security.jgss,"
            + "java.security.sasl,java.sql,java.xml,jdk.crypto.ec,jdk.httpserver,"
            + "jdk.unsupported",
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
    // Use the engine release version (from -PreleaseVersion, set by CI from the
    // engine-v<X.Y.Z> tag), NOT the Calcite project.version — otherwise installer
    // filenames carry the unrelated Calcite version (e.g. 1.42.0).
    val engineVersion = publishVersion
        .replace("[^0-9.]".toRegex(), "")
        .ifEmpty { "1.0.0" }
    // macOS rejects a CFBundleShortVersionString whose first component is 0, so
    // give jpackage a mac-safe internal app-version there (0.37.0 -> 1.37.0).
    // The user-facing PKG filename/version still uses the real engine version —
    // CI names the PKG from the engine-v<X.Y.Z> tag, not from this value.
    val version = if (isMac && (engineVersion.substringBefore('.').toIntOrNull() ?: 0) < 1) {
        "1." + engineVersion.substringAfter('.', "0.0")
    } else {
        engineVersion
    }
    val macResourceDir = project.file("src/packaging/mac").absolutePath
    val winResourceDir = project.file("src/packaging/windows").absolutePath

    if (isMac) {
        dependsOn("jlinkRuntime")
    }

    commandLine(
        jpackageTool,
        "--type", packageType,
        "--name", "AskAmerica MCP",
        "--app-version", version,
        "--vendor", "AskAmerica",
        "--description", "AskAmerica MCP - query US government data from Claude",
        "--input", jpackageInputDirFile.absolutePath,
        "--main-jar", launcherJar.get().archiveFileName.get(),
        "--main-class", "org.apache.calcite.adapter.askamerica.McpServerLauncher",
        "--dest", jpackageDirFile.absolutePath,
        "--temp", jpackageTempFile.absolutePath,
        "--java-options", "-Xms256m -Xmx2g",
        "--java-options", "-Dfile.encoding=UTF-8",
        // Windows: without these the MSI installs silently with no way to launch the
        // setup wizard. These add a Start-menu entry (+ desktop shortcut) so the user
        // can open "AskAmerica MCP", which shows SetupWindow to enter the API key.
        // (A finish-page overrides.wxi customization was tried but is structurally
        // incompatible with jpackage 21's WiX layout — dropped; re-add as its own task.)
        // Windows: Start-menu + desktop shortcut, plus a --resource-dir override of
        // jpackage's main.wxs that hooks the ExitDialog to launch the setup wizard on
        // Finish (Product-scope; overrides.wxi is fragment-scope and can't reach it).
        // --win-dir-chooser switches jpackage to the full WixUI dialog set (default
        // JpUI is empty — no finish page), which is what provides ExitDialog.
        *(if (os.contains("win"))
            arrayOf("--win-menu", "--win-menu-group", "AskAmerica", "--win-shortcut",
                    "--win-dir-chooser",
                    "--resource-dir", winResourceDir)
        else emptyArray()),
        // Linux: add an application-menu entry for the setup wizard.
        *(if (!isMac && !os.contains("win"))
            arrayOf("--linux-shortcut")
        else emptyArray()),
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
        // jpackage requires --temp to be absent/empty.
        jpackageTempFile.deleteRecursively()
        for (dir in listOf(jpackageInputDirFile, jpackageDirFile)) {
            dir.walkTopDown().filter { it.name.startsWith("._") }.forEach { it.delete() }
        }
    }
}
