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
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

plugins {
    id("com.github.johnrengelman.shadow")
    id("maven-publish")
}

publishing {
    publications {
        create<MavenPublication>("shadow") {
            artifact(tasks["shadowJar"])
            groupId = "io.simpleishard"
            artifactId = "govdata"
            // Must honour -PreleaseVersion, as every trino publication does. Without it the POM
            // carried project.version -- 1.42.0-SNAPSHOT -- while the bundle laid the artifacts
            // out under the real release version, so Central received a deployment whose POM
            // version was both a SNAPSHOT and inconsistent with its own coordinates. The upload
            // still returned 201; validation then rejected it, invisibly.
            version = (project.findProperty("releaseVersion") as String?
                ?: project.version.toString().replace("-SNAPSHOT", ""))
                .let { if (it.isBlank() || it == "unspecified") "0.0.1" else it }

            pom {
                name.set("GovData")
                description.set("Query US government datasets — SEC, BLS, Census, NOAA, FBI, FEC, and more via SQL")
                url.set("https://github.com/kenstott/calcite")
                licenses {
                    license {
                        name.set("Business Source License 1.1")
                        url.set("https://mariadb.com/bsl11/")
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
            url = uri("https://maven.pkg.github.com/kenstott/calcite")
            credentials {
                username = System.getenv("GITHUB_ACTOR") ?: ""
                password = System.getenv("GITHUB_TOKEN") ?: ""
            }
        }
    }
}

// govdata depends on file.etl which requires Java 11+. Skip compilation and tests on JDK 8
// so the upstream Calcite JDK 8 CI jobs don't fail on missing file.etl classes.
if (JavaVersion.current() < JavaVersion.VERSION_11) {
    afterEvaluate {
        tasks.withType<JavaCompile>().configureEach { enabled = false }
        tasks.withType<Test>().configureEach { enabled = false }
    }
}

dependencies {
    api(project(":core"))
    api(project(":linq4j"))
    api(project(":file"))
    api("org.checkerframework:checker-qual")

    implementation("com.google.guava:guava")
    implementation("org.apache.calcite.avatica:avatica-core")
    implementation("commons-io:commons-io")
    implementation("org.apache.commons:commons-lang3")

    // XML processing (SEC is XML-based, we'll use standard parsers)
    implementation("javax.xml.bind:jaxb-api:2.3.1")

    // Parquet dependencies (inherited from file adapter)
    implementation("org.apache.parquet:parquet-arrow:1.15.2")
    implementation("org.apache.parquet:parquet-avro:1.15.2")
    implementation("org.apache.parquet:parquet-column:1.15.2")
    implementation("org.apache.parquet:parquet-common:1.15.2")
    implementation("org.apache.parquet:parquet-encoding:1.15.2")
    implementation("org.apache.parquet:parquet-hadoop:1.15.2")

    // Hadoop dependencies needed for Parquet
    implementation("org.apache.hadoop:hadoop-common:3.4.3")
    implementation("org.apache.hadoop:hadoop-client:3.4.3")

    // HTTP client for SEC EDGAR API
    implementation("org.apache.httpcomponents:httpclient:4.5.14")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.17")

    // HTML parsing for inline XBRL
    implementation("org.jsoup:jsoup")

    // Excel/XLSX parsing for EIA bulk downloads (EIA-860, EIA-861, EIA-814)
    implementation("org.apache.poi:poi:5.3.0")
    implementation("org.apache.poi:poi-ooxml:5.3.0")

    // Pure-Java Access (.mdb) reader for the NTSB aviation accident database (avall.mdb)
    implementation("com.healthmarketscience.jackcess:jackcess:4.0.5")

    // Geometry processing with JTS (lightweight)
    implementation("org.locationtech.jts:jts-core:1.19.0")

    // Embedding model dependencies
    implementation("com.microsoft.onnxruntime:onnxruntime:1.16.3")
    implementation("ai.djl:api:0.25.0")
    implementation("ai.djl.huggingface:tokenizers:0.25.0")

    // DuckDB for JSON to Parquet conversion
    implementation("org.duckdb:duckdb_jdbc:1.4.4.0")

    testImplementation(project(":testkit"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("org.duckdb:duckdb_jdbc:1.4.4.0")
    implementation("org.apache.iceberg:iceberg-core:1.4.0")
    // hadoop-aws 3.4.x puts S3AFileSystem on AWS SDK v2; 3.3.x pulled the 296MB SDK v1
    // uber-bundle and was the sole reason v1 was on the classpath. Its v2 bundle is bigger
    // still, so it is excluded here in favour of the modular jars declared below.
    implementation("org.apache.hadoop:hadoop-aws:3.4.3") {
      exclude(group = "software.amazon.awssdk", module = "bundle")
    }
    testImplementation("org.apache.hadoop:hadoop-aws:3.4.3") {
      exclude(group = "software.amazon.awssdk", module = "bundle")
    }
    // AWS SDK v2, BOM-pinned to the version hadoop-project 3.4.3 expects. Declared here
    // rather than inherited through :file so the modules S3A needs are explicit in the
    // module that ships the shadow jar.
    implementation(platform("software.amazon.awssdk:bom:2.35.4"))
    implementation("software.amazon.awssdk:s3")
    implementation("software.amazon.awssdk:sts")
    implementation("software.amazon.awssdk:kms")
    implementation("software.amazon.awssdk:apache-client")
    implementation("software.amazon.awssdk:s3-transfer-manager")
    implementation("software.amazon.awssdk:netty-nio-client")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.23.1")
    testRuntimeOnly("org.apache.logging.log4j:log4j-core:2.23.1")

    // Runtime logging for ETL runner (included in shadow JAR)
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.23.1")
    runtimeOnly("org.apache.logging.log4j:log4j-core:2.23.1")
}

// Maven Central requires a javadoc jar carrying real generated documentation. The publish
// workflow used to fabricate one from a stub text file, which is why govdata has never
// published there while every other io.simpleishard artifact has.
//
// Javadoc does not currently pass doclint anywhere in this repo -- govdata reports 52 errors
// and file 84 -- so generation runs with doclint off rather than being gated on a bar no
// module meets. That is deliberately a packaging decision, not an endorsement: the underlying
// doc defects (unescaped & in MD&A/R&D, out-of-sequence headings, unresolved @link targets)
// are real and worth fixing separately.
tasks.named<Javadoc>("javadoc") {
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
    isFailOnError = false
}

tasks.register("cleanTestLogs") {
    // Always run — no UP-TO-DATE skipping on ExFAT/APFS where ._* sidecar files accumulate
    outputs.upToDateWhen { false }
    doLast {
        // macOS creates AppleDouble ._* files that block Gradle's directory deletion on ExFAT/APFS.
        // Gradle's fileTree excludes hidden files by default, so use exec(find) to reach them.
        val buildDir = layout.buildDirectory.get().asFile
        exec {
            commandLine("find", buildDir.absolutePath, "-name", "._*", "-delete")
            isIgnoreExitValue = true
        }
        val testLogs = layout.buildDirectory.dir("test-logs").get().asFile
        if (testLogs.exists()) {
            testLogs.deleteRecursively()
        }
    }
}

// Remove macOS resource fork (._*) files from build/test-* dirs before Gradle's own cleanup
// runs — these files block Java's File.delete() on external HFS+ / APFS volumes.
tasks.register<Exec>("cleanMacResourceForks") {
    commandLine("sh", "-c",
        "find '${layout.buildDirectory.get()}/test-results' " +
        "'${layout.buildDirectory.get()}/test-logs' " +
        "-name '._*' -delete 2>/dev/null; true")
}

tasks.named("compileJava") {
    dependsOn("cleanTestLogs")
}

tasks.test {
    dependsOn("cleanMacResourceForks", "cleanTestLogs")
    workingDir = layout.buildDirectory.get().asFile

    // SchemaListDriftTest reads this script to assert its ALL_SCHEMAS matches the Java schema map.
    // Gradle cannot infer a file a test opens at runtime, so without declaring it the task stays
    // UP-TO-DATE after a script-only edit and the drift check never re-runs — exactly the silent
    // skip the test exists to prevent.
    inputs.file("scripts/model-verify.sh").withPathSensitivity(PathSensitivity.RELATIVE)

    // Run tests serially to avoid DuckDB file lock conflicts
    maxParallelForks = 1

    // Disable JUnit5 parallel execution (overrides root build.gradle.kts setting)
    // DuckDB requires exclusive file locks, so tests must run sequentially
    systemProperty("junit.jupiter.execution.parallel.enabled", "false")
    systemProperty("junit.jupiter.execution.parallel.mode.default", "same_thread")

    // Increase heap size for tests that process large CSV files
    // BLS QCEW bulk downloads can have 250k+ rows per year, each with 20+ columns
    // Note: Keep maxHeapSize below system RAM to avoid OOM kills (exit code 137)
    minHeapSize = System.getenv("TEST_MIN_HEAP") ?: "1g"
    maxHeapSize = System.getenv("TEST_MAX_HEAP") ?: "4g"

    // JVM crash debugging - generates detailed crash logs and heap dumps
    jvmArgs(
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-XX:HeapDumpPath=${layout.buildDirectory.get()}/test-logs/heapdump.hprof",
        "-XX:ErrorFile=${layout.buildDirectory.get()}/test-logs/hs_err_pid%p.log",
        "-XX:+CrashOnOutOfMemoryError",  // Force crash with error file on OOM
        "-XX:NativeMemoryTracking=summary"  // Track native memory usage
    )

    testLogging {
        events("passed", "skipped", "failed", "standardOut", "standardError")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }

    useJUnitPlatform {
        includeTags("unit")
        if (project.hasProperty("runAllTests")) {
            includeTags()
        }
        if (project.hasProperty("includeTags")) {
            val tags = project.property("includeTags").toString().split(",")
            includeTags(*tags.toTypedArray())
        }
    }
}

// Shadow JAR configuration for fat JDBC driver (includes govdata + file adapters)
tasks.shadowJar {
    archiveBaseName.set("sih-govdata")
    archiveClassifier.set("")
    // The exclude belongs to the merge transformer, not the task: mergeServiceFiles() rebuilds
    // META-INF/services from the inputs, so a task-level exclude() is overwritten by it.
    mergeServiceFiles {
        exclude("META-INF/services/java.net.spi.InetAddressResolverProvider")
    }
    // The transformer only governs what it merges; the provider file also arrives as an ordinary
    // resource, so it has to be filtered out of the task inputs as well.
    exclude("META-INF/services/java.net.spi.InetAddressResolverProvider")

    // Enable zip64 for large JARs
    isZip64 = true

    // Exclude signature files
    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")

    // hadoop-common 3.4.x brings a dnsjava that registers a JDK InetAddressResolverProvider.
    // Its implementation class ships only under META-INF/versions/18 (multi-release), and this
    // shadow jar is not marked Multi-Release, so the JVM cannot load it while ServiceLoader
    // still reads the merged provider file -- every InetAddress.getLocalHost() then dies with
    // ServiceConfigurationError, which Log4j triggers during startup. The registration is dropped
    // in the mergeServiceFiles block above, leaving the JVM's built-in resolver in charge as it
    // was before the Hadoop upgrade; dnsjava itself stays on the classpath for the Hadoop code
    // paths that call it directly.

    manifest {
        attributes["Main-Class"] = "org.apache.calcite.adapter.govdata.etl.EtlRunner"
    }
}

// Task to download DuckDB extensions for all platforms (air-gapped operation)
tasks.register("downloadDuckDbExtensions") {
    group = "build"
    description = "Download DuckDB extensions for all platforms (linux_amd64, osx_amd64, osx_arm64, windows_amd64)"

    doLast {
        val duckdbVersion = "1.4.4"
        val extensionsDir = file("src/main/resources/duckdb/extensions")
        val platforms = listOf("linux_amd64", "osx_amd64", "osx_arm64", "windows_amd64")
        // NOTE: only core-repo extensions belong here. zipfs is a community extension (served from
        // community-extensions.duckdb.org, not extensions.duckdb.org) so it 404s here and is not
        // bundled; the Stooq bulk path extracts entries to disk and reads them without zipfs.
        val extensions = listOf("spatial", "httpfs", "iceberg", "h3", "excel", "fts", "quackformers", "parquet")
        val baseUrl = "http://extensions.duckdb.org/v$duckdbVersion"

        println("Downloading DuckDB $duckdbVersion extensions for ${platforms.size} platforms...")

        for (platform in platforms) {
            val platformDir = file("$extensionsDir/$platform")
            platformDir.mkdirs()

            for (ext in extensions) {
                val extFile = file("$platformDir/$ext.duckdb_extension")
                if (extFile.exists()) {
                    println("  ✓ $platform/$ext.duckdb_extension (already present, ${extFile.length() / 1024 / 1024} MB)")
                    continue
                }

                val url = "$baseUrl/$platform/$ext.duckdb_extension.gz"
                println("  ⬇ Downloading $ext for $platform...")

                try {
                    exec {
                        commandLine("sh", "-c", "curl -L '$url' | gunzip > '$extFile'")
                    }
                    println("    ✓ $ext ($platform) ${extFile.length() / 1024 / 1024} MB")
                } catch (e: Exception) {
                    println("    ✗ Error downloading $ext ($platform): ${e.message}")
                }
            }
        }

        println("\nDuckDB extensions download complete!")
        val totalBytes = extensionsDir.walk().sumOf { if (it.isFile) it.length() else 0L }
        println("Total size: ${totalBytes / 1024 / 1024 / 1024} GB (${totalBytes / 1024 / 1024} MB)")
    }
}

// Task to package the pre-built shared DuckDB catalog + per-schema .conversions.json trackers as a
// JAR-bundled seed that accelerates cold time-to-first-query. Run OUT-OF-BAND, after a full
// materialization run has populated the operating directory (default ~/.govdata). shadowJar then
// packages whatever src/main/resources/duckdb/seed contains — the JAR build itself needs no S3
// credentials. The seed must be built against s3:// URIs (govdata's S3 mode) so every view path and
// .conversions.json record is machine-independent; a local-parquet operating dir is NOT portable.
tasks.register("bundleGovdataSeed") {
    group = "build"
    description = "Zip the materialized shared DuckDB catalog + .conversions.json trackers into a JAR-bundled seed"

    doLast {
        val basePath = (project.findProperty("seedOperatingDir") as String?)
            ?: System.getenv("GOVDATA_DATA_DIR")
            ?: "${System.getProperty("user.home")}/.govdata"
        val base = file(basePath)

        val catalog = file("$base/.duckdb/govdata.duckdb")
        val wal = file("$base/.duckdb/govdata.duckdb.wal")
        val aperio = file("$base/.aperio")

        // Fail loudly on anything that would produce a broken or non-portable seed.
        if (!catalog.isFile) {
            throw GradleException("No shared catalog at $catalog — run a full materialization first, "
                + "or override the base with -PseedOperatingDir=/path.")
        }
        if (wal.exists()) {
            throw GradleException("Uncheckpointed WAL present ($wal) — CHECKPOINT/close the catalog "
                + "before bundling so the seed is a clean single file.")
        }
        val trackers = if (aperio.isDirectory) {
            aperio.walk().filter { it.isFile && it.name == ".conversions.json" }.toList()
        } else {
            emptyList()
        }
        if (trackers.isEmpty()) {
            throw GradleException("No .aperio/*/.conversions.json trackers under $base — nothing to seed.")
        }

        // Every declared schema must be represented. A partial seed installs and works, so a
        // missing schema is invisible: the 24 it covers start instantly while the 25th rebuilds
        // every one of its views from Iceberg metadata on the user's first query — the slow cold
        // start the seed exists to remove, now affecting one schema instead of all of them and
        // with nothing in the build to say so. The shipped seed was short exactly one schema
        // (fiscal) for this reason.
        val declaredSchemas = fileTree("src/main/resources") { include("**/*-schema.yaml") }
            .map { it.name.removeSuffix("-schema.yaml").replace('-', '_') }
            .toSortedSet()
        val seededSchemas = trackers.mapNotNull { it.parentFile?.name }.toSortedSet()
        val missingSchemas = declaredSchemas - seededSchemas
        if (missingSchemas.isNotEmpty()) {
            val message = ("Seed covers ${seededSchemas.size} of ${declaredSchemas.size} declared "
                + "schemas; missing: ${missingSchemas.joinToString(",")}. Materialize them into "
                + "$base before bundling. Pass -PseedAllowPartial=true to bundle anyway, "
                + "accepting that those schemas rebuild all their views on a user's first query.")
            if (project.hasProperty("seedAllowPartial")) {
                logger.warn("bundleGovdataSeed: {}", message)
            } else {
                throw GradleException(message)
            }
        }

        val seedDir = file("src/main/resources/duckdb/seed")
        seedDir.mkdirs()
        val zipFile = file("$seedDir/govdata-seed.zip")
        val versionFile = file("$seedDir/govdata-seed.version")
        val version = (project.findProperty("seedVersion") as String?) ?: project.version.toString()

        // (file -> zip entry name, relative to the operating base, forward-slash normalized)
        val members = ArrayList<Pair<File, String>>()
        members.add(catalog to ".duckdb/govdata.duckdb")
        for (t in trackers) {
            val rel = base.toPath().relativize(t.toPath()).toString()
                .replace(File.separatorChar, '/')
            members.add(t to rel)
        }

        ZipOutputStream(BufferedOutputStream(FileOutputStream(zipFile))).use { zos ->
            for ((f, name) in members) {
                zos.putNextEntry(ZipEntry(name))
                f.inputStream().use { it.copyTo(zos) }
                zos.closeEntry()
            }
        }
        versionFile.writeText(version)

        // The Iceberg schema cache ships as its own resource rather than a zip member. The zip is
        // extracted into the operating base, but the schema cache is read from the Iceberg cache
        // directory, which is a different location — a zip entry would land where nothing looks
        // for it. GovDataSeedInstaller installs this resource into the right directory instead.
        val schemaCacheSrc = file("$base/.iceberg_metadata_cache/iceberg-schema-cache.json")
        if (!schemaCacheSrc.isFile) {
            throw GradleException("No Iceberg schema cache at $schemaCacheSrc — generation did not "
                + "read any Iceberg table live, so a cold start would have nothing to seed.")
        }
        val schemaCacheDest = file("$seedDir/iceberg-schema-cache.json")
        schemaCacheSrc.copyTo(schemaCacheDest, overwrite = true)

        println("Bundled govdata seed: ${members.size} entries (1 catalog + ${trackers.size} trackers) "
            + "-> $zipFile (${zipFile.length() / 1024} KB, version $version)")
        println("Bundled Iceberg schema cache: $schemaCacheDest "
            + "(${schemaCacheDest.length() / 1024} KB)")
    }
}

// Task to run ETL runner directly from Gradle
tasks.register<JavaExec>("etlRunner") {
    group = "application"
    description = "Run the ETL runner for downloading historical government data"

    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("org.apache.calcite.adapter.govdata.etl.EtlRunner")

    // Default JVM memory settings for processing large datasets
    minHeapSize = System.getenv("ETL_MIN_HEAP") ?: "1g"
    maxHeapSize = System.getenv("ETL_MAX_HEAP") ?: "4g"

    // Pass through any command line arguments
    // Usage: ./gradlew :govdata:etlRunner --args="--model path/to/model.json"
}
// publish trigger: expose INFORMATION_SCHEMA via GovDataSchemaFactory
