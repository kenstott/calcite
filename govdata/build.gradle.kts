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
    id("maven-publish")
}

publishing {
    publications {
        create<MavenPublication>("shadow") {
            artifact(tasks["shadowJar"])
            groupId = "io.simpleishard"
            artifactId = "govdata"
            version = project.version.toString()

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
    implementation("org.apache.hadoop:hadoop-common:3.3.6")
    implementation("org.apache.hadoop:hadoop-client:3.3.6")

    // HTTP client for SEC EDGAR API
    implementation("org.apache.httpcomponents:httpclient:4.5.14")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.17")

    // HTML parsing for inline XBRL
    implementation("org.jsoup:jsoup")

    // PDF text extraction for security advisories (cyber_vuln)
    implementation("org.apache.pdfbox:pdfbox:2.0.31")

    // Excel/XLSX parsing for EIA bulk downloads (EIA-860, EIA-861, EIA-814)
    implementation("org.apache.poi:poi:5.3.0")
    implementation("org.apache.poi:poi-ooxml:5.3.0")

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
    implementation("com.amazonaws:aws-java-sdk-s3:1.12.565")
    implementation("org.apache.iceberg:iceberg-core:1.4.0")
    implementation("org.apache.hadoop:hadoop-aws:3.3.6")
    testImplementation("com.amazonaws:aws-java-sdk-s3:1.12.565")
    testImplementation("org.apache.hadoop:hadoop-aws:3.3.6")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.23.1")
    testRuntimeOnly("org.apache.logging.log4j:log4j-core:2.23.1")

    // Runtime logging for ETL runner (included in shadow JAR)
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.23.1")
    runtimeOnly("org.apache.logging.log4j:log4j-core:2.23.1")
}

tasks.register<JavaExec>("repairFec") {
    group = "govdata"
    description = "Fix FEC table column types in-place on R2 Iceberg (year→INT, dates→DATE)"
    mainClass.set("org.apache.calcite.adapter.govdata.FecDataRepair")
    classpath = sourceSets["main"].runtimeClasspath
    val tables = project.findProperty("tables") as String?
    if (tables != null) args = tables.split(",")
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
    mergeServiceFiles()

    // Enable zip64 for large JARs
    isZip64 = true

    // Exclude signature files
    exclude("META-INF/*.SF")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")

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
