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
import com.github.vlsi.gradle.ide.dsl.settings
import com.github.vlsi.gradle.ide.dsl.taskTriggers

plugins {
    id("com.github.vlsi.ide")
    id("com.github.johnrengelman.shadow")
}

dependencies {
    api(project(":core"))
    api(project(":linq4j"))
    api(project(":arrow")) // Need arrow for ParquetTable
    api("org.checkerframework:checker-qual")

    implementation("com.google.guava:guava")
    // PostgreSQL JDBC driver for PGPipelineTracker (the multi-writer tracker backend).
    // runtimeOnly: loaded reflectively via DriverManager, only needed when trackerBackend=pg.
    // Pinned to a modern version (NOT the BOM's ancient 9.3-1102-jdbc41, which predates
    // SCRAM-SHA-256 and fails against PG 14+ with "authentication type 10 is not supported").
    // Must be bundled in the govdata shadow jar or the pg backend fails with "No suitable driver".
    runtimeOnly("org.postgresql:postgresql:42.7.4")
    implementation("com.joestelmach:natty")
    implementation("com.opencsv:opencsv:5.7.1")
    implementation("org.apache.calcite.avatica:avatica-core")
    implementation("commons-io:commons-io")
    implementation("org.apache.commons:commons-lang3")
    implementation("org.jsoup:jsoup")
    implementation("org.apache.poi:poi:5.3.0")
    implementation("org.apache.poi:poi-ooxml:5.3.0")
    implementation("com.fasterxml.jackson.core:jackson-core:2.17.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.17.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.17.2")

    // Arrow dependencies for vectorized execution engine
    implementation("org.apache.arrow:arrow-vector:15.0.2")
    implementation("org.apache.arrow:arrow-memory-netty:15.0.2")
    implementation("org.apache.arrow:arrow-format:15.0.2")
    // Removed arrow-c-data - was only for DuckDB Arrow integration which has 400+ms overhead
    implementation("org.apache.arrow.gandiva:arrow-gandiva")

    // Parquet dependencies for Parquet execution engine
    implementation("org.apache.parquet:parquet-arrow:1.15.2")
    implementation("org.apache.parquet:parquet-avro:1.15.2")
    implementation("org.apache.parquet:parquet-column:1.15.2")
    implementation("org.apache.parquet:parquet-common:1.15.2")
    implementation("org.apache.parquet:parquet-encoding:1.15.2")
    implementation("org.apache.parquet:parquet-hadoop:1.15.2")

    // Hadoop dependencies for Parquet
    implementation("org.apache.hadoop:hadoop-common:3.3.6")
    implementation("org.apache.hadoop:hadoop-client:3.3.6")
    // Hadoop AWS for S3A FileSystem (required for Iceberg S3 support)
    implementation("org.apache.hadoop:hadoop-aws:3.3.6")

    // Storage provider dependencies
    implementation("com.amazonaws:aws-java-sdk-s3:1.12.565")
    implementation("commons-net:commons-net:3.9.0")
    implementation("com.jcraft:jsch:0.1.55")

    // Apache Iceberg support
    // iceberg-api is exposed as api() so dependent modules can use Iceberg Table type
    api("org.apache.iceberg:iceberg-api:1.4.0")
    implementation("org.apache.iceberg:iceberg-core:1.4.0")
    implementation("org.apache.iceberg:iceberg-common:1.4.0")
    implementation("org.apache.iceberg:iceberg-parquet:1.4.0")
    implementation("org.apache.iceberg:iceberg-data:1.4.0")
    // iceberg-aws provides S3FileIO (AWS SDK v2) so the read path can load Iceberg
    // tables from S3/MinIO without hadoop-aws + the AWS SDK v1 bundle (S3AFileSystem).
    implementation("org.apache.iceberg:iceberg-aws:1.4.0")
    // iceberg-aws's default AWS client factory references STS (assume-role support) at
    // class-load time; it is optional in iceberg-aws's POM so pull it explicitly.
    implementation("software.amazon.awssdk:sts:2.28.3")

    testImplementation(project(":testkit"))
    // DuckDB for performance comparison tests and optional execution engine
    compileOnly("org.duckdb:duckdb_jdbc:1.4.4.0")
    testImplementation("org.duckdb:duckdb_jdbc:1.4.4.0")
    // ClickHouse for optional execution engine
    compileOnly("com.clickhouse:clickhouse-jdbc:0.7.1:all")
    testImplementation("com.clickhouse:clickhouse-jdbc:0.7.1:all")
    // Trino JDBC driver for optional Trino execution engine
    compileOnly("io.trino:trino-jdbc:439")
    testImplementation("io.trino:trino-jdbc:439")
    // Spark SQL via Thrift Server (Hive JDBC driver - thin jar)
    // Uses the thin hive-jdbc jar with selective transitive deps to avoid the standalone
    // fat jar's SLF4J/Parquet conflicts. The driver is loaded dynamically via Class.forName().
    testRuntimeOnly("org.apache.hive:hive-jdbc:2.3.9") {
      exclude(group = "org.apache.logging.log4j")
      exclude(group = "org.slf4j")
      exclude(group = "org.apache.parquet")
      exclude(group = "org.apache.hadoop")
      exclude(group = "org.apache.hive", module = "hive-exec")
      exclude(group = "org.apache.hive", module = "hive-metastore")
      exclude(group = "org.apache.hive", module = "hive-shims")
      exclude(group = "com.fasterxml.jackson.core")
      exclude(group = "com.google.guava")
      exclude(group = "commons-codec")
      exclude(group = "commons-logging")
      exclude(group = "net.sf.opencsv")
    }
    // Test dependencies for mock-based tests
    testImplementation("org.mockito:mockito-core:5.11.0")
    testImplementation("org.apache.hadoop:hadoop-minicluster:3.3.6")
    // Testcontainers for live-service storage integration tests (S3/MinIO, FTP, SFTP, ClickHouse).
    // GenericContainer covers all images; these tests are @Tag("integration") and require a Docker daemon.
    testImplementation("org.testcontainers:testcontainers:1.20.4")
    testImplementation("org.testcontainers:junit-jupiter:1.20.4")
    // Testcontainers 1.20.4 ships docker-java zerodep 3.4.0, whose unix-socket transport
    // gets HTTP 400 from the Docker Desktop (WSL2) proxy on /info. The 3.5.x zerodep
    // transport fixes the request framing the Desktop proxy requires. Test-scoped override.
    testImplementation("com.github.docker-java:docker-java-transport-zerodep:3.5.3")

    annotationProcessor("org.immutables:value")
    compileOnly("org.immutables:value-annotations")
    compileOnly("com.google.code.findbugs:jsr305")
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.23.1")
    testRuntimeOnly("org.apache.logging.log4j:log4j-core:2.23.1")
}

fun JavaCompile.configureAnnotationSet(sourceSet: SourceSet) {
    source = sourceSet.java
    classpath = sourceSet.compileClasspath
    options.compilerArgs.add("-proc:only")
    org.gradle.api.plugins.internal.JvmPluginsHelper.configureAnnotationProcessorPath(sourceSet, sourceSet.java, options, project)
    destinationDirectory.set(temporaryDir)

    // only if we aren't running compileJava, since doing twice fails (in some places)
    onlyIf { !project.gradle.taskGraph.hasTask(sourceSet.getCompileTaskName("java")) }
}

val annotationProcessorMain by tasks.registering(JavaCompile::class) {
    configureAnnotationSet(sourceSets.main.get())
}

// The file module requires Java 11+ (uses List.of, Map.of, FileWriter(File,Charset), etc.).
// Skip compilation and tests entirely when running on JDK 8; only set Java 11 compatibility on JDK 11+.
if (JavaVersion.current() < JavaVersion.VERSION_11) {
    afterEvaluate {
        tasks.withType<JavaCompile>().configureEach { enabled = false }
        tasks.withType<Test>().configureEach { enabled = false }
    }
} else {
    plugins.withType<JavaPlugin> {
        configure<JavaPluginExtension> {
            sourceCompatibility = JavaVersion.VERSION_11
            targetCompatibility = JavaVersion.VERSION_11
        }
    }
}

// SIMDColumnBatch and AdaptiveColumnBatch use jdk.incubator.vector (JDK 16+).
// On older JDKs we exclude them from compilation; they are SIMD optimizations only.
// On JDK 16+, --add-modules=jdk.incubator.vector emits an "using incubating
// module" warning which -Werror (from the root build) would turn into an error.
// We remove -Werror from the file module's Java compile tasks on JDK 16+ to
// prevent that, and add --add-modules so the SIMD source compiles.
if (JavaVersion.current() >= JavaVersion.VERSION_16) {
    afterEvaluate {
        tasks.withType<JavaCompile>().configureEach {
            options.compilerArgs.add("--add-modules=jdk.incubator.vector")
            options.compilerArgs.removeIf { it == "-Werror" }
        }
    }
} else {
    sourceSets.main.get().java.exclude("**/execution/arrow/SIMDColumnBatch.java")
    sourceSets.main.get().java.exclude("**/execution/arrow/AdaptiveColumnBatch.java")
    sourceSets.test.get().java.exclude("**/execution/AdaptiveColumnBatchTest.java")
}

ide {
    fun generatedSource(compile: TaskProvider<JavaCompile>) {
        project.rootProject.configure<org.gradle.plugins.ide.idea.model.IdeaModel> {
            project {
                settings {
                    taskTriggers {
                        afterSync(compile.get())
                    }
                }
            }
        }
    }

    generatedSource(annotationProcessorMain)
}

tasks.test {
    // Set working directory to build directory for all tests
    workingDir = layout.buildDirectory.get().asFile

    testLogging {
        // Show standard out and standard error of the test JVM(s) on the console
        showStandardStreams = true

        // Show logging output from tests
        events("passed", "skipped", "failed", "standardOut", "standardError")

        // Show more detailed output
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }

    useJUnitPlatform {
        // Default: only unit tests for faster feedback.
        // -PincludeTags=<tags> runs EXACTLY those tags (replaces the default, enabling REQ-id isolation
        //   e.g. -PincludeTags=FILE-002). -PrunAllTests=true runs everything.
        if (project.hasProperty("includeTags")) {
            val tags = project.property("includeTags").toString().split(",")
            includeTags(*tags.toTypedArray())
        } else if (!project.hasProperty("runAllTests")) {
            includeTags("unit")
        }
        if (project.hasProperty("excludeTags")) {
            val tags = project.property("excludeTags").toString().split(",")
            excludeTags(*tags.toTypedArray())
        }
    }
    maxHeapSize = "2g"
    // Forward Testcontainers/Docker connection env to the forked test JVM. Gradle does not
    // propagate the launching environment to test workers by default, so Testcontainers
    // integration tests cannot otherwise see a custom DOCKER_HOST (needed on Docker Desktop
    // /WSL2 where the default unix socket is the CLI proxy). Only forwarded when set, so
    // normal unit-test runs are unaffected.
    listOf("DOCKER_HOST", "DOCKER_API_VERSION", "TESTCONTAINERS_RYUK_DISABLED",
        "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE").forEach { name ->
        System.getenv(name)?.let { environment(name, it) }
    }
    // Allow overriding the Docker host via -PdockerHost=tcp://... (more reliable than env,
    // which Gradle does not always forward to test workers). Used for Testcontainers on
    // Docker Desktop/WSL2 where the default unix socket is the CLI proxy, not the engine.
    if (project.hasProperty("dockerHost")) {
        val dh = project.property("dockerHost").toString()
        environment("DOCKER_HOST", dh)
        // Testcontainers' EnvironmentAndSystemPropertyClientProviderStrategy also reads
        // DOCKER_HOST as a JVM system property, which Gradle reliably forwards to the worker.
        systemProperty("DOCKER_HOST", dh)
        environment("TESTCONTAINERS_RYUK_DISABLED", "true")
        systemProperty("testcontainers.ryuk.disabled", "true")
        // tc-java 1.20.4's docker-java defaults to API 1.32, which Docker Engine 25+ rejects
        // (MinAPIVersion 1.40). Pin a supported version. docker-java reads the env var
        // DOCKER_API_VERSION and the system property api.version.
        val apiVer = if (project.hasProperty("dockerApiVersion"))
            project.property("dockerApiVersion").toString() else "1.43"
        environment("DOCKER_API_VERSION", apiVer)
        systemProperty("api.version", apiVer)
    }
    jvmArgs(
        "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED",
        "--add-opens=java.base/java.nio=org.apache.arrow.memory.netty,ALL-UNNAMED",
        "--add-modules=jdk.incubator.vector",
        // Mockito 5.x on Java 21 requires byte-buddy agent attachment
        "-XX:+EnableDynamicAgentLoading",
        "-Djdk.attach.allowAttachSelf=true",
        "-Djava.util.concurrent.ForkJoinPool.common.maximumSpares=256"
    )
    // Attach byte-buddy-agent as JVM agent to avoid self-attach failures under load
    doFirst {
        val byteBuddyAgent = configurations.testRuntimeClasspath.get().files
            .find { it.name.contains("byte-buddy-agent") }
        if (byteBuddyAgent != null) {
            jvmArgs("-javaagent:${byteBuddyAgent.absolutePath}")
        }
    }
}

// Task to print test classpath for performance test script
tasks.register("printTestClasspath") {
    doLast {
        println(sourceSets.test.get().runtimeClasspath.asPath)
    }
}

tasks.shadowJar {
    archiveBaseName.set("sih-aperio")
    archiveClassifier.set("")
    isZip64 = true
    mergeServiceFiles()
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}
