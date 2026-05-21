/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
plugins {
    id("com.github.johnrengelman.shadow")
    `maven-publish`
}

description = "AskAmerica MCP server — query US government data from Claude Desktop"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":driver-base"))
    implementation(project(":govdata"))
    implementation("com.formdev:flatlaf:3.3")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.2")
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
//     <groupId>io.askamerica</groupId>
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
            groupId    = "ai.askamerica"
            artifactId = "askamerica-engine"
            version    = publishVersion

            artifact(tasks.shadowJar) {
                classifier = ""
            }

            pom {
                name.set("AskAmerica Engine")
                description.set("JDBC driver for querying 15 US government datasets via Apache Calcite")
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
            url  = uri("https://maven.pkg.github.com/kenstott/calcite")
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
// On macOS produces a .dmg; on Linux a .deb; on Windows a .msi.

val jpackageDir = layout.buildDirectory.dir("jpackage")
val jpackageInputDir = layout.buildDirectory.dir("jpackage-input")

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
    into(jpackageInputDir)
}

tasks.register<Exec>("jpackage") {
    group = "distribution"
    description = "Package the query engine with a bundled JRE for distribution"
    dependsOn("prepareJpackageInput")

    val jpackageTool = "${System.getProperty("java.home")}/bin/jpackage"
    val os = System.getProperty("os.name").lowercase()
    val isMac = os.contains("mac")
    val packageType = when {
        isMac            -> "app-image"
        os.contains("win") -> "msi"
        else             -> "deb"
    }
    val version = project.version.toString().replace("-SNAPSHOT", "")
        .replace("[^0-9.]".toRegex(), "")
        .ifEmpty { "1.0.0" }

    val macResourceDir = project.file("src/packaging/mac").absolutePath

    commandLine(
        jpackageTool,
        "--type",              packageType,
        "--name",              "AskAmerica MCP",
        "--app-version",       version,
        "--vendor",            "AskAmerica",
        "--description",       "AskAmerica MCP — query US government data from Claude",
        "--input",             jpackageInputDir.get().asFile.absolutePath,
        "--main-jar",          launcherJar.get().archiveFileName.get(),
        "--main-class",        "org.apache.calcite.adapter.askamerica.McpServerLauncher",
        "--dest",              jpackageDir.get().asFile.absolutePath,
        "--java-options",      "-Xms256m -Xmx2g",
        "--java-options",      "-Dfile.encoding=UTF-8",
        // macOS-specific options
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
        jpackageDir.get().asFile.mkdirs()
    }
}
