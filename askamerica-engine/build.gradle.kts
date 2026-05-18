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
}

description = "AskAmerica MCP server — query US government data from Claude Desktop"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":govdata"))
    implementation("com.formdev:flatlaf:3.3")
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

// ─── jpackage task ───────────────────────────────────────────────────────────
// Creates a native installer with a bundled JRE — no Java installation required.
// Run: ./gradlew :askamerica-engine:jpackage
//
// Requires JDK 17+ (jpackage ships with JDK 14+).
// On macOS produces a .dmg; on Linux a .deb; on Windows a .msi.

val jpackageDir = layout.buildDirectory.dir("jpackage")
val jpackageInputDir = layout.buildDirectory.dir("jpackage-input")

tasks.register<Copy>("prepareJpackageInput") {
    dependsOn(tasks.shadowJar)
    from(tasks.shadowJar.get().archiveFile)
    into(jpackageInputDir)
}

tasks.register<Exec>("jpackage") {
    group = "distribution"
    description = "Package the query engine with a bundled JRE for distribution"
    dependsOn("prepareJpackageInput")

    val jpackageTool = "${System.getProperty("java.home")}/bin/jpackage"
    val os = System.getProperty("os.name").lowercase()
    val packageType = when {
        os.contains("mac")  -> "dmg"
        os.contains("win")  -> "msi"
        else                -> "deb"
    }
    val version = project.version.toString().replace("-SNAPSHOT", "")
        .replace("[^0-9.]".toRegex(), "")
        .ifEmpty { "1.0.0" }

    commandLine(
        jpackageTool,
        "--type",              packageType,
        "--name",              "AskAmerica MCP",
        "--app-version",       version,
        "--vendor",            "AskAmerica",
        "--description",       "AskAmerica MCP — query US government data from Claude",
        "--input",             jpackageInputDir.get().asFile.absolutePath,
        "--main-jar",          tasks.shadowJar.get().archiveFileName.get(),
        "--main-class",        "org.apache.calcite.adapter.askamerica.McpServer",
        "--dest",              jpackageDir.get().asFile.absolutePath,
        "--java-options",      "-Xms256m -Xmx2g",
        "--java-options",      "-Dfile.encoding=UTF-8",
        // macOS-specific: sign if ASKAMERICA_SIGN_IDENTITY is set
        *(if (os.contains("mac") && System.getenv("ASKAMERICA_SIGN_IDENTITY") != null)
            arrayOf("--mac-sign", "--mac-signing-key-user-name", System.getenv("ASKAMERICA_SIGN_IDENTITY"))
          else emptyArray())
    )

    doFirst {
        jpackageDir.get().asFile.mkdirs()
    }
}
