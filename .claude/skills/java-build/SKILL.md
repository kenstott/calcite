---
description: Gradle Kotlin DSL conventions, dependency management, module structure, build profiles for this project
model: haiku
effort: low
---

# Java Build System Guide

Reference this guide when working on build-related tasks for $ARGUMENTS.

## Build System: Gradle Kotlin DSL

Root config: `/root/calcite/build.gradle.kts`
Settings: `/root/calcite/settings.gradle.kts`
Properties: `/root/calcite/gradle.properties`

## Module Structure (31 submodules)

```
calcite/
├── core/            # SQL engine, parser, planner, adapters framework
├── linq4j/          # LINQ implementation for Java
├── testkit/         # Shared test utilities and fixtures
├── server/          # JDBC server
├── bom/             # Bill of Materials for version alignment
├── file/            # File adapter (CSV, Parquet, JSON, DuckDB, Iceberg)
├── splunk/          # Splunk adapter
├── sharepoint-list/ # SharePoint adapter
├── govdata/         # Government data sources adapter
├── arrow/           # Apache Arrow integration
├── babel/           # Extended SQL parser
├── cassandra/       # Cassandra adapter
├── druid/           # Druid adapter
├── elasticsearch/   # Elasticsearch adapter
├── spark/           # Spark adapter
├── mongodb/         # MongoDB adapter
├── redis/           # Redis adapter
├── kafka/           # Kafka adapter
├── salesforce/      # Salesforce adapter
├── graphql/         # GraphQL adapter
└── ...              # innodb, geode, linkedin4j, openapi, etc.
```

## Dependency Management

```kotlin
// BOM pattern for version alignment
dependencies {
  api(platform(project(":bom")))  // All versions from BOM

  // Scope conventions
  api(...)              // Public API (transitive to consumers)
  implementation(...)   // Internal (not transitive)
  testImplementation(...)  // Test-only
  annotationProcessor(...)  // Compile-time code gen (Immutables)
}
```

Key versions (from `gradle.properties`):
- JUnit 5: 5.9.1
- Mockito: 3.12.4
- Hamcrest: 2.1
- SLF4J: 1.7.25
- Guava: 33.4.8-jre
- Jackson: 2.18.4.1
- Immutables: 2.8.8
- Checker Framework: 3.10.0

## Java Compilation

```kotlin
// MANDATORY: Java 8 source and target
sourceCompatibility = JavaVersion.VERSION_1_8
targetCompatibility = JavaVersion.VERSION_1_8
```

JVM args for Gradle daemon: `-XX:+UseG1GC -Xmx2g -XX:MaxMetaspaceSize=512m`

## Build Commands

```bash
# Standard build
./gradlew build

# Single module
./gradlew :file:build

# Tests (see java-testing skill for tag details)
./gradlew :module:test
./gradlew :module:test -PincludeTags=integration

# Code quality
./gradlew autostyleApply          # Fix formatting
./gradlew checkstyleAll           # Static analysis

# Optional quality gates
./gradlew test -PenableCheckerframework  # Null safety checking
./gradlew test -PenableErrorprone        # Bug detection
./gradlew test -PenableSpotbugs          # Bug patterns
./gradlew test -PenableJacoco            # Code coverage
```

## Auto-Applied Plugins (every Java module)

- `java-library` — API/implementation dependency separation
- `com.github.autostyle` — Code formatting + license headers
- `maven-publish` — Artifact publishing
- `de.thetaphi.forbiddenapis` — Forbidden API detection
- `com.github.vlsi.jandex` — Classpath annotation indexing (skip with `-PskipJandex=true`)

## Adding a New Module

1. Create directory with `build.gradle.kts`
2. Add to `settings.gradle.kts`: `include("new-module")`
3. Use BOM: `api(platform(project(":bom")))`
4. Set Java 8 source/target
5. Add test dependencies: JUnit 5 BOM, Hamcrest, testkit

## Shadow/Fat JAR (govdata)

The govdata module builds a shadow JAR for ETL workers:
```bash
./gradlew :govdata:shadowJar
# Output: govdata/build/libs/calcite-govdata-*-all.jar
```