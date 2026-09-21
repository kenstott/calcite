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
import com.github.spotbugs.SpotBugsTask
import com.github.vlsi.gradle.crlf.CrLfSpec
import com.github.vlsi.gradle.crlf.LineEndings
import com.github.vlsi.gradle.dsl.configureEach
import com.github.vlsi.gradle.git.FindGitAttributes
import com.github.vlsi.gradle.git.dsl.gitignore
import com.github.vlsi.gradle.properties.dsl.lastEditYear
import com.github.vlsi.gradle.properties.dsl.props
import com.github.vlsi.gradle.release.RepositoryType
import de.thetaphi.forbiddenapis.gradle.CheckForbiddenApis
import de.thetaphi.forbiddenapis.gradle.CheckForbiddenApisExtension
import net.ltgt.gradle.errorprone.errorprone
import org.apache.calcite.buildtools.buildext.dsl.ParenthesisBalancer
import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    base
    // java-base is needed for platform(...) resolution,
    // see https://github.com/gradle/gradle/issues/14822
    `java-base`
    publishing
    // Verification
    checkstyle
    calcite.buildext
    jacoco
    id("jacoco-report-aggregation")
    id("org.checkerframework") apply false
    id("com.github.autostyle")
    id("org.nosphere.apache.rat")
    id("com.github.spotbugs")
    id("de.thetaphi.forbiddenapis") apply false
    id("net.ltgt.errorprone") apply false
    id("com.github.vlsi.jandex") apply false
    id("org.owasp.dependencycheck")
    id("com.github.johnrengelman.shadow") apply false
    id("org.sonarqube")
    // IDE configuration
    id("org.jetbrains.gradle.plugin.idea-ext")
    id("com.github.vlsi.ide")
    // Release
    id("com.github.vlsi.crlf")
    id("com.github.vlsi.gradle-extensions")
    id("com.github.vlsi.license-gather") apply false
    id("com.github.vlsi.stage-vote-release")
    id("com.autonomousapps.dependency-analysis") apply false
}

repositories {
    // At least for RAT
    mavenCentral()
    // GeoTools repository for TIGER shapefile parsing
    maven {
        url = uri("https://repo.osgeo.org/repository/release/")
        name = "OSGeo Release Repository"
    }
}

tasks.wrapper {
    distributionType = Wrapper.DistributionType.BIN
}

fun reportsForHumans() = !(System.getenv()["CI"]?.toBoolean() ?: false)

val lastEditYear by extra(lastEditYear())

// Do not enable spotbugs by default. Execute it only when -Pspotbugs is present
val enableSpotBugs = props.bool("spotbugs")
val enableCheckerframework by props()
val enableErrorprone by props()
val enableDependencyAnalysis by props()
val enableJacoco by props()
val skipJandex by props()
val skipCheckstyle by props()
val skipAutostyle by props()
val skipJavadoc by props()
val enableMavenLocal by props()
val enableGradleMetadata by props()
val werror by props(true) // treat javac warnings as errors

// Modules distributed under the Business Source License rather than Apache-2.0.
// They carry their own license headers and do not follow Apache Calcite's Java
// conventions, so the autostyle license/style rules must not rewrite them.
// .ratignore is the single source of truth: every `<module>/**` entry there that
// names a Gradle subproject is treated as BSL.
val bslModules: Set<String> =
    rootDir.resolve(".ratignore").readLines()
        .map { it.trim() }
        .filter { it.endsWith("/**") && it.count { c -> c == '/' } == 1 }
        .map { it.removeSuffix("/**") }
        .toSet()
// Inherited from stage-vote-release-plugin: skipSign, useGpgCmd
// Inherited from gradle-extensions-plugin: slowSuiteLogThreshold=0L, slowTestLogThreshold=2000L

// Java versions prior to 1.8.0u202 have known issues that cause invalid bytecode in certain patterns
// of annotation usage.
// So we require at least 1.8.0u202
System.getProperty("java.version").let { version ->
    version.takeIf { it.startsWith("1.8.0_") }
        ?.removePrefix("1.8.0_")
        ?.toIntOrNull()
        ?.let {
            require(it >= 202) {
                "Apache Calcite requires Java 1.8.0u202 or later. The current Java version is $version"
            }
        }
}

ide {
    copyrightToAsf()
    ideaInstructionsUri =
        uri("https://calcite.apache.org/docs/howto.html#setting-up-intellij-idea")
    doNotDetectFrameworks("android", "jruby")
}

// This task scans the project for gitignore / gitattributes, and that is reused for building
// source/binary artifacts with the appropriate eol/executable file flags
// It enables to automatically exclude patterns from .gitignore
val gitProps by tasks.registering(FindGitAttributes::class) {
    // Scanning for .gitignore and .gitattributes files in a task avoids doing that
    // when distribution build is not required (e.g. code is just compiled)
    root.set(rootDir)
}

val rat by tasks.getting(org.nosphere.apache.rat.RatTask::class) {
    gitignore(gitProps)
    verbose.set(true)
    // Note: patterns are in non-standard syntax for RAT, so we use exclude(..) instead of excludeFile
    exclude(rootDir.resolve(".ratignore").readLines())
}

tasks.validateBeforeBuildingReleaseArtifacts {
    dependsOn(rat)
}

val String.v: String get() = rootProject.extra["$this.version"] as String

val buildVersion = "calcite".v + releaseParams.snapshotSuffix

println("Building Apache Calcite $buildVersion")

releaseArtifacts {
    fromProject(":release")
}

// Configures URLs to SVN and Nexus
releaseParams {
    tlp.set("Calcite")
    componentName.set("Apache Calcite")
    releaseTag.set("calcite-$buildVersion")
    rcTag.set(rc.map { "calcite-$buildVersion-rc$it" })
    sitePreviewEnabled.set(false)
    nexus {
        // https://github.com/marcphilipp/nexus-publish-plugin/issues/35
        packageGroup.set("org.apache.calcite")
        if (repositoryType.get() == RepositoryType.PROD) {
            // org.apache.calcite at repository.apache.org
            stagingProfileId.set("778fd0d4358bb")
        }
    }
    svnDist {
        staleRemovalFilters {
            includes.add(Regex(".*apache-calcite-\\d.*"))
            validates.empty()
            validates.add(provider {
                Regex("release/calcite/apache-calcite-${version.toString().removeSuffix("-SNAPSHOT")}")
            })
        }
    }
}

reporting {
    reports {
        if (enableJacoco) {
            val jacocoAggregateTestReport by creating(JacocoCoverageReport::class) {
                testType.set(TestSuiteType.UNIT_TEST)
            }
        }
    }
}

val javadocAggregate by tasks.registering(Javadoc::class) {
    group = JavaBasePlugin.DOCUMENTATION_GROUP
    description = "Generates aggregate javadoc for all the artifacts"

    val sourceSets = subprojects
        .mapNotNull { it.extensions.findByType<SourceSetContainer>() }
        .filter { it.names.contains("main") }
        .map { it.named("main") }

    classpath = files(sourceSets.map { set -> set.map { it.output + it.compileClasspath } })
    setSource(sourceSets.map { set -> set.map { it.allJava } })
    setDestinationDir(file(layout.buildDirectory.get().file("docs/javadocAggregate")))
}

/** Similar to {@link #javadocAggregate} but includes tests.
 * CI uses this target to validate javadoc (e.g. checking for broken links). */
val javadocAggregateIncludingTests by tasks.registering(Javadoc::class) {
    description = "Generates aggregate javadoc for all the artifacts"

    val sourceSets = subprojects
        .filter { it.name != "bom" }
        .mapNotNull { it.extensions.findByType<SourceSetContainer>() }
        .flatMap { listOf(it.named("main"), it.named("test")) }

    classpath = files(sourceSets.map { set -> set.map { it.output + it.compileClasspath } })
    setSource(sourceSets.map { set -> set.map { it.allJava } })
    setDestinationDir(file(layout.buildDirectory.get().file("docs/javadocAggregateIncludingTests")))
}

val adaptersForSqlline = listOf(
    ":arrow", ":babel", ":cassandra", ":druid", ":elasticsearch",
    ":file", ":geode", ":govdata", ":graphql", ":innodb", ":kafka", ":mongodb",
    ":openapi", ":salesforce", // Added custom adapters
    ":plus", ":redis", ":server", ":spark", ":splunk")

val dataSetsForSqlline = listOf(
    "net.hydromatic:foodmart-data-hsqldb",
    "net.hydromatic:scott-data-hsqldb",
    "net.hydromatic:steelwheels-data-hsqldb",
    "net.hydromatic:chinook-data-hsqldb"
)

val sqllineClasspath by configurations.creating {
    isCanBeConsumed = false
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.CLASSES_AND_RESOURCES))
        attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, JavaVersion.current().majorVersion.toInt())
        attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.EXTERNAL))
    }
    // Exclude conflicting SLF4J bindings - only use log4j-slf4j-impl
    exclude(group = "org.slf4j", module = "slf4j-reload4j")
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
}

@CacheableRule
abstract class AddDependenciesRule @Inject constructor(val dependencies: List<String>) : ComponentMetadataRule {
    override fun execute(context: ComponentMetadataContext) {
        listOf("compile", "runtime").forEach { base ->
            context.details.withVariant(base) {
                withDependencies {
                    dependencies.forEach {
                        add(it)
                    }
                }
            }
        }
    }
}

dependencies {
    sqllineClasspath(platform(project(":bom")))
    sqllineClasspath(project(":testkit"))
    sqllineClasspath("sqlline:sqlline")
    sqllineClasspath("org.apache.logging.log4j:log4j-slf4j2-impl:2.23.1")
    sqllineClasspath("org.duckdb:duckdb_jdbc:1.4.4.0") // Required for DuckDB execution engine
    for (p in adaptersForSqlline) {
        sqllineClasspath(project(p))
    }

    components {
        for (m in dataSetsForSqlline) {
            withModule<AddDependenciesRule>(m)
        }
    }

    for (m in dataSetsForSqlline) {
        sqllineClasspath(m)
    }
    if (enableJacoco) {
        for (p in subprojects) {
            if (p.name != "bom") {
                jacocoAggregation(p)
            }
        }
    }
}

val buildSqllineClasspath by tasks.registering(Jar::class) {
    description = "Creates classpath-only jar for running SqlLine"
    // One can debug classpath with ./gradlew dependencies --configuration sqllineClasspath
    inputs.files(sqllineClasspath).withNormalizer(ClasspathNormalizer::class.java)
    archiveFileName.set("sqllineClasspath.jar")
    manifest {
        attributes(
            "Main-Class" to "sqlline.SqlLine",
            "Class-Path" to provider {
                // Class-Path is a list of URLs
                sqllineClasspath.joinToString(" ") {
                    it.toURI().toURL().toString()
                }
            }
        )
    }
}

if (enableDependencyAnalysis) {
    apply(plugin = "com.autonomousapps.dependency-analysis")
    configure<com.autonomousapps.DependencyAnalysisExtension> {
        // See https://github.com/autonomousapps/dependency-analysis-android-gradle-plugin
        // Most of the time the recommendations are good, however, there are cases the suggestsions
        // are off, so we don't include the dependency analysis to CI workflow yet
        // ./gradlew -PenableDependencyAnalysis buildHealth --no-parallel --no-daemon
        issues {
            all { // all projects
                onAny {
                    severity("fail")
                }
                onRedundantPlugins {
                    severity("ignore")
                }
            }
        }
    }
}

val javaccGeneratedPatterns = arrayOf(
    "org/apache/calcite/jdbc/CalciteDriverVersion.java",
    "**/parser/**/*ParserImpl.*",
    "**/parser/**/*ParserImplConstants.*",
    "**/parser/**/*ParserImplTokenManager.*",
//    "**/parser/**/PigletParser.*",
//    "**/parser/**/PigletParserConstants.*",
    "**/parser/**/ParseException.*",
    "**/parser/**/SimpleCharStream.*",
    "**/parser/**/Token.*",
    "**/parser/**/TokenMgrError.*",
    "**/org/apache/calcite/runtime/Resources.java",
    "**/parser/**/*ParserTokenManager.*"
)

fun PatternFilterable.excludeJavaCcGenerated() {
    exclude(*javaccGeneratedPatterns)
}

fun com.github.autostyle.gradle.BaseFormatExtension.license() {
    licenseHeader(rootProject.ide.licenseHeader) {
        copyrightStyle("bat", com.github.autostyle.generic.DefaultCopyrightStyle.PAAMAYIM_NEKUDOTAYIM)
        copyrightStyle("cmd", com.github.autostyle.generic.DefaultCopyrightStyle.PAAMAYIM_NEKUDOTAYIM)
    }
    trimTrailingWhitespace()
    endWithNewline()
}

sonarqube {
    properties {
        property("sonar.test.inclusions", "**/*Test*/**")
        property("sonar.coverage.jacoco.xmlReportPaths", layout.buildDirectory.get().file("reports/jacoco/jacocoAggregateTestReport/jacocoAggregateTestReport.xml"))
    }
}

// Numeric dot-version comparator for the CVE-override resolutionStrategy below (REQ-956): an
// exact-version allow-list (`requested.version == "x.y.z"`) kept missing real findings whenever
// a DIFFERENT module resolved a slightly different patch version of the same vulnerable package
// (verified live, 2026-09-21: nimbus-jose-jwt landed on 9.37.3 in cloud-ops vs. 9.30.2/9.37.2
// elsewhere; reactor-netty-http landed on 1.0.45 vs. 1.0.38/1.0.39) -- comparing "is this version
// still below the fixed one" instead of listing every observed vulnerable version fixes the
// whole class of miss, not just the one instance found. Scoped to plain numeric dot-versions
// (no ".Final"/".v20250814"-style suffixes some artifacts here use, e.g. Netty/Jetty) --
// non-numeric trailing segments compare as 0, which is safe as a floor but not a general
// version-string parser.
fun versionOlderThan(version: String, fixed: String): Boolean {
    fun parts(v: String) = v.split(".").map { it.takeWhile(Char::isDigit).toIntOrNull() ?: 0 }
    val a = parts(version)
    val b = parts(fixed)
    for (i in 0 until maxOf(a.size, b.size)) {
        val x = a.getOrElse(i) { 0 }
        val y = b.getOrElse(i) { 0 }
        if (x != y) return x < y
    }
    return false
}

allprojects {
    group = "org.apache.calcite"
    version = buildVersion

    apply(plugin = "com.github.vlsi.gradle-extensions")

    repositories {
        // RAT and Autostyle dependencies
        mavenCentral()
        // GeoTools repository for TIGER shapefile parsing
        maven {
            url = uri("https://repo.osgeo.org/repository/release/")
            name = "OSGeo Release Repository"
        }
    }

    val javaUsed = file("src/main/java").isDirectory
    if (javaUsed) {
        apply(plugin = "java-library")
        configurations {
            "implementation" {
                exclude(group = "org.jetbrains", module = "annotations")
                exclude(group = "org.bouncycastle", module = "bcprov-jdk15on")
            }
        }
    }

    // CVE overrides, applied to every configuration on every module (REQ-956): a transitive
    // dependency's OWN published module metadata `{strictly}`-locks netty-handler/bcprov-jdk18on
    // to a vulnerable version, which a plain version bump on the direct dependency that pulls
    // them in (org.redisson:redisson) does not override -- verified live via grype against the
    // pgwire-file bundle's real runtime classpath, 2026-09-21: netty-handler 4.1.127.Final
    // (GHSA-c4c3-7fpv-j4q5, fixed 4.1.137.Final) and bcprov-jdk18on 1.82 (GHSA-9pwp-9qqc-pr26,
    // fixed 1.85), both Critical.
    configurations.all {
        resolutionStrategy.eachDependency {
            // The whole io.netty family, forced as one group: netty components must stay in
            // lockstep (mixing release trains risks ABI mismatches, per dedupe_jars.py's own
            // comment on this exact class of bug) and grype found the WHOLE family behind, not
            // just netty-handler -- netty-codec-dns/haproxy/http/http2/redis/smtp/stomp/xml,
            // netty-handler-ssl-ocsp, netty-resolver-dns, netty-transport-sctp, at 4.1.100/
            // 4.1.112/4.1.127.Final, all High, verified live 2026-09-21.
            // netty-tcnative-* is a native OpenSSL binding with its OWN independent version
            // scheme (2.0.x), not the 4.1.x core/codec/handler/resolver/transport train --
            // forcing it to 4.1.138.Final (verified live) resolves to a nonexistent artifact.
            if (requested.group == "io.netty" && !requested.name.startsWith("netty-tcnative")) {
                useVersion("4.1.138.Final")
                because("whole netty family kept on one patched release train")
            }
            if (requested.group == "org.bouncycastle" && requested.name == "bcprov-jdk18on") {
                useVersion("1.85")
                because("GHSA-9pwp-9qqc-pr26 (Critical) -- fixed in 1.85")
            }
            // Transitive from com.azure.resourcemanager:azure-resourcemanager-keyvault:2.34.0
            // (cloud-ops) -- verified live via grype against the pgwire-cloudops bundle,
            // 2026-09-21.
            if (requested.group == "com.azure" && requested.name == "azure-security-keyvault-keys") {
                useVersion("4.10.6")
                because("GHSA-97jf-46m3-8953 (Critical) -- fixed in 4.10.6")
            }
            // jackson-core/-databind: two branches in play (2.18.x via core's redisson chain,
            // 2.21.x via the trino-* modules) -- bump each within its own branch rather than a
            // single useVersion() that would downgrade whichever branch is already ahead.
            if (requested.group == "com.fasterxml.jackson.core" &&
                (requested.name == "jackson-core" || requested.name == "jackson-databind")) {
                val fixed = if (requested.version?.startsWith("2.21") == true) "2.21.5" else "2.18.9"
                if (versionOlderThan(requested.version ?: "0", fixed)) {
                    useVersion(fixed)
                    because(
                        "GHSA-r7wm-3cxj-wff9 / GHSA-j3rv-43j4-c7qm / GHSA-rmj7-2vxq-3g9f / " +
                        "GHSA-5gvw-p9qm-jgwh / GHSA-5jmj-h7xm-6q6v / GHSA-mhm7-754m-9p8w"
                    )
                }
            }
            if (requested.group == "org.apache.httpcomponents.core5" &&
                (requested.name == "httpcore5" || requested.name == "httpcore5-h2")) {
                useVersion("5.4.3")
                because("GHSA-hf6x-8p5f-cgmf / GHSA-v3jc-474w-2wm6")
            }
            // The remainder come from org.apache.hadoop:hadoop-auth's own transitive footprint
            // (file/govdata's Parquet/Iceberg/S3 support), not from core or any Calcite-proper
            // dependency -- forced by EXACT vulnerable version, not by group/name alone, since
            // hadoop-auth already requests a newer, already-patched nimbus-jose-jwt (10.4) on
            // some paths; a blanket useVersion() here would downgrade that one back down.
            // GHSA-355h-qmc2-wpwf/GHSA-2fvj-hgj9-j2gr claim fixes at 9.4.60/9.4.63 for the 9.4.x
            // branch, but 9.4.58.v20250814 is the last 9.4.x version Eclipse Jetty ever
            // published (verified against Maven Central metadata, 2026-09-21) -- the 9.4 line is
            // EOL and no patched build exists there. Bumped to the latest available 9.4.x as a
            // partial mitigation; a full fix needs a major-version jump (10.x/11.x/12.x) into
            // hadoop-common's own jetty-server/jetty-servlet chain, a materially riskier change
            // out of scope for this pass.
            if (requested.group == "org.eclipse.jetty" &&
                (requested.name == "jetty-http" || requested.name == "jetty-security") &&
                requested.version == "9.4.57.v20241219") {
                useVersion("9.4.58.v20250814")
                because("latest available 9.4.x -- no true fix published on this EOL branch")
            }
            if (requested.group == "org.apache.zookeeper" && requested.name == "zookeeper" &&
                versionOlderThan(requested.version ?: "0", "3.8.6")) {
                useVersion("3.8.6")
                because("GHSA-7xrh-hqfc-g7qr / GHSA-crhr-qqj8-rpxc")
            }
            if (requested.group == "com.nimbusds" && requested.name == "nimbus-jose-jwt" &&
                versionOlderThan(requested.version ?: "0", "9.37.4")) {
                useVersion("9.37.4")
                because("GHSA-gvpg-vgmx-xg6w / GHSA-xwmg-2g98-w7v9")
            }
            if (requested.group == "commons-beanutils" && requested.name == "commons-beanutils" &&
                versionOlderThan(requested.version ?: "0", "1.11.0")) {
                useVersion("1.11.0")
                because("GHSA-wxr5-93ph-8wr9")
            }
            // The actual request is the old monolithic "jline" umbrella artifact (pre-split,
            // bundles what's now the separate jline-remote-telnet module's classes internally)
            // -- grype's package detection reports the split module name from the embedded
            // class evidence, not the umbrella artifact grype actually found it in. Verified
            // live via dependency:tree, 2026-09-21: org.jline:jline:3.9.0, not jline-remote-telnet.
            if (requested.group == "org.jline" && requested.name == "jline" &&
                requested.version == "3.9.0") {
                useVersion("3.30.17")
                because("GHSA-2r2c-cx56-8933 / GHSA-47qp-hqvx-6r3f")
            }
            if (requested.group == "org.jsoup" && requested.name == "jsoup" &&
                versionOlderThan(requested.version ?: "0", "1.23.2")) {
                useVersion("1.23.2")
                because("GHSA-m72m-mhq2-9p6c / GHSA-gp7f-rwcx-9369 / GHSA-pmhh-3w7g-xqp8")
            }
            if (requested.group == "io.airlift" && requested.name == "aircompressor" &&
                versionOlderThan(requested.version ?: "0", "2.0.3")) {
                useVersion("2.0.3")
                because("GHSA-vx9q-rhv9-3jvg")
            }
            if (requested.group == "org.postgresql" && requested.name == "postgresql" &&
                versionOlderThan(requested.version ?: "0", "42.7.12")) {
                useVersion("42.7.12")
                because("GHSA-98qh-xjc8-98pq / GHSA-hq9p-pm7w-8p54 / GHSA-j92g-9f8w-j867")
            }
            if (requested.group == "io.projectreactor.netty" &&
                (requested.name == "reactor-netty-core" || requested.name == "reactor-netty-http") &&
                versionOlderThan(requested.version ?: "0", "1.2.18")) {
                useVersion("1.2.18")
                because("GHSA-q24v-hpg3-v3jp / GHSA-xjhv-p3fv-x24r / GHSA-4q2v-9p7v-3v22")
            }
            if (requested.group == "com.azure" && requested.name == "azure-identity" &&
                versionOlderThan(requested.version ?: "0", "1.12.2")) {
                useVersion("1.12.2")
                because("GHSA-m5vv-6r4h-3vj9")
            }
            if (requested.group == "org.apache.commons" && requested.name == "commons-configuration2" &&
                versionOlderThan(requested.version ?: "0", "2.15.0")) {
                useVersion("2.15.0")
                because("GHSA-337m-mw94-2v6g")
            }
            if (requested.group == "org.apache.httpcomponents.client5" && requested.name == "httpclient5" &&
                versionOlderThan(requested.version ?: "0", "5.6.3")) {
                useVersion("5.6.3")
                because("GHSA-hjcp-jmpx-g3qm")
            }
            if (requested.group == "org.apache.logging.log4j" &&
                (requested.name == "log4j-api" || requested.name == "log4j-core") &&
                versionOlderThan(requested.version ?: "0", if (requested.name == "log4j-api") "2.25.5" else "2.25.4")) {
                useVersion(if (requested.name == "log4j-api") "2.25.5" else "2.25.4")
                because(
                    "GHSA-qv9r-c865-cp47 / GHSA-3pxv-7cmr-fjr4 / GHSA-445c-vh5m-36rj / " +
                    "GHSA-6hg6-v5c8-fphq / GHSA-vc5p-v9hr-52mj"
                )
            }
            if (requested.group == "org.apache.poi" && requested.name == "poi-ooxml" &&
                versionOlderThan(requested.version ?: "0", "5.4.0")) {
                useVersion("5.4.0")
                because("GHSA-gmg8-593g-7mv3")
            }
            // commons-lang:commons-lang:2.6 (the legacy pre-commons-lang3 artifact, no patched
            // 2.x release ever published -- GHSA-j288-q9x7-2f5v's own data says "-> None" for
            // it) arrives via com.joestelmach:natty:0.13 (file's natural-language date parser,
            // FileRowConverter.java) -> org.mnode.ical4j:ical4j:1.0.2. natty itself is
            // abandoned upstream (0.13, 2017, is the last release ever published -- cannot be
            // bumped), but ical4j is an independent transitive dep with real newer releases;
            // forcing IT drops commons-lang entirely (4.3.0 has no commons-lang dependency at
            // all). Verified live, 2026-09-21: file:compileJava succeeds unchanged, and a
            // runtime smoke test (new Parser().parse("next friday at 3pm")) against the forced
            // ical4j returns a correct parsed date -- natty's compiled bytecode does not break
            // against ical4j's newer API for the code path this adapter actually exercises.
            if (requested.group == "org.mnode.ical4j" && requested.name == "ical4j" &&
                versionOlderThan(requested.version ?: "0", "4.3.0")) {
                useVersion("4.3.0")
                because("GHSA-j288-q9x7-2f5v (transitively, by removing commons-lang entirely)")
            }
            // Transitive-only (no direct declaration anywhere in this repo) in every trino-*
            // module's shaded jar. Same-minor-line patch bump, verified live, 2026-09-21.
            if (requested.group == "ch.qos.logback" && requested.name == "logback-core" &&
                versionOlderThan(requested.version ?: "0", "1.5.34")) {
                useVersion("1.5.34")
                because("GHSA-jhq6-gfmj-v8fx / GHSA-p47f-322f-whfh")
            }
        }
    }

    plugins.withId("java-library") {
        dependencies {
            "annotationProcessor"(platform(project(":bom")))
            "implementation"(platform(project(":bom")))
            "testAnnotationProcessor"(platform(project(":bom")))
        }
    }

    val hasTests = file("src/test/java").isDirectory || file("src/test/kotlin").isDirectory
    if (hasTests) {
        plugins.withType<JavaPlugin> {
            // Add default tests dependencies
            dependencies {
                val testImplementation by configurations
                val testRuntimeOnly by configurations
                testImplementation(platform("org.junit:junit-bom"))
                testImplementation("org.junit.jupiter:junit-jupiter")
                testImplementation("org.hamcrest:hamcrest")
                if (project.props.bool("junit4", default = false)) {
                    // Allow projects to opt-out of junit dependency, so they can be JUnit5-only
                    testImplementation("junit:junit")
                    testRuntimeOnly("org.junit.vintage:junit-vintage-engine")
                }
            }
            if (enableJacoco) {
                apply(plugin = "jacoco")
            }
        }
    }

    if (!skipAutostyle && project.name !in bslModules) {
        apply(plugin = "com.github.autostyle")
        autostyle {
            kotlinGradle {
                filter {
                    // Autostyle does not support gitignore yet https://github.com/autostyle/autostyle/issues/13
                    exclude(".claude/**")
                }
                license()
                ktlint()
            }
            format("configs") {
                filter {
                    include("**/*.sh", "**/*.bsh", "**/*.cmd", "**/*.bat")
                    include("**/*.properties", "**/*.yml")
                    include("**/*.xsd", "**/*.xsl", "**/*.xml")
                    include("**/*.fmpp", "**/*.ftl", "**/*.jj")
                    // Autostyle does not support gitignore yet https://github.com/autostyle/autostyle/issues/13
                    exclude("bin/**", "out/**", "target/**", "gradlew*")
                    exclude(rootDir.resolve(".ratignore").readLines())
                }
                license()
                endWithNewline()
            }
            format("web") {
                filter {
                    include("**/*.md", "**/*.html")
                    exclude("**/test/**/*.html")
                    // Autostyle does not support gitignore yet https://github.com/autostyle/autostyle/issues/13
                    exclude(rootDir.resolve(".ratignore").readLines())
                }
                trimTrailingWhitespace()
                endWithNewline()
            }
            if (project == rootProject) {
                // Spotless does not exclude subprojects when using target(...)
                // So **/*.md is enough to scan all the md files in the codebase
                // See https://github.com/diffplug/spotless/issues/468
                format("markdown") {
                    filter.include("**/*.md")
                    // Flot is known to have trailing whitespace, so the files
                    // are kept in their original format (e.g. to simplify diff on library upgrade)
                    endWithNewline()
                }
            }
        }
        plugins.withId("org.jetbrains.kotlin.jvm") {
            autostyle {
                kotlin {
                    licenseHeader(rootProject.ide.licenseHeader)
                    ktlint {
                        userData(mapOf("disabled_rules" to "import-ordering"))
                    }
                    trimTrailingWhitespace()
                    endWithNewline()
                }
            }
        }
    }
    // BSL modules follow Google Java Format (4-space), not Apache Calcite's style,
    // so the Calcite checkstyle rules do not apply — the same exemption
    // askamerica-engine already declares for itself.
    if (!skipCheckstyle && project.name !in bslModules) {
        apply<CheckstylePlugin>()
        // This will be config_loc in Checkstyle (checker.xml)
        val configLoc = File(rootDir, "src/main/config/checkstyle")
        checkstyle {
            toolVersion = "checkstyle".v
            isShowViolations = true
            configDirectory.set(configLoc)
            configFile = configDirectory.get().file("checker.xml").asFile
        }
        tasks.register("checkstyleAll") {
            dependsOn(tasks.withType<Checkstyle>())
        }
        tasks.configureEach<Checkstyle> {
            // Excludes here are faster than in suppressions.xml
            // Since here we can completely remove file from the analysis.
            // On the other hand, supporessions.xml still analyzes the file, and
            // then it recognizes it should suppress all the output.
            excludeJavaCcGenerated()
            // Workaround for https://github.com/gradle/gradle/issues/13927
            // Absolute paths must not be used as they defeat Gradle build cache
            // Unfortunately, Gradle passes only config_loc variable by default, so we make
            // all the paths relative to config_loc
            configProperties!!["cache_file"] =
                layout.buildDirectory.asFile.get().resolve("checkstyle/cacheFile").relativeTo(configLoc)
        }
        // afterEvaluate is to support late sourceSet addition (e.g. jmh sourceset)
        afterEvaluate {
            tasks.configureEach<Checkstyle> {
                // Checkstyle 8.26 does not need classpath, see https://github.com/gradle/gradle/issues/14227
                classpath = files()
            }
        }
    }
    if (!skipAutostyle || !skipCheckstyle) {
        tasks.register("style") {
            group = LifecycleBasePlugin.VERIFICATION_GROUP
            description = "Formats code (license header, import order, whitespace at end of line, ...) and executes Checkstyle verifications"
            if (!skipAutostyle && project.name !in bslModules) {
                dependsOn("autostyleApply")
            }
            if (!skipCheckstyle) {
                dependsOn("checkstyleAll")
            }
        }
    }

    tasks.configureEach<AbstractArchiveTask> {
        // Ensure builds are reproducible
        isPreserveFileTimestamps = false
        isReproducibleFileOrder = true
        dirMode = "775".toInt(8)
        fileMode = "664".toInt(8)
    }

    tasks {
        configureEach<Javadoc> {
            excludeJavaCcGenerated()
            (options as StandardJavadocDocletOptions).apply {
                // Please refrain from using non-ASCII chars below since the options are passed as
                // javadoc.options file which is parsed with "default encoding"
                // locale should be placed at the head of any options: https://docs.gradle.org/current/javadoc/org/gradle/external/javadoc/CoreJavadocOptions.html#getLocale
                locale = "en_US"
                noTimestamp.value = true
                showFromProtected()
                // javadoc: error - The code being documented uses modules but the packages
                // defined in https://docs.oracle.com/en/java/javase/17/docs/api/ are in the unnamed module
                source = "1.8"
                docEncoding = "UTF-8"
                charSet = "UTF-8"
                encoding = "UTF-8"
                docTitle = "Apache Calcite API"
                windowTitle = "Apache Calcite API"
                header = "<b>Apache Calcite</b>"
                bottom =
                    "Copyright &copy; 2012-$lastEditYear Apache Software Foundation. All Rights Reserved."
                if (JavaVersion.current() >= JavaVersion.VERSION_1_9) {
                    addBooleanOption("html5", true)
                    links("https://docs.oracle.com/en/java/javase/17/docs/api/")
                } else {
                    links("https://docs.oracle.com/javase/8/docs/api/")
                }
            }
        }
    }

    plugins.withType<JavaPlugin> {
        configure<JavaPluginExtension> {
            sourceCompatibility = JavaVersion.VERSION_1_8
            targetCompatibility = JavaVersion.VERSION_1_8
        }
        configure<JavaPluginExtension> {
            consistentResolution {
                useCompileClasspathVersions()
            }
        }

        repositories {
            if (enableMavenLocal) {
                mavenLocal()
            }
            mavenCentral()
        }
        val sourceSets: SourceSetContainer by project

        apply(plugin = "de.thetaphi.forbiddenapis")
        // BSL modules are exempt from the Apache Calcite style checks, forbiddenApis
        // included — the same exemption askamerica-engine already declares for itself.
        // The plugin still has to be applied; the rest of the build configures its
        // extension unconditionally.
        if (project.name in bslModules) {
            tasks.withType<CheckForbiddenApis>().configureEach { enabled = false }
        }
        apply(plugin = "maven-publish")

        if (!skipJandex) {
            apply(plugin = "com.github.vlsi.jandex")

            project.configure<com.github.vlsi.jandex.JandexExtension> {
                toolVersion.set("jandex".v)
                skipIndexFileGeneration()
            }
        }

        if (!enableGradleMetadata) {
            tasks.withType<GenerateModuleMetadata> {
                enabled = false
            }
        }

        if (!skipAutostyle && project.name !in bslModules) {
            autostyle {
                java {
                    filter.exclude(*javaccGeneratedPatterns +
                            "**/test/java/*.java" +
                            "**/RelRule.java" /** remove as part of CALCITE-4831 **/)
                    license()
                    if (!project.props.bool("junit4", default = false)) {
                        replace("junit5: Test", "org.junit.Test", "org.junit.jupiter.api.Test")
                        replaceRegex("junit5: Before", "org.junit.Before\\b", "org.junit.jupiter.api.BeforeEach")
                        replace("junit5: BeforeClass", "org.junit.BeforeClass", "org.junit.jupiter.api.BeforeAll")
                        replaceRegex("junit5: After", "org.junit.After\\b", "org.junit.jupiter.api.AfterEach")
                        replace("junit5: AfterClass", "org.junit.AfterClass", "org.junit.jupiter.api.AfterAll")
                        replace("junit5: Ignore", "org.junit.Ignore", "org.junit.jupiter.api.Disabled")
                        replaceRegex("junit5: @Before", "@Before\\b", "@BeforeEach")
                        replace("junit5: @BeforeClass", "@BeforeClass", "@BeforeAll")
                        replaceRegex("junit5: @After", "@After\\b", "@AfterEach")
                        replace("junit5: @AfterClass", "@AfterClass", "@AfterAll")
                        replace("junit5: @Ignore", "@Ignore", "@Disabled")
                        replace("junit5: Assert.assertThat", "org.junit.Assert.assertThat", "org.hamcrest.MatcherAssert.assertThat")
                        replace("junit5: Assert.fail", "org.junit.Assert.fail", "org.junit.jupiter.api.Assertions.fail")
                    }
                    replaceRegex("side by side comments", "(\n\\s*+[*]*+/\n)(/[/*])", "\$1\n\$2")
                    replaceRegex("jsr305 nullable -> checkerframework", "javax\\.annotation\\.Nullable", "org.checkerframework.checker.nullness.qual.Nullable")
                    replaceRegex("jsr305 nonnull -> checkerframework", "javax\\.annotation\\.Nonnull", "org.checkerframework.checker.nullness.qual.NonNull")
                    importOrder(
                        "org.apache.calcite.",
                        "org.apache.",
                        "au.com.",
                        "com.",
                        "io.",
                        "mondrian.",
                        "net.",
                        "org.",
                        "scala.",
                        "java",
                        "",
                        "static com.",
                        "static org.apache.calcite.",
                        "static org.apache.",
                        "static org.",
                        "static java",
                        "static "
                    )
                    removeUnusedImports()
                    replaceRegex("Avoid 2+ blank lines after package", "^package\\s+([^;]+)\\s*;\\n{3,}", "package \$1;\n\n")
                    replaceRegex("Avoid 2+ blank lines after import", "^import\\s+([^;]+)\\s*;\\n{3,}", "import \$1;\n\n")
                    indentWithSpaces(2)
                    replaceRegex("@Override should not be on its own line", "(@Override)\\s{2,}", "\$1 ")
                    replaceRegex("@Test should not be on its own line", "(@Test)\\s{2,}", "\$1 ")
                    replaceRegex("Newline in string should be at end of line", """\\n" *\+""", "\\\\n\"\n  +")
                    replaceRegex("require message for requireNonNull", """(?<!#)requireNonNull\(\s*(\w+)\s*(?:,\s*"(?!\1")\w+"\s*)?\)""", "requireNonNull($1, \"$1\")")
                    // (?-m) disables multiline, so $ matches the very end of the file rather than end of line
                    replaceRegex("Remove '// End file.java' trailer", "(?-m)\n// End [^\n]+\\.\\w+\\s*$", "")
                    replaceRegex("<p> should not be placed at the end of the line", "(?-m)\\s*+<p> *+\n \\* ", "\n *\n * <p>")
                    replaceRegex("Method parameter list should not end in whitespace or newline", "(?<!;)\\s+\\) \\{", ") {")
                    replaceRegex("Method argument list should not end in whitespace or newline", "(?<!;)\\s+(\\)[);,])", "$1")
                    replaceRegex("Method argument list should not end in whitespace or newline", "(?<!;)(\\s+)(\\)+)[.]", "$2$1.")
                    replaceRegex("Long assignment should be broken after '=' (#1)", "^([^/*\"\\n]*) = ([^@\\n]*)\\(\n( *)", "$1 =\n$3$2(")
                    replaceRegex("Long assignment should be broken after '=' (#2)", "^([^/*\"\\n]*) = ([^@\\n]*)\\((.*,)\n( *)", "$1 =\n$4$2($3 ")
                    replaceRegex("Long assignment should be broken after '=' (#3)", "^([^/*\"\\n]*) = ([^@\\n\"?]*[ ][?][ ][^@\\n:]*)\n( *)", "$1 =\n$3$2\n$3    ")
                    replaceRegex("trailing keyword: implements", "^([^*]*) (implements)\\n( *)", "$1\n$3$2 ")
                    // Assume developer copy-pasted the link, and updated text only, so the url is old, and we replace it with the proper one
                    replaceRegex(">[CALCITE-...] link styles: 1", "<a(?:(?!CALCITE-)[^>])++CALCITE-\\d+[^>]++>\\s*+\\[?(CALCITE-\\d+)\\]?", "<a href=\"https://issues.apache.org/jira/browse/\$1\">[\$1]")
                    // If the link was crafted manually, ensure it has [CALCITE-...] in the link text
                    replaceRegex(">[CALCITE-...] link styles: 2", "<a(?:(?!CALCITE-)[^>])++(CALCITE-\\d+)[^>]++>\\s*+\\[?CALCITE-\\d+\\]?", "<a href=\"https://issues.apache.org/jira/browse/\$1\">[\$1]")
                    replace("hamcrest: allOf", "org.hamcrest.core.AllOf.allOf", "org.hamcrest.CoreMatchers.allOf")
                    replace("hamcrest: aMapWithSize", "org.hamcrest.collection.IsMapWithSize.aMapWithSize", "org.hamcrest.Matchers.aMapWithSize")
                    replace("hamcrest: anyOf", "org.hamcrest.core.AnyOf.anyOf", "org.hamcrest.CoreMatchers.anyOf")
                    replace("hamcrest: containsString", "org.hamcrest.core.StringContains.containsString", "org.hamcrest.CoreMatchers.containsString")
                    replace("hamcrest: CoreMatchers", "import org.hamcrest.CoreMatchers;", "import static org.hamcrest.CoreMatchers.anything;")
                    replace("hamcrest: empty", "org.hamcrest.collection.IsEmptyCollection.empty", "org.hamcrest.Matchers.empty")
                    replace("hamcrest: emptyArray", "org.hamcrest.collection.IsArrayWithSize.emptyArray", "org.hamcrest.Matchers.emptyArray")
                    replace("hamcrest: endsWidth", "org.hamcrest.core.StringEndsWith.endsWith", "org.hamcrest.CoreMatchers.endsWith")
                    replace("hamcrest: equalTo", "org.hamcrest.core.IsEqual.equalTo", "org.hamcrest.CoreMatchers.equalTo")
                    replace("hamcrest: greaterThanOrEqualTo", "org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo", "org.hamcrest.Matchers.greaterThanOrEqualTo")
                    replace("hamcrest: greaterThan", "org.hamcrest.number.OrderingComparison.greaterThan", "org.hamcrest.Matchers.greaterThan")
                    replace("hamcrest: hasItem", "org.hamcrest.core.IsIterableContaining.hasItem", "org.hamcrest.CoreMatchers.hasItem")
                    replace("hamcrest: hasItems", "org.hamcrest.core.IsIterableContaining.hasItems", "org.hamcrest.CoreMatchers.hasItems")
                    replace("hamcrest: hasSize", "org.hamcrest.collection.IsCollectionWithSize.hasSize", "org.hamcrest.Matchers.hasSize")
                    replace("hamcrest: hasToString", "org.hamcrest.object.HasToString.hasToString", "org.hamcrest.Matchers.hasToString")
                    replace("hamcrest: instanceOf", "org.hamcrest.core.IsInstanceOf.instanceOf", "org.hamcrest.CoreMatchers.instanceOf")
                    replace("hamcrest: instanceOf", "org.hamcrest.Matchers.instanceOf", "org.hamcrest.CoreMatchers.instanceOf")
                    replace("hamcrest: is", "org.hamcrest.core.Is.is", "org.hamcrest.CoreMatchers.is")
                    replace("hamcrest: is", "org.hamcrest.Matchers.is", "org.hamcrest.CoreMatchers.is")
                    replace("hamcrest: isA", "org.hamcrest.core.Is.isA", "org.hamcrest.CoreMatchers.isA")
                    replace("hamcrest: lessThanOrEqualTo", "org.hamcrest.number.OrderingComparison.lessThanOrEqualTo", "org.hamcrest.Matchers.lessThanOrEqualTo")
                    replace("hamcrest: lessThan", "org.hamcrest.number.OrderingComparison.lessThan", "org.hamcrest.Matchers.lessThan")
                    replace("hamcrest: Matchers", "import org.hamcrest.Matchers;", "import static org.hamcrest.Matchers.allOf;")
                    replace("hamcrest: notNullValue", "org.hamcrest.core.IsNull.notNullValue", "org.hamcrest.CoreMatchers.notNullValue")
                    replace("hamcrest: notNullValue", "org.hamcrest.Matchers.notNullValue", "org.hamcrest.CoreMatchers.notNullValue")
                    replace("hamcrest: not", "org.hamcrest.core.IsNot.not", "org.hamcrest.CoreMatchers.not")
                    replace("hamcrest: not", "org.hamcrest.Matchers.not", "org.hamcrest.CoreMatchers.not")
                    replace("hamcrest: nullValue", "org.hamcrest.core.IsNull.nullValue", "org.hamcrest.CoreMatchers.nullValue")
                    replace("hamcrest: nullValue", "org.hamcrest.Matchers.nullValue", "org.hamcrest.CoreMatchers.nullValue")
                    replace("hamcrest: sameInstance", "org.hamcrest.core.IsSame.sameInstance", "org.hamcrest.CoreMatchers.sameInstance")
                    replace("hamcrest: startsWith", "org.hamcrest.core.StringStartsWith.startsWith", "org.hamcrest.CoreMatchers.startsWith")
                    replaceRegex("hamcrest: hasToString", "\\.toString\\(\\), (is|equalTo)\\(", ", hasToString\\(")
                    replaceRegex("hamcrest: length", "\\.length, (is|equalTo)\\(", ", arrayWithSize\\(")
                    replaceRegex("hamcrest: size", "\\.size\\(\\), (is|equalTo)\\(", ", hasSize\\(")
                    // Commenting out these rules as they cause issues with method resolution
                    // replaceRegex("use static import: parseBoolean", "Boolean\\.(parseBoolean\\()", "$1")
                    // replaceRegex("use static import: parseByte", "Byte\\.(parseByte\\()", "$1")
                    // replaceRegex("use static import: parseDouble", "Double\\.(parseDouble\\()", "$1")
                    // replaceRegex("use static import: parseFloat", "Float\\.(parseFloat\\()", "$1")
                    // replaceRegex("use static import: parseInt", "Integer\\.(parseInt\\()", "$1")
                    // replaceRegex("use static import: parseLong", "Long\\.(parseLong\\()", "$1")
                    // replaceRegex("use static import: parseLong", "Short\\.(parseShort\\()", "$1")
                    replaceRegex("use static import: requireNonNull", "Objects\\.(requireNonNull\\()", "$1")
                    replaceRegex("use static import: toImmutableList", "ImmutableList\\.(toImmutableList\\(\\))", "$1")
                    replaceRegex("use static import: checkArgument", "Preconditions\\.(checkArgument\\()", "$1")
                    replaceRegex("use static import: checkArgument", "Preconditions\\.(checkState\\()", "$1")
                    custom("((() preventer", 1) { contents: String ->
                        ParenthesisBalancer.apply(contents)
                    }
                }
            }
        }
        if (enableSpotBugs) {
            apply(plugin = "com.github.spotbugs")
            spotbugs {
                toolVersion = "spotbugs".v
                reportLevel = "high"
                //  excludeFilter = file("$rootDir/src/main/config/spotbugs/spotbugs-filter.xml")
                // By default spotbugs verifies TEST classes as well, and we do not want that
                this.sourceSets = listOf(sourceSets["main"])
            }
            dependencies {
                // Parenthesis are needed here: https://github.com/gradle/gradle/issues/9248
                (constraints) {
                    "spotbugs"("org.ow2.asm:asm:${"asm".v}")
                    "spotbugs"("org.ow2.asm:asm-all:${"asm".v}")
                    "spotbugs"("org.ow2.asm:asm-analysis:${"asm".v}")
                    "spotbugs"("org.ow2.asm:asm-commons:${"asm".v}")
                    "spotbugs"("org.ow2.asm:asm-tree:${"asm".v}")
                    "spotbugs"("org.ow2.asm:asm-util:${"asm".v}")
                }
            }
        }

        configure<CheckForbiddenApisExtension> {
            failOnUnsupportedJava = false
            ignoreSignaturesOfMissingClasses = true
            suppressAnnotations.add("org.immutables.value.Generated")
            bundledSignatures.addAll(
                listOf(
                    "jdk-unsafe",
                    "jdk-deprecated",
                    "jdk-non-portable"
                )
            )
            signaturesFiles = files("$rootDir/src/main/config/forbidden-apis/signatures.txt")
        }

        if (enableErrorprone) {
            apply(plugin = "net.ltgt.errorprone")
            dependencies {
                "errorprone"("com.google.errorprone:error_prone_core:${"errorprone".v}")
                "annotationProcessor"("com.google.guava:guava-beta-checker:1.0")
            }
            tasks.withType<JavaCompile>().configureEach {
                options.errorprone {
                    disableWarningsInGeneratedCode.set(true)
                    errorproneArgs.add("-XepExcludedPaths:.*/javacc/.*")
                    enable(
                        "MethodCanBeStatic"
                    )
                    disable(
                        "ComplexBooleanConstant",
                        "EqualsGetClass",
                        "EqualsHashCode", // verified in Checkstyle
                        "OperatorPrecedence",
                        "MutableConstantField",
                        "ReferenceEquality",
                        "SameNameButDifferent",
                        "TypeParameterUnusedInFormals"
                    )
                    // Analyze issues, and enable the check
                    disable(
                        "BigDecimalEquals",
                        "DoNotCallSuggester",
                        "StringSplitter"
                    )
                }
            }
        }
        if (enableCheckerframework) {
            apply(plugin = "org.checkerframework")
            dependencies {
                "checkerFramework"("org.checkerframework:checker:${"checkerframework".v}")
                // CheckerFramework annotations might be used in the code as follows:
                // dependencies {
                //     "compileOnly"("org.checkerframework:checker-qual")
                //     "testCompileOnly"("org.checkerframework:checker-qual")
                // }
                if (JavaVersion.current() == JavaVersion.VERSION_1_8) {
                    // only needed for JDK 8
                    "checkerFrameworkAnnotatedJDK"("org.checkerframework:jdk8")
                }
            }
            configure<org.checkerframework.gradle.plugin.CheckerFrameworkExtension> {
                skipVersionCheck = true
                // See https://checkerframework.org/manual/#introduction
                checkers.add("org.checkerframework.checker.nullness.NullnessChecker")
                // Below checkers take significant time and they do not provide much value :-/
                // checkers.add("org.checkerframework.checker.optional.OptionalChecker")
                // checkers.add("org.checkerframework.checker.regex.RegexChecker")
                // https://checkerframework.org/manual/#creating-debugging-options-progress
                // extraJavacArgs.add("-Afilenames")
                extraJavacArgs.addAll(listOf("-Xmaxerrs", "10000"))
                // Consider Java assert statements for nullness and other checks
                extraJavacArgs.add("-AassumeAssertionsAreEnabled")
                // https://checkerframework.org/manual/#stub-using
                extraJavacArgs.add("-Astubs=" +
                        fileTree("$rootDir/src/main/config/checkerframework") {
                            include("**/*.astub")
                        }.asPath
                )
                if (project.path == ":core") {
                    extraJavacArgs.add("-AskipDefs=^org\\.apache\\.calcite\\.sql\\.parser\\.impl\\.")
                }
                if (project.path == ":server") {
                    extraJavacArgs.add("-AskipDefs=^org\\.apache\\.calcite\\.sql\\.parser\\.ddl\\.")
                }
            }
        }

        tasks {
            configureEach<Jar> {
                manifest {
                    attributes["Bundle-License"] = "Apache-2.0"
                    attributes["Implementation-Title"] = "Apache Calcite"
                    attributes["Implementation-Version"] = project.version
                    attributes["Specification-Vendor"] = "The Apache Software Foundation"
                    attributes["Specification-Version"] = project.version
                    attributes["Specification-Title"] = "Apache Calcite"
                    attributes["Implementation-Vendor"] = "Apache Software Foundation"
                    attributes["Implementation-Vendor-Id"] = "org.apache.calcite"
                }
            }

            configureEach<CheckForbiddenApis> {
                excludeJavaCcGenerated()
                exclude(
                    "**/org/apache/calcite/adapter/os/Processes${'$'}ProcessFactory.class",
                    "**/org/apache/calcite/adapter/os/OsAdapterTest.class",
                    "**/org/apache/calcite/runtime/Resources${'$'}Inst.class",
                    "**/org/apache/calcite/util/TestUnsafe.class",
                    "**/org/apache/calcite/util/Unsafe.class",
                    "**/org/apache/calcite/test/Unsafe.class"
                )
            }

            configureEach<JavaCompile> {
                inputs.property("java.version", System.getProperty("java.version"))
                inputs.property("java.vm.version", System.getProperty("java.vm.version"))
                options.encoding = "UTF-8"
                options.compilerArgs.add("-Xlint:deprecation")
                // JDK 1.8 is deprecated https://bugs.openjdk.org/browse/JDK-8173605
                // and now it requires -Xlint:-options to mute warnings about its deprecation
                options.compilerArgs.add("-Xlint:-options")
                // -source/-target alone does not restrict the API surface, so a newer
                // JDK silently accepts newer APIs and only a CI job running the older
                // JDK rejects them. --release enforces the API too. A convention, not
                // a value, so modules needing more can override it.
                if (JavaVersion.current() >= JavaVersion.VERSION_1_9) {
                    options.release.convention(11)
                    // --release applies lints that cross-compiling with -source 8
                    // suppressed. dep-ann fires on the javacc-generated
                    // SimpleCharStream, which carries an @deprecated javadoc with no
                    // annotation and cannot be edited, and -Werror makes it fatal.
                    options.compilerArgs.add("-Xlint:-dep-ann")
                }
                if (werror) {
                    options.compilerArgs.add("-Werror")
                }
                if (enableCheckerframework) {
                    options.forkOptions.memoryMaximumSize = "2g"
                }
            }
            configureEach<Test> {
                outputs.cacheIf("test results depend on the database configuration, so we shouldn't cache it") {
                    false
                }
                useJUnitPlatform {
                    excludeTags("slow")
                }
                testLogging {
                    exceptionFormat = TestExceptionFormat.FULL
                    showStandardStreams = true
                }
                exclude("**/*Suite*")
                if (JavaVersion.current() >= JavaVersion.VERSION_23) {
                    // Subject.doAs is deprecated and does not work in JDK 23
                    // and higher unless the (also deprecated) SecurityManager
                    // is enabled. However, we depend on libraries Avatica and
                    // Hadoop for our remote driver and Pig and Spark
                    // adapters. So as a workaround we require enabling the
                    // security manager on JDK 23 and higher. See
                    // [CALCITE-6587], [CALCITE-6590] (Avatica), [HADOOP-19212],
                    // https://openjdk.org/jeps/411.
                    jvmArgs("-Djava.security.manager=allow")
                }
                jvmArgs("-Xmx1536m")
                jvmArgs("-Djdk.net.URLClassPath.disableClassPathURLCheck=true")
                // Pass the property to tests
                fun passProperty(name: String, default: String? = null) {
                    val value = System.getProperty(name) ?: default
                    value?.let { systemProperty(name, it) }
                }
                passProperty("java.awt.headless")
                passProperty("junit.jupiter.execution.parallel.enabled", "true")
                passProperty("junit.jupiter.execution.parallel.mode.default", "concurrent")
                // Disabled timeout for long-running integration tests
                // passProperty("junit.jupiter.execution.timeout.default", "5 m")
                passProperty("user.language", "TR")
                passProperty("user.country", "tr")
                passProperty("user.timezone", "UTC")
                passProperty("calcite.avatica.version", props.string("calcite.avatica.version"))
                passProperty("gradle.rootDir", rootDir.toString())
                val props = System.getProperties()
                for (e in props.propertyNames() as `java.util`.Enumeration<String>) {
                    if (e.startsWith("calcite.") || e.startsWith("avatica.")) {
                        passProperty(e)
                    }
                }
            }
            // Cannot be moved above otherwise configure each will override
            // also the specific configurations below.
            register<Test>("testSlow") {
                group = LifecycleBasePlugin.VERIFICATION_GROUP
                description = "Runs the slow unit tests."
                useJUnitPlatform() {
                    includeTags("slow")
                }
                jvmArgs("-Xmx6g")
            }
            configureEach<SpotBugsTask> {
                group = LifecycleBasePlugin.VERIFICATION_GROUP
                if (enableSpotBugs) {
                    description = "$description (skipped by default, to enable it add -Dspotbugs)"
                }
                reports {
                    html.required.set(reportsForHumans())
                    xml.required.set(!reportsForHumans())
                }
                enabled = enableSpotBugs
            }
            configureEach<JacocoReport> {
                reports {
                    // The reports are mainly consumed by Sonar, which only uses the XML format
                    xml.required.set(true)
                    html.required.set(false)
                    csv.required.set(false)
                }
            }

            afterEvaluate {
                // Add default license/notice when missing
                configureEach<Jar> {
                    CrLfSpec(LineEndings.LF).run {
                        into("META-INF") {
                            filteringCharset = "UTF-8"
                            duplicatesStrategy = DuplicatesStrategy.EXCLUDE
                            // Note: we need "generic Apache-2.0" text without third-party items
                            // So we use the text from $rootDir/config/ since source distribution
                            // contains altered text at $rootDir/LICENSE
                            textFrom("$rootDir/src/main/config/licenses/LICENSE")
                            textFrom("$rootDir/NOTICE")
                        }
                    }
                }
            }
        }

        // Note: jars below do not normalize line endings.
        // Those jars, however are not included to source/binary distributions
        // so the normailzation is not that important

        val testJar by tasks.registering(Jar::class) {
            from(sourceSets["test"].output)
            archiveClassifier.set("tests")
        }

        val sourcesJar by tasks.registering(Jar::class) {
            from(sourceSets["main"].allJava)
            archiveClassifier.set("sources")
        }

        val javadocJar by tasks.registering(Jar::class) {
            from(tasks.named(JavaPlugin.JAVADOC_TASK_NAME))
            archiveClassifier.set("javadoc")
        }

        val archives by configurations.getting

        // Parenthesis needed to use Project#getArtifacts
        (artifacts) {
            archives(sourcesJar)
        }

        base {
            archivesName.set("calcite-$name")
        }

        configure<PublishingExtension> {
            if (project.path == ":") {
                // Do not publish "root" project. Java plugin is applied here for DSL purposes only
                return@configure
            }
            if (!project.props.bool("nexus.publish", default = true)) {
                // Some of the artifacts do not need to be published
                return@configure
            }
            publications {
                create<MavenPublication>(project.name) {
                    artifactId = base.archivesName.get()
                    version = rootProject.version.toString()
                    description = project.description
                    from(components["java"])

                    if (!skipJavadoc) {
                        // Eager task creation is required due to
                        // https://github.com/gradle/gradle/issues/6246
                        artifact(sourcesJar.get())
                        artifact(javadocJar.get())
                    }

                    // Use the resolved versions in pom.xml
                    // Gradle might have different resolution rules, so we set the versions
                    // that were used in Gradle build/test.
                    versionMapping {
                        usage(Usage.JAVA_RUNTIME) {
                            fromResolutionResult()
                        }
                        usage(Usage.JAVA_API) {
                            fromResolutionOf("runtimeClasspath")
                        }
                    }
                    pom {
                        withXml {
                            val sb = asString()
                            var s = sb.toString()
                            // <scope>compile</scope> is Maven default, so delete it
                            s = s.replace("<scope>compile</scope>", "")
                            // Cut <dependencyManagement> because all dependencies have the resolved versions
                            s = s.replace(
                                Regex(
                                    "<dependencyManagement>.*?</dependencyManagement>",
                                    RegexOption.DOT_MATCHES_ALL
                                ),
                                ""
                            )
                            sb.setLength(0)
                            sb.append(s)
                            // Re-format the XML
                            asNode()
                        }

                        fun capitalize(input: String): String {
                            return input.replaceFirstChar { it.uppercaseChar() }
                        }
                        name.set(
                            (project.findProperty("artifact.name") as? String) ?: "Calcite ${capitalize(project.name)}"
                        )
                        description.set(project.description ?: "Calcite ${capitalize(project.name)}")
                        inceptionYear.set("2012")
                        url.set("https://calcite.apache.org")
                        licenses {
                            license {
                                name.set("The Apache License, Version 2.0")
                                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                                comments.set("A business-friendly OSS license")
                                distribution.set("repo")
                            }
                        }
                        issueManagement {
                            system.set("Jira")
                            url.set("https://issues.apache.org/jira/browse/CALCITE")
                        }
                        mailingLists {
                            mailingList {
                                name.set("Apache Calcite developers list")
                                subscribe.set("dev-subscribe@calcite.apache.org")
                                unsubscribe.set("dev-unsubscribe@calcite.apache.org")
                                post.set("dev@calcite.apache.org")
                                archive.set("https://lists.apache.org/list.html?dev@calcite.apache.org")
                            }
                        }
                        scm {
                            connection.set("scm:git:https://gitbox.apache.org/repos/asf/calcite.git")
                            developerConnection.set("scm:git:https://gitbox.apache.org/repos/asf/calcite.git")
                            url.set("https://github.com/apache/calcite")
                            tag.set("HEAD")
                        }
                    }
                }
            }
        }
    }
}
