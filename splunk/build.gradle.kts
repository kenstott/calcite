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
    api("com.google.guava:guava")
    api("org.apache.calcite.avatica:avatica-core")
    api("org.slf4j:slf4j-api")

    // CSV parsing (keeping for legacy SearchResultListener support)
    implementation("com.opencsv:opencsv:5.7.1")

    // JSON parsing for new JSON-based enumerator
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    implementation("com.fasterxml.jackson.core:jackson-core:2.17.2")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.17.2")

    testImplementation(project(":testkit"))
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl")

    annotationProcessor("org.immutables:value")
    compileOnly("org.immutables:value-annotations")
    compileOnly("com.google.code.findbugs:jsr305")
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

ide {
    // generate annotation processed files on project import/sync.
    // adds to idea path but skip don't add to SourceSet since that triggers checkstyle
    fun generatedSource(compile: TaskProvider<JavaCompile>, sourceSetName: String) {
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

    generatedSource(annotationProcessorMain, "main")
}

// The tests tagged "integration" talk to a Splunk server, so an ordinary build leaves them
// out. They run when the build is given CALCITE_TEST_SPLUNK=true or
// -Dcalcite.test.splunk=true (as the Druid tests do with calcite.test.druid), or when
// -PincludeTags names the tags to run. The server comes from local-properties.settings.
//
// Those also tagged "splunk-data" need a server that has the CIM data models and indexed
// events; a newly started container has neither, so CI passes -PexcludeTags=splunk-data.
val tagsToRun = (project.findProperty("includeTags") as String?)?.split(",")
val tagsToSkip = (project.findProperty("excludeTags") as String?)?.split(",")
val splunkLive = System.getenv("CALCITE_TEST_SPLUNK") == "true" ||
    System.getProperty("calcite.test.splunk") == "true" ||
    tagsToRun?.contains("integration") == true

tasks.test {
    useJUnitPlatform {
        if (tagsToRun != null) {
            includeTags(*tagsToRun.toTypedArray())
        } else if (splunkLive) {
            excludeTags("performance")
        } else {
            excludeTags("integration", "performance")
        }
        if (tagsToSkip != null) {
            excludeTags(*tagsToSkip.toTypedArray())
        }
    }
    if (splunkLive) {
        // some of the tests are also switched on by this variable
        environment("CALCITE_TEST_SPLUNK", "true")
    }
}

tasks.shadowJar {
    archiveBaseName.set("sih-splunk")
    archiveClassifier.set("")
    isZip64 = true
    mergeServiceFiles()
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}
