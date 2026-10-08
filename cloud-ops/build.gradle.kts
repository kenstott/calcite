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

import java.util.Properties

plugins {
    `java-library`
    id("com.github.johnrengelman.shadow")
}

dependencies {
    implementation(project(":core"))
    implementation(project(":linq4j"))

    implementation("com.google.guava:guava")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.core:jackson-annotations")
    implementation("org.slf4j:slf4j-api")

    // Caching - using Java 8 compatible version
    implementation("com.github.ben-manes.caffeine:caffeine:2.9.3")

    // Azure SDK
    implementation("com.azure.resourcemanager:azure-resourcemanager:2.34.0")
    implementation("com.azure:azure-identity:1.11.0")
    implementation("com.azure.resourcemanager:azure-resourcemanager-resourcegraph:1.0.0")

    // GCP SDK
    implementation("com.google.cloud:google-cloud-storage:2.29.0")
    implementation("com.google.cloud:google-cloud-container:2.48.0")
    implementation("com.google.cloud:google-cloud-compute:1.44.0")

    // AWS SDK
    implementation(platform("software.amazon.awssdk:bom:2.39.6"))
    implementation("software.amazon.awssdk:config")
    implementation("software.amazon.awssdk:ec2")
    implementation("software.amazon.awssdk:s3")
    implementation("software.amazon.awssdk:iam")
    implementation("software.amazon.awssdk:eks")
    implementation("software.amazon.awssdk:ecr")
    implementation("software.amazon.awssdk:rds")
    implementation("software.amazon.awssdk:dynamodb")
    implementation("software.amazon.awssdk:elasticache")
    implementation("software.amazon.awssdk:sts")
    implementation("software.amazon.awssdk:resourcegroupstaggingapi")
    implementation("software.amazon.awssdk:cloudwatch")

    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("org.junit.jupiter:junit-jupiter-engine")
    testImplementation("org.hamcrest:hamcrest-core")
    testImplementation(project(":core").dependencyProject.sourceSets["test"].output)
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl")
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to "Apache Calcite Cloud Ops Adapter",
            "Implementation-Version" to project.version
        )
    }
}

// Whether local-test.properties holds the credentials of at least one cloud
fun hasIntegrationCredentials(): Boolean {
    val propsFile = file("src/test/resources/local-test.properties")
    if (!propsFile.exists()) {
        return false
    }

    val props = Properties()
    propsFile.inputStream().use { props.load(it) }
    fun has(vararg names: String) = names.all { !props.getProperty(it).isNullOrEmpty() }

    val hasAzure = has("azure.tenantId", "azure.clientId", "azure.clientSecret",
        "azure.subscriptionIds")
    val hasGCP = has("gcp.credentialsPath", "gcp.projectIds")
    val hasAWS = has("aws.accessKeyId", "aws.secretAccessKey", "aws.accountIds")

    return hasAzure || hasGCP || hasAWS
}

tasks.test {
    useJUnitPlatform {
        // Always exclude performance tests from default run
        excludeTags("performance")

        // Only exclude integration tests if credentials are not available
        if (!hasIntegrationCredentials()) {
            excludeTags("integration")
        }
    }
    maxHeapSize = "2g"
    systemProperty("java.awt.headless", "true")
}

// A Test task for the tests carrying one tag, or for all of them, with its own reports
fun taggedTests(name: String, tag: String?, heap: String, reports: String) =
    tasks.register<Test>(name) {
        testClassesDirs = sourceSets["test"].output.classesDirs
        classpath = sourceSets["test"].runtimeClasspath
        useJUnitPlatform {
            if (tag != null) {
                includeTags(tag)
            }
        }
        maxHeapSize = heap
        systemProperty("java.awt.headless", "true")
        this.reports {
            html.outputLocation.set(layout.buildDirectory.dir("reports/tests/$reports"))
            junitXml.outputLocation.set(layout.buildDirectory.dir("test-results/$reports"))
        }
    }

taggedTests("unitTest", "unit", "2g", "unit")
taggedTests("integrationTest", "integration", "2g", "integration")
taggedTests("performanceTest", "performance", "4g", "performance")
taggedTests("allTests", null, "4g", "all")

tasks.shadowJar {
    archiveBaseName.set("sih-cloudops")
    archiveClassifier.set("")
    isZip64 = true
    mergeServiceFiles()
    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}
