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
    `maven-publish`
}

description = "Calcite JDBC driver base — shared wrapper pattern and test harness for all custom drivers"

dependencies {
    api(project(":core"))
    api("org.apache.calcite.avatica:avatica-core")
}

// ─── Maven publishing (GitHub Packages) ──────────────────────────────────────
// Published as io.askamerica:calcite-driver-base so downstream driver modules
// can depend on it without pulling in the full Apache Calcite source tree.

publishing {
    publications {
        create<MavenPublication>("driverBase") {
            groupId    = "io.askamerica"
            artifactId = "calcite-driver-base"
            version    = project.version.toString().replace("-SNAPSHOT", "")
                .let { if (it.isBlank() || it == "unspecified") "0.0.1" else it }

            from(components["java"])

            pom {
                name.set("Calcite Driver Base")
                description.set("Shared JDBC driver wrapper base class and Python test harness for Calcite-based drivers")
                url.set("https://github.com/kenstott/calcite")
                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
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
