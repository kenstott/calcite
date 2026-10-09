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
dependencies {
    api(project(":core"))
    api(project(":linq4j"))
    api("org.apache.spark:spark-core_2.13") {
        exclude("org.slf4j", "slf4j-log4j12")
                .because("conflicts with log4j-slf4j-impl")
        exclude("org.slf4j", "slf4j-reload4j")
                .because("conflicts with log4j-slf4j-impl")
    }

    implementation("com.google.guava:guava")
    implementation("org.eclipse.jetty:jetty-server")
    implementation("org.eclipse.jetty:jetty-util")
    implementation("org.scala-lang:scala-library")

    runtimeOnly("xalan:xalan")
    runtimeOnly("xerces:xercesImpl")

    testImplementation(project(":testkit"))
    testRuntimeOnly("org.apache.logging.log4j:log4j-slf4j-impl")
}

tasks.withType<Test>().configureEach {
    // Spark reaches into JDK internals. These are the options Spark's own launcher passes
    // (org.apache.spark.launcher.JavaModuleOptions); a JVM that runs the adapter needs them too.
    jvmArgs(
        "-XX:+IgnoreUnrecognizedVMOptions",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
        "-Djdk.reflect.useDirectMethodHandle=false",
        "-Dio.netty.tryReflectionSetAccessible=true"
    )
}
