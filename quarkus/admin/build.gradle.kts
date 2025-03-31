/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import io.quarkus.gradle.tasks.QuarkusBuild

plugins {
  alias(libs.plugins.quarkus)
  alias(libs.plugins.jandex)
  alias(libs.plugins.openapi.generator)
  id("polaris-quarkus")
  // id("polaris-license-report")
  id("distribution")
}

val runScript by configurations.creating { description = "Used to reference the run.sh script" }

val distributionZip by
  configurations.creating { description = "Used to reference the distribution zip" }

val distributionTar by
  configurations.creating { description = "Used to reference the distribution tarball" }

dependencies {
  implementation(project(":polaris-core"))
  implementation(project(":polaris-version"))
  implementation(project(":polaris-api-management-service"))
  implementation(project(":polaris-api-iceberg-service"))

  runtimeOnly(project(":polaris-eclipselink"))
  runtimeOnly(project(":polaris-jdbc"))

  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-picocli")
  implementation("io.quarkus:quarkus-container-image-docker")

  implementation("org.jboss.slf4j:slf4j-jboss-logmanager")

  testFixturesApi(project(":polaris-core"))

  testFixturesApi(enforcedPlatform(libs.quarkus.bom))
  testFixturesApi("io.quarkus:quarkus-junit5")

  testFixturesApi(platform(libs.testcontainers.bom))
  testFixturesApi("org.testcontainers:testcontainers")
  testFixturesApi("org.testcontainers:postgresql")

  testRuntimeOnly(project(":polaris-eclipselink"))
  testRuntimeOnly(project(":polaris-jdbc"))
  testRuntimeOnly("org.postgresql:postgresql")

  runScript(project(":polaris-quarkus-run-script", "runScript"))
}

quarkus {
  quarkusBuildProperties.put("quarkus.package.type", "fast-jar")
  // Pull manifest attributes from the "main" `jar` task to get the
  // release-information into the jars generated by Quarkus.
  quarkusBuildProperties.putAll(
    provider {
      tasks
        .named("jar", Jar::class.java)
        .get()
        .manifest
        .attributes
        .map { e -> "quarkus.package.jar.manifest.attributes.\"${e.key}\"" to e.value.toString() }
        .toMap()
    }
  )
}

distributions {
  main {
    contents {
      from(runScript)
      from(project.layout.buildDirectory.dir("quarkus-app"))
      from("../../NOTICE")
      from("../../LICENSE-BINARY-DIST").rename("LICENSE-BINARY-DIST", "LICENSE")
    }
  }
}

val quarkusBuild = tasks.named<QuarkusBuild>("quarkusBuild")

val distTar =
  tasks.named<Tar>("distTar") {
    dependsOn(quarkusBuild)
    // Trigger resolution (and build) of the run-script artifact
    inputs.files(runScript)
    compression = Compression.GZIP
  }

val distZip =
  tasks.named<Zip>("distZip") {
    dependsOn(quarkusBuild)
    // Trigger resolution (and build) of the run-script artifact
    inputs.files(runScript)
  }

// Expose runnable jar via quarkusRunner configuration for integration-tests that require the
// server.
artifacts {
  add(distributionTar.name, provider { distTar.get().archiveFile }) { builtBy(distTar) }
  add(distributionZip.name, provider { distZip.get().archiveFile }) { builtBy(distZip) }
}

afterEvaluate {
  publishing {
    publications {
      named<MavenPublication>("maven") {
        artifact(distTar.get().archiveFile) { builtBy(distTar) }
        artifact(distZip.get().archiveFile) { builtBy(distZip) }
      }
    }
  }
}
