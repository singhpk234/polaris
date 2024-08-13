/*
 * Copyright (c) 2024 Snowflake Computing Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.nosphere.apache.rat.RatTask

buildscript {
  repositories { maven { url = java.net.URI("https://plugins.gradle.org/m2/") } }
  dependencies {
    classpath("com.diffplug.spotless:spotless-plugin-gradle:${libs.plugins.spotless.get().version}")
  }
}

plugins {
  id("idea")
  id("eclipse")
  id("polaris-root")
  alias(libs.plugins.rat)
}

val projectName = rootProject.file("ide-name.txt").readText().trim()
val ideName = "$projectName ${rootProject.version.toString().replace("^([0-9.]+).*", "\\1")}"

if (System.getProperty("idea.sync.active").toBoolean()) {
  // There's no proper way to set the name of the IDEA project (when "just importing" or
  // syncing the Gradle project)
  val ideaDir = rootProject.layout.projectDirectory.dir(".idea")
  ideaDir.asFile.mkdirs()
  ideaDir.file(".name").asFile.writeText(ideName)

  val icon = ideaDir.file("icon.png").asFile
  if (!icon.exists()) {
    val img =
      java.net
        .URI(
          "https://raw.githubusercontent.com/polaris-catalog/polaris/main/docs/img/logos/polaris-brandmark.png"
        )
        .toURL()
        .openConnection()
        .getInputStream()
        .use { inp -> inp.readAllBytes() }
    ideaDir.file("icon.png").asFile.outputStream().use { out -> out.write(img) }
  }
}

eclipse { project { name = ideName } }

tasks.named<RatTask>("rat").configure {
  // These are Gradle file pattern syntax
  excludes.add("**/build/**")

  excludes.add("docs/CNAME")
  excludes.add("docs/index.html")

  excludes.add("LICENSE")
  excludes.add("NOTICE")

  excludes.add("ide-name.txt")
  excludes.add("version.txt")
  excludes.add(".git")
  excludes.add(".gradle")
  excludes.add(".idea")
  excludes.add(".java-version")
  excludes.add("**/.keep")
  excludes.add("**/poetry.lock")

  excludes.add(".github/pull_request_template.md")

  excludes.add("spec/docs.yaml")
  excludes.add("spec/index.yml")

  excludes.add("gradle/wrapper/gradle-wrapper*.jar*")

  excludes.add("polaris-service/src/**/banner.txt")
  excludes.add("polaris-service/logs")

  excludes.add("regtests/**/py.typed")
  excludes.add("regtests/**/*.ref")
  excludes.add("regtests/client/python/.openapi-generator/**")

  excludes.add("**/*.ipynb")
  excludes.add("**/*.iml")
  excludes.add("**/*.iws")

  excludes.add("**/*.md")

  excludes.add("**/*.png")
  excludes.add("**/*.svg")

  excludes.add("**/*.lock")
}