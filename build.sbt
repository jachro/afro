organization := "io.jachro"
name := "afro"
ThisBuild / scalaVersion := "3.5.1"

// This project contains nothing to package, like pure POM maven project
packagedArtifacts := Map.empty

releaseVersionBump := sbtrelease.Version.Bump.Minor
releaseIgnoreUntrackedFiles := true
releaseTagName := (ThisBuild / version).value

addCommandAlias(
  "lint",
  "; scalafmtSbtCheck; scalafmtCheckAll;"
)
addCommandAlias("fix", "; scalafmtSbt; scalafmtAll")

lazy val root = project
  .in(file("."))
  .withId("afro")
  .settings(
    publish / skip := true,
    publishTo := Some(
      Resolver.file("Unused transient repository", file("target/unusedrepo"))
    ),
    libraryDependencies ++= (
      Dependencies.catsCore ++
        Dependencies.scodec
    ),
    libraryDependencies ++= (
      Dependencies.avro ++
        Dependencies.scalacheck ++
        Dependencies.scalatest ++
        Dependencies.scalatestScalaCheck
    ).map(_ % Test)
  )

lazy val commonSettings = Seq(
  organization := "io.jachro",
  publish / skip := true,
  publishTo := Some(
    Resolver.file("Unused transient repository", file("target/unusedrepo"))
  ),
  Compile / packageDoc / publishArtifact := false,
  Compile / packageSrc / publishArtifact := false,
  // format: off
  scalacOptions ++= Seq(
    "-language:postfixOps", // enabling postfixes
    "-deprecation", // Emit warning and location for usages of deprecated APIs.
    "-encoding", "utf-8", // Specify character encoding used by source files.
    "-explaintypes", // Explain type errors in more detail.
    "-feature", // Emit warning and location for usages of features that should be imported explicitly.
    "-unchecked", // Enable additional warnings where generated code depends on assumptions.
    "-language:higherKinds", // Allow higher-kinded types
    "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
    "-Xfatal-warnings",
    "-Wunused:imports", // Warn if an import selector is not referenced.
    "-Wunused:locals", // Warn if a local definition is unused.
    "-Wunused:explicits", // Warn if an explicit parameter is unused.
    "-Wvalue-discard" // Warn when non-Unit expression results are unused.
  ),
  Compile / console / scalacOptions := (Compile / scalacOptions).value.filterNot(_ == "-Xfatal-warnings"),
  Test / console / scalacOptions := (Compile / console / scalacOptions).value,
  // Format: on
  organizationName := "io.jachro",
  startYear := Some(java.time.LocalDate.now().getYear),
  licenses += ("Apache-2.0", new URI(
    "https://www.apache.org/licenses/LICENSE-2.0.txt"
  ).toURL),
  headerLicense := Some(
    HeaderLicense.Custom(
      s"""|Copyright ${java.time.LocalDate.now().getYear} jachro
          |
          |Licensed under the Apache License, Version 2.0 (the "License");
          |you may not use this file except in compliance with the License.
          |You may obtain a copy of the License at
          |
          |    http://www.apache.org/licenses/LICENSE-2.0
          |
          |Unless required by applicable law or agreed to in writing, software
          |distributed under the License is distributed on an "AS IS" BASIS,
          |WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
          |See the License for the specific language governing permissions and
          |limitations under the License.""".stripMargin
    )
  )
)
