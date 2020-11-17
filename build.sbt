import scala.Some
import scala.collection.Seq

ThisBuild / crossScalaVersions := Seq("2.12.12", "2.13.1")
ThisBuild / scalaVersion := "2.12.12"

ThisBuild / githubRepository := "fs2-job"

homepage in ThisBuild := Some(url("https://github.com/precog/fs2-job"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/precog/fs2-job"),
  "scm:git@github.com:precog/fs2-job.git"))

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  publishArtifact in (Test, packageBin) := true)

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = project
  .in(file("core"))
  .settings(name := "fs2-job")
  .settings(
    libraryDependencies ++= Seq(
        "co.fs2" %% "fs2-core" % "2.4.5",
        "org.specs2" %% "specs2-core" % "4.10.5" % "test"),

    addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
    performMavenCentralSync := false,
    publishAsOSSProject := true)
  .enablePlugins(AutomateHeaderPlugin)
