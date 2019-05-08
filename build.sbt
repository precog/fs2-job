import scala.collection.Seq

homepage in ThisBuild := Some(url("https://github.com/slamdata/fs2-job"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/slamdata/fs2-job"),
  "scm:git@github.com:slamdata/fs2-job.git"))

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
    performMavenCentralSync := false,
    publishAsOSSProject := true)
  .enablePlugins(AutomateHeaderPlugin)
