import scala.collection.Seq

ThisBuild / scalaVersion := "2.12.10"

ThisBuild / githubRepository := "quasar-destination-gbq"

homepage in ThisBuild := Some(url("https://github.com/precog/quasar-destination-gbq"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/precog/quasar-destination-gbq"),
  "scm:git@github.com:precog/quasar-destination-gbq.git"))

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)

lazy val core = project
  .in(file("core"))
  .settings(name := "quasar-destination-gbq")
  .settings(
    performMavenCentralSync := false,
    publishAsOSSProject := true)
