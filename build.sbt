import scala.collection.Seq

ThisBuild / scalaVersion := "2.12.10"

ThisBuild / githubRepository := "quasar-destination-gbq"

homepage in ThisBuild := Some(url("https://github.com/precog/quasar-destination-gbq"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/precog/quasar-destination-gbq"),
  "scm:git@github.com:precog/quasar-destination-gbq.git"))

ThisBuild / githubWorkflowBuildPreamble +=
  WorkflowStep.Sbt(
    List("decryptSecret core/src/test/resources/precog-ci-275718-e913743ebfeb.json.enc"),
    name = Some("Decrypt gcp service account json key"))

val ArgonautVersion = "6.3.0"
val Http4sVersion = "0.21.6"
val SpecsVersion = "4.8.3"
val Slf4s = "1.7.25"
val GoogleAuthLib = "0.20.0"

lazy val buildSettings = Seq(
  logBuffered in Test := githubIsWorkflowBuild.value)

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

lazy val commonSettings = buildSettings ++ publishTestsSettings

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .settings(commonSettings)
  .aggregate(core)

lazy val core = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(name := "quasar-destination-gbq")
  .settings(
    performMavenCentralSync := false,
    publishAsOSSProject := true,

    quasarPluginName := "gbq",
    quasarPluginQuasarVersion := managedVersions.value("precog-quasar"),
    quasarPluginDestinationFqcn := Some("quasar.destination.gbq.GBQDestinationModule$"),

    quasarPluginDependencies ++= Seq(
      "com.google.auth" % "google-auth-library-oauth2-http" % GoogleAuthLib,
      "org.slf4s" %% "slf4s-api" % Slf4s,
      "org.http4s" %% "http4s-argonaut" % Http4sVersion,
      "org.http4s" %% "http4s-async-http-client" % Http4sVersion,
      "io.argonaut" %% "argonaut" % ArgonautVersion),

    libraryDependencies ++= Seq(
      "com.precog" %% "quasar-foundation" % managedVersions.value("precog-quasar") % Test classifier "tests",
      "org.specs2" %% "specs2-scalacheck" % SpecsVersion % Test,
      "org.specs2" %% "specs2-scalaz" % SpecsVersion % Test),

    publishAsOSSProject := true)
  .enablePlugins(QuasarPlugin)
