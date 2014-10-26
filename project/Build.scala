import sbt._
import Keys._

object ApplicationBuild extends Build {

  val appName         = "java-wrapper"
  val appVersion      = "0.4-SNAPSHOT"
  val appScalaVersion = "2.11.1"
  //val appScalaBinaryVersion = "2.10"
  val appScalaCrossVersions = Seq("2.11.1", "2.10.4")

  val local: Project.Initialize[Option[sbt.Resolver]] = version { (version: String) =>
    val localPublishRepo = "./repository"
    if(version.trim.endsWith("SNAPSHOT"))
      Some(Resolver.file("snapshots", new File(localPublishRepo + "/snapshots")))
    else Some(Resolver.file("releases", new File(localPublishRepo + "/releases")))
  }

  lazy val baseSettings = Defaults.defaultSettings ++ Seq(
    scalaVersion := appScalaVersion,
    //scalaBinaryVersion := appScalaBinaryVersion,
    crossScalaVersions := appScalaCrossVersions,
    parallelExecution in Test := false
  )

  lazy val root = Project("root", base = file("."))
    .settings(baseSettings: _*)
    .settings(
      publishLocal := {},
      publish := {}
    ).aggregate(
      javawrapper
    )

  lazy val javawrapper = Project(appName, base = file("javawrapper"))
    .settings(baseSettings: _*)
    .settings(
      resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      libraryDependencies += "org.reactivecouchbase" %% "reactivecouchbase-core" % "0.4-SNAPSHOT",
      libraryDependencies += "org.reactivecouchbase" % "json-lib" % "0.4-SNAPSHOT",
      libraryDependencies += "com.google.code.findbugs" % "jsr305" % "1.3.+",
      organization := "org.reactivecouchbase",
      version := appVersion,
      publishTo <<= local,
      publishMavenStyle := true,
      publishArtifact in Test := false,
      pomIncludeRepository := { _ => false }
    )
}
