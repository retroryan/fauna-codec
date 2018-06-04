import Dependencies._
import sbt.Keys.libraryDependencies

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      scalaVersion := "2.12.3",
      version      := "0.1"
    )),
    name := "Hello",
    libraryDependencies ++= Seq(
      "com.faunadb" %% "faunadb-scala" % "2.2.0",
      scalaTest % Test
    )
  )
