ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "quill-vs-doobie"
  )

libraryDependencies ++= Seq(
  "mysql" % "mysql-connector-java" % "8.0.32",
  "org.xerial" % "sqlite-jdbc" % "3.40.1.0",

  "dev.zio" %% "zio" % "2.0.6",
  "io.getquill" %% "quill-jdbc-zio" % "4.6.0",

)
