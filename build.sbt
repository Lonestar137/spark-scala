import Dependencies._

ThisBuild / scalaVersion     := "2.11.12"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"



lazy val spark = Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-hive" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8"

)

lazy val jdbc = Seq(
  "org.apache.hive" % "hive-jdbc" % "3.1.2"
)

lazy val csv = Seq(
  "com.github.tototoshi" %% "scala-csv" % "1.3.10"
)

lazy val root = (project in file("."))
  .settings(
    name := "spark-scala",
    libraryDependencies += scalaTest % Test,
    libraryDependencies ++= spark,
    libraryDependencies ++= csv,
    libraryDependencies ++= jdbc
  )


// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
