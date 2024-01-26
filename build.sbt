ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.2"

val SparkVersion = "3.3.2"

val uTest = "com.lihaoyi" %% "utest" % "0.7.11" % "test"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion,
  "org.apache.spark" %% "spark-sql"  % SparkVersion
)

lazy val root = (project in file("."))
  .settings(
    name := "spark-utilities",
    libraryDependencies += uTest,
    libraryDependencies ++= sparkDependencies.map(_ % "provided"),
    assemblyPackageScala / assembleArtifact := false,
    assemblyPackageDependency / assembleArtifact := false
  )
