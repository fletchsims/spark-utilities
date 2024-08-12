ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.2"

val SparkVersion = "3.3.2"

val uTest = "com.lihaoyi" %% "utest" % "0.7.11" % "test"
val fastTest = "com.github.mrpowers" %% "spark-fast-tests" % "1.1.0" % "test"
val awsSdkBom = "com.amazonaws" % "aws-java-sdk-bom" % "1.12.501" pomOnly ()
val awsSdkS3 = "com.amazonaws" % "aws-java-sdk-s3" % awsSdkBom.revision

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion,
  "org.apache.spark" %% "spark-sql"  % SparkVersion
)

lazy val root = (project in file("."))
  .aggregate()
  .settings(
    name := "spark-utilities",
    libraryDependencies += fastTest,
    libraryDependencies += uTest,
    libraryDependencies += awsSdkS3,
    libraryDependencies ++= sparkDependencies.map(_ % "provided"),
    assemblyPackageScala / assembleArtifact := false,
    assemblyPackageDependency / assembleArtifact := false
  )

