ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2"
ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2"

lazy val root = (project in file("."))
  .settings(
    name := "Silver-Vulture"
  )

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
"Silver-Vulture.jar" }
