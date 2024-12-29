name := "Sens"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.12" % Test
libraryDependencies += "org.scala-lang" % "scala-parser-combinators" % "2.11.0-M4"
libraryDependencies += "org.apache.calcite" % "calcite-core" % "1.32.0"

val mainClassPath = "org.sens.cli.MainApp"

// sbt-assembly configurations
assembly / assemblyJarName := "sens.jar"
assembly / mainClass := Some(
  mainClassPath
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

