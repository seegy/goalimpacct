name := "goalimpacct"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.5"

// fork in run := true

mainClass in (Compile, run) := Some("miningData.CalculatePointSetApp")

assemblyJarName in assembly := "goalimpacct-standalone.jar"
test in assembly := {}
mainClass in assembly := Some("miningData.CalculatePointSetApp")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) =>
    xs map {_.toLowerCase} match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "services" :: _ =>  MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}

run / javaOptions += "-Xms4G -Xmx16G"


