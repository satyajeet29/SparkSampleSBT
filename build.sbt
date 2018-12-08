resolvers += "jitpack" at "https://jitpack.io"

name := "SparkSampleSBT"

version := "1.0"

scalaVersion := "2.11.12"

sparkVersion := "2.1.1"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.1" % "provided"

// adding sbt assembly plugin
libraryDependencies ++= Seq(
  "net.gpedro.integrations.slack" % "slack-webhook" % "1.2.1",
  "org.json4s" %% "json4s-native" % "3.3.0"
)
// ensures all retrieved files are packaged and created with dependancies
retrieveManaged := true

//JAR file settings
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${sparkVersion.value}_${version.value}.jar"



