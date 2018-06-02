organization := "com.oripwk"
scalaVersion := "2.11.11"
version := "0.1.0-SNAPSHOT"
name := "spark"

libraryDependencies ++= Seq(
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.1",
  "org.apache.spark" %% "spark-streaming" % "2.3.1",
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1"
)
