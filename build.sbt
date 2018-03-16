name := "cassandra-assignment"

version := "0.1"

scalaVersion := "2.12.4"


libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.4.0",
  "com.typesafe"               %  "config"           % "1.3.2",
  "log4j" % "log4j" % "1.2.17"
)