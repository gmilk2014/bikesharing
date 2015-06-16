name := "trip_batch"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
"com.datastax.spark" %% "spark-cassandra-connector" % "1.3.0-M1",
"org.apache.spark" %% "spark-core" % "1.3.0" % "provided"
)
