name := "LBSImport"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka_2.10" % "0.8.1.1",
  "org.apache.spark" %% "spark-core" % "1.1.0",
  "org.apache.spark" % "spark-streaming_2.10" % "1.1.0",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.1.0",
  "org.apache.spark" %% "spark-sql" % "1.1.0",
  "org.apache.hbase" % "hbase-client" % "0.98.6-hadoop2",
  "org.apache.hbase" % "hbase-common" % "0.98.6-hadoop2",
  "org.apache.hbase" % "hbase-protocol" % "0.98.6-hadoop2",
  "org.apache.hbase" % "hbase-server" % "0.98.6-hadoop2",
  "org.cloudera.htrace" % "htrace-core" % "2.05"
)
