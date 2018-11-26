name := "hadoop_definitive_guide"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.7.3" % "provided",
  "org.apache.hbase" % "hbase-common" % "2.0.0",
  "org.apache.hbase" % "hbase-client" % "2.0.0",
  "commons-logging" % "commons-logging" % "1.2",
  "org.apache.httpcomponents" % "httpclient" % "4.5.2",
  "org.apache.zookeeper" % "zookeeper" % "3.4.10",
  "junit" % "junit" % "4.12" % "test",
  "org.hamcrest" % "hamcrest-all" % "1.3" % "test",
  "org.apache.mrunit" % "mrunit" % "1.1.0" % "test" classifier "hadoop2"
)
