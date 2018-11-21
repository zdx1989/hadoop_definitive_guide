name := "hadoop_definitive_guide"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.7.3" % "provided",
  "org.apache.hive" % "hive-serde" % "2.3.3",
  "junit" % "junit" % "4.12" % "test",
  "org.hamcrest" % "hamcrest-all" % "1.3" % "test",
  "org.apache.mrunit" % "mrunit" % "1.1.0" % "test" classifier "hadoop2"
)
