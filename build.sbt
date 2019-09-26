name := "module"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.2"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.2"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "0.96.1.1-hadoop2"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "0.96.1.1-hadoop2"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "0.96.1.1-hadoop2"
// https://mvnrepository.com/artifact/org.lucee/postgresql
libraryDependencies += "org.lucee" % "postgresql" % "8.3-606.jdbc4"
// https://mvnrepository.com/artifact/joda-time/joda-time
libraryDependencies += "joda-time" % "joda-time" % "1.6"
// https://mvnrepository.com/artifact/org.apache.flink/flink-core
libraryDependencies += "org.apache.flink" % "flink-core" % "1.9.0"
// https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-scala
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.9.0"
// https://mvnrepository.com/artifact/org.apache.flink/flink-scala
libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.9.0"
