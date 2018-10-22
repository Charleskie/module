name := "module"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.2"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.2"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "0.96.1.1-hadoop2"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "0.96.1.1-hadoop2"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "0.96.1.1-hadoop2"