name := "kudu-meetup"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.5.1"
libraryDependencies += "org.kududb" % "kudu-client" % "0.5.0"
libraryDependencies += "org.kududb" % "kudu-mapreduce" % "0.5.0" exclude("javax.servlet", "servlet-api")

resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/repo/"
