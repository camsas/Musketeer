name := "pagerank"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.0-incubating"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.5.0"

resolvers ++= Seq("Akka Repository" at "http://repo.akka.io/releases/","Spray Repository" at "http://repo.spray.cc/", "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/")
