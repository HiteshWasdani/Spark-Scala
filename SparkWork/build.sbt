name := "SparkWork"

version := "0.1"

scalaVersion := "2.11.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.1" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.1" 