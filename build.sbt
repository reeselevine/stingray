name := "stingray"

version := "0.1"

scalaVersion := "2.13.4"

//idePackagePrefix := Some("org.ucsc.stingray")
libraryDependencies += "com.yugabyte" % "cassandra-driver-core" % "3.8.0-yb-5"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.14"
libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.0.1"