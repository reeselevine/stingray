name := "stingray"

version := "0.1"

scalaVersion := "2.13.4"

//idePackagePrefix := Some("org.ucsc.stingray")
libraryDependencies += "com.yugabyte" % "cassandra-driver-core" % "3.8.0-yb-5"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.14"
libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.0.1"
libraryDependencies += "com.typesafe" % "config" % "1.4.1"

/** Discard matching meta-inf files in dependencies */
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}