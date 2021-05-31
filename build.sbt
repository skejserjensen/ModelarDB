name := "ModelarDB"
version := "1.0"
scalaVersion := "2.12.13"
scalacOptions ++= Seq("-opt:l:inline", "-opt-inline-from:<sources>", "-feature", "-deprecation", "-Xlint:_")

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,

  "com.h2database" % "h2" % "1.4.200",
  "org.apache.spark" %% "spark-core" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided",

  "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.1" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "3.2.0",
  "org.apache.parquet" % "parquet-hadoop" % "1.10.1",
  "org.apache.orc" % "orc-core" % "1.5.12")

Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
