name := "ModelarDB"
version := "1.0"
scalaVersion := "2.11.8"
scalacOptions ++= Seq("-optimise", "-feature", "-deprecation", "-Xlint:_")

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.xerial" % "sqlite-jdbc" % "3.18.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.3",
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided")

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
