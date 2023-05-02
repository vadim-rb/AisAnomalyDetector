ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

val sparkVersion = "3.2.2"
//val sparkVersion = "2.4.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

lazy val root = (project in file("."))
  .settings(
    name := "D"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion ,
)

libraryDependencies ++= Seq(
  "com.nrinaudo" %% "kantan.csv-generic" % "0.7.0",
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % "2.13.5",
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.13.5",
  "org.apache.kafka" % "kafka-clients" % "3.4.0" )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
)

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "42.2.18"
)

libraryDependencies += "io.scalaland" %% "chimney" % "0.6.2"
