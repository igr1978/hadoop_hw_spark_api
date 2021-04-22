name := "hadoop_hw3"

version := "0.1"

scalaVersion := "2.12.13"

val sparkVersion = "3.1.1"
val scalaTestVersion = "3.2.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
//  "org.apache.spark" %% "spark-hive" % sparkVersion % Test,
//  "org.apache.spark" %% "spark-hive" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
)

libraryDependencies ++= Seq(
//  "org.scalactic" %% "scalactic" % scalaTestVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
//  "org.scalatest" %% "scalatest-suite" % scalaTestVersion % Test,
  "org.scalacheck" %% "scalacheck" % "1.15.3",
  "org.scalacheck" %% "scalacheck" % "1.15.3" % Test,
  "org.postgresql" % "postgresql" % "42.2.19",
//  "org.scalikejdbc" %% "scalikejdbc" % "3.5.0",
//  "org.scalikejdbc" %% "scalikejdbc" % "3.5.0" % Test,
  "com.dimafeng" %% "testcontainers-scala-postgresql" % "0.39.3" % Test,
  "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.39.3" % Test
)
