name := "spark_lib"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
libraryDependencies += "org.specs2" %% "specs2-core" % "4.5.1" % Test
libraryDependencies += "org.specs2" %% "specs2-junit" % "4.5.1" % Test
libraryDependencies += "junit" % "junit" % "4.10" % Test
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.9"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.9"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.9"

scalacOptions in Test ++= Seq("-Yrangepos")

