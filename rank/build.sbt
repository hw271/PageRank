name := "COSC282_Project"

organization := "edu.gu.cs"

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "COSC282_Homework",
    libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.3.0" % "provided",
    libraryDependencies += "org.skife.com.typesafe.config" % "typesafe-config" % "0.3.0",
    libraryDependencies += "joda-time" % "joda-time" % "2.7" % "provided"
  )
  
mainClass in assembly := Some("cosc282.Homework")
