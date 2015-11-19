name := "spark-netezza"

version := "0.1"

organization := "com.ibm"

spName := "ibm/spark-netezza"

scalaVersion := "2.10.5"

crossScalaVersions := Seq("2.10.5", "2.11.7")

sparkVersion := "1.5.2"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

// Add Spark components this package depends on.
sparkComponents := Seq("sql", "hive")

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-csv" % "1.2"
)
