name := "spark-netezza"

version := "0.1.1"

organization := "com.ibm"

spName := "SparkTC/spark-netezza"

scalaVersion := "2.10.5"

crossScalaVersions := Seq("2.10.5", "2.11.7")

sparkVersion := "1.5.2"

// Add Spark components this package depends on.
sparkComponents := Seq("sql", "hive")

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-csv" % "1.2",
  "org.scalatest" %% "scalatest" % "2.1.3" % "test"
)

spAppendScalaVersion := true

spIncludeMaven := true

publishMavenStyle := true

publishArtifact in Test := false

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra := (
  <url>https://github.com/SparkTC/spark-netezza</url>
  <licenses>
    <license>
      <name>Apache License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:SparkTC/spark-netezza.git</url>
    <connection>scm:git:git@github.com:SparkTC/spark-netezza.git</connection>
  </scm>
  <developers>
    <developer>
      <id>sureshthalamati</id>
      <name>Suresh Thalamati</name>
      <url>https://github.com/sureshthalamati</url>
    </developer>
    <developer>
      <id>xguo27</id>
      <name>Xiu Guo</name>
      <url>https://github.com/xguo27</url>
    </developer>
  </developers>
)
