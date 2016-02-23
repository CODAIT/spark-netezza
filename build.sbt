name := "spark-netezza"

version := "0.1.1"

organization := "com.ibm.SparkTC"

spName := "SparkTC/spark-netezza"

scalaVersion := "2.10.5"

crossScalaVersions := Seq("2.10.5", "2.11.7")

sparkVersion := "1.6.0"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

// Add Spark components this package depends on.
sparkComponents := Seq("sql", "hive")

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-csv" % "1.2",
  "org.scalatest" %% "scalatest" % "2.1.3",
  "com.typesafe" % "config" % "1.2.1"
)

//Developers only: for integration test, obtain netezza jdbc jar locally and uncomment below line
//unmanagedJars in Compile += file("/pathTo/nzjdbc.jar")

lazy val root =
    Project("root", file("."))
      .configs( IntegrationTest )
      .settings( Defaults.itSettings : _*)

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
