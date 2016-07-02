/**
  * (C) Copyright IBM Corp. 2015, 2016
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */
name := "spark-netezza"

version := "0.1.1"

organization := "com.ibm.SparkTC"

spName := "SparkTC/spark-netezza"

scalaVersion := "2.10.5"

crossScalaVersions := Seq("2.10.5", "2.11.7")

sparkVersion := "1.5.2"

// All Spark Packages need a license
licenses := Seq("Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"))

// Add Spark components this package depends on.
sparkComponents := Seq("sql", "hive")

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-csv" % "1.2",
  "org.scalatest" %% "scalatest" % "2.1.3" % "it,test",
  "com.typesafe" % "config" % "1.2.1"
)

parallelExecution in Test := false

// Developers only: for integration test, obtain netezza jdbc jar locally and uncomment below line
// unmanagedJars in Compile += file("/pathTo/nzjdbc.jar")

lazy val IntegrationTest = config("it") extend Test

val testSparkVersion = settingKey[String]("Spark version to test against")

lazy val root =
    Project("root", file("."))
      .configs( IntegrationTest )
      .settings( Defaults.itSettings : _*)
      .settings(
        testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value),
        libraryDependencies ++= Seq(
          "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test" force(),
          "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "test" force(),
          "org.apache.spark" %% "spark-hive" % testSparkVersion.value % "test,it" force()
        )
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
