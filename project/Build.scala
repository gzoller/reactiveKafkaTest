import sbt._
import Keys._

import com.typesafe.sbt.SbtScalariform._
import scalariform.formatter.preferences._

object Build extends Build {
  import Dependencies._

  lazy val basicSettings = Seq(
    organization                := "co.blocke",
    scalaVersion                := "2.11.8",
    version                     := "0.4.2",
    parallelExecution in Test   := false,
    ScalariformKeys.preferences := ScalariformKeys.preferences.value
      .setPreference(AlignArguments, true)
      .setPreference(AlignParameters, true)
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(DoubleIndentClassDeclaration, true)
      .setPreference(PreserveDanglingCloseParenthesis, true),
    scalacOptions in ThisBuild  ++= Seq("-Ywarn-unused-import", "-Xlint", "-feature", "-deprecation", "-encoding", "UTF8", "-unchecked")
  )

  lazy val root = Project(id = "latekafka", base = file("."))
    .settings(basicSettings: _*)
    .settings(libraryDependencies ++=
      dep_compile(akafka, kafka, zkclient, kafka_client, akka_stream, akka_slf4j) ++
      dep_test(scalatest)
  )
}
