name := "weightedroundrobin"

version := "0.1"

scalaVersion := "2.12.4"

lazy val specs2Version         = "2.4.17"
lazy val catsVersion           = "1.0.0-MF"

libraryDependencies ++= Seq(
  "org.typelevel"     %% "cats-core"          % catsVersion withSources(),
  "org.scala-lang"     % "scala-reflect"      % scalaVersion.value,
  "com.typesafe"       % "config"             % "1.3.0",

  "org.scalatest"     %% "scalatest"          % "3.0.1"                     % "test",
  "org.specs2"        %% "specs2"             % specs2Version               % "test",
  "org.specs2"        %% "specs2-mock"        % specs2Version               % "test",
  "org.specs2"        %% "specs2-scalacheck"  % specs2Version               % "test"
)


dependencyOverrides ++= Seq("org.scalacheck" %% "scalacheck" % "1.13.4")
