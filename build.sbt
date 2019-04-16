name := "kafkaStream"

version := "0.1"

scalaVersion := "2.11.12"

lazy val root = (project in file(".")).enablePlugins(JavaAppPackaging)

val kafkaVersion = "2.2.0"
val fasteVersion = "2.9.0"
val playVersion = "1.1.0"

libraryDependencies ++= Seq(
  jdbc,
  ws,
  "org.scalatestplus.play"           %% "scalatestplus-play"          % "1.5.1"           % Test,
  "org.scalatest"                    %% "scalatest"                   % "2.2.6"           % Test,
  "ch.qos.logback"                   % "logback-classic"              % "1.2.3",
  "com.typesafe.scala-logging"       %% "scala-logging"               % "3.7.2",
  "com.typesafe"                     % "config"                       % "1.2.1",
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml"       % fasteVersion,
  "com.fasterxml.jackson.module"     % "jackson-module-scala_2.11"    % fasteVersion,
  "org.apache.kafka"                 %% "kafka"                       % kafkaVersion,
  "org.apache.kafka"                 % "kafka-streams"                % kafkaVersion,
  "mysql"                            % "mysql-connector-java"         % "5.1.38",
  "com.typesafe.play"                %% "play-ahc-ws-standalone"      % playVersion,
  "com.typesafe.play"                %% "play-ws-standalone-json"     % playVersion
)

mainClass in Compile:= Some("com.kafka.stream.RealTimeApplication")

import NativePackagerHelper._

bashScriptExtraDefines += """addJava "-Dconfig.file=${app_home}/../conf/application.conf""""
bashScriptExtraDefines += """addJava "-Dlogback.configurationFile=${app_home}/../conf/logback.xml""""

mappings in Universal ++= directory("src/main/conf")
unmanagedResourceDirectories in Compile += baseDirectory.value / "src" / "main" / "conf"