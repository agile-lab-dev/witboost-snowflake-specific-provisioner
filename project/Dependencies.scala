import Versions._
import sbt._

object Dependencies {

  private[this] object akka {
    lazy val namespace       = "com.typesafe.akka"
    lazy val actorTyped      = namespace                       %% "akka-actor-typed"     % akkaVersion
    lazy val stream          = namespace                       %% "akka-stream-typed"    % akkaVersion
    lazy val http            = namespace                       %% "akka-http"            % akkaHttpVersion
    lazy val httpSprayJson   = namespace                       %% "akka-http-spray-json" % akkaHttpVersion
    lazy val httpCirceJson   = "de.heikoseeberger"             %% "akka-http-circe"      % akkaHttpJsonVersion
    lazy val httpJson4s      = "de.heikoseeberger"             %% "akka-http-json4s"     % akkaHttpJsonVersion
    lazy val management      = "com.lightbend.akka.management" %% "akka-management"      % akkaManagementVersion
    lazy val slf4j           = namespace                       %% "akka-slf4j"           % akkaVersion
    lazy val akkaTestKit     = "com.typesafe.akka"             %% "akka-testkit"         % akkaVersion
    lazy val akkaHttpTestKit = "com.typesafe.akka"             %% "akka-http-testkit"    % akkaHttpVersion
  }

  private[this] object circe {
    lazy val namespace = "io.circe"
    lazy val core      = namespace %% "circe-core"           % circeVersion
    lazy val generic   = namespace %% "circe-generic"        % circeVersion
    lazy val parser    = namespace %% "circe-parser"         % circeVersion
    lazy val yaml      = namespace %% "circe-yaml"           % circeVersion
    lazy val extras    = namespace %% "circe-generic-extras" % circeVersion
  }

  private[this] object json4s {
    lazy val namespace = "org.json4s"
    lazy val jackson   = namespace %% "json4s-jackson" % json4sVersion
    lazy val ext       = namespace %% "json4s-ext"     % json4sVersion
  }

  private[this] object jackson {
    lazy val namespace   = "com.fasterxml.jackson.core"
    lazy val core        = namespace % "jackson-core"        % jacksonVersion
    lazy val annotations = namespace % "jackson-annotations" % jacksonVersion
    lazy val databind    = namespace % "jackson-databind"    % jacksonVersion
  }

  private[this] object scalamodules {
    val namespace         = "org.scala-lang.modules"
    val parserCombinators = namespace %% "scala-parser-combinators" % "2.1.1"
  }

  private[this] object logging {
    val namespace = "com.typesafe.scala-logging"
    val scala     = namespace %% "scala-logging" % scalaLogging
  }

  private[this] object logback {
    lazy val namespace = "ch.qos.logback"
    lazy val classic   = namespace % "logback-classic" % logbackVersion
  }

  private[this] object commons {
    lazy val fileUpload = "commons-fileupload" % "commons-fileupload" % commonsFileUploadVersion
  }

  private[this] object mustache {
    lazy val mustache = "com.github.spullara.mustache.java" % "compiler" % mustacheVersion
  }

  private[this] object openapi4j {
    lazy val namespace          = "org.openapi4j"
    lazy val operationValidator = namespace % "openapi-operation-validator" % openapi4jVersion
  }

  private[this] object scalatest {
    lazy val namespace = "org.scalatest"
    lazy val core      = namespace %% "scalatest" % scalatestVersion
  }

  private[this] object scalamock {
    lazy val namespace = "org.scalamock"
    lazy val core      = namespace %% "scalamock" % "5.2.0"
  }

  private[this] object snowflake {
    lazy val namespace = "net.snowflake"
    lazy val jdbc      = namespace % "snowflake-jdbc" % snowflakeJDBCVersion
  }

  object Jars {

    lazy val overrides: Seq[ModuleID] = Seq(
      jackson.annotations            % Compile,
      jackson.core                   % Compile,
      jackson.databind               % Compile,
      scalamodules.parserCombinators % Compile,
      akka.httpSprayJson             % Compile
    )

    lazy val `server`: Seq[ModuleID] = Seq(
      akka.actorTyped              % Compile,
      akka.stream                  % Compile,
      akka.slf4j                   % Compile,
      akka.management              % Compile,
      akka.http                    % Compile,
      akka.httpCirceJson           % Compile,
      akka.httpSprayJson           % Compile,
      circe.core                   % Compile,
      circe.generic                % Compile,
      circe.parser                 % Compile,
      circe.yaml                   % Compile,
      circe.extras                 % Compile,
      commons.fileUpload           % Compile,
      mustache.mustache            % Compile,
      logging.scala                % Compile,
      logback.classic              % Compile,
      openapi4j.operationValidator % Compile,
      snowflake.jdbc               % Compile,
      scalatest.core               % Test,
      scalamock.core               % Test,
      akka.akkaTestKit             % Test,
      akka.akkaHttpTestKit         % Test
    )

    lazy val client: Seq[ModuleID] = Seq(
      akka.stream     % Compile,
      akka.http       % Compile,
      akka.httpJson4s % Compile,
      json4s.jackson  % Compile,
      json4s.ext      % Compile
    )
  }
}
