import ProjectSettings.ProjectFrom

ThisBuild / scalaVersion        := "2.13.8"
ThisBuild / organization        := "it.agilelab.datamesh"
ThisBuild / organizationName    := "AgileLab S.r.L."
ThisBuild / libraryDependencies := Dependencies.Jars.`server`
ThisBuild / dependencyOverrides ++= Dependencies.Jars.overrides
ThisBuild / version             := ComputeVersion.version
ThisBuild / scalacOptions       := Seq(
  "-deprecation",           // Emit warning and location for usages of deprecated APIs.
  "-encoding",
  "utf-8",                  // Specify character encoding used by source files.
  "-explaintypes",          // Explain type errors in more detail.
  "-feature",               // Emit warning and location for usages of features that should be imported explicitly.
  "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
  "-language:experimental.macros", // Allow macro definition (besides implementation and application)
  "-language:higherKinds",         // Allow higher-kinded types
  "-language:implicitConversions", // Allow definition of implicit functions called views
  "-unchecked",                    // Enable additional warnings where generated code depends on assumptions.
  "-Xcheckinit",                   // Wrap field accessors to throw an exception on uninitialized access.
  "-Xfatal-warnings",              // Fail the compilation if there are any warnings.
  "-Xlint:adapted-args",           // Warn if an argument list is modified to match the receiver.
  "-Xlint:constant",               // Evaluation of a constant arithmetic expression results in an error.
  "-Xlint:delayedinit-select",     // Selecting member of DelayedInit.
  "-Xlint:doc-detached",           // A Scaladoc comment appears to be detached from its element.
  "-Xlint:inaccessible",           // Warn about inaccessible types in method signatures.
  "-Xlint:infer-any",              // Warn when a type argument is inferred to be `Any`.
  "-Xlint:missing-interpolator",   // A string literal appears to be missing an interpolator id.
  "-Xlint:nullary-unit",           // Warn when nullary methods return Unit.
  "-Xlint:option-implicit",        // Option.apply used implicit view.
  "-Xlint:package-object-classes", // Class or object defined in package object.
  "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
  "-Xlint:private-shadow",         // A private field (or class parameter) shadows a superclass field.
  "-Xlint:stars-align",            // Pattern sequence wildcard must align with sequence component.
  "-Xlint:type-parameter-shadow",  // A local type parameter shadows a type already in scope.
  "-Ywarn-dead-code",              // Warn when dead code is identified.
  "-Ywarn-extra-implicit",         // Warn when more than one implicit parameter section is defined.
  "-Ywarn-numeric-widen",          // Warn when numerics are widened.
  "-Ywarn-unused:implicits",       // Warn if an implicit parameter is unused.
  "-Ywarn-unused:imports",         // Warn if an import selector is not referenced.
  "-Ywarn-unused:locals",          // Warn if a local definition is unused.
  "-Ywarn-unused:params",          // Warn if a value parameter is unused.
  "-Ywarn-unused:patvars",         // Warn if a variable bound in a pattern is unused.
  "-Ywarn-unused:privates",        // Warn if a private member is unused.
  "-Ywarn-value-discard"           // Warn when non-Unit expression results are unused.
)

val scalaFixSettings: List[Def.Setting[_]] = List[Def.Setting[_]](scalafixScalaBinaryVersion := "2.13")

val generateCode = taskKey[Unit]("A task for generating the code starting from the swagger definition")

val packagePrefix = settingKey[String]("The package prefix derived from the uservice name")

packagePrefix := name.value.replaceFirst("uservice-", "uservice.").replaceAll("-", "")

def commandOS(command: String) = System.getProperty("os.name").toLowerCase match {
  case win if win.contains("win") => "cmd /C " + command
  case _                          => command
}

generateCode := {
  import sys.process._

  Process(commandOS(s"""openapi-generator-cli generate -t template/scala-akka-http-server
             |                               -i src/main/resources/interface-specification.yml
             |                               -g scala-akka-http-server
             |                               -p projectName=${name.value}
             |                               -p invokerPackage=it.agilelab.${packagePrefix.value}.server
             |                               -p modelPackage=it.agilelab.${packagePrefix.value}.model
             |                               -p apiPackage=it.agilelab.${packagePrefix.value}.api
             |                               -p dateLibrary=java8
             |                               -p entityStrictnessTimeout=15
             |                               -o server-generated""").stripMargin).!!

  Process(commandOS(s"""openapi-generator-cli generate -t template/scala-akka-http-client
             |                               -i src/main/resources/interface-specification.yml
             |                               -g scala-akka
             |                               -p projectName=${name.value}
             |                               -p invokerPackage=it.agilelab.${packagePrefix.value}.client.invoker
             |                               -p modelPackage=it.agilelab.${packagePrefix.value}.client.model
             |                               -p apiPackage=it.agilelab.${packagePrefix.value}.client.api
             |                               -p dateLibrary=java8
             |                               -o client-generated""").stripMargin).!!

}

cleanFiles += baseDirectory.value / "server-generated" / "src"

cleanFiles += baseDirectory.value / "server-generated" / "target"

cleanFiles += baseDirectory.value / "client-generated" / "src"

cleanFiles += baseDirectory.value / "client-generated" / "target"

lazy val serverGenerated = project.in(file("server-generated"))
  .settings(name := "datamesh.snowflakespecificprovisioner.server.generated", scalacOptions := Seq()).setupBuildInfo

lazy val clientGenerated = project.in(file("client-generated")).settings(
  name                                                 := "datamesh.snowflakespecificprovisioner.client",
  scalacOptions                                        := Seq(),
  libraryDependencies                                  := Dependencies.Jars.client,
  coverageEnabled                                      := false,
  app.k8ty.sbt.gitlab.K8tyGitlabPlugin.gitlabProjectId := "40084569"
).enablePlugins(K8tyGitlabPlugin)

lazy val root = (project in file(".")).settings(
  name                        := "datamesh.snowflakespecificprovisioner",
  Test / parallelExecution    := false,
  dockerBuildOptions ++= Seq("--network=host"),
  dockerBaseImage             := "adoptopenjdk:11-jdk-hotspot",
  dockerUpdateLatest          := true,
  daemonUser                  := "daemon",
  Docker / version            := (ThisBuild / version).value,
  Docker / packageName        :=
    s"registry.gitlab.com/agilefactory/witboost.mesh/provisioning/sandbox/witboost.mesh.provisioning.sandbox.snowflakespecificprovisioner",
  Docker / dockerExposedPorts := Seq(8080),
  onChangedBuildSource        := ReloadOnSourceChanges,
  scalafixOnCompile           := true,
  semanticdbEnabled           := true,
  semanticdbVersion           := scalafixSemanticdb.revision
).aggregate(clientGenerated).dependsOn(serverGenerated).enablePlugins(JavaAppPackaging).setupBuildInfo
