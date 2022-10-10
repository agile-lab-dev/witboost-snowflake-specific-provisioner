package it.agilelab.datamesh.specificprovisioner.server.impl

import akka.actor
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.directives.{Credentials, SecurityDirectives}
import buildinfo.BuildInfo
import it.agilelab.datamesh.specificprovisioner.api.intepreter.{ProvisionerApiMarshallerImpl, ProvisionerApiServiceImpl}
import it.agilelab.datamesh.specificprovisioner.api.{SpecificProvisionerApi, SpecificProvisionerApiService}
import it.agilelab.datamesh.specificprovisioner.server.Controller
import it.agilelab.datamesh.specificprovisioner.system.ApplicationConfiguration.httpPort

import scala.jdk.CollectionConverters._

object Main {

  def run(port: Int, impl: SpecificProvisionerApiService): ActorSystem[Nothing] = ActorSystem[Nothing](
    Behaviors.setup[Nothing] { context =>
      import akka.actor.typed.scaladsl.adapter._
      implicit val classicSystem: actor.ActorSystem = context.system.toClassic

      val api = new SpecificProvisionerApi(
        impl,
        new ProvisionerApiMarshallerImpl(),
        SecurityDirectives.authenticateBasic("SecurityRealm", (_: Credentials) => Some(Seq.empty[(String, String)]))
      )

      val controller = new Controller(
        api,
        validationExceptionToRoute = Some { e =>
          println(e)
          val results = e.results()
          if (Option(results).isDefined) {
            results.crumbs().asScala.foreach(crumb => println(crumb.crumb()))
            results.items().asScala.foreach { item =>
              println(item.dataCrumbs())
              println(item.dataJsonPointer())
              println(item.schemaCrumbs())
              println(item.message())
              println(item.severity())
            }
            val message = e.results().items().asScala.map(_.message()).mkString("\n")
            complete((400, message))
          } else complete((400, e.getMessage))
        }
      )

      val _ = Http().newServerAt("0.0.0.0", port).bind(controller.routes)
      Behaviors.empty
    },
    BuildInfo.name.replaceAll("""\.""", "-")
  )

  def main(args: Array[String]): Unit = { val _ = run(httpPort, new ProvisionerApiServiceImpl()) }
}
