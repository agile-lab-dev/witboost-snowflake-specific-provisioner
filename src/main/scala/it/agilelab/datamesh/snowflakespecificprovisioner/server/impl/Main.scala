package it.agilelab.datamesh.snowflakespecificprovisioner.server.impl

import akka.actor
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.directives.{Credentials, SecurityDirectives}
import buildinfo.BuildInfo
import com.typesafe.scalalogging.LazyLogging
import it.agilelab.datamesh.snowflakespecificprovisioner.api.SpecificProvisionerApi
import it.agilelab.datamesh.snowflakespecificprovisioner.api.intepreter.{
  ProvisionerApiMarshallerImpl,
  ProvisionerApiServiceImpl
}
import it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper.PrincipalsMapperFactory
import it.agilelab.datamesh.snowflakespecificprovisioner.server.Controller
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.{
  QueryHelper,
  ReverseProvisioning,
  SnowflakeExecutor,
  SnowflakeManager,
  SnowflakeTableInformationHelper
}
import it.agilelab.datamesh.snowflakespecificprovisioner.system.ApplicationConfiguration.httpPort

import scala.jdk.CollectionConverters._

object Main extends LazyLogging {

  def run(port: Int): ActorSystem[Nothing] = ActorSystem[Nothing](
    Behaviors.setup[Nothing] { context =>
      import akka.actor.typed.scaladsl.adapter._
      implicit val classicSystem: actor.ActorSystem = context.system.toClassic

      val snowflakeExecutor               = new SnowflakeExecutor
      val queryHelper                     = new QueryHelper
      val snowflakeTableInformationHelper = new SnowflakeTableInformationHelper(queryHelper)
      val reverseProvisioning             = new ReverseProvisioning(snowflakeExecutor, snowflakeTableInformationHelper)
      val principalsMapper                = PrincipalsMapperFactory.create(snowflakeExecutor, queryHelper)
      principalsMapper.fold(
        error => {
          logger.error(error)
          Behaviors.stopped
        },
        mapper => {
          val snowflakeManager = new SnowflakeManager(
            snowflakeExecutor,
            queryHelper,
            snowflakeTableInformationHelper,
            reverseProvisioning,
            mapper
          )
          val impl             = new ProvisionerApiServiceImpl(snowflakeManager)

          val api = new SpecificProvisionerApi(
            impl,
            new ProvisionerApiMarshallerImpl(),
            SecurityDirectives.authenticateBasic("SecurityRealm", (_: Credentials) => Some(Seq.empty[(String, String)]))
          )

          val controller = new Controller(
            api,
            validationExceptionToRoute = Some { e =>
              logger.error("Error: ", e)
              val results = e.results()
              if (Option(results).isDefined) {
                results.crumbs().asScala.foreach(crumb => logger.info(crumb.crumb()))
                results.items().asScala.foreach { item =>
                  logger.info(item.dataCrumbs())
                  logger.info(item.dataJsonPointer())
                  logger.info(item.schemaCrumbs())
                  logger.info(item.message())
                  logger.info("Severity: ", item.severity.getValue)
                }
                val message = e.results().items().asScala.map(_.message()).mkString("\n")
                complete((400, message))
              } else complete((400, e.getMessage))
            }
          )

          val _ = Http().newServerAt("0.0.0.0", port).bind(controller.routes)
          logger.info("Server bound to port {}", port)
          Behaviors.empty
        }
      )
    },
    BuildInfo.name.replaceAll("""\.""", "-")
  )

  def main(args: Array[String]): Unit = { val _ = run(httpPort) }
}
