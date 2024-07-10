package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import com.typesafe.scalalogging.LazyLogging
import it.agilelab.datamesh.snowflakespecificprovisioner.model.{ProvisioningRequestDescriptor, Specific}
import it.agilelab.datamesh.snowflakespecificprovisioner.api.dto.{
  OutputPortDetailsType,
  SnowflakeOutputPortDetailsDto,
  SnowflakeOutputPortDetailsLinkType,
  SnowflakeOutputPortDetailsStringType
}
import it.agilelab.datamesh.snowflakespecificprovisioner.model.ComponentDescriptor.OutputPort
import it.agilelab.datamesh.snowflakespecificprovisioner.system.ApplicationConfigurationWrapper

class ProvisionInfoHelper(config: ApplicationConfigurationWrapper) extends LazyLogging {
  val queryBuilder = new QueryHelper

  def getProvisioningInfo(
      descriptor: ProvisioningRequestDescriptor
  ): Either[SnowflakeError, SnowflakeOutputPortDetailsDto] = queryBuilder.getComponent(descriptor) match {
    case Right(component) => component match {
        case outputPort: OutputPort =>
          val dbName       = getDatabaseNameInfo(descriptor, outputPort.specific)
          val schemaName   = getSchemaInfo(descriptor, outputPort.specific)
          val jdbcUrl      = getJdbcInfo
          val viewsName    = getViewsInfo(outputPort)
          val snowflakeUrl = getSnowflakeUrlInfo()
          Right(SnowflakeOutputPortDetailsDto(Map(
            "aString1" ->
              SnowflakeOutputPortDetailsStringType(OutputPortDetailsType.StringType, "Database Name", dbName),
            "aString2" ->
              SnowflakeOutputPortDetailsStringType(OutputPortDetailsType.StringType, "Schema Name", schemaName),
            "aString3" ->
              SnowflakeOutputPortDetailsStringType(OutputPortDetailsType.StringType, "Database Connection", jdbcUrl),
            "aString4" -> SnowflakeOutputPortDetailsStringType(OutputPortDetailsType.StringType, "View", viewsName),
            "aLink5"   -> SnowflakeOutputPortDetailsLinkType(
              OutputPortDetailsType.LinkType,
              "Snowflake Url",
              "View on Snowflake",
              snowflakeUrl
            )
          )))
        case _                      => Left(ParseError(Option("Invalid Component type!")))
      }
    case Left(error)      => Left(error)
  }

  def getDatabaseNameInfo(descriptor: ProvisioningRequestDescriptor, specific: Specific): String =
    queryBuilder.getDatabaseName(descriptor, specific) match { case database: String => database }

  def getSchemaInfo(descriptor: ProvisioningRequestDescriptor, specific: Specific): String =
    queryBuilder.getSchemaName(descriptor, specific) match { case schemaName: String => schemaName }

  def getJdbcInfo: String = stripCredentialsFromURL(this.config.jdbcUrl)

  def getSnowflakeUrlInfo(): String = this.config.accountLocatorUrl

  def getViewsInfo(outputPort: OutputPort): String = queryBuilder.getViewName(outputPort) match {
    case Right(viewName) => viewName
    case Left(_)         => ""
  }

  def stripCredentialsFromURL(url: String): String = {
    val regex = "(user=)[^&]+&(password=)[^&]+&".r
    regex.replaceAllIn(url, "")
  }

}
