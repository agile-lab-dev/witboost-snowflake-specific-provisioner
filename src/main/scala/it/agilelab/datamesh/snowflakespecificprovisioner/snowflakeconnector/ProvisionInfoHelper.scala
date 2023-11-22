package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import com.typesafe.scalalogging.LazyLogging
import it.agilelab.datamesh.snowflakespecificprovisioner.model.ProvisioningRequestDescriptor
import io.circe.Json
import it.agilelab.datamesh.snowflakespecificprovisioner.model.ComponentDescriptor
import it.agilelab.datamesh.snowflakespecificprovisioner.api.dto.SnowflakeOutputPortDetailsDto
import it.agilelab.datamesh.snowflakespecificprovisioner.api.dto.OutputPortDetailsType
import it.agilelab.datamesh.snowflakespecificprovisioner.api.dto.SnowflakeOutputPortDetailsStringType
import it.agilelab.datamesh.snowflakespecificprovisioner.system.ApplicationConfigurationWrapper

class ProvisionInfoHelper(config: ApplicationConfigurationWrapper) extends LazyLogging {
  val queryBuilder = new QueryHelper

  def getProvisioningInfo(
      descriptor: ProvisioningRequestDescriptor
  ): Either[SnowflakeError, SnowflakeOutputPortDetailsDto] =
    for {
      component <- queryBuilder.getComponent(descriptor)
      dbName       = getDatabaseNameInfo(descriptor, component.specific)
      schemaName   = getSchemaInfo(descriptor, component.specific)
      jdbcUrl      = getJdbcInfo()
      viewsName    = getViewsInfo(component)
      snowflakeUrl = getSnowflakeUrlInfo()
    } yield SnowflakeOutputPortDetailsDto(Map(
      "aString1" -> SnowflakeOutputPortDetailsStringType(OutputPortDetailsType.StringType, "Database Name", dbName),
      "aString2" -> SnowflakeOutputPortDetailsStringType(OutputPortDetailsType.StringType, "Schema Name", schemaName),
      "aLink3"   ->
        SnowflakeOutputPortDetailsStringType(OutputPortDetailsType.StringType, "Database Connection", jdbcUrl),
      "aString4" -> SnowflakeOutputPortDetailsStringType(OutputPortDetailsType.StringType, "View", viewsName),
      "aLink5" -> SnowflakeOutputPortDetailsStringType(OutputPortDetailsType.StringType, "Snowflake Url", snowflakeUrl)
    ))

  def getDatabaseNameInfo(descriptor: ProvisioningRequestDescriptor, specific: Json): String =
    queryBuilder.getDatabaseName(descriptor, specific) match { case database: String => database }

  def getSchemaInfo(descriptor: ProvisioningRequestDescriptor, specific: Json): String =
    queryBuilder.getSchemaName(descriptor, specific) match { case schemaName: String => schemaName }

  def getJdbcInfo(): String = stripCredentialsFromURL(this.config.jdbcUrl)

  def getSnowflakeUrlInfo(): String = this.config.accountLocatorUrl

  def getViewsInfo(descriptor: ComponentDescriptor): String = queryBuilder.getViewName(descriptor) match {
    case Right(viewName) => viewName
    case Left(_)         => ""
  }

  def stripCredentialsFromURL(url: String): String = {
    val regex = "(user=)[^&]+&(password=)[^&]+&".r
    regex.replaceAllIn(url, "")
  }

}
