package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import io.circe.syntax.EncoderOps
import it.agilelab.datamesh.snowflakespecificprovisioner.model.TagInfo
import it.agilelab.datamesh.snowflakespecificprovisioner.system.ApplicationConfiguration

class ReverseProvisioning(executor: QueryExecutor, snowflakeTableInformationHelper: SnowflakeTableInformationHelper)
    extends LazyLogging {

  def executeReverseProvisioningStorageArea(reverseProvisioningParams: Json): Either[SnowflakeError, Option[Json]] = {

    logger.info("Executing reverse provisioning for output port with parameters: {}", reverseProvisioningParams)
    val maybeParams = (for {
      database   <- reverseProvisioningParams.hcursor.downField("database").as[String]
        .filterOrElse(_.nonEmpty, "Database was not provided, but is required")
      schema     <- reverseProvisioningParams.hcursor.downField("schema").as[String]
        .filterOrElse(_.nonEmpty, "Schema was not provided, but is required")
      tableNames <- reverseProvisioningParams.hcursor.downField("tables").as[List[String]]
        .filterOrElse(_.nonEmpty, "Table names were not provided, but are required")
    } yield (database, schema, tableNames)).left
      .map(err => ParseError(Some(reverseProvisioningParams.spaces2), None, List(err.toString), List.empty))

    logger.info("Parsing reverse provisioning params with result: {}", maybeParams)

    for {
      params     <- maybeParams
      connection <- executor.getConnection
      metadata   <- {
        val (database, schema, tableNames) = params
        logger.info(
          "Querying Snowflake for schema metadata for tables: {} on schema {}.{} ",
          tableNames.mkString(", "),
          database,
          schema
        )
        snowflakeTableInformationHelper.getExistingMetaData(connection, database, schema, tableNames)
      }
    } yield Some(Json.fromFields(List("spec.mesh.specific.tables" -> metadata.asJson)))
  }

  /** This method implements the Reverse Provisioning logic for Output Ports.
   *  @param reverseProvisioningParams Reverse provisioning input params for example: {"inputA":"value A","inputB":1}
   *  @return Either[ReverseProvisioningError, Option[ReverseProvisioningStatus]
   */
  def executeReverseProvisioningOutputPort(reverseProvisioningParams: Json): Either[SnowflakeError, Option[Json]] = {

    val maybeParams = (for {
      database <- reverseProvisioningParams.hcursor.downField("database").as[String]
        .filterOrElse(_.nonEmpty, "Database was not provided, but is required")
      schema   <- reverseProvisioningParams.hcursor.downField("schema").as[String]
        .filterOrElse(_.nonEmpty, "Schema was not provided, but is required")
      viewName <- reverseProvisioningParams.hcursor.downField("viewName").as[String]
        .filterOrElse(_.nonEmpty, "viewName was not provided, but is required")
    } yield (database, schema, viewName)).left
      .map(err => ParseError(Some(reverseProvisioningParams.spaces2), None, List(err.toString), List.empty))

    logger.info("Parsing reverse provisioning params with result: {}", maybeParams)

    for {
      params     <- maybeParams
      connection <- executor.getConnection
      metadata   <- {
        val (database, schema, viewName) = params
        logger.info("Querying Snowflake for schema metadata for table/view: {}.{}.{} ", database, schema, viewName)
        // TODO This doesn't retrieve column tags, so currently column tags will be removed when reverse provisioning
        snowflakeTableInformationHelper.getExistingMetaData(connection, database, schema, List(viewName))
      }
      tags       <-
        if (ApplicationConfiguration.ignoreSnowflakeTags) Right(List())
        else {
          val (database, schema, viewName) = params
          logger.info("Querying Snowflake for tags for table/view: {}.{}.{} ", database, schema, viewName)
          snowflakeTableInformationHelper.getViewExistingTags(connection, database, schema, viewName)
        }
    } yield {
      logger.info("Schema metadata result: {}", metadata)
      val tagInfo: List[TagInfo] = tags.headOption.flatMap(_.tags).getOrElse(List.empty).flatMap(_.map(TagInfo(_)))
      logger.info("Table/View tags result: {}", tagInfo)

      Some(Json.fromFields(
        List("spec.mesh.dataContract.schema" -> metadata.flatMap(_.schema).asJson, "spec.mesh.tags" -> tagInfo.asJson)
      ))
    }
  }
}
