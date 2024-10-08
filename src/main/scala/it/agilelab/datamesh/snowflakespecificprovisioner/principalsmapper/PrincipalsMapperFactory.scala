package it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper

import com.typesafe.scalalogging.LazyLogging
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.{QueryExecutor, QueryHelper}
import it.agilelab.datamesh.snowflakespecificprovisioner.system.ApplicationConfiguration

object PrincipalsMapperFactory extends LazyLogging {

  def create(executor: QueryExecutor, queryHelper: QueryHelper): Either[String, PrincipalsMapper[SnowflakePrincipals]] =
    ApplicationConfiguration.principalsMapperStrategy match {
      case "identity"    => Right(new SnowflakePrincipalsMapper())
      case "table-based" => Right(new TableBasedPrincipalsMapper(executor, queryHelper))
      case others        =>
        val errorMessage =
          s"Invalid configuration for snowflake.principals-mapping.strategy. Current value: $others. Allowed values: identity, table-based"
        logger.error(errorMessage)
        Left(errorMessage)
    }

}
