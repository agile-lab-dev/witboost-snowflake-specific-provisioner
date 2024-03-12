package it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper

import com.typesafe.scalalogging.LazyLogging
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.{PrincipalMappingError, QueryExecutor}
import it.agilelab.datamesh.snowflakespecificprovisioner.system.ApplicationConfiguration

class TableBasedPrincipalsMapper(executor: QueryExecutor) extends PrincipalsMapper[String] with LazyLogging {

  /** This method defines the main mapping logic
   *
   *  @param subjects Set of subjects, i.e. witboost users and groups
   *  @return the mapping. For each subject, either an PrincipalMappingError, or the successfully mapped principal is returned
   */
  override def map(subjects: Set[String]): Map[String, Either[PrincipalMappingError, String]] = subjects.map { s =>
    val res = for {
      connection <- executor.getConnection
      resultSet  <- executor.executeQuery(
        connection,
        getMappingsStatement(
          ApplicationConfiguration.principalsMapperTableBasedDatabase,
          ApplicationConfiguration.principalsMapperTableBasedSchema,
          ApplicationConfiguration.principalsMapperTableBasedTable,
          s
        )
      )
    } yield resultSet
    res match {
      case Left(error) =>
        val errorMessage =
          s"An error occurred while mapping the identity $s. Please try again or contact the platform team. Details: ${error
            .problems.mkString(",")}"
        logger.error(errorMessage)
        s -> Left(PrincipalMappingError(Some(s), List(errorMessage)))
      case Right(rs)   =>
        if (!rs.next()) {
          val errorMessage = s"Unable to map the identity $s. Entry not found in the mapping table"
          logger.error(errorMessage)
          s -> Left(PrincipalMappingError(Some(s), List(errorMessage)))
        } else {
          val mappedRef = rs.getString("SNOWFLAKE_IDENTITY")
          s -> Right(mappedRef.toUpperCase)
        }
    }
  }.toMap

  private def getMappingsStatement(
      dbName: String,
      schemaName: String,
      tableName: String,
      witboostIdentity: String
  ): String =
    s"SELECT SNOWFLAKE_IDENTITY FROM $dbName.$schemaName.$tableName WHERE WITBOOST_IDENTITY = '$witboostIdentity'"
}
