package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import it.agilelab.datamesh.snowflakespecificprovisioner.schema.TableSpec
import it.agilelab.datamesh.snowflakespecificprovisioner.system.ApplicationConfiguration.{
  tagReferencesDatabase,
  tagReferencesSchema,
  tagReferencesView
}

import java.sql.Connection
import scala.util.{Failure, Success, Try}

class SnowflakeTableInformationHelper(queryBuilder: QueryHelper) extends TableSpecBuilder {

  /** Helper method to fetch the existing metadata of a view from INFORMATION_SCHEMA.COLUMNS table
   *  @param connection JDBC Connection which is used to connect to Snowflake.
   *  @param database Name of the Database.
   *  @param schema Name of the Schema.
   *  @param tables Name of the tables.
   *  @return Either[ExecuteStatementError, List[TableSpec]]
   */
  def getExistingMetaData(
      connection: Connection,
      database: String,
      schema: String,
      tables: List[String]
  ): Either[ExecuteStatementError, List[TableSpec]] = {

    val listOfTables              = tables.map(name => s"'${name.toUpperCase}'").mkString(", ")
    val getSchemaInformationQuery = queryBuilder.createTableSchemaInformationQuery()
    val schemaInformationResult   = Try {
      val alterPrepared  = connection.prepareStatement("ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'")
      val alterStatement = alterPrepared.executeQuery()

      val querySchemaInformation     = getSchemaInformationQuery.replace("{CATALOG}", database)
        .replace("{TABLES}", listOfTables)
      val schemaInformationStatement = connection.prepareStatement(querySchemaInformation)
      schemaInformationStatement.setString(1, schema)
      schemaInformationStatement.setString(2, database)

      try {
        val schemaInformationResultSet = schemaInformationStatement.executeQuery()
        try Right(readTableListFromResultSet(schemaInformationResultSet))
        finally {
          schemaInformationResultSet.close()
          alterStatement.close()
        }
      } finally schemaInformationStatement.close()
    } match {
      case Success(value)     => value
      case Failure(exception) =>
        Left(ExecuteStatementError(Some(getSchemaInformationQuery), List(exception.getMessage)))
    }
    schemaInformationResult
  }

  /** Helper method to fetch the tags defined at the level of view.
   *  @param connection JDBC Connection which is used to connect to Snowflake.
   *  @param database Name of the Database.
   *  @param schema Name of the Schema.
   *  @param viewName Name of the View.
   *  @return Either[SnowflakeError, List[TableSpec]]
   */
  def getViewExistingTags(
      connection: Connection,
      database: String,
      schema: String,
      viewName: String
  ): Either[SnowflakeError, List[TableSpec]] = {

    val getSchemaInformationQuery = queryBuilder.getSingleTableTagsInformationQuery()

    for {
      tagReferencesView       <- getSnowflakeTagReferencesView(connection)
      schemaInformationResult <- Try {
        val alterPrepared          = connection.prepareStatement("ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'")
        val alterStatement         = alterPrepared.executeQuery()
        val querySchemaInformation = getSchemaInformationQuery.replace("{TAG_REFERENCES_VIEW}", tagReferencesView)
        val schemaInformationStatement = connection.prepareStatement(querySchemaInformation)
        schemaInformationStatement.setString(1, schema)
        schemaInformationStatement.setString(2, database)
        schemaInformationStatement.setString(3, viewName.toUpperCase())
        schemaInformationStatement.setString(4, "TABLE")

        try {
          val schemaInformationResultSet = schemaInformationStatement.executeQuery()
          try Right(readTableTagsListFromResultSet(schemaInformationResultSet))
          finally {
            schemaInformationResultSet.close()
            alterStatement.close()
          }
        } finally schemaInformationStatement.close()
      } match {
        case Success(list)      => list
        case Failure(exception) =>
          Left(ExecuteStatementError(Some(getSchemaInformationQuery), List(exception.getMessage)))
      }
    } yield schemaInformationResult
  }

  /** Get tag references view from where to pick the table/view/columns tags information. The target view is taken from
   *  configuration `snowflake.tag-references`. If the provided view doesn't exist, it defaults to the Snowflake account
   *  default view `SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES`
   *  @param connection Snowflake Connection
   *  @return Either an error if the connection to the Snowflake instance failed,
   *          or a fully qualified name of the view containing the tag information
   */
  def getSnowflakeTagReferencesView(connection: Connection): Either[SnowflakeError, String] = {

    val getSchemaInformationQuery = queryBuilder.findTagReferenceViewStatement()
    for {
      schemaInformationResult <- Try {

        val alterPrepared          = connection.prepareStatement("ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'")
        val alterStatement         = alterPrepared.executeQuery()
        val querySchemaInformation = getSchemaInformationQuery
          .replace("{TAG_REFERENCES_DATABASE}", tagReferencesDatabase)
          .replace("{TAG_REFERENCES_SCHEMA}", tagReferencesSchema).replace("{TAG_REFERENCES_VIEW}", tagReferencesView)
        val schemaInformationStatement = connection.prepareStatement(querySchemaInformation)

        try {
          val schemaInformationResultSet = schemaInformationStatement.executeQuery()
          val tagReferencesViewFQN       =
            if (!schemaInformationResultSet.next()) "SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES"
            else s"$tagReferencesDatabase.$tagReferencesSchema.$tagReferencesView"
          schemaInformationResultSet.close()
          Right(tagReferencesViewFQN)
        } finally alterStatement.close()
      } match {
        case Success(list)      => list
        case Failure(exception) =>
          Left(ExecuteStatementError(Some(getSchemaInformationQuery), List(exception.getMessage)))
      }
    } yield schemaInformationResult
  }

}
