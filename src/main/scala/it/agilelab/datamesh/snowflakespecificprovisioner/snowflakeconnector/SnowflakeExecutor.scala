package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.StrictLogging
import it.agilelab.datamesh.snowflakespecificprovisioner.system.ApplicationConfiguration._

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import scala.util.{Failure, Success, Try}

trait QueryExecutor {
  def getConnection: Either[SnowflakeError, Connection]

  /** Executes a statement on a Snowflake connection, yielding the result of the affected ros
   *
   *  @param connection Connection to be used
   *  @param statementString String with an SQL statement to be executed
   *  @return Either an [[ExecuteStatementError]] or the number of affected rows
   */
  def executeStatement(connection: Connection, statementString: String): Either[ExecuteStatementError, Int]

  def executeQuery(connection: Connection, query: String): Either[ExecuteStatementError, ResultSet]

  /** Executes a list of statements on a Snowflake connection, yielding the result for each statement
   *
   *  @param connection Connection to be used
   *  @param statements List of statements to be executed
   *  @return Sequence of results for each executed statement, which internally can be Either an [[ExecuteStatementError]] or the number of affected rows
   */
  def executeMultipleStatements(
      connection: Connection,
      statements: Seq[String]
  ): Seq[Either[ExecuteStatementError, Int]]

  /** Executes a list of statements on a Snowflake connection, failing at the first failed execution
   *
   *  @param connection Connection to be used
   *  @param statements List of statements to be executed
   *  @return Either an [[ExecuteStatementError]] of the failed statements, or if all executions were successful a sequence of the number of affected rows by each statement
   */
  def traverseMultipleStatements(
      connection: Connection,
      statements: Seq[String]
  ): Either[ExecuteStatementError, Seq[Int]]
}

class SnowflakeExecutor extends QueryExecutor with StrictLogging {

  def executeStatement(connection: Connection, statementString: String): Either[ExecuteStatementError, Int] = Try {
    logger.info("Executing SQL statement: " + statementString.replaceAll("\\n", ""))
    val statement    = connection.createStatement()
    val affectedRows = statement.executeUpdate(statementString)
    statement.close()
    affectedRows
  }.toEither.left.map(ex => ExecuteStatementError(Some(statementString), List(ex.getMessage)))

  def executeMultipleStatements(
      connection: Connection,
      statements: Seq[String]
  ): Seq[Either[ExecuteStatementError, Int]] = statements.map(statement => executeStatement(connection, statement))

  def traverseMultipleStatements(
      connection: Connection,
      statements: Seq[String]
  ): Either[ExecuteStatementError, Seq[Int]] = statements.traverse(statement => executeStatement(connection, statement))

  override def executeQuery(connection: Connection, query: String): Either[ExecuteStatementError, ResultSet] = Try {
    val validatePrepared = connection.prepareStatement(query)
    validatePrepared.executeQuery()
  }.toEither.left.map(ex => ExecuteStatementError(Some(query), List(ex.getMessage)))

  def getConnection: Either[SnowflakeError, Connection] = Try {
    logger.info("Getting connection to Snowflake account...")
    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)
    properties.put("role", role)
    properties.put("account", account)
    properties.put("warehouse", warehouse)
    DriverManager.setLoginTimeout(snowflakeConnectionTimeout.getSeconds.intValue)
    DriverManager.getConnection(jdbcUrl, properties)
  } match {
    case Failure(exception)  => Left(GetConnectionError(
        List(exception.getMessage),
        List("Please check that the connection configuration variables are set correctly")
      ))
    case Success(connection) => Right(connection)
  }
}
