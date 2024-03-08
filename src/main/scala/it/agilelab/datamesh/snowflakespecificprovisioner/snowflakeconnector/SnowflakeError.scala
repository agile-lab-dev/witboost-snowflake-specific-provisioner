package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import cats.kernel.Semigroup

sealed trait SnowflakeError extends Throwable {
  def userMessage: String
  def problems: List[String]
  def solutions: List[String]
  def input: Option[String]           = None
  def inputErrorField: Option[String] = None

  override def getMessage: String = s"Error: $userMessage\nProblems:\n${problems.mkString("\t- ", "\n\t- ", "")}"
}

trait SnowflakeSystemError extends SnowflakeError

final case class GetConnectionError(
    override val problems: List[String] = List.empty,
    override val solutions: List[String] = List.empty
) extends SnowflakeSystemError {
  override def userMessage: String = s"Error while getting the JDBC connection"
}

final case class GetTableNameError(
    override val problems: List[String] = List.empty,
    override val solutions: List[String] = List.empty
) extends SnowflakeSystemError {
  override def userMessage: String = s"Error while getting the table name from the specific"
}

final case class GetViewNameError(
    override val problems: List[String] = List.empty,
    override val solutions: List[String] = List.empty
) extends SnowflakeSystemError {
  override def userMessage: String = s"Error while parsing the view name from the specific"
}

final case class ExecuteStatementError(
    sqlStatement: Option[String] = None,
    otherProblems: List[String] = List.empty,
    override val solutions: List[String] = List.empty
) extends SnowflakeSystemError {
  override def userMessage: String    = s"Error while executing an SQL statement"
  override val problems: List[String] = sqlStatement.map(sql => s"SQL: $sql").toList ++ otherProblems
  override def input: Option[String]  = sqlStatement
}

final case class SchemaChangesError(
    override val problems: List[String] = List.empty,
    override val solutions: List[String] = List.empty
) extends SnowflakeSystemError {
  override def userMessage: String = s"Error while validating table schema changes"
}

object ExecuteStatementError {

  implicit def executeStatementErrorSemigroup: Semigroup[ExecuteStatementError] =
    (x: ExecuteStatementError, y: ExecuteStatementError) =>
      ExecuteStatementError(
        sqlStatement = None,
        otherProblems = x.problems ++ y.problems,
        solutions = x.solutions ++ y.solutions
      )
}

final case class GetComponentError(componentToProvisionId: String, override val problems: List[String] = List.empty)
    extends SnowflakeSystemError {
  override def userMessage: String = s"Unable to parse the component to provision with id $componentToProvisionId"

  override val solutions: List[String] =
    List("Make sure that the ids match and that the component exists in the descriptor")
}

final case class UnsupportedOperationError(
    unsupportedOp: String,
    override val problems: List[String] = List.empty,
    override val solutions: List[String] = List.empty
) extends SnowflakeSystemError {
  override def userMessage: String = s"Unsupported operation: $unsupportedOp"
}

final case class GetNoInformationError(override val problems: List[String] = List.empty) extends SnowflakeSystemError {
  override def userMessage: String = s"Skipping table creation - no information provided"

  override val solutions: List[String] = List("Make sure that the table information exists in the descriptor")
}

trait SnowflakeValidationError extends SnowflakeError

final case class ProvisioningValidationError(
    override val input: Option[String] = None,
    override val inputErrorField: Option[String] = None,
    override val problems: List[String] = List.empty,
    override val solutions: List[String] = List.empty
) extends SnowflakeValidationError {
  override val userMessage: String = s"Error while executing the provisioning phase"
}

final case class ParseError(
    override val input: Option[String] = None,
    override val inputErrorField: Option[String] = None,
    override val problems: List[String] = List.empty,
    override val solutions: List[String] = List.empty
) extends SnowflakeValidationError {
  override val userMessage: String = s"Error while parsing the schema received in the descriptor"
}

final case class SchemaValidationError(
    override val input: Option[String] = None,
    override val problems: List[String] = List.empty,
    override val solutions: List[String] = List.empty
) extends SnowflakeValidationError {
  override val userMessage: String = s"Error while validating the schema received in the descriptor"
}

final case class PrincipalMappingError(
    override val input: Option[String] = None,
    override val problems: List[String] = List.empty,
    override val solutions: List[String] = List.empty
) extends SnowflakeValidationError {

  override val userMessage: String =
    s"Error while mapping a principal. The received principal is not supported by Snowflake Specific Provisioner"
}

object PrincipalMappingError {

  implicit def principalMappingErrorSemigroup: Semigroup[PrincipalMappingError] =
    (x: PrincipalMappingError, y: PrincipalMappingError) =>
      PrincipalMappingError(input = None, problems = x.problems ++ y.problems, solutions = x.solutions ++ y.solutions)
}
