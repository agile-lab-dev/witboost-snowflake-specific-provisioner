package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

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
  override def userMessage: String = s"Unsupported opperation: $unsupportedOp"
}

trait SnowflakeValidationError extends SnowflakeError

final case class ProvisioningValidationError(
    override val input: Option[String] = None,
    override val inputErrorField: Option[String] = None,
    override val problems: List[String] = List.empty,
    override val solutions: List[String] = List.empty
) extends SnowflakeValidationError {
  override val userMessage: String = s"Error while execute the provisioning phase"
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
