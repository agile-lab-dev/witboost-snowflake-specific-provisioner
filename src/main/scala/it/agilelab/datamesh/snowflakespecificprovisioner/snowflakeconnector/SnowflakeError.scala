package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

sealed trait SnowflakeError {
  def errorMessage: String
}

final case class GetConnectionError(error: String) extends SnowflakeError {
  override def errorMessage: String = s"GetConnectionError($error)"
}

final case class GetTableNameError(error: String) extends SnowflakeError {
  override def errorMessage: String = s"GetStorageNameError($error)"
}

final case class CreateRoleStatementError(storageName: String, error: String) extends SnowflakeError {
  override def errorMessage: String = s"CreateRoleStatementError($storageName, $error)"
}

final case class AssignPrivilegesToRoleStatementError(storageName: String, error: String) extends SnowflakeError {
  override def errorMessage: String = s"AssignPrivilegesToRoleStatementError($storageName, $error)"
}

final case class AssignRoleToUserStatementError(user: String, storageName: String, error: String)
    extends SnowflakeError {
  override def errorMessage: String = s"AssignRoleToUserStatementError($user, $storageName, $error)"
}

final case class AssignRoleToUsersStatementError(error: List[String]) extends SnowflakeError {
  override def errorMessage: String = s"AssignRoleToUsersStatementError($error)"
}
