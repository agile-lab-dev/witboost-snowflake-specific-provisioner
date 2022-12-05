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

final case class GetViewNameError(error: String) extends SnowflakeError {
  override def errorMessage: String = s"GetViewNameError($error)"
}

final case class ExecuteStatementError(error: String) extends SnowflakeError {
  override def errorMessage: String = s"ExecuteStatementError($error)"
}

final case class GetComponentError(error: String) extends SnowflakeError {
  override def errorMessage: String = s"GetComponentError($error)"
}

final case class GetSchemaError(error: String) extends SnowflakeError {
  override def errorMessage: String = s"GetSchemaError($error)"
}

final case class UnsupportedOperationError(error: String) extends SnowflakeError {
  override def errorMessage: String = s"UnsupportedOperationError($error)"
}
