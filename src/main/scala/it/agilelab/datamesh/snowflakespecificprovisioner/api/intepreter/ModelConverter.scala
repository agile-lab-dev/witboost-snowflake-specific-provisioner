package it.agilelab.datamesh.snowflakespecificprovisioner.api.intepreter

import it.agilelab.datamesh.snowflakespecificprovisioner.model._
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.SnowflakeError

object ModelConverter {

  def buildValidationError(snowflakeError: SnowflakeError): ValidationError = {
    val moreInfo    =
      if (snowflakeError.problems.isEmpty && snowflakeError.solutions.isEmpty) None
      else Some(ErrorMoreInfo(problems = snowflakeError.problems, solutions = snowflakeError.solutions))
    val userMessage = snowflakeError.userMessage
    ValidationError(
      errors = List(userMessage) ++ snowflakeError.problems,
      userMessage = Some(userMessage),
      moreInfo = moreInfo,
      input = snowflakeError.input,
      inputErrorField = snowflakeError.inputErrorField
    )

  }

  def buildSystemError(snowflakeError: SnowflakeError): SystemError = {
    val moreInfo    =
      if (snowflakeError.problems.isEmpty && snowflakeError.solutions.isEmpty) None
      else Some(ErrorMoreInfo(problems = snowflakeError.problems, solutions = snowflakeError.solutions))
    val userMessage = snowflakeError.userMessage
    SystemError(
      error = userMessage,
      userMessage = Some(userMessage),
      moreInfo = moreInfo,
      input = snowflakeError.input,
      inputErrorField = snowflakeError.inputErrorField
    )

  }
}
