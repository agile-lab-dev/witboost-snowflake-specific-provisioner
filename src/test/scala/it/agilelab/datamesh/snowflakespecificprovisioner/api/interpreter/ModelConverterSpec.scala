package it.agilelab.datamesh.snowflakespecificprovisioner.api.interpreter

import it.agilelab.datamesh.snowflakespecificprovisioner.api.intepreter.ModelConverter
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.{
  ProvisioningValidationError,
  UnsupportedOperationError
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class ModelConverterSpec extends AnyFlatSpec with should.Matchers {

  it should "convert correctly a validation error" in {
    val err = ProvisioningValidationError(Some("ayamldescriptor"), Some("field"), List("Problem1"), List("Problem2"))

    val valErr = ModelConverter.buildRequestValidationError(err)
    valErr.errors should contain theSameElementsAs List(err.userMessage) ++ err.problems
    valErr.moreInfo shouldNot be(None)
    valErr.moreInfo.get.problems shouldEqual err.problems
    valErr.moreInfo.get.solutions shouldEqual err.solutions
    valErr.input shouldEqual err.input
    valErr.inputErrorField shouldEqual err.inputErrorField
    valErr.userMessage shouldEqual Some(err.userMessage)
  }

  it should "convert correctly a system error" in {
    val err = UnsupportedOperationError("dpId", List("Problem1"), List("Solution2"))

    val sysErr = ModelConverter.buildSystemError(err)
    sysErr.error shouldEqual err.userMessage
    sysErr.moreInfo shouldNot be(None)
    sysErr.moreInfo.get.problems shouldEqual err.problems
    sysErr.moreInfo.get.solutions shouldEqual err.solutions
    sysErr.input shouldEqual err.input
    sysErr.inputErrorField shouldEqual err.inputErrorField
    sysErr.userMessage shouldEqual Some(err.userMessage)
  }

  it should "convert correctly an error with no problems or solutions" in {
    val err = UnsupportedOperationError("dpId")

    val sysErr = ModelConverter.buildSystemError(err)
    sysErr.error shouldEqual err.userMessage
    sysErr.moreInfo shouldBe None
    sysErr.input shouldEqual err.input
    sysErr.inputErrorField shouldEqual err.inputErrorField
    sysErr.userMessage shouldEqual Some(err.userMessage)

    val valErr = ModelConverter.buildRequestValidationError(err)
    valErr.errors shouldEqual List(err.userMessage)
    valErr.moreInfo shouldBe None
    valErr.input shouldEqual err.input
    valErr.inputErrorField shouldEqual err.inputErrorField
    valErr.userMessage shouldEqual Some(err.userMessage)

  }
}
