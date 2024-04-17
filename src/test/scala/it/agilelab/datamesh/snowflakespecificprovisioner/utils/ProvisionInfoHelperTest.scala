package it.agilelab.datamesh.snowflakespecificprovisioner.utils

import it.agilelab.datamesh.snowflakespecificprovisioner.common.test.getTestResourceAsString
import it.agilelab.datamesh.snowflakespecificprovisioner.model.ProvisioningRequestDescriptor
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.ProvisionInfoHelper
import it.agilelab.datamesh.snowflakespecificprovisioner.system.ApplicationConfigurationWrapper
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import it.agilelab.datamesh.snowflakespecificprovisioner.api.dto.{
  SnowflakeOutputPortDetailsLinkType,
  SnowflakeOutputPortDetailsStringType
}

class ProvisionInfoHelperTest extends AnyFlatSpec with Matchers with MockFactory {
  val mockConfig: ApplicationConfigurationWrapper = mock[ApplicationConfigurationWrapper]

  val provisionInfoHelper = new ProvisionInfoHelper(mockConfig)

  "the getProvisioningInfo method" should "return output port provisioning info" in {

    (() => mockConfig.jdbcUrl).expects().returning(
      "jdbc:snowflake://myaccount.snowflakecomputing.com/?user=myuser&password=mypassword&warehouse=mywh&db=mydb"
    )
    (() => mockConfig.accountLocatorUrl).expects().returning("https://test.region.snowflakecomputing.com")

    val yaml       = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_6.yml")
    val descriptor = ProvisioningRequestDescriptor(yaml)
    val res        = provisionInfoHelper.getProvisioningInfo(descriptor.toOption.get);
    res shouldBe a[Right[_, _]]
    res.toOption.get.outputPortDetailItems should have size 5

    res match {
      case Right(value) =>
        def checkDetail(key: String, expectedValue: String): Unit = value.outputPortDetailItems.get(key) match {
          case Some(details: SnowflakeOutputPortDetailsStringType) =>
            details.value shouldEqual expectedValue
            ()
          case Some(details: SnowflakeOutputPortDetailsLinkType)   =>
            details.href shouldEqual expectedValue
            ()
          case Some(_) => fail(s"Incorrect type for '$key' in outputPortDetailItems")
          case None    => fail(s"Key '$key' not found in outputPortDetailItems")
        }
        checkDetail("aString1", "test")
        checkDetail("aString2", "test")
        checkDetail("aString3", "jdbc:snowflake://myaccount.snowflakecomputing.com/?warehouse=mywh&db=mydb")
        checkDetail("aString4", "output_port_view")
        checkDetail("aLink5", "https://test.region.snowflakecomputing.com")
      case Left(_)      => fail("Should not be left")
    }

  }

  "the getJdbcInfo method" should "return striped down JDBC url" in {
    (() => mockConfig.jdbcUrl).expects().returning(
      "jdbc:snowflake://myaccount.snowflakecomputing.com/?user=myuser&password=mypassword&warehouse=mywh&db=mydb"
    )
    val res = provisionInfoHelper.getJdbcInfo();
    res shouldEqual "jdbc:snowflake://myaccount.snowflakecomputing.com/?warehouse=mywh&db=mydb"
  }

}
