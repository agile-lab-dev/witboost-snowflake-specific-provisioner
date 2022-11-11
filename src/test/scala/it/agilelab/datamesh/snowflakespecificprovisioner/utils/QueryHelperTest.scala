package it.agilelab.datamesh.snowflakespecificprovisioner.utils

import it.agilelab.datamesh.snowflakespecificprovisioner.common.test.getTestResourceAsString
import it.agilelab.datamesh.snowflakespecificprovisioner.model.ProvisioningRequestDescriptor
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.{ColumnSchemaSpec, ConstraintType, DataType}
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.QueryHelper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QueryHelperTest extends AnyFlatSpec with Matchers {
  val queryHelper = new QueryHelper
  "the formatSnowflakeStatement method" should "format correctly the create table statement" in {
    val cols = List(
      ColumnSchemaSpec("name", DataType.TEXT, ConstraintType.NOCONSTRAINT),
      ColumnSchemaSpec("phone_number", DataType.TEXT, ConstraintType.NOCONSTRAINT),
      ColumnSchemaSpec("id", DataType.TEXT, ConstraintType.NOCONSTRAINT),
      ColumnSchemaSpec("age", DataType.NUMBER, ConstraintType.NOCONSTRAINT)
    )

    queryHelper.formatSnowflakeStatement("my-test", "your", "test_table", cols) should be(
      "CREATE TABLE IF NOT EXISTS my-test.your.TEST_TABLE (name TEXT,\n" + "phone_number TEXT,\n" + "id TEXT,\n" +
        "age NUMBER);"
    )
  }

  "buildCreateTableStatement method" should
    "correctly retrieve fields from descriptor and build the create table statement" in {
      val yaml       = getTestResourceAsString("pr_descriptors/provision_request_descriptor.yaml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      queryHelper.buildCreateTableStatement(descriptor.toOption.get).toOption.get should be(
        "CREATE TABLE IF NOT EXISTS TEST_AIRBYTE.PUBLIC.SNOWFLAKE_TABLE (id TEXT PRIMARY KEY,\n" + "name TEXT,\n" +
          "phone NUMBER NULL);"
      )
    }

  "buildCreateTableStatement method" should "create a table even when database is not specified" in {
    val yaml       = getTestResourceAsString("pr_descriptors/provision_request_descriptor_2.yaml")
    val descriptor = ProvisioningRequestDescriptor(yaml)

    queryHelper.buildCreateTableStatement(descriptor.toOption.get).toOption.get should be(
      "CREATE TABLE IF NOT EXISTS MARKETING.PUBLIC.SNOWFLAKE_TABLE (id TEXT PRIMARY KEY,\n" + "name TEXT,\n" +
        "phone NUMBER NULL);"
    )
  }

  "buildCreateTableStatement method" should "create a table even when specific.schema is not specified" in {
    val yaml       = getTestResourceAsString("pr_descriptors/provision_request_descriptor_3.yaml")
    val descriptor = ProvisioningRequestDescriptor(yaml)

    queryHelper.buildCreateTableStatement(descriptor.toOption.get).toOption.get should be(
      "CREATE TABLE IF NOT EXISTS TEST_AIRBYTE.DPOWNERTEST_100.SNOWFLAKE_TABLE (id TEXT PRIMARY KEY,\n" +
        "name TEXT,\n" + "phone NUMBER NULL);"
    )
  }

  "buildCreateTableStatement method" should "create a table even when schema and database are not specified" in {
    val yaml       = getTestResourceAsString("pr_descriptors/provision_request_descriptor_4.yaml")
    val descriptor = ProvisioningRequestDescriptor(yaml)

    queryHelper.buildCreateTableStatement(descriptor.toOption.get).toOption.get should be(
      "CREATE TABLE IF NOT EXISTS MARKETING.DPOWNERTEST_100.SNOWFLAKE_TABLE (id TEXT PRIMARY KEY,\n" + "name TEXT,\n" +
        "phone NUMBER NULL);"
    )
  }
}
