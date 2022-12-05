package it.agilelab.datamesh.snowflakespecificprovisioner.utils

import it.agilelab.datamesh.snowflakespecificprovisioner.common.test.getTestResourceAsString
import it.agilelab.datamesh.snowflakespecificprovisioner.model.ProvisioningRequestDescriptor
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.{ColumnSchemaSpec, ConstraintType, DataType}
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.QueryHelper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.OperationType.{
  CREATE_DB,
  CREATE_SCHEMA,
  CREATE_TABLES,
  CREATE_VIEW,
  DELETE_SCHEMA,
  DELETE_TABLES
}

class QueryHelperTest extends AnyFlatSpec with Matchers {
  val queryHelper = new QueryHelper

  "the createTableStatement method" should "format correctly the create table statement" in {
    val cols = List(
      ColumnSchemaSpec("name", DataType.TEXT, ConstraintType.NOCONSTRAINT),
      ColumnSchemaSpec("phone_number", DataType.TEXT, ConstraintType.NOCONSTRAINT),
      ColumnSchemaSpec("id", DataType.TEXT, ConstraintType.NOCONSTRAINT),
      ColumnSchemaSpec("age", DataType.NUMBER, ConstraintType.NOCONSTRAINT)
    )

    queryHelper.createTableStatement("my-test", "your", "test_table", cols) should be(
      "CREATE TABLE IF NOT EXISTS my-test.your.TEST_TABLE (name TEXT,\n" + "phone_number TEXT,\n" + "id TEXT,\n" +
        "age NUMBER);"
    )
  }

  "the createViewStatement method" should "format correctly the create view statement" in {
    val cols = List(
      ColumnSchemaSpec("name", DataType.TEXT, ConstraintType.NOCONSTRAINT),
      ColumnSchemaSpec("phone_number", DataType.TEXT, ConstraintType.NOCONSTRAINT),
      ColumnSchemaSpec("id", DataType.TEXT, ConstraintType.NOCONSTRAINT),
      ColumnSchemaSpec("age", DataType.NUMBER, ConstraintType.NOCONSTRAINT)
    )

    queryHelper.createViewStatement("my-test-view", "my-test", "your", "test_table", cols) should be(
      "CREATE VIEW IF NOT EXISTS my-test.your.my-test-view AS (SELECT name,\nphone_number,\nid,\nage FROM my-test.your.test_table);"
    )
  }

  "buildOutputPortStatement method" should
    "correctly retrieve fields from descriptor and build the create view statement" in {
      val yaml       = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_2.yml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      queryHelper.buildOutputPortStatement(descriptor.toOption.get, CREATE_VIEW).toOption
        .getOrElse("Cannot Create View") should be(
        "CREATE VIEW IF NOT EXISTS TEST_AIRBYTE.PUBLIC.snowflake_view AS (SELECT id,\nname,\nphone FROM TEST_AIRBYTE.PUBLIC.snowflake_table);"
      )

    }

  "buildOutputPortStatement method" should "create a view even when database is not specified" in {
    val yaml       = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_3.yml")
    val descriptor = ProvisioningRequestDescriptor(yaml)

    queryHelper.buildOutputPortStatement(descriptor.toOption.get, CREATE_VIEW).toOption
      .getOrElse("Cannot Create View") should be(
      "CREATE VIEW IF NOT EXISTS MARKETING.PUBLIC.snowflake_view AS (SELECT id,\nname,\nphone FROM MARKETING.PUBLIC.snowflake_table);"
    )
  }

  "buildOutputPortStatement method" should "create a view even when specific.schema is not specified" in {
    val yaml       = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_4.yml")
    val descriptor = ProvisioningRequestDescriptor(yaml)

    queryHelper.buildOutputPortStatement(descriptor.toOption.get, CREATE_VIEW).toOption
      .getOrElse("Cannot Create View") should be(
      "CREATE VIEW IF NOT EXISTS TEST_AIRBYTE.DPOWNERTEST_1.snowflake_view AS (SELECT id,\n" + "name,\n" +
        "phone FROM TEST_AIRBYTE.DPOWNERTEST_1.snowflake_table);"
    )
  }

  "buildOutputPortStatement method" should "create a view even when schema and database are not specified" in {
    val yaml       = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_5.yml")
    val descriptor = ProvisioningRequestDescriptor(yaml)

    queryHelper.buildOutputPortStatement(descriptor.toOption.get, CREATE_VIEW).toOption
      .getOrElse("Cannot Create View") should be(
      "CREATE VIEW IF NOT EXISTS MARKETING.DPOWNERTEST_1.snowflake_view AS (SELECT id,\n" + "name,\n" +
        "phone FROM MARKETING.DPOWNERTEST_1.snowflake_table);"
    )
  }

  "buildOutputPortStatement method" should "create a view using the specified custom sql" in {
    val yaml       = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_5_custom_view.yml")
    val descriptor = ProvisioningRequestDescriptor(yaml)

    queryHelper.buildOutputPortStatement(descriptor.toOption.get, CREATE_VIEW).toOption
      .getOrElse("Cannot Create View") should be(
      "CREATE VIEW IF NOT EXISTS TEST_AIRBYTE.PUBLIC.snowflake_view AS (SELECT * FROM TEST_AIRBYTE.PUBLIC.snowflake_table);"
    )
  }

  "buildStorageStatement method" should
    "correctly retrieve fields from descriptor and build the create database statement" in {
      val yaml       = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_6.yml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      queryHelper.buildStorageStatement(descriptor.toOption.get, CREATE_DB).toOption
        .getOrElse("Cannot Create Database") should be("CREATE DATABASE IF NOT EXISTS TEST_AIRBYTE;")
    }

  "buildStorageStatement method" should
    "correctly retrieve fields from descriptor and build the create schema statement" in {
      val yaml       = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_6.yml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      queryHelper.buildStorageStatement(descriptor.toOption.get, CREATE_SCHEMA).toOption
        .getOrElse("Cannot Create Schema") should be("CREATE SCHEMA IF NOT EXISTS TEST_AIRBYTE.PUBLIC;")
    }

  "buildStorageStatement method" should
    "correctly build the create database statement when specific.database is not specified" in {
      val yaml       = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_6_no_optional.yml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      queryHelper.buildStorageStatement(descriptor.toOption.get, CREATE_DB).toOption
        .getOrElse("Cannot Create Database") should be("CREATE DATABASE IF NOT EXISTS MARKETING;")
    }

  "buildStorageStatement method" should
    "correctly build the create schema statement when optional fields are not specified" in {
      val yaml       = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_6_no_optional.yml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      queryHelper.buildStorageStatement(descriptor.toOption.get, CREATE_SCHEMA).toOption
        .getOrElse("Cannot Create Schema") should be("CREATE SCHEMA IF NOT EXISTS MARKETING.DPOWNERTEST_1;")
    }

  "buildStorageStatement method" should
    "correctly build the create tables statement when optional fields are not specified" in {
      val yaml       = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_6_no_optional.yml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      queryHelper.buildMultipleStatement(descriptor.toOption.get, CREATE_TABLES).toOption
        .getOrElse("Cannot Create Tables") should be(List(
        "CREATE TABLE IF NOT EXISTS MARKETING.DPOWNERTEST_1.TABLE1 (id TEXT PRIMARY KEY,\n" + "name TEXT,\n" +
          "phone NUMBER NULL);",
        "CREATE TABLE IF NOT EXISTS MARKETING.DPOWNERTEST_1.TABLE2 (id TEXT PRIMARY KEY,\n" + "name TEXT,\n" +
          "phone NUMBER NULL);"
      ))
    }

  "buildStorageStatement method" should
    "correctly retrieve fields from descriptor and build the delete schema statement" in {
      val yaml       = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_6.yml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      queryHelper.buildStorageStatement(descriptor.toOption.get, DELETE_SCHEMA).toOption
        .getOrElse("Cannot Delete Schema") should be("DROP SCHEMA IF EXISTS TEST_AIRBYTE.PUBLIC;")
    }

  "buildStorageStatement method" should
    "correctly build the delete tables statement when optional fields are not specified" in {
      val yaml       = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_6_no_optional.yml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      queryHelper.buildMultipleStatement(descriptor.toOption.get, DELETE_TABLES).toOption
        .getOrElse("Cannot Delete Tables") should be(List(
        "DROP TABLE IF EXISTS MARKETING.DPOWNERTEST_1.TABLE1;",
        "DROP TABLE IF EXISTS MARKETING.DPOWNERTEST_1.TABLE2;"
      ))
    }

}
