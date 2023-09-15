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
      ColumnSchemaSpec("name", DataType.TEXT),
      ColumnSchemaSpec("phone_number", DataType.TEXT),
      ColumnSchemaSpec("id", DataType.TEXT),
      ColumnSchemaSpec("age", DataType.NUMBER)
    )

    queryHelper.createTableStatement("my-test", "your", "test_table", cols) should be(
      "CREATE TABLE IF NOT EXISTS my-test.your.TEST_TABLE\n(\n name TEXT,\nphone_number TEXT,\nid TEXT,\nage NUMBER\n);"
    )
  }

  "the createViewStatement method" should "format correctly the create view statement" in {
    val cols = List(
      ColumnSchemaSpec("name", DataType.TEXT),
      ColumnSchemaSpec("phone_number", DataType.TEXT),
      ColumnSchemaSpec("id", DataType.TEXT),
      ColumnSchemaSpec("age", DataType.NUMBER)
    )

    queryHelper.createViewStatement("my-test-view", "my-test", "your", "test_table", cols) should be(
      "CREATE VIEW IF NOT EXISTS my-test.your.my-test-view AS (SELECT name,\nphone_number,\nid,\nage FROM my-test.your.test_table);"
    )
  }

  "buildOutputPortStatement method" should
    "correctly retrieve fields from descriptor and build the create view statement" in {
      val yaml       = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_2.yml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      val res = queryHelper.buildOutputPortStatement(descriptor.toOption.get, CREATE_VIEW)

      res shouldBe a[Right[_, _]]
      res.foreach(script =>
        script should be(
          "CREATE VIEW IF NOT EXISTS TEST_AIRBYTE.PUBLIC.snowflake_view AS (SELECT id,\nname,\nphone FROM TEST_AIRBYTE.PUBLIC.snowflake_table);"
        )
      )

    }

  "buildOutputPortStatement method" should "create a view even when database is not specified" in {
    val yaml       = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_3.yml")
    val descriptor = ProvisioningRequestDescriptor(yaml)

    val res = queryHelper.buildOutputPortStatement(descriptor.toOption.get, CREATE_VIEW)

    res shouldBe a[Right[_, _]]
    res.foreach(script =>
      script should be(
        "CREATE VIEW IF NOT EXISTS MARKETING.PUBLIC.snowflake_view AS (SELECT id,\nname,\nphone FROM MARKETING.PUBLIC.snowflake_table);"
      )
    )
  }

  "buildOutputPortStatement method" should "create a view even when specific.schema is not specified" in {
    val yaml       = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_4.yml")
    val descriptor = ProvisioningRequestDescriptor(yaml)

    val res = queryHelper.buildOutputPortStatement(descriptor.toOption.get, CREATE_VIEW)

    res shouldBe a[Right[_, _]]
    res.foreach(script =>
      script should be(
        "CREATE VIEW IF NOT EXISTS TEST_AIRBYTE.DPOWNERTEST_1.snowflake_view AS (SELECT id,\n" + "name,\n" +
          "phone FROM TEST_AIRBYTE.DPOWNERTEST_1.snowflake_table);"
      )
    )
  }

  "buildOutputPortStatement method" should "create a view even when schema and database are not specified" in {
    val yaml       = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_5.yml")
    val descriptor = ProvisioningRequestDescriptor(yaml)

    val res = queryHelper.buildOutputPortStatement(descriptor.toOption.get, CREATE_VIEW)

    res shouldBe a[Right[_, _]]
    res.foreach(script =>
      script should be(
        "CREATE VIEW IF NOT EXISTS MARKETING.DPOWNERTEST_1.snowflake_view AS (SELECT id,\n" + "name,\n" +
          "phone FROM MARKETING.DPOWNERTEST_1.snowflake_table);"
      )
    )
  }

  "buildOutputPortStatement method" should "create a view using the specified custom sql" in {
    val yaml       = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_5_custom_view.yml")
    val descriptor = ProvisioningRequestDescriptor(yaml)

    val res = queryHelper.buildOutputPortStatement(descriptor.toOption.get, CREATE_VIEW)

    res shouldBe a[Right[_, _]]
    res.foreach(script =>
      script should be(
        "CREATE VIEW IF NOT EXISTS TEST_AIRBYTE.PUBLIC.snowflake_view AS (SELECT * FROM TEST_AIRBYTE.PUBLIC.snowflake_table);"
      )
    )
  }

  "buildStorageStatement method" should
    "correctly retrieve fields from descriptor and build the create database statement" in {
      val yaml       = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_6.yml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      val res = queryHelper.buildStorageStatement(descriptor.toOption.get, CREATE_DB)

      res shouldBe a[Right[_, _]]
      res.foreach(script => script should be("CREATE DATABASE IF NOT EXISTS TEST_AIRBYTE;"))
    }

  "buildStorageStatement method" should
    "correctly retrieve fields from descriptor and build the create schema statement" in {
      val yaml       = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_6.yml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      val res = queryHelper.buildStorageStatement(descriptor.toOption.get, CREATE_SCHEMA)

      res shouldBe a[Right[_, _]]
      res.foreach(script => script should be("CREATE SCHEMA IF NOT EXISTS TEST_AIRBYTE.PUBLIC;"))
    }

  "buildStorageStatement method" should
    "correctly build the create database statement when specific.database is not specified" in {
      val yaml       = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_6_no_optional.yml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      val res = queryHelper.buildStorageStatement(descriptor.toOption.get, CREATE_DB)

      res shouldBe a[Right[_, _]]
      res.foreach(script => script should be("CREATE DATABASE IF NOT EXISTS MARKETING;"))
    }

  "buildStorageStatement method" should
    "correctly build the create schema statement when optional fields are not specified" in {
      val yaml       = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_6_no_optional.yml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      val res = queryHelper.buildStorageStatement(descriptor.toOption.get, CREATE_SCHEMA)

      res shouldBe a[Right[_, _]]
      res.foreach(script => script should be("CREATE SCHEMA IF NOT EXISTS MARKETING.DPOWNERTEST_1;"))
    }

  "buildStorageStatement method" should
    "correctly build the create tables statement when optional fields are not specified" in {
      val yaml       = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_6_no_optional.yml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      val res = queryHelper.buildMultipleStatement(descriptor.toOption.get, CREATE_TABLES)

      res shouldBe a[Right[_, _]]
      res.foreach(script =>
        script should be(List(
          "CREATE TABLE IF NOT EXISTS MARKETING.DPOWNERTEST_1.TABLE1\n(\n id TEXT,\nname TEXT,\nphone NUMBER,\nCONSTRAINT table1_primary_key PRIMARY KEY (id)\n);",
          "CREATE TABLE IF NOT EXISTS MARKETING.DPOWNERTEST_1.TABLE2\n(\n id TEXT,\nname TEXT NOT NULL,\nphone NUMBER UNIQUE,\nCONSTRAINT table2_primary_key PRIMARY KEY (id)\n);"
        ))
      )
    }

  "buildStorageStatement method" should
    "correctly retrieve fields from descriptor and build the delete schema statement" in {
      val yaml       = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_6.yml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      val res = queryHelper.buildStorageStatement(descriptor.toOption.get, DELETE_SCHEMA)

      res shouldBe a[Right[_, _]]
      res.foreach(script => script should be("DROP SCHEMA IF EXISTS TEST_AIRBYTE.PUBLIC;"))
    }

  "buildStorageStatement method" should
    "correctly build the delete tables statement when optional fields are not specified" in {
      val yaml       = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_6_no_optional.yml")
      val descriptor = ProvisioningRequestDescriptor(yaml)

      val res = queryHelper.buildMultipleStatement(descriptor.toOption.get, DELETE_TABLES)

      res shouldBe a[Right[_, _]]
      res.foreach(script =>
        script should be(List(
          "DROP TABLE IF EXISTS MARKETING.DPOWNERTEST_1.TABLE1;",
          "DROP TABLE IF EXISTS MARKETING.DPOWNERTEST_1.TABLE2;"
        ))
      )
    }

  "primaryKeyConstraintStatement" should "correctly build a primary key constraint" in {
    val cols      = List(
      ColumnSchemaSpec("id", DataType.TEXT, Some(ConstraintType.PRIMARY_KEY)),
      ColumnSchemaSpec("cf", DataType.TEXT, Some(ConstraintType.PRIMARY_KEY)),
      ColumnSchemaSpec("name", DataType.TEXT),
      ColumnSchemaSpec("age", DataType.NUMBER)
    )
    val tableName = "my_test_table"
    val res       = queryHelper.primaryKeyConstraintStatement(tableName, cols)

    res.foreach(constraint => constraint should be("CONSTRAINT my_test_table_primary_key PRIMARY KEY (id,cf)"))
  }

  "getCustomDatabaseName method" should "correctly return the custom database name" in {
    val customViewStatement = "CREATE VIEW IF NOT EXISTS myDb.mySchema.myView AS ..."
    val customDatabaseName  = queryHelper.getCustomDatabaseName(customViewStatement)

    customDatabaseName should be(Some("myDb"))
  }

  "getCustomSchemaName method" should "correctly return the custom schema name" in {
    val customViewStatement = "CREATE VIEW myDb.mySchema.myView AS ..."
    val customSchemaName    = queryHelper.getCustomSchemaName(customViewStatement)

    customSchemaName should be(Some("mySchema"))
  }

  "getCustomViewDetails method" should "return an empty map in case of invalid custom view query" in {
    val customViewStatement = "CREATE VIEW incompleteViewStatement AS ..."
    val customViewDetails   = queryHelper.getCustomViewDetails(customViewStatement)

    customViewDetails.size should be(0)
  }

  "getCustomDatabaseName method" should "return None in case of invalid custom view query" in {
    val customViewStatement = "CREATE VIEW incompleteViewStatement AS ..."
    val customDatabaseName  = queryHelper.getCustomDatabaseName(customViewStatement)

    customDatabaseName should be(None)
  }

  "getCustomViewName method" should "return None in case of invalid custom view query" in {
    val customViewStatement = "CREATE VIEW incompleteViewStatement AS ..."
    val customViewName      = queryHelper.getCustomViewName(customViewStatement)

    customViewName should be(None)
  }

  "mapUserToSnowflakeUser method" should "return the correct Snowflake user. Use case 1" in {
    val user          = "user:marco.pisasale_agilelab.it"
    val snowflakeUser = queryHelper.mapUserToSnowflakeUser(user)

    snowflakeUser should be("MARCO.PISASALE@AGILELAB.IT")
  }

  "mapUserToSnowflakeUser method" should "return the correct Snowflake user. Use case 2" in {
    val user          = "marco.pisasale@agilelab.it"
    val snowflakeUser = queryHelper.mapUserToSnowflakeUser(user)

    snowflakeUser should be("MARCO.PISASALE@AGILELAB.IT")
  }

  "mapUserToSnowflakeUser method" should "return the correct Snowflake user. Use case 3" in {
    val user          = "user:marco_pisasale_agilelab.it"
    val snowflakeUser = queryHelper.mapUserToSnowflakeUser(user)

    snowflakeUser should be("MARCO_PISASALE@AGILELAB.IT")
  }

}
