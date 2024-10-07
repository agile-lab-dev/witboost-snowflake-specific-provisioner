package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import io.circe.{parser, Json}
import io.circe.syntax.EncoderOps
import it.agilelab.datamesh.snowflakespecificprovisioner.model.TagInfo
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.{ColumnSchemaSpec, ConstraintType, DataType, TableSpec}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.sql.Connection

class ReverseProvisioningSpec extends AnyFlatSpec with MockFactory with should.Matchers {

  "reverse provision on output port" should "return Right if snowflake queries went ok" in {
    val params     = s"""
                    |{
                    |"database": "TESTDB",
                    |"schema": "PUBLIC",
                    |"viewName": "TABLE4"
                    |}
                    |""".stripMargin
    val jsonParams = parser.parse(params).toOption.get
    val table      = TableSpec(
      "TABLE4",
      List(ColumnSchemaSpec("id", DataType.TEXT, None, None), ColumnSchemaSpec("name", DataType.TEXT, None, None)),
      Some(List(Map("tagName" -> "tagValue")))
    )

    val executorMock                    = mock[QueryExecutor]
    val snowflakeTableInformationHelper = mock[SnowflakeTableInformationHelper]
    val reverseProvisioning             = new ReverseProvisioning(executorMock, snowflakeTableInformationHelper)
    val connectionMock                  = mock[Connection]
    val _                               = (() => executorMock.getConnection).expects().returns(Right(connectionMock))

    val _ = (snowflakeTableInformationHelper.getExistingMetaData _)
      .expects(connectionMock, "TESTDB", "PUBLIC", List("TABLE4")).returns(Right(List(table)))
    val _ = (snowflakeTableInformationHelper.getViewExistingTags _)
      .expects(connectionMock, "TESTDB", "PUBLIC", "TABLE4").returns(Right(List(table)))

    val result = reverseProvisioning.executeReverseProvisioningOutputPort(jsonParams)

    val expectedTags = List(TagInfo("tagName" -> "tagValue")).asJson

    result shouldBe a[Right[_, _]]
    result.foreach { maybeJson =>
      maybeJson shouldNot be(None)
      maybeJson.get shouldEqual
        Json.obj("spec.mesh.dataContract.schema" -> table.schema.asJson, "spec.mesh.tags" -> expectedTags)
    }
  }

  it should "return Left if parse error happened" in {
    val params     = s"""
                    |{
                    |"database": "",
                    |"schema": "PUBLIC",
                    |"viewName": "TABLE4"
                    |}
                    |""".stripMargin
    val jsonParams = parser.parse(params).toOption.get

    val executorMock                    = mock[QueryExecutor]
    val snowflakeTableInformationHelper = mock[SnowflakeTableInformationHelper]
    val reverseProvisioning             = new ReverseProvisioning(executorMock, snowflakeTableInformationHelper)

    val result = reverseProvisioning.executeReverseProvisioningOutputPort(jsonParams)

    result shouldBe a[Left[_, _]]
    result.left.foreach { error =>
      error shouldBe a[ParseError]
      error.problems should contain("Database was not provided, but is required")
    }
  }

  it should "return Left if query error happened" in {
    val params     = s"""
                    |{
                    |"database": "TESTDB",
                    |"schema": "PUBLIC",
                    |"viewName": "TABLE4"
                    |}
                    |""".stripMargin
    val jsonParams = parser.parse(params).toOption.get

    val expectedError = ExecuteStatementError(Some("SELECT * FROM METADATTABLE"), List("Error!"))

    val executorMock                    = mock[QueryExecutor]
    val snowflakeTableInformationHelper = mock[SnowflakeTableInformationHelper]
    val reverseProvisioning             = new ReverseProvisioning(executorMock, snowflakeTableInformationHelper)
    val connectionMock                  = mock[Connection]
    val _                               = (() => executorMock.getConnection).expects().returns(Right(connectionMock))
    val _                               = (snowflakeTableInformationHelper.getExistingMetaData _)
      .expects(connectionMock, "TESTDB", "PUBLIC", List("TABLE4")).returns(Left(expectedError))

    val result = reverseProvisioning.executeReverseProvisioningOutputPort(jsonParams)

    result shouldBe a[Left[_, _]]
    result.left.foreach(actualError => actualError shouldEqual expectedError)
  }

  "reverse provision on storage area" should "return Right if snowflake queries went ok" in {
    val params     = s"""
                    |{
                    |"database": "TESTDB",
                    |"schema": "PUBLIC",
                    |"tables": ["TABLE1", "TABLE4"]
                    |}
                    |""".stripMargin
    val jsonParams = parser.parse(params).toOption.get
    val table1     = TableSpec(
      "TABLE1",
      List(
        ColumnSchemaSpec("id", DataType.NUMBER, Some(ConstraintType.PRIMARY_KEY), None),
        ColumnSchemaSpec("name", DataType.TEXT, None, None)
      ),
      None
    )
    val table4     = TableSpec(
      "TABLE4",
      List(
        ColumnSchemaSpec("id", DataType.NUMBER, Some(ConstraintType.PRIMARY_KEY), None),
        ColumnSchemaSpec("name", DataType.TEXT, None, None)
      ),
      None
    )

    val executorMock                    = mock[QueryExecutor]
    val snowflakeTableInformationHelper = mock[SnowflakeTableInformationHelper]
    val reverseProvisioning             = new ReverseProvisioning(executorMock, snowflakeTableInformationHelper)
    val connectionMock                  = mock[Connection]
    val _                               = (() => executorMock.getConnection).expects().returns(Right(connectionMock))

    val _ = (snowflakeTableInformationHelper.getExistingMetaData _)
      .expects(connectionMock, "TESTDB", "PUBLIC", List("TABLE1", "TABLE4")).returns(Right(List(table1, table4)))

    val result = reverseProvisioning.executeReverseProvisioningStorageArea(jsonParams)

    result shouldBe a[Right[_, _]]
    result.foreach { maybeJson =>
      maybeJson shouldNot be(None)
      maybeJson.get shouldEqual Json.obj(("spec.mesh.specific.tables", List(table1, table4).asJson))
    }

  }

  it should "return Left if parse error happened" in {
    val params     = s"""
                    |{
                    |"database": "TESTDB",
                    |"schema": "PUBLIC",
                    |"tables": []
                    |}
                    |""".stripMargin
    val jsonParams = parser.parse(params).toOption.get

    val executorMock                    = mock[QueryExecutor]
    val snowflakeTableInformationHelper = mock[SnowflakeTableInformationHelper]
    val reverseProvisioning             = new ReverseProvisioning(executorMock, snowflakeTableInformationHelper)

    val result = reverseProvisioning.executeReverseProvisioningStorageArea(jsonParams)

    result shouldBe a[Left[_, _]]
    result.left.foreach { error =>
      error shouldBe a[ParseError]
      error.problems should contain("Table names were not provided, but are required")
    }
  }

  it should "return Left if query error happened" in {
    val params     = s"""
                    |{
                    |"database": "TESTDB",
                    |"schema": "PUBLIC",
                    |"tables": ["TABLE1", "TABLE4"]
                    |}
                    |""".stripMargin
    val jsonParams = parser.parse(params).toOption.get

    val expectedError = ExecuteStatementError(Some("SELECT * FROM METADATTABLE"), List("Error!"))

    val executorMock                    = mock[QueryExecutor]
    val snowflakeTableInformationHelper = mock[SnowflakeTableInformationHelper]
    val reverseProvisioning             = new ReverseProvisioning(executorMock, snowflakeTableInformationHelper)
    val connectionMock                  = mock[Connection]
    val _                               = (() => executorMock.getConnection).expects().returns(Right(connectionMock))
    val _                               = (snowflakeTableInformationHelper.getExistingMetaData _)
      .expects(connectionMock, "TESTDB", "PUBLIC", List("TABLE1", "TABLE4")).returns(Left(expectedError))

    val result = reverseProvisioning.executeReverseProvisioningStorageArea(jsonParams)

    result shouldBe a[Left[_, _]]
    result.left.foreach(actualError => actualError shouldEqual expectedError)
  }

}
