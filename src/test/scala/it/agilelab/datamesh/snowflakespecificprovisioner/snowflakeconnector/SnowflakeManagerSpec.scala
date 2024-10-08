package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import io.circe.{parser, Json}
import it.agilelab.datamesh.snowflakespecificprovisioner.common.test.getTestResourceAsString
import it.agilelab.datamesh.snowflakespecificprovisioner.model.{
  ProvisioningRequestDescriptor,
  ReverseProvisioningRequest,
  ReverseProvisioningStatusEnums
}
import it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper.SnowflakePrincipalsMapper
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.OperationType.{CREATE_TABLES, DELETE_TABLES}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.sql.Connection

class SnowflakeManagerSpec extends AnyFlatSpec with MockFactory with should.Matchers {

  val executorMock: QueryExecutor              = mock[QueryExecutor]
  val principalsMapper                         = new SnowflakePrincipalsMapper
  val queryBuilder                             = new QueryHelper
  val snowflakeTableInformationHelper          = new SnowflakeTableInformationHelper(queryBuilder)
  val reverseProvisioning: ReverseProvisioning = mock[ReverseProvisioning]

  val snowflakeManager = new SnowflakeManager(
    executorMock,
    queryBuilder,
    snowflakeTableInformationHelper,
    reverseProvisioning,
    principalsMapper
  )

  "update acl on an output port" should "return Right if all users are granted access correctly" in {

    val refs        = List("user:user1_agilelab.it", "user:user2_agilelab.it")
    val mappedUsers = principalsMapper.map(refs.toSet).values.partitionMap(identity)

    val yaml    = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_2.yml")
    val prDescr = ProvisioningRequestDescriptor(yaml).toOption.get

    val connectionMock = mock[Connection]
    val _              = (() => executorMock.getConnection).expects().returns(Right(connectionMock)).anyNumberOfTimes()
    val _ = (executorMock.executeStatement _).expects(connectionMock, *).returns(Right(1)).anyNumberOfTimes()
    val _ = (executorMock.executeMultipleStatements _).expects(connectionMock, *)
      .returns(List.fill(mappedUsers._2.size)(Right(1))).anyNumberOfTimes()

    val resUsers = snowflakeManager.executeUpdateAcl(prDescr, refs)
    resUsers shouldBe a[Right[_, _]]
    resUsers.toOption.get shouldEqual mappedUsers._2
  }

  "update acl on an output port" should "return Left if there is an invalid ref but only after granting access" in {

    val refs         = List("user:user1_agilelab.it", "owner:bigData", "user:user2_agilelab.it")
    val mappedGroups = principalsMapper.map(refs.toSet).values.partitionMap(identity)

    val yaml    = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_2.yml")
    val prDescr = ProvisioningRequestDescriptor(yaml).toOption.get

    val connectionMock = mock[Connection]
    val _              = (() => executorMock.getConnection).expects().returns(Right(connectionMock)).anyNumberOfTimes()
    val _ = (executorMock.executeStatement _).expects(connectionMock, *).returns(Right(1)).anyNumberOfTimes()
    val _ = (executorMock.executeMultipleStatements _).expects(connectionMock, *)
      .returns(List.fill(mappedGroups._2.size)(Right(1))).anyNumberOfTimes()

    val resGroups = snowflakeManager.executeUpdateAcl(prDescr, refs)
    resGroups shouldBe a[Left[_, _]]
    resGroups.left.foreach(err => err.problems should contain("Unexpected subject in user principal mapping"))
  }

  "update acl on an output port" should "return Left if there is a wrong user to grant access" in {

    val refs = List("user:user1_agilelab.it", "user:no.user_agilelab.it")

    val yaml    = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_2.yml")
    val prDescr = ProvisioningRequestDescriptor(yaml).toOption.get

    val wrongResult = List(Right(1), Left(ExecuteStatementError()))

    val connectionMock = mock[Connection]
    val _              = (() => executorMock.getConnection).expects().returns(Right(connectionMock)).anyNumberOfTimes()
    val _ = (executorMock.executeStatement _).expects(connectionMock, *).returns(Right(1)).anyNumberOfTimes()
    val _ = (executorMock.executeMultipleStatements _).expects(connectionMock, *).returns(wrongResult)
      .anyNumberOfTimes()

    val resUsers = snowflakeManager.executeUpdateAcl(prDescr, refs)
    resUsers shouldBe a[Left[_, _]]
    resUsers.left.foreach(err => err shouldBe a[ExecuteStatementError])

  }

  "merge sequence" should "return Right of the list if all results are Right" in {
    import SnowflakeManagerImplicits._
    val list: Seq[Either[ExecuteStatementError, Int]] = List(Right(1), Right(2), Right(3))
    val expectedList                                  = List(1, 2, 3)

    list.mergeSequence() shouldEqual Right(expectedList)
  }

  it should "return Left if there are execute statement errors with the errors combined" in {
    import SnowflakeManagerImplicits._
    val list: Seq[Either[ExecuteStatementError, Int]] = List(
      Left(ExecuteStatementError(None, List("error1", "error2"), List("sol1", "sol2"))),
      Right(2),
      Left(ExecuteStatementError(None, List("error3", "error4"), List("sol3", "sol4")))
    )
    val expectedError                                 =
      ExecuteStatementError(None, List("error1", "error2", "error3", "error4"), List("sol1", "sol2", "sol3", "sol4"))

    list.mergeSequence() shouldEqual Left(expectedError)
  }

  it should "successfully validate a correct descriptor" in {

    val yaml = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_6.yml")
    val prd  = ProvisioningRequestDescriptor(yaml).toOption.get

    val snowflakeManager = new SnowflakeManager(
      new SnowflakeExecutor,
      queryBuilder,
      snowflakeTableInformationHelper,
      reverseProvisioning,
      principalsMapper
    )
    val res              = snowflakeManager.validateDescriptor(prd)

    res.isRight shouldBe true

  }

  it should "fail to validate a descriptor with mismatching schemas" in {

    val yaml = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_7_custom_view_wrong.yml")
    val prd  = ProvisioningRequestDescriptor(yaml).toOption.get

    val snowflakeManager = new SnowflakeManager(
      new SnowflakeExecutor,
      queryBuilder,
      snowflakeTableInformationHelper,
      reverseProvisioning,
      principalsMapper
    )
    val res              = snowflakeManager.validateDescriptor(prd)

    res.isRight shouldBe false
    res.left.foreach(value => value.problems.head should include("jhon").and(include("snowflake_view")))

  }

  it should "fail to validate a descriptor without a specified custom view" in {

    val yaml = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_5.yml")
    val prd  = ProvisioningRequestDescriptor(yaml).toOption.get

    val snowflakeManager = new SnowflakeManager(
      new SnowflakeExecutor,
      queryBuilder,
      snowflakeTableInformationHelper,
      reverseProvisioning,
      principalsMapper
    )
    val res              = snowflakeManager.validateDescriptor(prd)

    res.isRight shouldBe true

  }

  it should "fail to validate if the descriptor contains a wrong view name in the customView" in {

    val yaml             = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_8_view_wrong.yml")
    val prd              = ProvisioningRequestDescriptor(yaml).toOption.get
    val snowflakeManager = new SnowflakeManager(
      new SnowflakeExecutor,
      queryBuilder,
      snowflakeTableInformationHelper,
      reverseProvisioning,
      principalsMapper
    )
    val res              = snowflakeManager.validateDescriptor(prd)

    res.isLeft shouldBe true
    res.left.foreach(value => value.problems.head should include("Error while retrieving the view name"))

  }

  it should "successfully validate a descriptor with matching views" in {

    val yaml             = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_5_custom_view.yml")
    val prd              = ProvisioningRequestDescriptor(yaml).toOption.get
    val snowflakeManager = new SnowflakeManager(
      new SnowflakeExecutor,
      queryBuilder,
      snowflakeTableInformationHelper,
      reverseProvisioning,
      principalsMapper
    )
    val res              = snowflakeManager.validateDescriptor(prd)

    res.isRight shouldBe true

  }

  it should "fail to validate an outputport descriptor with empty schema" in {

    val yaml             = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_11_empty_schema_array.yml")
    val prd              = ProvisioningRequestDescriptor(yaml).toOption.get
    val snowflakeManager = new SnowflakeManager(
      new SnowflakeExecutor,
      queryBuilder,
      snowflakeTableInformationHelper,
      reverseProvisioning,
      principalsMapper
    )

    val res1 = snowflakeManager.validateDescriptor(prd)
    res1.isRight shouldBe true

    val res2 = snowflakeManager.validateSpecificFields(prd)

    res2.isLeft shouldBe true
    res2.left.foreach(value => value.problems.head should include("Data Contract schema is empty"))
  }

  it should "fail to validate a storage descriptor with empty tables" in {

    val yaml             = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_10_empty_tables_list.yml")
    val prd              = ProvisioningRequestDescriptor(yaml).toOption.get
    val snowflakeManager = new SnowflakeManager(
      new SnowflakeExecutor,
      queryBuilder,
      snowflakeTableInformationHelper,
      reverseProvisioning,
      principalsMapper
    )

    val res2 = snowflakeManager.validateSpecificFields(prd)

    res2.isLeft shouldBe true
    res2.left.foreach(value =>
      value.problems.head should include("The provided request does not contain tables since it is empty")
    )
  }

  it should "fail to validate a storage descriptor with different component kind" in {

    val yaml             = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_11_different_kind.yml")
    val prd              = ProvisioningRequestDescriptor(yaml).toOption.get
    val snowflakeManager = new SnowflakeManager(
      new SnowflakeExecutor,
      queryBuilder,
      snowflakeTableInformationHelper,
      reverseProvisioning,
      principalsMapper
    )

    val res2 = snowflakeManager.validateSpecificFields(prd)

    res2.isLeft shouldBe true
    res2.left.foreach(value => value.problems.head should include("The specified kind workload is not supported"))
  }

  it should "successfully retrieve information schema of the tables from Snowflake" in {

    val yaml             = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_9_with_tags.yml")
    val prd              = ProvisioningRequestDescriptor(yaml).toOption.get
    val queryBuilder     = new QueryHelper
    val executor         = new SnowflakeExecutor
    val snowflakeManager = new SnowflakeManager(
      executor,
      queryBuilder,
      snowflakeTableInformationHelper,
      reverseProvisioning,
      principalsMapper
    )

    val res = for {
      connection          <- executor.getConnection
      deleteStatement     <- queryBuilder.buildMultipleStatement(prd, DELETE_TABLES, None, None)
      _                   <- deleteStatement match {
        case statement if statement.nonEmpty => executor.traverseMultipleStatements(connection, deleteStatement)
        case _ => Right(GetComponentError("Skipping delete tables - no information provided"))
      }
      createStatement     <- queryBuilder.buildMultipleStatement(prd, CREATE_TABLES, None, None)
      _                   <- createStatement match {
        case statement if statement.nonEmpty => executor.traverseMultipleStatements(connection, createStatement)
        case _ => Right(GetComponentError("Skipping create tables - no information provided"))
      }
      existingTableSchema <- snowflakeManager.getExistingTableSchema(connection, prd)
    } yield existingTableSchema
    res match { case _ => res.isRight shouldBe true }
  }

  "executeReverseProvisioning" should "return a Right in case of OutputPort if ReverseProvisioning returned Right" in {
    val params = s"""
                    |{
                    |"database": "TESTDB",
                    |"schema": "PUBLIC",
                    |"viewName": "TABLE4"
                    |}
                    |""".stripMargin

    val reverseProvisioningRequest = ReverseProvisioningRequest(
      useCaseTemplateId = "urn:dmb:utm:snowflake-outputport-template:0.0.0",
      environment = "development",
      params = Some(params)
    )

    val mockQueryExecutor   = mock[QueryExecutor]
    val principalsMapper    = mock[SnowflakePrincipalsMapper]
    val reverseProvisioning = mock[ReverseProvisioning]

    val snowflakeManager = new SnowflakeManager(
      mockQueryExecutor,
      queryBuilder,
      snowflakeTableInformationHelper,
      reverseProvisioning,
      principalsMapper
    )

    val response = parser.parse(s""" {
                                   | "spec.mesh.dataContract.schema": {},
                                   | "spec.mesh.tags": {}
                                   | }""".stripMargin).toOption.get

    val _ = (reverseProvisioning.executeReverseProvisioningOutputPort(_: Json))
      .expects(parser.parse(params).toOption.get).returns(Right(Some(response)))

    val result = snowflakeManager.executeReverseProvisioning(reverseProvisioningRequest)

    result shouldBe a[Right[_, _]]
    result.foreach { status =>
      status shouldNot be(None)
      status.get.status shouldEqual ReverseProvisioningStatusEnums.StatusEnum.COMPLETED
      status.get.updates shouldEqual response
    }
  }

  "executeReverseProvisioning" should "return a Left in case of OutputPort if ReverseProvisioning returned Right" in {
    val params = s"""
                    |{
                    |"database": "TESTDB",
                    |"schema": "PUBLIC",
                    |"viewName": "TABLE4"
                    |}
                    |""".stripMargin

    val reverseProvisioningRequest = ReverseProvisioningRequest(
      useCaseTemplateId = "urn:dmb:utm:snowflake-outputport-template:0.0.0",
      environment = "development",
      params = Some(params)
    )

    val mockQueryExecutor   = mock[QueryExecutor]
    val principalsMapper    = mock[SnowflakePrincipalsMapper]
    val reverseProvisioning = mock[ReverseProvisioning]

    val snowflakeManager = new SnowflakeManager(
      mockQueryExecutor,
      queryBuilder,
      snowflakeTableInformationHelper,
      reverseProvisioning,
      principalsMapper
    )

    val error = ExecuteStatementError(Some("SELECT * FROM TABLE"), List("Error"))
    val _     = (reverseProvisioning.executeReverseProvisioningOutputPort(_: Json))
      .expects(parser.parse(params).toOption.get).returns(Left(error))

    val result = snowflakeManager.executeReverseProvisioning(reverseProvisioningRequest)

    result shouldBe a[Left[_, _]]
    result.left.foreach(actual => actual shouldEqual error)
  }

  "executeReverseProvisioning" should "return a Right in case of StorageArea if ReverseProvisioning returned Right" in {
    val params = s"""
                    |{
                    |"database": "TESTDB",
                    |"schema": "PUBLIC",
                    |"tables": ["TABLE1", "TABLE4"]
                    |}
                    |""".stripMargin

    val reverseProvisioningRequest = ReverseProvisioningRequest(
      useCaseTemplateId = "urn:dmb:utm:snowflake-storage-template:0.0.0",
      environment = "development",
      params = Some(params)
    )

    val mockQueryExecutor   = mock[QueryExecutor]
    val principalsMapper    = mock[SnowflakePrincipalsMapper]
    val reverseProvisioning = mock[ReverseProvisioning]

    val snowflakeManager = new SnowflakeManager(
      mockQueryExecutor,
      queryBuilder,
      snowflakeTableInformationHelper,
      reverseProvisioning,
      principalsMapper
    )

    val response = parser.parse(s""" {
                                   | "spec.mesh.specific.tables": {}
                                   | }""".stripMargin).toOption.get

    val _ = (reverseProvisioning.executeReverseProvisioningStorageArea(_: Json))
      .expects(parser.parse(params).toOption.get).returns(Right(Some(response)))

    val result = snowflakeManager.executeReverseProvisioning(reverseProvisioningRequest)

    result shouldBe a[Right[_, _]]
    result.foreach { status =>
      status shouldNot be(None)
      status.get.status shouldEqual ReverseProvisioningStatusEnums.StatusEnum.COMPLETED
      status.get.updates shouldEqual response
    }
  }

  "executeReverseProvisioning" should "return a Left in case of StorageArea if ReverseProvisioning returned Right" in {
    val params = s"""
                    |{
                    |"database": "TESTDB",
                    |"schema": "PUBLIC",
                    |"tables": ["TABLE1", "TABLE4"]
                    |}
                    |""".stripMargin

    val reverseProvisioningRequest = ReverseProvisioningRequest(
      useCaseTemplateId = "urn:dmb:utm:snowflake-storage-template:0.0.0",
      environment = "development",
      params = Some(params)
    )

    val mockQueryExecutor   = mock[QueryExecutor]
    val principalsMapper    = mock[SnowflakePrincipalsMapper]
    val reverseProvisioning = mock[ReverseProvisioning]

    val snowflakeManager = new SnowflakeManager(
      mockQueryExecutor,
      queryBuilder,
      snowflakeTableInformationHelper,
      reverseProvisioning,
      principalsMapper
    )

    val error = ExecuteStatementError(Some("SELECT * FROM TABLE"), List("Error"))
    val _     = (reverseProvisioning.executeReverseProvisioningStorageArea(_: Json))
      .expects(parser.parse(params).toOption.get).returns(Left(error))

    val result = snowflakeManager.executeReverseProvisioning(reverseProvisioningRequest)

    result shouldBe a[Left[_, _]]
    result.left.foreach(actual => actual shouldEqual error)
  }

  "executeReverseProvisioning" should "return a Left in case of wrong useCaseTemplateId" in {

    val reverseProvisioningRequest = ReverseProvisioningRequest(
      useCaseTemplateId = "urn:dmb:utm:dbt-workload-template:0.0.0",
      environment = "development",
      params = Some(s"""
                       |{
                       |"database": "TESTDB",
                       |"schema": "PUBLIC",
                       |"tables": ["TABLE1", "TABLE4"]
                       |}
                       |""".stripMargin)
    )

    val result = snowflakeManager.executeReverseProvisioning(reverseProvisioningRequest)

    result shouldBe a[Left[_, _]]
    result.left.foreach { err =>
      err.problems should
        contain("Unsupported useCaseTemplateId. Please verify whether the templateId provided is appropriate.")
    }

  }

}
