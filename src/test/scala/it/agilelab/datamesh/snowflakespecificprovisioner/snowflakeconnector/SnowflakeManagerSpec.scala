package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import it.agilelab.datamesh.snowflakespecificprovisioner.common.test.getTestResourceAsString
import it.agilelab.datamesh.snowflakespecificprovisioner.model.ProvisioningRequestDescriptor
import it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper.SnowflakePrincipalsMapper
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.OperationType.{CREATE_TABLES, DELETE_TABLES}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.sql.Connection

class SnowflakeManagerSpec extends AnyFlatSpec with MockFactory with should.Matchers {

  val executorMock     = mock[QueryExecutor]
  val principalsMapper = new SnowflakePrincipalsMapper
  val snowflakeManager = new SnowflakeManager(executorMock, principalsMapper)

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

    val refs         = List("user:user1_agilelab.it", "group:bigData", "user:user2_agilelab.it")
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
    resGroups.left.foreach { err =>
      err.problems should contain("Groups are not supported by Snowflake to grant roles")
    }
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

    val snowflakeManager = new SnowflakeManager(new SnowflakeExecutor, principalsMapper)
    val res              = snowflakeManager.validateDescriptor(prd)

    res.isRight shouldBe true

  }

  it should "fail to validate a descriptor with mismatching schemas" in {

    val yaml = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_7_custom_view_wrong.yml")
    val prd  = ProvisioningRequestDescriptor(yaml).toOption.get

    val snowflakeManager = new SnowflakeManager(new SnowflakeExecutor, principalsMapper)
    val res              = snowflakeManager.validateDescriptor(prd)

    res.isRight shouldBe false
    res.left.foreach(value => value.problems.head should (include("jhon").and(include("snowflake_view"))))

  }

  it should "fail to validate a descriptor without a specified custom view" in {

    val yaml = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_5.yml")
    val prd  = ProvisioningRequestDescriptor(yaml).toOption.get

    val snowflakeManager = new SnowflakeManager(new SnowflakeExecutor, principalsMapper)
    val res              = snowflakeManager.validateDescriptor(prd)

    res.isRight shouldBe true

  }

  it should "fail to validate if the descriptor contains a wrong view name in the customView" in {

    val yaml             = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_8_view_wrong.yml")
    val prd              = ProvisioningRequestDescriptor(yaml).toOption.get
    val snowflakeManager = new SnowflakeManager(new SnowflakeExecutor, principalsMapper)
    val res              = snowflakeManager.validateDescriptor(prd)

    res.isLeft shouldBe true
    res.left.foreach(value => value.problems.head should (include("Error while retrieving the view name")))

  }

  it should "successfully validate a descriptor with matching views" in {

    val yaml             = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_5_custom_view.yml")
    val prd              = ProvisioningRequestDescriptor(yaml).toOption.get
    val snowflakeManager = new SnowflakeManager(new SnowflakeExecutor, principalsMapper)
    val res              = snowflakeManager.validateDescriptor(prd)

    res.isRight shouldBe true

  }

  it should "fail to validate an outputport descriptor without dataContract field" in {

    val yaml             = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_9_no_data_contract.yml")
    val prd              = ProvisioningRequestDescriptor(yaml).toOption.get
    val snowflakeManager = new SnowflakeManager(new SnowflakeExecutor, principalsMapper)

    val res1 = snowflakeManager.validateDescriptor(prd)
    res1.isRight shouldBe true

    val res2 = snowflakeManager.validateSpecificFields(prd)

    res2.isLeft shouldBe true
    res2.left.foreach(value =>
      value.problems.head should (include("Attempt to decode value on failed cursor").and(include("dataContract")))
    )
  }

  it should "fail to validate a storage descriptor without tables field" in {

    val yaml             = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_7_no_tables.yml")
    val prd              = ProvisioningRequestDescriptor(yaml).toOption.get
    val snowflakeManager = new SnowflakeManager(new SnowflakeExecutor, principalsMapper)

    val res1 = snowflakeManager.validateDescriptor(prd)
    res1.isRight shouldBe true

    val res2 = snowflakeManager.validateSpecificFields(prd)

    res2.isLeft shouldBe true
    res2.left.foreach(value =>
      value.problems.head should (include("Attempt to decode value on failed cursor").and(include("tables")))
    )
  }

  it should "fail to validate a descriptor with a wrong kind" in {

    val yaml             = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_8_wrong_kind.yml")
    val prd              = ProvisioningRequestDescriptor(yaml).toOption.get
    val snowflakeManager = new SnowflakeManager(new SnowflakeExecutor, principalsMapper)

    val res1 = snowflakeManager.validateDescriptor(prd)
    res1.isRight shouldBe true

    val res2 = snowflakeManager.validateSpecificFields(prd)

    res2.isLeft shouldBe true
    res2.left
      .foreach(value => value.problems.head should (include("The specified kind").and(include("is not supported"))))
  }

  it should "successfully retrieve information schema of the tables from Snowflake" in {

    val yaml             = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_9_with_tags.yml")
    val prd              = ProvisioningRequestDescriptor(yaml).toOption.get
    val queryBuilder     = new QueryHelper
    val executor         = new SnowflakeExecutor
    val snowflakeManager = new SnowflakeManager(executor, principalsMapper)

    val res = for {
      connection         <- executor.getConnection
      deleteStatement    <- queryBuilder.buildMultipleStatement(prd, DELETE_TABLES, None, None)
      _                  <- deleteStatement match {
        case statement if statement.nonEmpty => executor.traverseMultipleStatements(connection, deleteStatement)
        case _ => Right(GetComponentError("Skipping delete tables - no information provided"))
      }
      createStatement    <- queryBuilder.buildMultipleStatement(prd, CREATE_TABLES, None, None)
      _                  <- createStatement match {
        case statement if statement.nonEmpty => executor.traverseMultipleStatements(connection, createStatement)
        case _ => Right(GetComponentError("Skipping create tables - no information provided"))
      }
      exitingTableSchema <- snowflakeManager.getExistingTableSchema(connection, prd)
    } yield exitingTableSchema
    res match { case _ => res.isRight shouldBe true }
  }

}
