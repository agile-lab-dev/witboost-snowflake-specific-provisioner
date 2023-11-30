package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import it.agilelab.datamesh.snowflakespecificprovisioner.common.test.getTestResourceAsString
import it.agilelab.datamesh.snowflakespecificprovisioner.model.ProvisioningRequestDescriptor
import it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper.SnowflakePrincipalsMapper
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.sql.Connection

class SnowflakeManagerSpec extends AnyFlatSpec with MockFactory with should.Matchers {

  val executorMock     = mock[QueryExecutor]
  val snowflakeManager = new SnowflakeManager(executorMock)

  "update acl on an output port" should "return Right if all users are granted access correctly" in {

    val refs        = List("user:sergio.mejia_agilelab.it", "user:nicolo.bidotti_agilelab.it")
    val mappedUsers = SnowflakePrincipalsMapper.map(refs.toSet).values.partitionMap(identity)

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

    val refs         = List("user:sergio.mejia_agilelab.it", "group:bigData", "user:nicolo.bidotti_agilelab.it")
    val mappedGroups = SnowflakePrincipalsMapper.map(refs.toSet).values.partitionMap(identity)

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

    val refs = List("user:sergio.mejia_agilelab.it", "user:no.user_agilelab.it")

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

}
