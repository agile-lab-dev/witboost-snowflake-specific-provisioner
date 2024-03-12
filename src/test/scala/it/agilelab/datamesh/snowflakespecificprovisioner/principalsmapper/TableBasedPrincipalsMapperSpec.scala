package it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper

import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.{ExecuteStatementError, QueryExecutor}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Connection, ResultSet}

class TableBasedPrincipalsMapperSpec extends AnyFlatSpec with Matchers with MockFactory {

  "map method" should "return a right is the mapping succeeds" in {
    val queryExecutor  = mock[QueryExecutor]
    val connectionMock = mock[Connection]
    val resultSetMock  = mock[ResultSet]
    val mapper         = new TableBasedPrincipalsMapper(queryExecutor)
    val users          = Set("user:name.surname_email.com")
    val _              = (() => queryExecutor.getConnection).expects().returns(Right(connectionMock))
    val _              = (() => resultSetMock.next).expects().returns(true)
    val _              = (resultSetMock.getString(_: String)).expects("SNOWFLAKE_IDENTITY").returns("mapped_identity")
    val _              = (queryExecutor.executeQuery _).expects(connectionMock, *).returns(Right(resultSetMock))

    val result = mapper.map(users)

    result.foreach {
      case "user:name.surname_email.com" -> v =>
        v shouldBe a[Right[_, _]]
        v.toOption.get shouldEqual "MAPPED_IDENTITY"
      case _                                  => fail("unexpected entry in result map")
    }
  }

  "map method" should "return a left is there's not a corresponding entry in mapping table" in {
    val queryExecutor  = mock[QueryExecutor]
    val connectionMock = mock[Connection]
    val resultSetMock  = mock[ResultSet]
    val mapper         = new TableBasedPrincipalsMapper(queryExecutor)
    val users          = Set("user:name.surname_email.com")
    val _              = (() => queryExecutor.getConnection).expects().returns(Right(connectionMock))
    val _              = (() => resultSetMock.next).expects().returns(false)
    val _              = (queryExecutor.executeQuery _).expects(connectionMock, *).returns(Right(resultSetMock))

    val result = mapper.map(users)

    result.foreach {
      case "user:name.surname_email.com" -> v => v shouldBe a[Left[_, _]]
      case _                                  => fail("unexpected entry in result map")
    }
  }

  "map method" should "return a left is there's an error while querying the mapping table" in {
    val queryExecutor  = mock[QueryExecutor]
    val connectionMock = mock[Connection]
    val mapper         = new TableBasedPrincipalsMapper(queryExecutor)
    val users          = Set("user:name.surname_email.com")
    val _              = (() => queryExecutor.getConnection).expects().returns(Right(connectionMock))
    val _ = (queryExecutor.executeQuery _).expects(connectionMock, *).returns(Left(ExecuteStatementError()))

    val result = mapper.map(users)

    result.foreach {
      case "user:name.surname_email.com" -> v => v shouldBe a[Left[_, _]]
      case _                                  => fail("unexpected entry in result map")
    }
  }

}
