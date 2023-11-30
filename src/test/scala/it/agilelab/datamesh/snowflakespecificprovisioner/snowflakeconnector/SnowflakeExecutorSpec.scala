package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Statement}

@SuppressWarnings(Array("scalafix:DisableSyntax.null"))
class SnowflakeExecutorSpec extends AnyFlatSpec with MockFactory with should.Matchers {

  val snowflakeExecutor = new SnowflakeExecutor

  "execute a correct statement" should "return Right" in {

    val connectionStub = mock[Connection]
    val mockStatement  = mock[Statement]
    val statement      = "UPDATE A_TABLE SET A_ROW = 'a_value'"

    val _ = (connectionStub.createStatement: () => Statement).expects().returns(mockStatement)
    val _ = (mockStatement.executeUpdate(_: String)).expects(statement).returns(1)
    val _ = (mockStatement.close: () => Unit).expects().returns(())

    val result = snowflakeExecutor.executeStatement(connectionStub, statement)

    result shouldBe a[Right[_, _]]
  }

  "execute a wrong statement" should "return Left" in {
    val connectionStub = mock[Connection]
    val mockStatement  = mock[Statement]
    val statement      = "UPDATE A_TABLE SET A_ROW = 'a_value'"

    val _ = (connectionStub.createStatement: () => Statement).expects().returns(mockStatement)
    val _ = (mockStatement.executeUpdate(_: String)).expects(statement).throws(new SQLException("error"))

    val result = snowflakeExecutor.executeStatement(connectionStub, statement)

    result shouldBe a[Left[_, _]]
  }

  "execute a series of statements" should "return Right for all correct statements" in {
    val connectionStub = mock[Connection]
    val mockStatement  = mock[Statement]
    val statements     = List("UPDATE A_TABLE SET A_ROW = 'a_value'", "GRANT ROLE A_ROLE TO A_USER")

    val _ = (connectionStub.createStatement: () => Statement).expects().returns(mockStatement).anyNumberOfTimes()
    statements.foreach { statement => val _ = (mockStatement.executeUpdate(_: String)).expects(statement).returns(1) }
    val _ = (mockStatement.close: () => Unit).expects().returns(()).anyNumberOfTimes()

    val result = snowflakeExecutor.executeMultipleStatements(connectionStub, statements)

    result.foreach(res => res shouldBe a[Right[_, _]])
  }

  it should "return Right for correct statements and Left for wrong" in {
    val connectionStub = mock[Connection]
    val mockStatement  = mock[Statement]
    val statements     = List("UPDATE A_TABLE SET A_ROW = 'a_value'" -> true, "GRANT ROLE A_ROLE TO A_USER" -> true)

    val _ = (connectionStub.createStatement: () => Statement).expects().returns(mockStatement).anyNumberOfTimes()
    statements.foreach { case (statement, isCorrect) =>
      if (isCorrect) { val _ = (mockStatement.executeUpdate(_: String)).expects(statement).returns(1) }
      else { val _ = (mockStatement.executeUpdate(_: String)).expects(statement).throws(new SQLException("Error")) }
    }
    val _ = (mockStatement.close: () => Unit).expects().returns(()).anyNumberOfTimes()

    val result = snowflakeExecutor.executeMultipleStatements(connectionStub, statements.map(_._1))

    statements.map(_._2).zip(result).foreach { case (isRight, res) => res.isRight shouldEqual isRight }
  }

  "traverse a series of statements" should "return Right if all statements were executed correctly" in {
    val connectionStub = mock[Connection]
    val mockStatement  = mock[Statement]
    val statements     = List("UPDATE A_TABLE SET A_ROW = 'a_value'" -> true, "GRANT ROLE A_ROLE TO A_USER" -> true)

    val _ = (connectionStub.createStatement: () => Statement).expects().returns(mockStatement).anyNumberOfTimes()
    statements.foreach { case (statement, isCorrect) =>
      if (isCorrect) { val _ = (mockStatement.executeUpdate(_: String)).expects(statement).returns(1) }
      else { val _ = (mockStatement.executeUpdate(_: String)).expects(statement).throws(new SQLException("Error")) }
    }
    val _ = (mockStatement.close: () => Unit).expects().returns(()).anyNumberOfTimes()

    val result = snowflakeExecutor.traverseMultipleStatements(connectionStub, statements.map(_._1))

    result shouldBe a[Right[_, _]]

  }

  it should "return Left if an error occurred" in {
    val connectionStub = mock[Connection]
    val mockStatement  = mock[Statement]
    val statements     = List("UPDATE A_TABLE SET A_ROW = 'a_value'" -> true, "GRANT ROLE A_ROLE TO A_GROUP" -> false)

    val _ = (connectionStub.createStatement: () => Statement).expects().returns(mockStatement).anyNumberOfTimes()
    statements.foreach { case (statement, isCorrect) =>
      if (isCorrect) { val _ = (mockStatement.executeUpdate(_: String)).expects(statement).returns(1) }
      else { val _ = (mockStatement.executeUpdate(_: String)).expects(statement).throws(new SQLException("Error")) }
    }
    val _ = (mockStatement.close: () => Unit).expects().returns(()).anyNumberOfTimes()

    val result = snowflakeExecutor.traverseMultipleStatements(connectionStub, statements.map(_._1))

    result shouldBe a[Left[_, _]]
  }

  "executing a query" should "return a Right if query was executed correctly" in {

    val connectionStub = mock[Connection]
    val mockStatement  = mock[PreparedStatement]
    val statement      = "SELECT * FROM A_TABLE"

    val _ = (connectionStub.prepareStatement(_: String)).expects(statement).returns(mockStatement)

    val _ = (mockStatement.executeQuery: () => ResultSet).expects().returns(null)

    val result = snowflakeExecutor.executeQuery(connectionStub, statement)

    result shouldBe a[Right[_, _]]
  }

  "executing a wrong query" should "return Left" in {

    val connectionStub = mock[Connection]
    val mockStatement  = mock[PreparedStatement]
    val statement      = "SELECT FROM A_TABLE"

    val _ = (connectionStub.prepareStatement(_: String)).expects(statement).returns(mockStatement)
    val _ = (mockStatement.executeQuery: () => ResultSet).expects().throws(new SQLException("Error"))

    val result = snowflakeExecutor.executeQuery(connectionStub, statement)

    result shouldBe a[Left[_, _]]
  }
}
