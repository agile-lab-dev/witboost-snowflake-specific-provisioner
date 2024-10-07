package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import it.agilelab.datamesh.snowflakespecificprovisioner.schema.DataType
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Connection, PreparedStatement, ResultSet}

class SnowflakeTableInformationHelperSpec extends AnyFlatSpec with Matchers with MockFactory {

  "getExistingMetaData" should "return the metadata of a list of tables" in {

    val executorQueryHelper: QueryHelper = mock[QueryHelper]

    val connectionMock = mock[Connection]

    val preparedStatementMock = mock[PreparedStatement]

    val resultSetMock = mock[ResultSet]

    val _ = (() => preparedStatementMock.executeQuery()).expects().returns(resultSetMock).anyNumberOfTimes()

    val reverseProvisioningHelper = new SnowflakeTableInformationHelper(executorQueryHelper)

    val _ = (() => executorQueryHelper.createTableSchemaInformationQuery()).expects().returns("")

    val _ = (connectionMock.prepareStatement(_: String)).expects("ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'")
      .returns(preparedStatementMock).anyNumberOfTimes()

    val _ = (connectionMock.prepareStatement(_: String)).expects("").returns(preparedStatementMock).anyNumberOfTimes()

    val _ = (preparedStatementMock.setString _).expects(*, *).returning(()).anyNumberOfTimes()

    val _ = (() => resultSetMock.next()).expects().returns(true).once()

    val _ = (() => resultSetMock.next()).expects().returns(false).once()

    val _ = (resultSetMock.getString(_: String)).expects("TABLE_NAME").returns("viewName").anyNumberOfTimes()

    val _ = (resultSetMock.getString(_: String)).expects("COLUMN_NAME").returns("columnOne").anyNumberOfTimes()

    val _ = (resultSetMock.getString(_: String)).expects("DATA_TYPE").returns(DataType.snowflakeTypeToDataType("FLOAT"))
      .anyNumberOfTimes()

    val _ = (resultSetMock.getString(_: String)).expects("IS_NULLABLE").returns("YES").anyNumberOfTimes()

    val _ = (resultSetMock.getString(_: String)).expects("CONSTRAINT_TYPE").returns("NOT_NULL").anyNumberOfTimes()

    val _ = (() => resultSetMock.close()).expects().returns(()).anyNumberOfTimes()

    val _ = (() => preparedStatementMock.close()).expects().returns(()).anyNumberOfTimes()

    val result = reverseProvisioningHelper.getExistingMetaData(connectionMock, "database", "schema", List("viewName"))

    result shouldBe a[Right[_, _]]
  }

  "getViewExistingTags" should "return the tags of a view" in {
    val executorQueryHelper: QueryHelper = mock[QueryHelper]

    val connectionMock = mock[Connection]

    val preparedStatementMock = mock[PreparedStatement]

    val resultSetMock = mock[ResultSet]

    val _ = (() => preparedStatementMock.executeQuery()).expects().returns(resultSetMock).anyNumberOfTimes()

    val reverseProvisioningHelper = new SnowflakeTableInformationHelper(executorQueryHelper)

    val _ = (() => executorQueryHelper.findTagReferenceViewStatement()).expects().returns("")
    val _ = (() => executorQueryHelper.getSingleTableTagsInformationQuery()).expects().returns("")

    val _ = (connectionMock.prepareStatement(_: String)).expects("ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'")
      .returns(preparedStatementMock).anyNumberOfTimes()

    val _ = (connectionMock.prepareStatement(_: String)).expects("").returns(preparedStatementMock).anyNumberOfTimes()
    val _ = (preparedStatementMock.setString(_: Int, _: String)).expects(*, *).anyNumberOfTimes()

    // Once for getSnowflakeTagReferencesView, once for the tags query
    val _ = (() => resultSetMock.next()).expects().returns(true).twice()
    val _ = (() => resultSetMock.next()).expects().returns(false).once()

    val _ = (() => resultSetMock.close()).expects().returns(()).anyNumberOfTimes()

    val _ = (() => preparedStatementMock.close()).expects().returns(()).anyNumberOfTimes()

    val _ = (resultSetMock.getString(_: String)).expects("TABLE_NAME").returns("viewName")

    val _ = (resultSetMock.getString(_: String)).expects("COLUMN_NAME").returns(null) // scalafix:ok
    val _ = (resultSetMock.getString(_: String)).expects("TAG_NAME").returns("tagName")
    val _ = (resultSetMock.getString(_: String)).expects("TAG_VALUE").returns("tagValue")

    val result = reverseProvisioningHelper.getViewExistingTags(connectionMock, "database", "schema", "viewName")

    result shouldBe a[Right[_, _]]
    result.foreach { tables =>
      tables.size shouldEqual 1
      val table = tables.head
      table.tags shouldNot be(None)
      table.tags.get shouldEqual List(Map("tagName" -> "tagValue"))
    }

  }

  "getSnowflakeTagReferencesView" should "return the view name containing the tag information" in {

    val executorQueryHelper: QueryHelper = mock[QueryHelper]

    val connectionMock = mock[Connection]

    val preparedStatementMock = mock[PreparedStatement]

    val resultSetMock = mock[ResultSet]

    val _ = (() => preparedStatementMock.executeQuery()).expects().returns(resultSetMock).anyNumberOfTimes()

    val reverseProvisioningHelper = new SnowflakeTableInformationHelper(executorQueryHelper)

    val _ = (() => executorQueryHelper.findTagReferenceViewStatement()).expects().returns("")

    val _ = (connectionMock.prepareStatement(_: String)).expects("ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'")
      .returns(preparedStatementMock).anyNumberOfTimes()

    val _ = (connectionMock.prepareStatement(_: String)).expects("").returns(preparedStatementMock).anyNumberOfTimes()

    val _ = (() => resultSetMock.next()).expects().returns(true).once()

    val _ = (() => resultSetMock.close()).expects().returns(()).anyNumberOfTimes()

    val _ = (() => preparedStatementMock.close()).expects().returns(()).anyNumberOfTimes()

    val result = reverseProvisioningHelper.getSnowflakeTagReferencesView(connectionMock)

    result shouldBe a[Right[_, _]]

  }
}
