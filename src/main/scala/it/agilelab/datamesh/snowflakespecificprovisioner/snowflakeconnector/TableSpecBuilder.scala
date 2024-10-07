package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import it.agilelab.datamesh.snowflakespecificprovisioner.schema.{ColumnSchemaSpec, ConstraintType, DataType, TableSpec}

import java.sql.ResultSet
import scala.annotation.tailrec

trait TableSpecBuilder {

  @tailrec
  private def readResultSetRecursively(resultSet: ResultSet, acc: List[TableSpec]): List[TableSpec] =
    if (!resultSet.next()) acc
    else {
      val tableName      = resultSet.getString("TABLE_NAME")
      val columnName     = resultSet.getString("COLUMN_NAME")
      val dataType       = DataType.snowflakeTypeToDataType(resultSet.getString("DATA_TYPE"))
      val isNullable     = if (resultSet.getString("IS_NULLABLE").equals("NO")) false else true
      val constraintType = resultSet.getString("CONSTRAINT_TYPE")
      val constraint     = constraintType match {
        case "PRIMARY KEY"    => Some(ConstraintType.PRIMARY_KEY)
        case _ if !isNullable => Some(ConstraintType.NOT_NULL)
        case _                => None
      }

      val existing = acc.find(a => a.tableName.equals(tableName))
      if (existing.isEmpty) readResultSetRecursively(
        resultSet,
        acc :+ TableSpec(tableName, List(ColumnSchemaSpec(columnName, dataType, constraint)), None)
      )
      else {
        val newSpec     = existing.get.schema ++ List(ColumnSchemaSpec(columnName, dataType, constraint))
        val clearedList = acc.filterNot(a => a.tableName.equals(tableName))
        readResultSetRecursively(resultSet, clearedList :+ TableSpec(tableName, newSpec, None))
      }
    }

  private case class TableTagRowInfo(tableName: String, columnName: String, tag: (String, String))

  @tailrec
  private def readTagsResultSetRecursively(resultSet: ResultSet, acc: List[TableTagRowInfo]): List[TableTagRowInfo] =
    if (!resultSet.next()) acc
    else {
      val tableName  = resultSet.getString("TABLE_NAME")
      val columnName = resultSet.getString("COLUMN_NAME")
      val tagName    = resultSet.getString("TAG_NAME")
      val tagValue   = resultSet.getString("TAG_VALUE")

      readTagsResultSetRecursively(resultSet, acc :+ TableTagRowInfo(tableName, columnName, tagName -> tagValue))
    }

  def readTableTagsListFromResultSet(resultSet: ResultSet): List[TableSpec] = {
    val result = readTagsResultSetRecursively(resultSet, List.empty)
    result.groupBy(_.tableName).map { case (tableName, tagList) =>
      val tableTags  = tagList.filter(row => Option(row.columnName).isEmpty)
      val columnTags = tagList.filter(row => Option(row.columnName).isDefined).groupBy(_.columnName)
        .map { case (columnName, tagList) => columnName -> tagList.map(_.tag) }

      TableSpec(
        tableName,
        columnTags.map(column => ColumnSchemaSpec(column._1, DataType.TEXT, None, Some(column._2.map(t => Map(t)))))
          .toList,
        Option.when(tableTags.nonEmpty)(tableTags.map(t => Map(t.tag)))
      )
    }.toList
  }

  def readTableListFromResultSet(resultSet: ResultSet): List[TableSpec] =
    readResultSetRecursively(resultSet, List.empty)
}
