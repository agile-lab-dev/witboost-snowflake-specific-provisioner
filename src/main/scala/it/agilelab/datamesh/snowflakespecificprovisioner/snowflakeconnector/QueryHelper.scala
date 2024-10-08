package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import cats.data.NonEmptyList
import com.typesafe.scalalogging.LazyLogging
import it.agilelab.datamesh.snowflakespecificprovisioner.model.ComponentDescriptor.{
  OutputPort,
  SpecificOutputPort,
  SpecificStorageArea,
  StorageArea
}
import it.agilelab.datamesh.snowflakespecificprovisioner.model.{
  ComponentDescriptor,
  ProvisioningRequestDescriptor,
  Specific
}
import it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper.{
  SnowflakeGroup,
  SnowflakePrincipals,
  SnowflakeUser
}
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.DataType.DataType
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.{
  ColumnSchemaSpec,
  ConstraintType,
  DataType,
  SchemaChanges,
  TableSpec
}
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.OperationType.{
  ASSIGN_ROLE,
  CREATE_DB,
  CREATE_ROLE,
  CREATE_SCHEMA,
  CREATE_TABLES,
  CREATE_TAGS,
  CREATE_VIEW,
  DELETE_ROLE,
  DELETE_SCHEMA,
  DELETE_TABLES,
  DELETE_VIEW,
  DESCRIBE_VIEW,
  OperationType,
  SELECT_ON_VIEW,
  UPDATE_TABLES,
  UPDATE_VIEW,
  USAGE_ON_DB,
  USAGE_ON_SCHEMA,
  USAGE_ON_WH
}
import it.agilelab.datamesh.snowflakespecificprovisioner.system.ApplicationConfiguration.{
  snowflakeTagNameField,
  snowflakeTagValueField,
  warehouse
}

import java.sql.{Connection, ResultSet}
import scala.collection.mutable.ListBuffer

class QueryHelper extends LazyLogging {

  def buildStorageStatement(
      descriptor: ProvisioningRequestDescriptor,
      operation: OperationType
  ): Either[SnowflakeError, String] = {
    logger.info("Starting building storage statement")
    for {
      component <- getComponent(descriptor).flatMap {
        case storageArea: StorageArea => Right(storageArea)
        case _                        => Left(ParseError(Some("Component to provision is not a storage area!")))
      }
      dbName = getDatabaseName(descriptor, component.specific)
      schemaName = getSchemaName(descriptor, component.specific)
    } yield operation match {
      case CREATE_DB     => Right(createDatabaseStatement(dbName))
      case CREATE_SCHEMA => Right(createSchemaStatement(dbName, schemaName))
      case DELETE_SCHEMA => Right(deleteSchemaStatement(dbName, schemaName))
      case unsupportedOp => Left(UnsupportedOperationError(unsupportedOp))
    }
  }.flatten

  def buildMultipleStatement(
      descriptor: ProvisioningRequestDescriptor,
      operation: OperationType,
      schemaChanges: Option[List[SchemaChanges]],
      existingColumnTags: Option[List[TableSpec]]
  ): Either[SnowflakeError, Seq[String]] = {
    logger.info("Starting building multiple statements")
    val result =
      for {
        component <- getComponent(descriptor).flatMap {
          case storageArea: StorageArea => Right(storageArea)
          case _                        => Left(ParseError(Some("Invalid component type!")))
        }
        dbName = getDatabaseName(descriptor, component.specific)
        schemaName = getSchemaName(descriptor, component.specific)
        tables <- getTables(component)
      } yield operation match {
        case CREATE_TABLES => Right(createTablesStatement(dbName, schemaName, tables))
        case UPDATE_TABLES => updateTablesStatement(dbName, schemaName, tables, schemaChanges.getOrElse(List.empty))
        case CREATE_TAGS   =>
          createStorageTagStatements(dbName, schemaName, tables, existingColumnTags.getOrElse(List.empty))
        case DELETE_TABLES => Right(deleteTablesStatement(dbName, schemaName, tables))
        case unsupportedOp => Left(UnsupportedOperationError(unsupportedOp))
      }
    result match {
      case Left(error)      => Left(error)
      case Right(eitherSeq) => eitherSeq
    }
  }

  def buildOutputPortStatement(
      descriptor: ProvisioningRequestDescriptor,
      operation: OperationType
  ): Either[SnowflakeError, String] = {
    logger.info("Starting building output port statement")
    for {
      component <- getComponent(descriptor).flatMap {
        case outputPort: OutputPort => Right(outputPort)
        case _                      => Left(ParseError(Some("Invalid component type!")))
      }
      customViewStatement = getCustomViewStatement(component)
      dbName     = getCustomDatabaseName(customViewStatement) match {
        case Some(customDbName) => customDbName
        case _                  => getDatabaseName(descriptor, component.specific)
      }
      schemaName = getCustomSchemaName(customViewStatement) match {
        case Some(customSchemaName) => customSchemaName
        case _                      => getSchemaName(descriptor, component.specific)
      }
      viewName <- getCustomViewName(customViewStatement) match {
        case Some(customViewName) => Right(customViewName)
        case _                    => getViewName(component)
      }
      roleName   = buildRoleName(dbName, schemaName, viewName)
      tableName <- getTableName(component)
      schema    <- getTableSchema(component)
    } yield operation match {
      case CREATE_DB       => Right(createDatabaseStatement(dbName))
      case CREATE_SCHEMA   => Right(createSchemaStatement(dbName, schemaName))
      case DELETE_SCHEMA   => Right(deleteSchemaStatement(dbName, schemaName))
      case CREATE_VIEW     => customViewStatement match {
          case customStatement if customStatement.isEmpty =>
            Right(createViewStatement(viewName, dbName, schemaName, tableName, schema))
          case customStatement                            => Right(customStatement)
        }
      case UPDATE_VIEW     => customViewStatement match {
          case customStatement if customStatement.isEmpty =>
            Right(updateViewStatement(viewName, dbName, schemaName, tableName, schema))
          case customStatement                            => Right(customStatement)
        }
      case DELETE_VIEW     => Right(deleteViewStatement(dbName, schemaName, viewName))
      case CREATE_ROLE     => Right(createRoleStatement(roleName))
      case DELETE_ROLE     => Right(deleteRoleStatement(roleName))
      case USAGE_ON_WH     => Right(grantUsageStatement("warehouse", warehouse, roleName))
      case USAGE_ON_DB     => Right(grantUsageStatement("database", dbName, roleName))
      case USAGE_ON_SCHEMA => Right(grantUsageStatement("schema", s"""$dbName"."$schemaName""", roleName))
      case SELECT_ON_VIEW  => Right(grantSelectOnViewStatement(dbName, schemaName, viewName, roleName))
      case DESCRIBE_VIEW   => Right(describeView(dbName, schemaName, viewName))
      case unsupportedOp   => Left(UnsupportedOperationError(unsupportedOp))
    }
  }.flatten

  def buildOutputPortTagStatement(
      descriptor: ProvisioningRequestDescriptor,
      existingColumnTags: List[TableSpec],
      operation: OperationType
  ): Either[SnowflakeError, Seq[String]] = {
    logger.info("Starting building output port statement")
    for {
      component <- getComponent(descriptor).flatMap {
        case outputPort: OutputPort => Right(outputPort)
        case _                      => Left(ParseError(Some("Invalid component type!")))
      }
      customViewStatement = getCustomViewStatement(component)
      dbName        = getCustomDatabaseName(customViewStatement) match {
        case Some(customDbName) => customDbName
        case _                  => getDatabaseName(descriptor, component.specific)
      }
      schemaName    = getCustomSchemaName(customViewStatement) match {
        case Some(customSchemaName) => customSchemaName
        case _                      => getSchemaName(descriptor, component.specific)
      }
      viewName <- getCustomViewName(customViewStatement) match {
        case Some(customViewName) => Right(customViewName)
        case _                    => getViewName(component)
      }
      schema <- getTableSchema(component)
      componentTags = getComponentTags(component)
    } yield operation match {
      case CREATE_TAGS => Right(createOutputportTagStatement(
          dbName,
          schemaName,
          viewName,
          schema,
          existingColumnTags.find(_.tableName.equals(viewName.toUpperCase())),
          componentTags
        ))
    }
  }.flatten

  def buildRefsStatement(
      descriptor: ProvisioningRequestDescriptor,
      refs: Seq[SnowflakePrincipals],
      operation: OperationType
  ): Either[SnowflakeError, Seq[String]] = {
    logger.info("Starting building refs statements")
    for {
      component <- getComponent(descriptor).flatMap {
        case outputPort: OutputPort => Right(outputPort)
        case _                      => Left(ParseError(Some("Invalid component type!")))
      }
      customViewStatement = getCustomViewStatement(component)
      dbName     = getCustomDatabaseName(customViewStatement) match {
        case Some(customDbName) => customDbName
        case _                  => getDatabaseName(descriptor, component.specific)
      }
      schemaName = getCustomSchemaName(customViewStatement) match {
        case Some(customSchemaName) => customSchemaName
        case _                      => getSchemaName(descriptor, component.specific)
      }
      viewName <- getCustomViewName(customViewStatement) match {
        case Some(customViewName) => Right(customViewName)
        case _                    => getViewName(component)
      }
      roleName   = buildRoleName(dbName, schemaName, viewName)
    } yield operation match {
      case ASSIGN_ROLE   => Right(assignRoleStatement(roleName, refs))
      case unsupportedOp => Left(UnsupportedOperationError(unsupportedOp))
    }
  }.flatten

  def describeView(dbName: String, schemaName: String, viewName: String): String =
    s"""SELECT * FROM "$dbName"."$schemaName"."$viewName" LIMIT 1"""

  def createDatabaseStatement(dbName: String) = s"""CREATE DATABASE IF NOT EXISTS "$dbName";"""

  def createSchemaStatement(dbName: String, schemaName: String) =
    s"""CREATE SCHEMA IF NOT EXISTS "$dbName"."$schemaName";"""

  def deleteSchemaStatement(dbName: String, schemaName: String) = s"""DROP SCHEMA IF EXISTS "$dbName"."$schemaName";"""

  def primaryKeyConstraintStatement(tableName: String, columns: List[ColumnSchemaSpec]): Option[String] = {
    val primaryKeys = columns.filter(_.constraint.exists(_.equals(ConstraintType.PRIMARY_KEY))).map(_.name)
    NonEmptyList.fromList(primaryKeys)
      .map(_.toList.mkString(s"CONSTRAINT ${tableName}_primary_key PRIMARY KEY (", ",", ")"))
  }

  def createTableStatement(
      dbName: String,
      schemaName: String,
      tableName: String,
      schema: List[ColumnSchemaSpec]
  ): String = {
    val primaryKeyConstraint = primaryKeyConstraintStatement(tableName, schema)
    val columns              = schema.map(_.toColumnStatement).mkString(",\n")
    val constraint           = primaryKeyConstraint.fold("")(constraint => s",\n$constraint")
    s"""CREATE TABLE IF NOT EXISTS "$dbName"."$schemaName"."${tableName.toUpperCase}"
       |(
       | $columns$constraint
       |);""".stripMargin
  }

  def createTableSchemaInformationQuery(): String = s"""
            SELECT C.COLUMN_NAME, C.DATA_TYPE,C.IS_NULLABLE,NULL CONSTRAINT_TYPE, C.TABLE_NAME
            FROM "{CATALOG}"."INFORMATION_SCHEMA"."COLUMNS" as C
            WHERE C.TABLE_NAME IN ({TABLES}) and C.TABLE_SCHEMA = ? and C.TABLE_CATALOG = ?
            """

  def getPrimaryKeyInformationQuery(): String = s"""
            SHOW PRIMARY KEYS IN TABLE "{CATALOG}"."{SCHEMA}"."{TABLE}"
            """

  def getUniqueKeyInformationQuery(): String = s"""
            SHOW UNIQUE KEYS IN TABLE "{CATALOG}"."{SCHEMA}"."{TABLE}"
            """

  def getMultipleTablesTagsInformationQuery(): String = s"""
            SELECT OBJECT_NAME TABLE_NAME, COLUMN_NAME, TAG_NAME, TAG_VALUE FROM {TAG_REFERENCES_VIEW}
            where TAG_SCHEMA = ? AND TAG_DATABASE = ? AND OBJECT_NAME in ({TABLES}) AND DOMAIN = ?
            """

  def getSingleTableTagsInformationQuery(): String = s"""
            SELECT OBJECT_NAME TABLE_NAME, COLUMN_NAME, TAG_NAME, TAG_VALUE FROM {TAG_REFERENCES_VIEW}
            where TAG_SCHEMA = ? AND TAG_DATABASE = ? AND OBJECT_NAME = ? AND DOMAIN = ?
            """

  def findTagReferenceViewStatement(): String = s"""
            SHOW VIEWS IN {TAG_REFERENCES_DATABASE}.{TAG_REFERENCES_SCHEMA} STARTS WITH '{TAG_REFERENCES_VIEW}'
            """

  def getTableKeysInformation(
      connection: Connection,
      dbName: String,
      schemaName: String,
      tableName: String,
      existingSchemaInformationOriginal: Option[TableSpec]
  ): Option[TableSpec] = {

    val getSchemaPrimaryKeyQuery   = getPrimaryKeyInformationQuery()
    val getSchemaUniqueKeyQuery    = getUniqueKeyInformationQuery()
    val queryPrimaryKeyInformation = getSchemaPrimaryKeyQuery.replace("{CATALOG}", dbName)
      .replace("{SCHEMA}", schemaName).replace("{TABLE}", tableName)
    val queryUniqueKeyInformation = getSchemaUniqueKeyQuery.replace("{CATALOG}", dbName).replace("{SCHEMA}", schemaName)
      .replace("{TABLE}", tableName)
    val primaryKeyStatement       = connection.prepareStatement(queryPrimaryKeyInformation)
    val primaryKeyResultSet       = primaryKeyStatement.executeQuery()
    val uniqueKeyStatement        = connection.prepareStatement(queryUniqueKeyInformation)
    val uniqueKeyResultSet        = uniqueKeyStatement.executeQuery()

    def readPrimaryKeyRecursively(resultSet: ResultSet, acc: List[ColumnSchemaSpec]): List[ColumnSchemaSpec] =
      if (!resultSet.next()) acc
      else {
        val columnName = primaryKeyResultSet.getString("column_name")
        val dataType   = existingSchemaInformationOriginal.get.schema.find(p => p.name.equals(columnName)).get.dataType
        val constraint = Some(ConstraintType.PRIMARY_KEY)

        readPrimaryKeyRecursively(resultSet, acc :+ ColumnSchemaSpec(columnName, dataType, constraint))
      }

    val existingSchemaInformationWithPrimaryKey = readPrimaryKeyRecursively(primaryKeyResultSet, List.empty)

    def readUniqueKeyRecursively(resultSet: ResultSet, acc: List[ColumnSchemaSpec]): List[ColumnSchemaSpec] =
      if (!resultSet.next()) acc
      else {
        val columnName = resultSet.getString("column_name")
        val dataType   = existingSchemaInformationOriginal.get.schema.find(p => p.name.equals(columnName)).get.dataType
        val constraint = Some(ConstraintType.UNIQUE)

        readUniqueKeyRecursively(resultSet, acc :+ ColumnSchemaSpec(columnName, dataType, constraint))
      }

    val existingSchemaInformationWithKey =
      readUniqueKeyRecursively(uniqueKeyResultSet, existingSchemaInformationWithPrimaryKey)

    primaryKeyStatement.close()
    primaryKeyResultSet.close()
    uniqueKeyStatement.close()
    uniqueKeyResultSet.close()

    val otherColumns = existingSchemaInformationOriginal.get.schema
      .filter(p => !existingSchemaInformationWithKey.map(_.name).toSet.contains(p.name))

    val existingSchemaInformation = existingSchemaInformationWithKey ++ otherColumns

    Option(TableSpec(tableName, existingSchemaInformation, None))
  }

  def updateTableStatement(
      dbName: String,
      schemaName: String,
      tableName: String,
      schemaChanges: Option[SchemaChanges]
  ): Either[SnowflakeError, Seq[String]] = {
    val columnsToAdd: List[ColumnSchemaSpec]              = schemaChanges.get.columnsToAdd
    val columnsToDelete: List[ColumnSchemaSpec]           = schemaChanges.get.columnsToDelete
    val columnsToUpdateType: List[ColumnSchemaSpec]       = schemaChanges.get.columnsToUpdateType
    val columnsToRemoveConstraint: List[ColumnSchemaSpec] = schemaChanges.get.columnsToRemoveConstraint
    val columnsToAddConstraint: List[ColumnSchemaSpec]    = schemaChanges.get.columnsToAddConstraint

    val addColumnsStatement = columnsToAdd
      .map(c => s"""ALTER TABLE IF EXISTS "$dbName"."$schemaName"."${tableName.toUpperCase}"
                   | ADD COLUMN
                   | ${c.toColumnStatement}
                   | ;""".stripMargin)

    val dropColumnStatements = columnsToDelete.map(_.toDropColumnStatement)
      .map(st => s"""ALTER TABLE IF EXISTS "$dbName"."$schemaName"."${tableName.toUpperCase}"
                    | $st
                    | ;""".stripMargin)

    val updateColumnTypeStatements = columnsToUpdateType
      .map(c => s"""ALTER TABLE IF EXISTS "$dbName"."$schemaName"."${tableName.toUpperCase}"
                   | ${c.toUpdateColumnStatementDataType}
                   | ;""".stripMargin)

    val updateColumnConstraintStatements = columnsToRemoveConstraint
      .map(c => s"""ALTER TABLE IF EXISTS "$dbName"."$schemaName"."${tableName.toUpperCase}"
                   | ${c.toRemoveColumnStatementConstraint}
                   |;""".stripMargin) ++
      columnsToAddConstraint.map(c => s"""ALTER TABLE IF EXISTS "$dbName"."$schemaName"."${tableName.toUpperCase}"
                                         | ${c.toAddColumnStatementConstraint}
                                         |;""".stripMargin)

    Right(addColumnsStatement ++ dropColumnStatements ++ updateColumnTypeStatements ++ updateColumnConstraintStatements)
  }

  def createStorageTagStatement(
      dbName: String,
      schemaName: String,
      tableName: String,
      columnsList: List[ColumnSchemaSpec],   // descriptor table
      existingColumnTags: Option[TableSpec], // snowflake retrieved table
      componentTags: List[Map[String, String]]
  ): Either[SnowflakeError, Seq[String]] = {

    val removeTagStatements = existingColumnTags.map(ex =>
      ex.schema.flatMap(col =>
        col.tags.map(tag =>
          tag.flatten.flatMap(tg =>
            columnsList.map(desCol =>
              if (desCol.name.toUpperCase().equals(col.name.toUpperCase()) && !existsTagInDescriptor(tg, desCol))
                createDropTagStatement("TABLE", dbName, schemaName, tableName, col, tg._1)
              else ""
            ).filter(_.nonEmpty)
          )
        )
      ).flatten
    ).getOrElse(List.empty)

    val addTagOnColumnStatements = columnsList.flatMap(c =>
      c.tags.map(t =>
        t.flatten.map(v =>
          if (v._1.equals(snowflakeTagNameField))
            createColumnTagWithValueStatement("TABLE", dbName, schemaName, tableName, c, t, v)
          else ""
        )
      )
    ).flatten.filter(_.nonEmpty)

    val createTagOnColumnStatements = columnsList.flatMap(c =>
      c.tags.map(t =>
        t.flatten.map(v =>
          if (v._1.equals(snowflakeTagNameField)) s"""CREATE TAG IF NOT EXISTS $dbName.$schemaName."${v._2}";""" else ""
        )
      )
    ).flatten.filter(_.nonEmpty)

    val addTagOnComponentStatements = componentTags.flatten.map(v =>
      if (v._1.equals(snowflakeTagNameField))
        createComponentTagWithValueStatement("TABLE", dbName, schemaName, tableName, componentTags, v)
      else ""
    ).filter(_.nonEmpty)

    val createTagOnComponentStatements = componentTags.flatten.map(v =>
      if (v._1.equals(snowflakeTagNameField)) s"""CREATE TAG IF NOT EXISTS $dbName.$schemaName."${v._2}";""" else ""
    ).filter(_.nonEmpty)

    Right(
      removeTagStatements ++ addTagOnColumnStatements ++ addTagOnComponentStatements ++
        createTagOnColumnStatements.distinct ++ createTagOnComponentStatements.distinct
    )
  }

  def existsTagInDescriptor(existingTag: (String, String), descriptorColumn: ColumnSchemaSpec): Boolean = {

    val verifiedTags = descriptorColumn.tags.getOrElse(List.empty)
      .flatMap(tag => tag.filter(tg => (tg._1.equals(snowflakeTagNameField) && tg._2.equals(existingTag._1))))
      .filter(_._1.nonEmpty)

    if (verifiedTags.nonEmpty) true else false

  }

  def createColumnTagWithValueStatement(
      componentType: String,
      dbName: String,
      schemaName: String,
      tableName: String,
      column: ColumnSchemaSpec,
      tags: List[Map[String, String]],
      tag: (String, String)
  ): String = {

    val fieldForTagValue = tags.flatten.filter(v => v._1.equals(snowflakeTagValueField))
    val tagValue         = fieldForTagValue.headOption.fold("")(_._2)

    s"""ALTER $componentType IF EXISTS $dbName.$schemaName.${tableName.toUpperCase}
       | ${column.toUpdateColumnStatementTag}
       | $dbName.$schemaName."${tag._2}" = '$tagValue';""".stripMargin
  }

  def createComponentTagWithValueStatement(
      componentType: String,
      dbName: String,
      schemaName: String,
      tableName: String,
      tags: List[Map[String, String]],
      tag: (String, String)
  ): String = {

    val valueTag = tags.flatten.find(v => v._1.equals(snowflakeTagValueField))
    val tagValue = valueTag.fold("")(_._2)

    s"""ALTER $componentType IF EXISTS $dbName.$schemaName.${tableName.toUpperCase}
       | SET TAG $dbName.$schemaName."${tag._2}" = '$tagValue';""".stripMargin

  }

  def createDropTagStatement(
      componentType: String,
      dbName: String,
      schemaName: String,
      tableName: String,
      column: ColumnSchemaSpec,
      tag: String
  ): String = s"""ALTER $componentType IF EXISTS $dbName.$schemaName.${tableName.toUpperCase}
                 | ${column.toRemoveColumnStatementTag}
                 | $dbName.$schemaName."$tag";""".stripMargin

  def createOutputportTagStatement(
      dbName: String,
      schemaName: String,
      tableName: String,
      columnsList: List[ColumnSchemaSpec],
      existingColumnTags: Option[TableSpec],
      componentTags: List[Map[String, String]]
  ): Seq[String] = {

    val removeTagStatements = existingColumnTags.map(ex =>
      ex.schema.flatMap(col =>
        col.tags.map(tag =>
          tag.flatten.flatMap(tg =>
            columnsList.map(desCol =>
              if (desCol.name.toUpperCase().equals(col.name.toUpperCase()) && !existsTagInDescriptor(tg, desCol))
                createDropTagStatement("VIEW", dbName, schemaName, tableName, col, tg._1)
              else ""
            ).filter(_.nonEmpty)
          )
        )
      ).flatten
    ).getOrElse(List.empty)

    val addTagOnColumnStatements = columnsList.flatMap(c =>
      c.tags.map(t =>
        t.flatten.map(v =>
          if (v._1.equals(snowflakeTagNameField))
            createColumnTagWithValueStatement("VIEW", dbName, schemaName, tableName, c, t, v)
          else ""
        )
      )
    ).flatten.filter(_.nonEmpty)

    val createTagOnColumnStatements = columnsList.flatMap(c =>
      c.tags.map(t =>
        t.flatten.map(v =>
          if (v._1.equals(snowflakeTagNameField)) s"""CREATE TAG IF NOT EXISTS $dbName.$schemaName."${v._2}";""" else ""
        )
      )
    ).flatten.filter(_.nonEmpty)

    val addTagOnComponentStatements = componentTags.flatten.map(v =>
      if (v._1.equals(snowflakeTagNameField))
        createComponentTagWithValueStatement("VIEW", dbName, schemaName, tableName, componentTags, v)
      else ""
    ).filter(_.nonEmpty)

    val createTagOnComponentStatements = componentTags.flatten.map(v =>
      (if (v._1.equals(snowflakeTagNameField)) s"""CREATE TAG IF NOT EXISTS $dbName.$schemaName."${v._2}";""" else "")
    ).filter(_.nonEmpty)

    removeTagStatements ++ addTagOnColumnStatements ++ addTagOnComponentStatements ++
      createTagOnColumnStatements.distinct ++ createTagOnComponentStatements.distinct
  }

  def checkSchemaChanges(
      descriptor: ProvisioningRequestDescriptor,
      component: ComponentDescriptor,
      connection: Connection,
      existingSchemaInformation: List[TableSpec],
      tables: List[TableSpec],
      dropColumnEnabled: Boolean
  ): Either[SnowflakeError, List[SchemaChanges]] = component match {
    case storageArea: StorageArea =>
      val dbName         = getDatabaseName(descriptor, storageArea.specific)
      val schemaName     = getSchemaName(descriptor, storageArea.specific)
      val existingTables = existingSchemaInformation.map(_.tableName)

      val results = tables.filter(t => existingTables.contains(t.tableName.toUpperCase()))
        .foldLeft[Either[SnowflakeError, List[SchemaChanges]]](Right(Nil)) { (acc, table) =>
          acc match {
            case Right(schemaChanges) => checkTableSchemaChanges(
                dbName,
                schemaName,
                table.tableName.toUpperCase(),
                connection,
                existingSchemaInformation.find(_.tableName.equals(table.tableName.toUpperCase())),
                table.schema,
                dropColumnEnabled
              ) match {
                case Right(newSchemaChanges) => Right(schemaChanges ++ List(newSchemaChanges))
                case Left(error)             => Left(error)
              }
            case Left(error)          => Left(error)
          }
        }
      results.map(_.reverse.toSeq)
    case _                        => Left(ParseError(Option("Invalid component type!")))
  }

  def checkTableSchemaChanges(
      dbName: String,
      schemaName: String,
      tableName: String,
      connection: Connection,
      existingSchemaInformationOriginal: Option[TableSpec],
      incomingShameOriginal: List[ColumnSchemaSpec],
      dropColumnEnabled: Boolean
  ): Either[SnowflakeError, SchemaChanges] = {

    val existingSchemaInformation =
      getTableKeysInformation(connection, dbName, schemaName, tableName, existingSchemaInformationOriginal)

    val statementErrors = new ListBuffer[String]()
    val incomingShame   = incomingShameOriginal.map(col => col.copy(name = col.name.toUpperCase()))
    val existingColumns = existingSchemaInformation.get.schema.map(_.name).toSet
    val newColumns      = incomingShame.map(_.name).toSet

    val columnsToAdd    = incomingShame.filter(spec => !existingColumns.contains(spec.name))
    val columnsToDelete = existingSchemaInformation.get.schema.filter(spec => !newColumns.contains(spec.name))

    if (!dropColumnEnabled && columnsToDelete.nonEmpty) {
      statementErrors += "It's not possible to drop any existing columns"
    }

    val columnsToUpdate = incomingShame
      .filter(spec => existingColumns.contains(spec.name) && newColumns.contains(spec.name))

    val existingPrimaryKeyColumns = existingSchemaInformation.get.schema
      .filter(c => c.constraint.contains(ConstraintType.PRIMARY_KEY)).sortBy(c => c.name)

    val newPrimaryKeyColumns = incomingShame.filter(c => c.constraint.contains(ConstraintType.PRIMARY_KEY))
      .distinctBy(c => c.name).sortBy(c => c.name)

    if (
      existingPrimaryKeyColumns.exists(p => !newPrimaryKeyColumns.map(n => n.name).contains(p.name)) ||
      newPrimaryKeyColumns.exists(p => !existingPrimaryKeyColumns.map(n => n.name).contains(p.name))
    ) statementErrors += "PRIMARY KEY is changed and this operation is incompatible"

    val noConstraintColumns = existingSchemaInformation.get.schema.filter(c => c.constraint.isEmpty)
    val columnsWithIncompatibleConstraintChanges = columnsToUpdate
      .filter(c => noConstraintColumns.exists(a => a.name.equals(c.name)) && c.constraint.isDefined)

    if (columnsWithIncompatibleConstraintChanges.nonEmpty) statementErrors +=
      "There are some columns with incompatible constraints"

    val implicitConversionsMapping: Map[DataType, Set[DataType]] = Map(
      DataType.TEXT    -> Set(DataType.TEXT),
      DataType.NUMBER  -> Set(DataType.NUMBER),
      DataType.DATE    -> Set(DataType.DATE),
      DataType.BOOLEAN -> Set(DataType.BOOLEAN)
    )

    val columnsToUpdateType: List[ColumnSchemaSpec] = columnsToUpdate.filter { columnToUpdate =>
      val existingColumnSpec = existingSchemaInformation.get.schema.find(_.name.equals(columnToUpdate.name)).get
      val existingColumnType = existingColumnSpec.dataType
      if (!implicitConversionsMapping(existingColumnType)(columnToUpdate.dataType)) {
        statementErrors += s"For the column ${columnToUpdate.name}, " +
          s"implicit conversion from $existingColumnType to ${columnToUpdate.dataType} is not allowed."
        false
      } else { !existingColumnType.equals(columnToUpdate.dataType) }
    }

    val columnsToRemoveConstraint: List[ColumnSchemaSpec] = existingSchemaInformation.get.schema
      .filter { existingColumnSpec =>
        val columnToUpdate = columnsToUpdate.find(columnToUpdate => columnToUpdate.name.equals(existingColumnSpec.name))
        existingColumnSpec.constraint.isDefined && columnToUpdate.isDefined && columnToUpdate.get.constraint.isEmpty
      }

    val columnsToAddConstraint: List[ColumnSchemaSpec] = columnsToUpdate.filter { columnToUpdate =>
      val existingColumnSpec = existingSchemaInformation.get.schema.find(_.name.equals(columnToUpdate.name))
      existingColumnSpec.isDefined && existingColumnSpec.get.constraint.isEmpty && columnToUpdate.constraint.isDefined
    }

    if (statementErrors.nonEmpty) {
      Left(SchemaChangesError(statementErrors.toList, List("Please review the table schema descriptor")))
    } else {
      Right(SchemaChanges(
        columnsToAdd,
        columnsToDelete,
        columnsToUpdateType,
        columnsToRemoveConstraint,
        columnsToAddConstraint,
        dbName,
        schemaName,
        tableName
      ))
    }
  }

  def deleteTableStatement(dbName: String, schemaName: String, tableName: String) =
    s"""DROP TABLE IF EXISTS "$dbName"."$schemaName"."${tableName.toUpperCase}";"""

  def createTablesStatement(dbName: String, schemaName: String, tables: List[TableSpec]): Seq[String] = tables
    .map(table => createTableStatement(dbName, schemaName, table.tableName, table.schema))

  def updateTablesStatement(
      dbName: String,
      schemaName: String,
      tables: List[TableSpec],
      schemaChanges: List[SchemaChanges]
  ): Either[SnowflakeError, Seq[String]] = {

    val existingTables = schemaChanges.map(a => a.table)

    val results = tables.filter(t => existingTables.contains(t.tableName.toUpperCase()))
      .foldLeft[Either[SnowflakeError, List[String]]](Right(Nil)) { (acc, table) =>
        acc match {
          case Right(statements) => updateTableStatement(
              dbName,
              schemaName,
              table.tableName.toUpperCase(),
              schemaChanges.find(_.table.equals(table.tableName.toUpperCase))
            ) match {
              case Right(newStatements) => Right(statements ++ newStatements)
              case Left(error)          => Left(error)
            }
          case Left(error)       => Left(error)
        }
      }

    results.map(_.reverse.toSeq)
  }

  def createStorageTagStatements(
      dbName: String,
      schemaName: String,
      tables: List[TableSpec],
      existingColumnTags: List[TableSpec]
  ): Either[SnowflakeError, Seq[String]] = {

    val results = tables.foldLeft[Either[SnowflakeError, List[String]]](Right(Nil)) { (acc, table) =>
      acc match {
        case Right(statements) => createStorageTagStatement(
            dbName,
            schemaName,
            table.tableName.toUpperCase(),
            table.schema,
            existingColumnTags.find(_.tableName.equals(table.tableName.toUpperCase())),
            table.tags.getOrElse(List.empty)
          ) match {
            case Right(newStatements) => Right(statements ++ newStatements)
            case Left(error)          => Left(error)
          }
        case Left(error)       => Left(error)
      }
    }

    results.map(_.reverse.toSeq)
  }

  def deleteTablesStatement(dbName: String, schemaName: String, tables: List[TableSpec]): Seq[String] = tables
    .map(table => deleteTableStatement(dbName, schemaName, table.tableName))

  def createRoleStatement(roleName: String) = s"CREATE ROLE IF NOT EXISTS $roleName;"

  def grantUsageStatement(resource: String, resourceName: String, roleName: String): String =
    s"""GRANT USAGE ON $resource "$resourceName" TO ROLE $roleName;"""

  def grantSelectOnViewStatement(dbName: String, schemaName: String, viewName: String, roleName: String): String =
    s"""GRANT SELECT ON VIEW "$dbName"."$schemaName"."$viewName" TO ROLE $roleName;"""

  def deleteRoleStatement(roleName: String): String = s"DROP ROLE IF EXISTS $roleName;"

  def assignRoleStatement(roleName: String, refs: Seq[SnowflakePrincipals]): Seq[String] = refs.map {
    case SnowflakeUser(user)       => s"GRANT ROLE $roleName TO USER \"$user\";"
    case SnowflakeGroup(groupName) => s"GRANT ROLE $roleName TO ROLE \"$groupName\";"
  }

  def buildRoleName(dbName: String, schemaName: String, viewName: String): String =
    s"${dbName}_${schemaName}_${viewName}_ACCESS"

  def createViewStatement(
      viewName: String,
      dbName: String,
      schemaName: String,
      tableName: String,
      schema: List[ColumnSchemaSpec]
  ): String = s"""CREATE VIEW IF NOT EXISTS "$dbName"."$schemaName"."$viewName" AS """ + s"""(SELECT ${schema
    .map(_.name).mkString(",\n")} FROM "$dbName"."$schemaName"."$tableName");"""

  def deleteViewStatement(dbName: String, schemaName: String, viewName: String) =
    s"""DROP VIEW IF EXISTS "$dbName"."$schemaName"."$viewName""""

  def updateViewStatement(
      dbName: String,
      schemaName: String,
      viewName: String,
      tableName: String,
      schema: List[ColumnSchemaSpec]
  ) = s"""ALTER VIEW IF EXISTS "$dbName"."$schemaName"."$viewName" AS """ + s"""(SELECT ${schema.map(_.name)
    .mkString(",\n")} FROM "$dbName"."$schemaName"."$tableName");"""

  def getDatabaseName(descriptor: ProvisioningRequestDescriptor, specific: Specific): String = specific match {
    case sp: SpecificOutputPort  => sp.database.getOrElse {
        logger.info("The database is not in the specific field, fetching domain...")
        descriptor.dataProduct.domain.toUpperCase
      }
    case sp: SpecificStorageArea => sp.database.getOrElse {
        logger.info("The database is not in the specific field, fetching domain...")
        descriptor.dataProduct.domain.toUpperCase
      }
  }

  def getSchemaName(descriptor: ProvisioningRequestDescriptor, specific: Specific): String = specific match {
    case sp: SpecificOutputPort  => sp.schema.getOrElse {
        logger.info("Database schema not found in specific field, taking data product name and version...")
        s"${descriptor.dataProduct.name.toUpperCase.replaceAll(" ", "")}_${descriptor.dataProduct.version.split('.')(0)}"
      }
    case sp: SpecificStorageArea => sp.schema.getOrElse {
        logger.info("Database schema not found in specific field, taking data product name and version...")
        s"${descriptor.dataProduct.name.toUpperCase.replaceAll(" ", "")}_${descriptor.dataProduct.version.split('.')(0)}"
      }
  }

  def getTableSchema(outputPort: OutputPort): Either[SnowflakeError, List[ColumnSchemaSpec]] = {
    val schema = outputPort.dataContract.schema
    if (schema.isEmpty) {
      Left(ParseError(Some(outputPort.toString), Some("dataContract"), List("Data Contract schema is empty")))
    } else { Right(schema.toList) }
  }

  def getComponentTags(outputPort: OutputPort): List[Map[String, String]] = outputPort.specific.tags
    .getOrElse(List.empty)

  def getTableName(outputPort: OutputPort): Either[SnowflakeError, String] = {
    val tableName = outputPort.specific.tableName
    if (tableName.isEmpty) {
      Left(GetTableNameError(
        List("If a custom view query is not provided, please provide a tableName in the specific section")
      ))
    } else { Right(tableName) }
  }

  def getViewName(outputPort: OutputPort): Either[GetViewNameError, String] = {
    val viewName = outputPort.specific.viewName
    if (viewName.isEmpty) { Left(GetViewNameError(List("Please provide the view name in the specific section!"))) }
    else { Right(viewName) }
  }

  def getCustomViewStatement(outputPort: OutputPort): String = outputPort.specific.customView.getOrElse("")

  def getCustomViewDetails(query: String): Map[String, String] = {
    val emptyDetails = Map[String, String]()
    if (query.nonEmpty) {
      val upperCase   = query.toUpperCase
      val keyWord     = if (upperCase.contains("IF NOT EXISTS")) "EXISTS" else "VIEW"
      val indexedSeq  = upperCase.split(' ').indexOf(keyWord) + 1
      val viewDetails = query.split(' ')(indexedSeq).split('.')

      if (viewDetails.length.equals(3)) {
        val keyNames = Array("dbName", "schemaName", "viewName")
        keyNames.zip(viewDetails).toMap
      } else { emptyDetails }

    } else { emptyDetails }
  }

  def getCustomDatabaseName(query: String): Option[String] = getCustomViewDetails(query).get("dbName")
  def getCustomSchemaName(query: String): Option[String]   = getCustomViewDetails(query).get("schemaName")
  def getCustomViewName(query: String): Option[String]     = getCustomViewDetails(query).get("viewName")

  val alterSessionToJsonResult: String = "ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'"

  def getTables(storageArea: StorageArea): Either[SnowflakeError, List[TableSpec]] = {
    val tablesList = storageArea.specific.tables.toList
    if (tablesList.isEmpty) {
      Left(ParseError(problems = List("The provided request does not contain tables since it is empty")))
    } else { Right(tablesList) }
  }

  def getComponent(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, ComponentDescriptor] = descriptor
    .getComponentToProvision.toRight(GetComponentError(descriptor.componentIdToProvision))
}
