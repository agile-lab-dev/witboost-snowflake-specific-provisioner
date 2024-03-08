package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import cats.data.NonEmptyList
import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import it.agilelab.datamesh.snowflakespecificprovisioner.model.{ComponentDescriptor, ProvisioningRequestDescriptor}
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
import it.agilelab.datamesh.snowflakespecificprovisioner.system.ApplicationConfiguration.warehouse

import java.sql.{Connection, ResultSet}
import scala.collection.mutable.ListBuffer

class QueryHelper extends LazyLogging {

  def buildStorageStatement(
      descriptor: ProvisioningRequestDescriptor,
      operation: OperationType
  ): Either[SnowflakeError, String] = {
    logger.info("Starting building storage statement")
    for {
      component <- getComponent(descriptor)
      dbName     = getDatabaseName(descriptor, component.specific)
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
      schemaChanges: Option[List[SchemaChanges]]
  ): Either[SnowflakeError, Seq[String]] = {
    logger.info("Starting building multiple statements")
    val result =
      for {
        component <- getComponent(descriptor)
        dbName     = getDatabaseName(descriptor, component.specific)
        schemaName = getSchemaName(descriptor, component.specific)
        tables <- getTables(component)
      } yield operation match {
        case CREATE_TABLES => Right(createTablesStatement(dbName, schemaName, tables))
        case UPDATE_TABLES => updateTablesStatement(dbName, schemaName, tables, schemaChanges.getOrElse(List.empty))
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
      component <- getComponent(descriptor)
      customViewStatement = getCustomViewStatement(component)
      dbName              = getCustomDatabaseName(customViewStatement) match {
        case Some(customDbName) => customDbName
        case _                  => getDatabaseName(descriptor, component.specific)
      }
      schemaName          = getCustomSchemaName(customViewStatement) match {
        case Some(customSchemaName) => customSchemaName
        case _                      => getSchemaName(descriptor, component.specific)
      }
      viewName <- getCustomViewName(customViewStatement) match {
        case Some(customViewName) => Right(customViewName)
        case _                    => getViewName(component)
      }
      roleName            = buildRoleName(dbName, schemaName, viewName)
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
      case USAGE_ON_SCHEMA => Right(grantUsageStatement("schema", s"$dbName.$schemaName", roleName))
      case SELECT_ON_VIEW  => Right(grantSelectOnViewStatement(dbName, schemaName, viewName, roleName))
      case DESCRIBE_VIEW   => Right(describeView(dbName, schemaName, viewName))
      case unsupportedOp   => Left(UnsupportedOperationError(unsupportedOp))
    }
  }.flatten

  def buildRefsStatement(
      descriptor: ProvisioningRequestDescriptor,
      refs: Seq[String],
      operation: OperationType
  ): Either[SnowflakeError, Seq[String]] = {
    logger.info("Starting building refs statements")
    for {
      component <- getComponent(descriptor)
      customViewStatement = getCustomViewStatement(component)
      dbName              = getCustomDatabaseName(customViewStatement) match {
        case Some(customDbName) => customDbName
        case _                  => getDatabaseName(descriptor, component.specific)
      }
      schemaName          = getCustomSchemaName(customViewStatement) match {
        case Some(customSchemaName) => customSchemaName
        case _                      => getSchemaName(descriptor, component.specific)
      }
      viewName <- getCustomViewName(customViewStatement) match {
        case Some(customViewName) => Right(customViewName)
        case _                    => getViewName(component)
      }
      roleName            = buildRoleName(dbName, schemaName, viewName)
    } yield operation match {
      case ASSIGN_ROLE   => Right(assignRoleToUserStatement(roleName, refs))
      case unsupportedOp => Left(UnsupportedOperationError(unsupportedOp))
    }
  }.flatten

  def describeView(dbName: String, schemaName: String, viewName: String): String =
    s"SELECT * FROM $dbName.$schemaName.$viewName"

  def createDatabaseStatement(dbName: String) = s"CREATE DATABASE IF NOT EXISTS $dbName;"

  def createSchemaStatement(dbName: String, schemaName: String) = s"CREATE SCHEMA IF NOT EXISTS $dbName.$schemaName;"

  def deleteSchemaStatement(dbName: String, schemaName: String) = s"DROP SCHEMA IF EXISTS $dbName.$schemaName;"

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
    s"""CREATE TABLE IF NOT EXISTS $dbName.$schemaName.${tableName.toUpperCase}
       |(
       | $columns$constraint
       |);""".stripMargin
  }

  def createTableSchemaInformationQuery(): String = s"""
            SELECT C.COLUMN_NAME, C.DATA_TYPE,C.IS_NULLABLE,NULL CONSTRAINT_TYPE, C.TABLE_NAME
            FROM {CATALOG}.INFORMATION_SCHEMA.COLUMNS as C
            WHERE C.TABLE_NAME IN ({TABLES}) and C.TABLE_SCHEMA = ? and C.TABLE_CATALOG =?
            """

  def getPrimaryKeyInformationQuery(): String = s"""
            SHOW PRIMARY KEYS IN TABLE {CATALOG}.{SCHEMA}.{TABLE}
            """

  def getUniqueKeyInformationQuery(): String = s"""
            SHOW UNIQUE KEYS IN TABLE {CATALOG}.{SCHEMA}.{TABLE}
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

    Option(TableSpec(tableName, existingSchemaInformation))
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
      .map(c => s"""ALTER TABLE IF EXISTS $dbName.$schemaName.${tableName.toUpperCase}
                   | ADD COLUMN
                   | ${c.toColumnStatement}
                   | ;""".stripMargin)

    val dropColumnStatements = columnsToDelete.map(_.toDropColumnStatement)
      .map(st => s"""ALTER TABLE IF EXISTS $dbName.$schemaName.${tableName.toUpperCase}
                    | $st
                    | ;""".stripMargin)

    val updateColumnTypeStatements = columnsToUpdateType
      .map(c => s"""ALTER TABLE IF EXISTS $dbName.$schemaName.${tableName.toUpperCase}
                   | ${c.toUpdateColumnStatementDataType}
                   | ;""".stripMargin)

    val updateColumnConstraintStatements = columnsToRemoveConstraint
      .map(c => s"""ALTER TABLE $dbName.$schemaName.${tableName.toUpperCase}
                   | ${c.toRemoveColumnStatementConstraint}
                   |;""".stripMargin) ++
      columnsToAddConstraint.map(c => s"""ALTER TABLE $dbName.$schemaName.${tableName.toUpperCase}
                                         | ${c.toAddColumnStatementConstraint}
                                         |;""".stripMargin)

    Right(addColumnsStatement ++ dropColumnStatements ++ updateColumnTypeStatements ++ updateColumnConstraintStatements)
  }

  def checkSchemaChanges(
      descriptor: ProvisioningRequestDescriptor,
      component: ComponentDescriptor,
      connection: Connection,
      existingSchemaInformation: List[TableSpec],
      tables: List[TableSpec],
      dropColumnEnabled: Boolean
  ): Either[SnowflakeError, List[SchemaChanges]] = {

    val dbName     = getDatabaseName(descriptor, component.specific)
    val schemaName = getSchemaName(descriptor, component.specific)

    val results = tables.foldLeft[Either[SnowflakeError, List[SchemaChanges]]](Right(Nil)) { (acc, table) =>
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
    val statementErrors           = new ListBuffer[String]()
    val incomingShame             = incomingShameOriginal.map(col => col.copy(name = col.name.toUpperCase()))
    val existingColumns           = existingSchemaInformation.get.schema.map(_.name).toSet
    val newColumns                = incomingShame.map(_.name).toSet

    val columnsToAdd    = incomingShame.filter(spec => !existingColumns.contains(spec.name))
    val columnsToDelete = existingSchemaInformation.get.schema.filter(spec => !newColumns.contains(spec.name))

    if (!dropColumnEnabled && !columnsToDelete.isEmpty) {
      statementErrors += "It's not possible to drop any existing columns"
    }

    val columnsToUpdate = incomingShame
      .filter(spec => existingColumns.contains(spec.name) && newColumns.contains(spec.name))

    val existingPrimaryKeyColumns = existingSchemaInformation.get.schema
      .filter(c => c.constraint.contains(ConstraintType.PRIMARY_KEY)).sortBy(c => c.name)

    val newPrimaryKeyColumns = incomingShame.filter(c => c.constraint.contains(ConstraintType.PRIMARY_KEY))
      .distinctBy(c => c.name).sortBy(c => c.name)

    if (!existingPrimaryKeyColumns.sortBy(_.name).sameElements(newPrimaryKeyColumns.sortBy(_.name))) statementErrors +=
      "PRIMARY KEY is changed and this operation is incompatible"

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

    if (!statementErrors.isEmpty) {
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
    s"DROP TABLE IF EXISTS $dbName.$schemaName.${tableName.toUpperCase};"

  def createTablesStatement(dbName: String, schemaName: String, tables: List[TableSpec]): Seq[String] = tables
    .map(table => createTableStatement(dbName, schemaName, table.tableName, table.schema))

  def updateTablesStatement(
      dbName: String,
      schemaName: String,
      tables: List[TableSpec],
      schemaChanges: List[SchemaChanges]
  ): Either[SnowflakeError, Seq[String]] = {

    val results = tables.foldLeft[Either[SnowflakeError, List[String]]](Right(Nil)) { (acc, table) =>
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

  def deleteTablesStatement(dbName: String, schemaName: String, tables: List[TableSpec]): Seq[String] = tables
    .map(table => deleteTableStatement(dbName, schemaName, table.tableName))

  def createRoleStatement(roleName: String) = s"CREATE ROLE IF NOT EXISTS $roleName;"

  def grantUsageStatement(resource: String, resourceName: String, roleName: String): String =
    s"GRANT USAGE ON $resource $resourceName TO ROLE $roleName;"

  def grantSelectOnViewStatement(dbName: String, schemaName: String, viewName: String, roleName: String): String =
    s"GRANT SELECT ON VIEW $dbName.$schemaName.$viewName TO ROLE $roleName;"

  def deleteRoleStatement(roleName: String): String = s"DROP ROLE IF EXISTS $roleName;"

  def assignRoleToUserStatement(roleName: String, users: Seq[String]): Seq[String] = users
    .map(user => s"GRANT ROLE $roleName TO USER \"$user\";")

  def buildRoleName(dbName: String, schemaName: String, viewName: String): String =
    s"${dbName}_${schemaName}_${viewName}_ACCESS"

  def createViewStatement(
      viewName: String,
      dbName: String,
      schemaName: String,
      tableName: String,
      schema: List[ColumnSchemaSpec]
  ): String = s"CREATE VIEW IF NOT EXISTS $dbName.$schemaName.$viewName AS " +
    s"(SELECT ${schema.map(_.name).mkString(",\n")} FROM $dbName.$schemaName.$tableName);"

  def deleteViewStatement(dbName: String, schemaName: String, viewName: String) =
    s"DROP VIEW IF EXISTS $dbName.$schemaName.$viewName"

  def updateViewStatement(
      dbName: String,
      schemaName: String,
      viewName: String,
      tableName: String,
      schema: List[ColumnSchemaSpec]
  ) = s"ALTER VIEW IF EXISTS $dbName.$schemaName.$viewName AS " +
    s"(SELECT ${schema.map(_.name).mkString(",\n")} FROM $dbName.$schemaName.$tableName);"

  def getDatabaseName(descriptor: ProvisioningRequestDescriptor, specific: Json): String =
    specific.hcursor.downField("database").as[String] match {
      case Left(_)         =>
        logger.info("The database is not in the specific field, fetching domain...")
        descriptor.dataProduct.domain.toUpperCase
      case Right(database) =>
        logger.info("Database field found")
        database
    }

  def getSchemaName(descriptor: ProvisioningRequestDescriptor, specific: Json): String =
    specific.hcursor.downField("schema").as[String] match {
      case Left(_)           =>
        logger.info("Database schema not found in specific field, taking data product name and version...")
        s"${descriptor.dataProduct.name.toUpperCase.replaceAll(" ", "")}_${descriptor.dataProduct.version.split('.')(0)}"
      case Right(schemaName) =>
        logger.info("Database schema found")
        schemaName
    }

  def getTableSchema(component: ComponentDescriptor): Either[SnowflakeError, List[ColumnSchemaSpec]] = for {
    dataContract <- component.header.hcursor.downField("dataContract").as[Json].left.map(error =>
      ParseError(Some(component.toString), Some("dataContract"), List(s"Parse error: ${error.getMessage}"))
    )
    schema       <- dataContract.hcursor.downField("schema").as[List[ColumnSchemaSpec]].left
      .map(error => ParseError(Some(component.toString), Some("schema"), List(s"Parse error: ${error.getMessage}")))
  } yield schema

  def getTableName(component: ComponentDescriptor): Either[SnowflakeError, String] = component.specific.hcursor
    .downField("tableName").as[String].left.map(error =>
      GetTableNameError(
        List(error.message),
        List("If a custom view query is not provided, please provide a tableName in the specific section")
      )
    )

  def getViewName(component: ComponentDescriptor): Either[GetViewNameError, String] = component.specific.hcursor
    .downField("viewName").as[String].left
    .map(error => GetViewNameError(List(error.message), List("Please provide the view name in the specific section")))

  def getCustomViewStatement(component: ComponentDescriptor): String =
    component.specific.hcursor.downField("customView").as[String] match {
      case Right(customView) => customView
      case Left(_)           => ""
    }

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

  def getTables(component: ComponentDescriptor): Either[SnowflakeError, List[TableSpec]] =
    component.specific.hcursor.downField("tables").as[List[TableSpec]] match {
      case Right(tables) => Right(tables)
      case Left(error)   =>
        Left(ParseError(Some(component.toString), Some("schema"), List(s"Parse error: ${error.getMessage}")))
    }

  def getComponent(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, ComponentDescriptor] = descriptor
    .getComponentToProvision.toRight(GetComponentError(descriptor.componentIdToProvision))
}
