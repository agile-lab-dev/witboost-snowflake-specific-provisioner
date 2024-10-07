package it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector

import cats.Semigroup
import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import io.circe.parser.parse
import it.agilelab.datamesh.snowflakespecificprovisioner.api.dto.SnowflakeOutputPortDetailsDto
import it.agilelab.datamesh.snowflakespecificprovisioner.common.Constants
import it.agilelab.datamesh.snowflakespecificprovisioner.model.ComponentDescriptor.{OutputPort, StorageArea}
import it.agilelab.datamesh.snowflakespecificprovisioner.model._
import it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper.PrincipalsMapper
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.OperationType.{
  ASSIGN_ROLE,
  CREATE_DB,
  CREATE_ROLE,
  CREATE_SCHEMA,
  CREATE_TABLES,
  CREATE_TAGS,
  CREATE_VIEW,
  DELETE_ROLE,
  DELETE_TABLES,
  DELETE_VIEW,
  DESCRIBE_VIEW,
  SELECT_ON_VIEW,
  UPDATE_TABLES,
  USAGE_ON_DB,
  USAGE_ON_SCHEMA,
  USAGE_ON_WH
}
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.{ColumnSchemaSpec, DataType, SchemaChanges, TableSpec}
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.SnowflakeManagerImplicits.SeqEitherExecuteStatementOps
import it.agilelab.datamesh.snowflakespecificprovisioner.system.ApplicationConfiguration._
import it.agilelab.datamesh.snowflakespecificprovisioner.system.{ApplicationConfiguration, RealApplicationConfiguration}

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Right, Success, Try}

class SnowflakeManager(
    executor: QueryExecutor,
    queryBuilder: QueryHelper,
    snowflakeTableInformationHelper: SnowflakeTableInformationHelper,
    reverseProvisioning: ReverseProvisioning,
    principalsMapper: PrincipalsMapper[String]
) extends LazyLogging {

  val provisionInfoHelper = new ProvisionInfoHelper(RealApplicationConfiguration)

  def provisionOutputPort(
      descriptor: ProvisioningRequestDescriptor
  ): Either[SnowflakeError, Option[ProvisioningStatus]] = {
    logger.info("Starting output port provisioning")
    for {
      connection         <- executor.getConnection
      dbStatement        <- queryBuilder.buildOutputPortStatement(descriptor, CREATE_DB)
      _                  <- executor.executeStatement(connection, dbStatement)
      schemaStatement    <- queryBuilder.buildOutputPortStatement(descriptor, CREATE_SCHEMA)
      _                  <- executor.executeStatement(connection, schemaStatement)
      viewStatement      <- queryBuilder.buildOutputPortStatement(descriptor, CREATE_VIEW)
      _                  <- executor.executeStatement(connection, viewStatement)
      existingColumnTags <-
        if (!ignoreSnowflakeTags) getViewExistingColumnTags(connection, descriptor) else Right(List.empty)
      tagStatements      <-
        if (!ignoreSnowflakeTags) queryBuilder.buildOutputPortTagStatement(descriptor, existingColumnTags, CREATE_TAGS)
        else Right(Seq.empty[String])
      createTagStatement    = tagStatements.filter(t => t.startsWith("CREATE TAG")).distinct
      addColumnTagStatement = tagStatements.filter(t => !t.startsWith("CREATE TAG"))
      _                      <- createTagStatement match {
        case statement if statement.nonEmpty => executor.traverseMultipleStatements(connection, createTagStatement)
        case _                               => Right(GetComponentError(
            descriptor.componentIdToProvision,
            List("Skipping tag creation - no information provided")
          ))
      }
      _                      <- addColumnTagStatement match {
        case statement if statement.nonEmpty => executor.traverseMultipleStatements(connection, addColumnTagStatement)
        case _                               => Right(GetComponentError(
            descriptor.componentIdToProvision,
            List("Skipping tag creation - no information provided")
          ))
      }
      _                      <- executeUpdateAcl(descriptor, Seq(descriptor.dataProduct.dataProductOwner))
      _                      <- validateSchema(descriptor).left.map { err =>
        unprovisionOutputPort(descriptor)
        err
      }
      outputProvisioningInfo <- provisionInfoHelper.getProvisioningInfo(descriptor)
      _                      <- Right(descriptor)
    } yield Some(ProvisioningStatus(
      ProvisioningStatusEnums.StatusEnum.COMPLETED,
      "Provisioning completed",
      Some(Info(publicInfo = SnowflakeOutputPortDetailsDto.encode(outputProvisioningInfo), privateInfo = "")),
      None
    ))
  }

  def unprovisionOutputPort(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] = {
    logger.info("Starting output port unprovisioning")
    for {
      connection          <- executor.getConnection
      deleteViewStatement <- queryBuilder.buildOutputPortStatement(descriptor, DELETE_VIEW)
      _                   <- executor.executeStatement(connection, deleteViewStatement)
      deleteRoleStatement <- queryBuilder.buildOutputPortStatement(descriptor, DELETE_ROLE)
      _                   <- executor.executeStatement(connection, deleteRoleStatement)
    } yield ()
  }

  /** Upserts the role with the appropriate grants and assigns the role to the incoming refs.
   *  Method will fail if an invalid ref is receiving, but only after granting the role to the valid received refs
   *
   *  @param descriptor Descriptor with all the output port information
   *  @param refs       List of principals to be granted the select on view role
   *  @return Either an error if something failed during the process, or a sequence of the refs that were successfully granted the role
   */
  def updateAclOutputPort(
      descriptor: ProvisioningRequestDescriptor,
      refs: Seq[String]
  ): Either[SnowflakeError, Seq[String]] = {
    logger.info("Starting executing executeUpdateAcl for users: {}...", refs.mkString(", "))
    for {
      _                    <- upsertRole(descriptor)
      connection           <- executor.getConnection
      mappedRefs           <- {
        logger.info("Mapping refs to Snowflake users: {}", refs)
        val refMapping = principalsMapper.map(refs.toSet).values.partitionMap(identity)
        refMapping._1.foreach { err =>
          logger.warn("Error while mapping ref \"{}\": {}", err.input.getOrElse(""), err.getMessage)
        }
        Right(refMapping)
      }
      // we could have the same mapping for different witboost identities, .toSet.toList removes eventual duplicates
      assignRoleStatements <- queryBuilder.buildRefsStatement(descriptor, mappedRefs._2.toSet.toList, ASSIGN_ROLE)
      grantedRefs <- executor.executeMultipleStatements(connection, assignRoleStatements).zip(mappedRefs._2.toSet).map {
        case (Left(err), _)  => Left(err)
        case (Right(_), ref) => Right(ref)
      }.mergeSequence()
      /* Even if all grant executions are ok, if there was a failed mapped ref we ignored, we still have to throw an
       * error */
      _           <- Semigroup.combineAllOption(mappedRefs._1).toLeft(())
    } yield grantedRefs
  }

  private def upsertRole(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] = for {
    connection             <- executor.getConnection
    createRoleStatement    <- queryBuilder.buildOutputPortStatement(descriptor, CREATE_ROLE)
    _                      <- executor.executeStatement(connection, createRoleStatement)
    usageOnWhStatement     <- queryBuilder.buildOutputPortStatement(descriptor, USAGE_ON_WH)
    _                      <- executor.executeStatement(connection, usageOnWhStatement)
    usageOnDbStatement     <- queryBuilder.buildOutputPortStatement(descriptor, USAGE_ON_DB)
    _                      <- executor.executeStatement(connection, usageOnDbStatement)
    usageOnSchemaStatement <- queryBuilder.buildOutputPortStatement(descriptor, USAGE_ON_SCHEMA)
    _                      <- executor.executeStatement(connection, usageOnSchemaStatement)
    selectOnViewStatement  <- queryBuilder.buildOutputPortStatement(descriptor, SELECT_ON_VIEW)
    _                      <- executor.executeStatement(connection, selectOnViewStatement)
  } yield ()

  def provisionStorage(
      descriptor: ProvisioningRequestDescriptor
  ): Either[SnowflakeError, Option[ProvisioningStatus]] = {
    logger.info("Starting storage provisioning")
    for {
      connection          <- executor.getConnection
      dbStatement         <- queryBuilder.buildStorageStatement(descriptor, CREATE_DB)
      _                   <- executor.executeStatement(connection, dbStatement)
      schemaStatement     <- queryBuilder.buildStorageStatement(descriptor, CREATE_SCHEMA)
      _                   <- executor.executeStatement(connection, schemaStatement)
      existingTableSchema <- getExistingTableSchema(connection, descriptor)
      existingColumnTags  <-
        if (!ignoreSnowflakeTags) getExistingColumnTags(connection, descriptor) else Right(List.empty)
      component           <- queryBuilder.getComponent(descriptor).flatMap {
        case storageArea: StorageArea => Right(storageArea)
        case _ => Left(GetComponentError(descriptor.componentIdToProvision, List(s"Unsupported component type!")))
      }
      tables              <- queryBuilder.getTables(component)
      schemaChanges       <-
        if (existingTableSchema.nonEmpty) queryBuilder
          .checkSchemaChanges(descriptor, component, connection, existingTableSchema, tables, dropColumnEnabled = true)
        else Right(List.empty[SchemaChanges])
      updateStatement     <-
        if (schemaChanges.nonEmpty) queryBuilder
          .buildMultipleStatement(descriptor, UPDATE_TABLES, Some(schemaChanges), None)
        else Right(Seq.empty[String])
      tablesStatement     <- queryBuilder.buildMultipleStatement(descriptor, CREATE_TABLES, None, None)
      tagsStatement       <-
        if (!ignoreSnowflakeTags) queryBuilder
          .buildMultipleStatement(descriptor, CREATE_TAGS, None, Some(existingColumnTags))
        else Right(Seq.empty[String])
      createTagStatement    = tagsStatement.filter(t => t.startsWith("CREATE TAG")).distinct
      addColumnTagStatement = tagsStatement.filter(t => !t.startsWith("CREATE TAG"))
      _ <- createTagStatement match {
        case statement if statement.nonEmpty => executor.traverseMultipleStatements(connection, createTagStatement)
        case _                               => Right(GetComponentError(
            descriptor.componentIdToProvision,
            List("Skipping tag creation - no information provided")
          ))
      }
      fullStatement = tablesStatement ++ updateStatement
      _ <- fullStatement match {
        case statement if statement.nonEmpty => executor.traverseMultipleStatements(connection, fullStatement)
        case _                               => Left(GetComponentError(
            descriptor.componentIdToProvision,
            List("Skipping table creation - no information provided")
          ))
      }
      _ <- addColumnTagStatement match {
        case statement if statement.nonEmpty => executor.traverseMultipleStatements(connection, addColumnTagStatement)
        case _                               => Right(GetComponentError(
            descriptor.componentIdToProvision,
            List("Skipping tag creation - no information provided")
          ))
      }
    } yield None
  }

  def unprovisionStorage(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] = {
    logger.info("Starting storage unprovisioning")
    for {
      connection      <- executor.getConnection
      tablesStatement <- queryBuilder.buildMultipleStatement(descriptor, DELETE_TABLES, None, None)
      _               <- tablesStatement match {
        case statement if statement.nonEmpty => executor.traverseMultipleStatements(connection, tablesStatement)
        case _                               => Right(logger.info("Skipping table deletion - no tables to delete"))
      }
    } yield ()
  }

  def executeProvision(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Option[ProvisioningStatus]] =
    descriptor.getComponentToProvision match {
      case Some(component) => component match {
          case _: StorageArea  => provisionStorage(descriptor)
          case _: OutputPort   => provisionOutputPort(descriptor)
          case unsupportedKind => Left(ProvisioningValidationError(
              descriptor.getComponentToProvision.map(_.toString),
              Some(Constants.KIND_FIELD),
              List("Unsupported component kind: " + unsupportedKind.kind),
              List("The Snowflake Specific Provisioner can only deploy storage and output port components")
            ))
        }
      case _               => Left(ParseError(
          Some(descriptor.dataProduct.toString),
          Some(descriptor.componentIdToProvision),
          List("The yaml is not a correct Provisioning Request: ")
        ))
    }

  def executeUnprovision(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] =
    descriptor.getComponentToProvision match {
      case Some(component) => component match {
          case _: StorageArea  => unprovisionStorage(descriptor)
          case _: OutputPort   => unprovisionOutputPort(descriptor)
          case unsupportedKind => Left(ProvisioningValidationError(
              descriptor.getComponentToProvision.map(_.toString),
              Some(Constants.KIND_FIELD),
              List("Unsupported component kind: " + unsupportedKind.kind),
              List("The Snowflake Specific Provisioner can only undeploy storage and output port components")
            ))
        }
      case _               => Left(ParseError(
          Some(descriptor.dataProduct.toString),
          Some(descriptor.componentIdToProvision),
          List("The yaml is not a correct Provisioning Request: ")
        ))
    }

  def executeUpdateAcl(
      descriptor: ProvisioningRequestDescriptor,
      refs: Seq[String]
  ): Either[SnowflakeError, Seq[String]] = {
    logger.info("Starting executing executeUpdateAcl for users: {}", refs.mkString(", "))
    descriptor.getComponentToProvision match {
      case Some(component) => component match {
          case _: OutputPort   => updateAclOutputPort(descriptor, refs)
          case unsupportedKind => Left(ProvisioningValidationError(
              descriptor.getComponentToProvision.map(_.toString),
              Some(Constants.KIND_FIELD),
              List("Unsupported component kind: " + unsupportedKind.kind),
              List("The Snowflake Specific Provisioner can only update ACL on output port components")
            ))
        }
      case _               => Left(ParseError(
          Some(descriptor.dataProduct.toString),
          Some(descriptor.componentIdToProvision),
          List("The yaml is not a correct Provisioning Request: ")
        ))
    }
  }

  def executeReverseProvisioning(
      reverseProvisioningRequest: ReverseProvisioningRequest
  ): Either[SnowflakeError, Option[ReverseProvisioningStatus]] = {
    logger.info("Starting executing reverse provisioning with request: {}", reverseProvisioningRequest)
    reverseProvisioningRequest.useCaseTemplateId match {
      case ApplicationConfiguration.storageAreaUseCaseTemplateId => reverseProvisioningRequest.params match {
          case Some(value) => parse(value.toString).left
              .map(err => ParseError(Some(value.toString), None, List(err.getMessage), List.empty))
              .flatMap { jsonResult =>
                val result = reverseProvisioning.executeReverseProvisioningStorageArea(jsonResult).map { success =>
                  Some(ReverseProvisioningStatus(
                    ReverseProvisioningStatusEnums.StatusEnum.COMPLETED,
                    success.getOrElse(Json.Null)
                  ))
                }
                logger.info("Storage Area reverse provisioning result: {}", result)
                result
              }
          case None        => Left(ParseError(None, None, List.empty, List.empty))
        }
      case ApplicationConfiguration.outputPortUseCaseTemplateId  => reverseProvisioningRequest.params match {
          case Some(value) => parse(value.toString).left
              .map(err => ParseError(Some(value.toString), None, List(err.getMessage), List.empty))
              .flatMap { jsonResult =>
                val result = reverseProvisioning.executeReverseProvisioningOutputPort(jsonResult).map { success =>
                  Some(ReverseProvisioningStatus(
                    ReverseProvisioningStatusEnums.StatusEnum.COMPLETED,
                    success.getOrElse(Json.Null)
                  ))
                }
                logger.info("Output Port reverse provisioning result: {}", result)
                result
              }
          case None        => Left(ParseError(None, None, List.empty, List.empty))
        }
      case _                                                     => Left(ParseError(
          Some(reverseProvisioningRequest.useCaseTemplateId),
          None,
          List("Unsupported useCaseTemplateId. Please verify whether the templateId provided is appropriate.")
        ))
    }
  }

  /** This methods checks the consistency of the descriptor
   *  @param descriptor the request descriptor
   *  @return a SnowflakeError if the validation fails, Unit otherwise
   */
  def validateDescriptor(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] = queryBuilder
    .getComponent(descriptor).flatMap {
      case outputPort: OutputPort =>
        val customViewStatement = queryBuilder.getCustomViewStatement(outputPort)
        if (customViewStatement.isEmpty) {
          logger.info("No custom view found. Skipping schema validation")
          Right(())
        } else {
          for {
            // Retrieves view name both from the custom view and from the view name specific field
            customViewName <- queryBuilder.getCustomViewName(customViewStatement).toRight(ParseError(
              Some(customViewStatement),
              None,
              List("Error while retrieving the view name from the custom view statement")
            ))
            viewName       <- queryBuilder.getViewName(outputPort)
            // Compares custom view name and view name and these must be equal
            result         <-
              if (!viewName.equals(customViewName)) {
                val problem = "The view name from the custom statement (" + customViewName +
                  ") does not match with the one specified inside the descriptor (" + viewName + ")"
                logger.info(problem)
                Left(SchemaValidationError(descriptor.getComponentToProvision.map(_.toString), List(problem)))
              } else Right(())
          } yield result
        }
      case _: StorageArea         => Right(())
      case _ => Left(GetComponentError(descriptor.componentIdToProvision, List("Unsupported component type!")))
    }

  /** This methods checks the consistency of the specific fields of the descriptor. The validation logic depends on the Kind
   *  @param descriptor the request descriptor
   *  @return a SnowflakeError if the validation fails, Unit otherwise
   */
  def validateSpecificFields(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] =
    queryBuilder.getComponent(descriptor) match {
      case Right(component) => component match {
          case storageArea: StorageArea => queryBuilder.getTables(storageArea).map(_ => ())
          case outputPort: OutputPort   => queryBuilder.getTableSchema(outputPort).map(_ => ())
          case unsupported              =>
            Left(ParseError(problems = List(s"The specified kind ${unsupported.kind} is not supported")))
        }
      case Left(_)          => Left(ParseError(Option("Invalid component type!")))
    }

  def getExistingTableSchema(
      connection: Connection,
      descriptor: ProvisioningRequestDescriptor
  ): Either[SnowflakeError, List[TableSpec]] = queryBuilder.getComponent(descriptor) match {
    case Right(component) => component match {
        case storageArea: StorageArea =>
          val database = queryBuilder.getDatabaseName(descriptor, storageArea.specific)
          val schema   = queryBuilder.getSchemaName(descriptor, storageArea.specific)
          queryBuilder.getTables(storageArea).flatMap { tables =>
            val tableNamesForSql = tables.map(_.tableName)
            snowflakeTableInformationHelper.getExistingMetaData(connection, database, schema, tableNamesForSql)
          }
        case _                        => Left(ParseError(Option("Invalid component type!")))
      }
    case Left(_)          => Left(ParseError(Option("Cannot perform this operation!")))
  }

  def getExistingColumnTags(
      connection: Connection,
      descriptor: ProvisioningRequestDescriptor
  ): Either[SnowflakeError, List[TableSpec]] = {

    def readResultSetRecursively(resultSet: ResultSet, acc: List[TableSpec]): List[TableSpec] =
      if (!resultSet.next()) acc
      else {
        val tableName  = resultSet.getString("TABLE_NAME")
        val columnName = resultSet.getString("COLUMN_NAME")
        val tagName    = resultSet.getString("TAG_NAME")
        val tagValue   = resultSet.getString("TAG_VALUE")

        val tagsMap = Map(tagName -> tagValue)

        val tags = ListBuffer[Map[String, String]]()
        tags += tagsMap

        val existing = acc.find(a => a.tableName.equals(tableName))
        if (existing.isEmpty) readResultSetRecursively(
          resultSet,
          acc :+
            TableSpec(tableName, List(ColumnSchemaSpec(columnName, DataType.TEXT, None, Option(tags.toList))), None)
        )
        else {
          val newSpec     = existing.get.schema ++
            List(ColumnSchemaSpec(columnName, DataType.TEXT, None, Option(tags.toList)))
          val clearedList = acc.filterNot(a => a.tableName.equals(tableName))
          readResultSetRecursively(resultSet, clearedList :+ TableSpec(tableName, newSpec, None))
        }

      }

    def readResultSet(resultSet: ResultSet): List[TableSpec] = readResultSetRecursively(resultSet, List.empty)
    val getSchemaInformationQuery                            = queryBuilder.getMultipleTablesTagsInformationQuery()
    for {
      component <- queryBuilder.getComponent(descriptor).flatMap {
        case storageArea: StorageArea => Right(storageArea)
        case _                        => Left(ParseError(problems = List("Cannot fetch the required component!")))
      }
      database = queryBuilder.getDatabaseName(descriptor, component.specific)
      schema = queryBuilder.getSchemaName(descriptor, component.specific)
      tables            <- queryBuilder.getTables(component)
      tagReferencesView <- snowflakeTableInformationHelper.getSnowflakeTagReferencesView(connection)
      tableNamesForSql = tables.map(table => s"'${table.tableName.toUpperCase}'").mkString(", ")
      schemaInformationResult <- Try {
        val alterPrepared          = connection.prepareStatement("ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'")
        val alterStatement         = alterPrepared.executeQuery()
        val querySchemaInformation = getSchemaInformationQuery.replace("{TABLES}", tableNamesForSql)
          .replace("{TAG_REFERENCES_VIEW}", tagReferencesView)
        val schemaInformationStatement = connection.prepareStatement(querySchemaInformation)
        schemaInformationStatement.setString(1, schema)
        schemaInformationStatement.setString(2, database)
        schemaInformationStatement.setString(3, "COLUMN")
        try {
          val schemaInformationResultSet = schemaInformationStatement.executeQuery()
          try Right(readResultSet(schemaInformationResultSet))
          finally {
            schemaInformationResultSet.close()
            alterStatement.close()
          }
        } finally schemaInformationStatement.close()
      } match {
        case Success(list)      => list
        case Failure(exception) =>
          Left(ExecuteStatementError(Some(getSchemaInformationQuery), List(exception.getMessage)))
      }
    } yield schemaInformationResult
  }

  def getViewExistingColumnTags(
      connection: Connection,
      descriptor: ProvisioningRequestDescriptor
  ): Either[SnowflakeError, List[TableSpec]] = {

    def readResultSetRecursively(resultSet: ResultSet, acc: List[TableSpec]): List[TableSpec] =
      if (!resultSet.next()) acc
      else {
        val tableName  = resultSet.getString("TABLE_NAME")
        val columnName = resultSet.getString("COLUMN_NAME")
        val tagName    = resultSet.getString("TAG_NAME")
        val tagValue   = resultSet.getString("TAG_VALUE")

        val tagsMap = Map(tagName -> tagValue)

        val tags = ListBuffer[Map[String, String]]()
        tags += tagsMap

        val existing = acc.find(a => a.tableName.equals(tableName))
        if (existing.isEmpty) readResultSetRecursively(
          resultSet,
          acc :+
            TableSpec(tableName, List(ColumnSchemaSpec(columnName, DataType.TEXT, None, Option(tags.toList))), None)
        )
        else {
          val newSpec     = existing.get.schema ++
            List(ColumnSchemaSpec(columnName, DataType.TEXT, None, Option(tags.toList)))
          val clearedList = acc.filterNot(a => a.tableName.equals(tableName))
          readResultSetRecursively(resultSet, clearedList :+ TableSpec(tableName, newSpec, None))
        }

      }

    def readResultSet(resultSet: ResultSet): List[TableSpec] = readResultSetRecursively(resultSet, List.empty)
    val getSchemaInformationQuery                            = queryBuilder.getSingleTableTagsInformationQuery()

    for {
      component <- queryBuilder.getComponent(descriptor).flatMap {
        case outputPort: OutputPort => Right(outputPort)
        case _                      => Left(ParseError(problems = List("Cannot fetch the required component")))
      }
      customViewStatement = queryBuilder.getCustomViewStatement(component)
      database = queryBuilder.getCustomDatabaseName(customViewStatement) match {
        case Some(customDbName) => customDbName
        case _                  => queryBuilder.getDatabaseName(descriptor, component.specific)
      }
      schema   = queryBuilder.getCustomSchemaName(customViewStatement) match {
        case Some(customSchemaName) => customSchemaName
        case _                      => queryBuilder.getSchemaName(descriptor, component.specific)
      }
      viewName <- queryBuilder.getCustomViewName(customViewStatement) match {
        case Some(customViewName) => Right(customViewName)
        case _                    => queryBuilder.getViewName(component)
      }
      tagReferencesView <- snowflakeTableInformationHelper.getSnowflakeTagReferencesView(connection)
      schemaInformationResult <- Try {
        val alterPrepared          = connection.prepareStatement("ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'")
        val alterStatement         = alterPrepared.executeQuery()
        val querySchemaInformation = getSchemaInformationQuery.replace("{TAG_REFERENCES_VIEW}", tagReferencesView)
        val schemaInformationStatement = connection.prepareStatement(querySchemaInformation)
        schemaInformationStatement.setString(1, schema)
        schemaInformationStatement.setString(2, database)
        schemaInformationStatement.setString(3, viewName.toUpperCase())
        schemaInformationStatement.setString(4, "COLUMN")

        try {
          val schemaInformationResultSet = schemaInformationStatement.executeQuery()
          try Right(readResultSet(schemaInformationResultSet))
          finally {
            schemaInformationResultSet.close()
            alterStatement.close()
          }
        } finally schemaInformationStatement.close()
      } match {
        case Success(list)      => list
        case Failure(exception) =>
          Left(ExecuteStatementError(Some(getSchemaInformationQuery), List(exception.getMessage)))
      }
    } yield schemaInformationResult
  }

  def validateSchema(descriptor: ProvisioningRequestDescriptor): Either[SnowflakeError, Unit] = for {
    _                 <- validateDescriptor(descriptor)
    connection        <- executor.getConnection
    validateStatement <- queryBuilder.buildOutputPortStatement(descriptor, DESCRIBE_VIEW)
    component         <- queryBuilder.getComponent(descriptor).flatMap {
      case outputPort: OutputPort => Right(outputPort)
      case _ => Left(GetComponentError(descriptor.componentIdToProvision, List("Unsupported component type!")))
    }
    result            <- for {
      // Retrieves schema from the component descriptor
      schema          <- queryBuilder.getTableSchema(component)
      // Compare existing schema on Snowflake with the schema in the descriptor using a Describe View statement
      alterStatement  <- executor.executeQuery(connection, queryBuilder.alterSessionToJsonResult)
      resultSet       <- executor.executeQuery(connection, validateStatement)
      executionResult <- Either.cond[SnowflakeError, Unit](
        compareSchemas(schema, resultSet),
        (), {
          val err = SchemaValidationError(
            descriptor.getComponentToProvision.map(_.toString),
            List(
              "Schema validation failed: the custom view schema doesn't match with the one specified inside the schema field on the descriptor"
            )
          )
          logger.error("Error, schemas are not equal: ", err)
          err
        }
      )
      _               <- {
        alterStatement.close()
        Right(())
      }
    } yield executionResult

  } yield result

  def compareSchemas(schemaFromDescriptor: List[ColumnSchemaSpec], schemaFromCustomView: ResultSet): Boolean = {
    val columnCount           = schemaFromCustomView.getMetaData.getColumnCount
    val descriptorColumnCount = schemaFromDescriptor.length
    if (columnCount.equals(descriptorColumnCount)) {
      val resultList    = (1 to columnCount).map { i =>
        ColumnSchemaSpec(
          schemaFromCustomView.getMetaData.getColumnName(i),
          DataType.snowflakeTypeToDataType(schemaFromCustomView.getMetaData.getColumnTypeName(i))
        )
      }
      val mappedResults = resultList.map(x => (x.name.toUpperCase(), x.dataType))
      schemaFromDescriptor.forall(y => mappedResults.contains((y.name.toUpperCase(), y.dataType)))
    } else { false }
  }

  def getConnection: Either[SnowflakeError, Connection] = Try {
    logger.info("Getting connection to Snowflake account...")
    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)
    properties.put("role", role)
    properties.put("account", account)
    properties.put("warehouse", warehouse)
    DriverManager.setLoginTimeout(snowflakeConnectionTimeout.getSeconds.intValue)
    DriverManager.getConnection(jdbcUrl, properties)
  } match {
    case Failure(exception)  => Left(GetConnectionError(
        List(exception.getMessage),
        List("Please check that the connection configuration variables are set correctly")
      ))
    case Success(connection) => Right(connection)
  }

}

object SnowflakeManagerImplicits {

  implicit class SeqEitherExecuteStatementOps[A](private val it: Seq[Either[ExecuteStatementError, A]]) {

    /** Merges a sequence of Either of [[ExecuteStatementError]] such that if there is any Left outcome, all of the errors are combined.
     *  Otherwise it returns the sequence of results
     *
     *  @param merger Implicit semigroup to combine [[ExecuteStatementError]] instances
     *  @return Either a combined [[ExecuteStatementError]] with all the gathered errors, or a sequence of the Right results
     */
    def mergeSequence()(implicit merger: Semigroup[ExecuteStatementError]): Either[ExecuteStatementError, Seq[A]] = {
      val partitioned = it.partitionMap(identity)
      merger.combineAllOption(partitioned._1) match {
        case Some(value) => Left(value)
        case None        => Right(partitioned._2)
      }
    }
  }
}
