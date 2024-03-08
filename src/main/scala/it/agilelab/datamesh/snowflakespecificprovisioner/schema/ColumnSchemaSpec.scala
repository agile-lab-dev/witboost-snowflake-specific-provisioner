package it.agilelab.datamesh.snowflakespecificprovisioner.schema

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.ConstraintType.ConstraintType
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.DataType.DataType

case class ColumnSchemaSpec(name: String, dataType: DataType, constraint: Option[ConstraintType] = None) {

  def toColumnStatement: String = constraint match {
    case Some(value) => value match {
        case ConstraintType.NOT_NULL    => s"$name $dataType NOT NULL"
        case ConstraintType.PRIMARY_KEY => s"$name $dataType" // Primary key constraint is created at table level
        case ConstraintType.UNIQUE      => s"$name $dataType UNIQUE"
      }
    case _           => s"$name $dataType"
  }

  def toUpdateColumnStatementDataType: String = s"ALTER COLUMN $name SET DATA TYPE $dataType"

  def toAddColumnStatementConstraint: String = constraint match {
    case Some(value) => value match {
        case ConstraintType.NOT_NULL    => s"ALTER COLUMN $name SET NOT NULL"
        case ConstraintType.PRIMARY_KEY =>
          s"ALTER COLUMN $name $dataType" // Primary key constraint is created at table level
        case ConstraintType.UNIQUE      =>
          s"ALTER COLUMN $name SET COMMENT ='UNIQUE'" // UNIQUE constraint is just a comment on Snowflake and is not enforced
      }
    case _           => s"$name $dataType"
  }

  def toRemoveColumnStatementConstraint: String = constraint match {
    case Some(value) => value match {
        case ConstraintType.NOT_NULL    => s"ALTER COLUMN $name DROP NOT NULL"
        case ConstraintType.PRIMARY_KEY =>
          s"ALTER COLUMN $name $dataType" // Primary key constraint is created at table level
        case ConstraintType.UNIQUE      =>
          s"DROP UNIQUE ($name)" // UNIQUE constraint is just a comment on Snowflake and is not enforced
      }
    case _           => s"$name $dataType"
  }
  def toDropColumnStatement: String             = s"DROP COLUMN $name"
}

object ColumnSchemaSpec {
  implicit val columnSchemaSpecDecoder: Decoder[ColumnSchemaSpec] = deriveDecoder[ColumnSchemaSpec]
}
