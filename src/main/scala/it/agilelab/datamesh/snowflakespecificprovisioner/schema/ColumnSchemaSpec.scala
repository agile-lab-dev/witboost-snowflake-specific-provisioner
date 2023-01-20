package it.agilelab.datamesh.snowflakespecificprovisioner.schema

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.ConstraintType.ConstraintType
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.DataType.DataType

case class ColumnSchemaSpec(name: String, dataType: DataType, constraint: Option[ConstraintType] = None) {

  def toColumnStatement: String = constraint match {
    case Some(value) => value match {
        case ConstraintType.NOT_NULL    => s"$name $dataType NOT NULL"
        case ConstraintType.PRIMARY_KEY => s"$name $dataType PRIMARY KEY"
        case ConstraintType.UNIQUE      => s"$name $dataType UNIQUE"
      }
    case _           => s"$name $dataType"
  }
}

object ColumnSchemaSpec {
  implicit val columnSchemaSpecDecoder: Decoder[ColumnSchemaSpec] = deriveDecoder[ColumnSchemaSpec]
}
