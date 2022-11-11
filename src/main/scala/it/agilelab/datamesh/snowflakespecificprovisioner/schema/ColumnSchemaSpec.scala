package it.agilelab.datamesh.snowflakespecificprovisioner.schema

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.ConstraintType.ConstraintType
import it.agilelab.datamesh.snowflakespecificprovisioner.schema.DataType.DataType

case class ColumnSchemaSpec(name: String, dataType: DataType, constraint: ConstraintType) {

  def toColumnStatement: String = constraint match {
    case ConstraintType.NOCONSTRAINT => s"$name $dataType"
    case ConstraintType.NULL         => s"$name $dataType NULL"
    case _                           => s"$name $dataType $constraint"
  }
}

object ColumnSchemaSpec {
  implicit val columnSchemaSpecDecoder: Decoder[ColumnSchemaSpec] = deriveDecoder[ColumnSchemaSpec]
}
