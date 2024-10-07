package it.agilelab.datamesh.snowflakespecificprovisioner.schema

import io.circe.{Decoder, Encoder}
import it.agilelab.datamesh.snowflakespecificprovisioner.schema

case class ConstraintType()

object ConstraintType extends Enumeration {
  type ConstraintType = Value

  implicit val constraintTypeDecoder: Decoder[ConstraintType.Value] = Decoder.decodeEnumeration(ConstraintType)
  implicit val constraintTypeEncoder: Encoder[ConstraintType.Value] = Encoder.encodeEnumeration(ConstraintType)

  implicit def stringToConstraintType(s: String): ConstraintType = ConstraintType.withName(s)

  implicit def constraintTypeToString(c: ConstraintType): String = c.toString

  val NOT_NULL: schema.ConstraintType.Value    = Value("NOT_NULL")
  val PRIMARY_KEY: schema.ConstraintType.Value = Value("PRIMARY_KEY")
  val UNIQUE: schema.ConstraintType.Value      = Value("UNIQUE")
}
