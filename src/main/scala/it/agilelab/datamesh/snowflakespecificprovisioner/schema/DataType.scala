package it.agilelab.datamesh.snowflakespecificprovisioner.schema

import io.circe.Decoder
import it.agilelab.datamesh.snowflakespecificprovisioner.schema

object DataType extends Enumeration {
  type DataType = Value

  implicit val dataTypeDecoder: Decoder[DataType.Value] = Decoder.decodeEnumeration(DataType)

  implicit def dataTypeToString(d: DataType): String = d.toString

  implicit def stringToDataType(s: String): DataType = DataType.withName(s)

  val TEXT: schema.DataType.Value    = Value("TEXT")
  val NUMBER: schema.DataType.Value  = Value("NUMBER")
  val DATE: schema.DataType.Value    = Value("DATE")
  val BOOLEAN: schema.DataType.Value = Value("BOOLEAN")
}
