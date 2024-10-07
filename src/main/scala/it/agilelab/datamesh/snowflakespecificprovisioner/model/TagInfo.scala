package it.agilelab.datamesh.snowflakespecificprovisioner.model

import io.circe.generic.semiauto.deriveDecoder
import io.circe.{Decoder, Encoder, Json}
import it.agilelab.datamesh.snowflakespecificprovisioner.system.ApplicationConfiguration._

case class TagInfo(
    tagFQN: String,
    source: String,
    labelType: String,
    state: String,
    additionalProperties: Map[String, String] = Map.empty
)

object TagInfo {

  def apply(snowflakeTag: (String, String)): TagInfo = {
    val (name, value) = snowflakeTag
    if (snowflakeTagNameField.equals("tagFQN")) {
      TagInfo(name, "Tag", "Manual", "Confirmed", Map(snowflakeTagValueField -> value))
    } else {
      // The key of Snowflake tags is not set to tagFQN so we save it as another property,
      // but we still use it as tagFQN as this is a required field on OpenMetadata tags
      TagInfo(name, "Tag", "Manual", "Confirmed", Map(snowflakeTagValueField -> value, snowflakeTagNameField -> name))
    }
  }

  implicit val encodeTagInfo: Encoder[TagInfo] = (a: TagInfo) =>
    Json.fromFields(
      List(
        "tagFQN"    -> Json.fromString(a.tagFQN),
        "source"    -> Json.fromString(a.source),
        "labelType" -> Json.fromString(a.labelType),
        "state"     -> Json.fromString(a.state)
      ) ++ a.additionalProperties.toList.map { case (key, value) => key -> Json.fromString(value) }
    )
  implicit val decodeTagInfo: Decoder[TagInfo] = deriveDecoder[TagInfo]
}
