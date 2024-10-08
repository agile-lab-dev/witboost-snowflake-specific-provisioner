package it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper

import com.typesafe.scalalogging.LazyLogging
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.PrincipalMappingError

class SnowflakePrincipalsMapper extends PrincipalsMapper[SnowflakePrincipals] with LazyLogging {

  /** This method defines the main mapping logic
   *
   *  @param subjects Set of subjects, i.e. witboost users and groups
   *  @return the mapping. For each subject, either an PrincipalMappingError, or the successfully mapped principal is returned
   */
  override def map(subjects: Set[String]): Map[String, Either[PrincipalMappingError, SnowflakePrincipals]] = {
    logger.info("Mapping refs {} to Snowflake Users using Identity strategy", subjects)
    subjects.map {
      case ref @ s"user:$cleanUser"  =>
        val underscoreIndex = cleanUser.lastIndexOf("_")
        val mappedUser      =
          if (underscoreIndex.equals(-1)) { cleanUser }
          else { cleanUser.substring(0, underscoreIndex) + "@" + cleanUser.substring(underscoreIndex + 1) }
        ref -> Right(SnowflakeUser(mappedUser.toUpperCase))
      case ref @ s"group:$groupName" => ref -> Right(SnowflakeGroup(groupName.toUpperCase))
      case ref => ref -> Left(PrincipalMappingError(Some(ref), List("Unexpected subject in user principal mapping")))
    }.toMap
  }
}
