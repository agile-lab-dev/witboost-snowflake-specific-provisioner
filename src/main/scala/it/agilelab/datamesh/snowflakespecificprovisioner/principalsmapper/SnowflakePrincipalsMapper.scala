package it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper

import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.PrincipalMappingError

class SnowflakePrincipalsMapper extends PrincipalsMapper[String] {

  /** This method defines the main mapping logic
   *
   *  @param subjects Set of subjects, i.e. witboost users and groups
   *  @return the mapping. For each subject, either an PrincipalMappingError, or the successfully mapped principal is returned
   */
  override def map(subjects: Set[String]): Map[String, Either[PrincipalMappingError, String]] = subjects.map {
    case ref @ s"user:$cleanUser" =>
      val underscoreIndex = cleanUser.lastIndexOf("_")
      val mappedUser      =
        if (underscoreIndex.equals(-1)) { cleanUser }
        else { cleanUser.substring(0, underscoreIndex) + "@" + cleanUser.substring(underscoreIndex + 1) }
      ref -> Right(mappedUser.toUpperCase)
    case ref @ s"group:$_"        => ref -> Left(PrincipalMappingError(
        Some(ref),
        List("Groups are not supported by Snowflake to grant roles"),
        List("Retry the operation by granting individuals rather than the whole group")
      ))
    case ref => ref -> Left(PrincipalMappingError(Some(ref), List("Unexpected subject in user principal mapping")))
  }.toMap
}
