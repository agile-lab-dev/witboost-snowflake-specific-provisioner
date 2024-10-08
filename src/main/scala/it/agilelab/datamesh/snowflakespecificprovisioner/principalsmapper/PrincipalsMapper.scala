package it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper

import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.PrincipalMappingError

sealed trait SnowflakePrincipals

case class SnowflakeUser(user: String)       extends SnowflakePrincipals
case class SnowflakeGroup(groupName: String) extends SnowflakePrincipals

trait PrincipalsMapper[PRINCIPAL <: SnowflakePrincipals] {

  /** This method defines the main mapping logic
   *
   *  @param subjects Set of subjects, i.e. witboost users and groups
   *  @return the mapping. For each subject, either an PrincipalMappingError, or the successfully mapped principal is returned
   */
  def map(subjects: Set[String]): Map[String, Either[PrincipalMappingError, PRINCIPAL]]
}
