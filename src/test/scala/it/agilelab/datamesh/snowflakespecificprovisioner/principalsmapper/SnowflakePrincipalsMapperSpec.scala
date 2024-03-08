package it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SnowflakePrincipalsMapperSpec extends AnyFlatSpec with Matchers {

  "mapUserToSnowflakeUser method" should "return the correct Snowflake user. Use case 1" in {
    val user          = "user:user_agilelab.it"
    val snowflakeUser = SnowflakePrincipalsMapper.map(Set(user))
    val expectedRef   = "USER@AGILELAB.IT"

    snowflakeUser.foreach { case (_, mappedRef) =>
      mappedRef shouldBe a[Right[_, _]]
      mappedRef.foreach(_ shouldEqual expectedRef)
    }
  }

  "mapUserToSnowflakeUser method" should "return the correct Snowflake user. Use case 2" in {
    val user          = "user:user_name_agilelab.it"
    val snowflakeUser = SnowflakePrincipalsMapper.map(Set(user))
    val expectedRef   = "USER_NAME@AGILELAB.IT"

    snowflakeUser.foreach { case (_, mappedRef) =>
      mappedRef shouldBe a[Right[_, _]]
      mappedRef.foreach(_ shouldEqual expectedRef)
    }
  }

  "mapUserToSnowflakeUser method" should "return the left on wrong Snowflake refs" in {
    val wrong         = Set("user@agilelab.it", "group:bigData")
    val snowflakeUser = SnowflakePrincipalsMapper.map(wrong)

    snowflakeUser.foreach { case (_, mappedRef) => mappedRef shouldBe a[Left[_, _]] }
  }

}
