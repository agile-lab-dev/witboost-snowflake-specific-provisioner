package it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SnowflakePrincipalsMapperSpec extends AnyFlatSpec with Matchers {

  "mapUserToSnowflakeUser method" should "return the correct Snowflake user. Use case 1" in {
    val user          = "user:marco.pisasale_agilelab.it"
    val snowflakeUser = SnowflakePrincipalsMapper.map(Set(user))
    val expectedRef   = "MARCO.PISASALE@AGILELAB.IT"

    snowflakeUser.foreach { case (_, mappedRef) =>
      mappedRef shouldBe a[Right[_, _]]
      mappedRef.foreach(_ shouldEqual expectedRef)
    }
  }

  "mapUserToSnowflakeUser method" should "return the correct Snowflake user. Use case 2" in {
    val user          = "user:marco_pisasale_agilelab.it"
    val snowflakeUser = SnowflakePrincipalsMapper.map(Set(user))
    val expectedRef   = "MARCO_PISASALE@AGILELAB.IT"

    snowflakeUser.foreach { case (_, mappedRef) =>
      mappedRef shouldBe a[Right[_, _]]
      mappedRef.foreach(_ shouldEqual expectedRef)
    }
  }

  "mapUserToSnowflakeUser method" should "return the left on wrong Snowflake refs" in {
    val wrong         = Set("marco.pisasale@agilelab.it", "group:bigData")
    val snowflakeUser = SnowflakePrincipalsMapper.map(wrong)

    snowflakeUser.foreach { case (_, mappedRef) => mappedRef shouldBe a[Left[_, _]] }
  }

}
