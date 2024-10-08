package it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SnowflakePrincipalsMapperSpec extends AnyFlatSpec with Matchers {

  val snowflakePrincipalsMapper = new SnowflakePrincipalsMapper

  "map method" should "return the correct Snowflake user. Use case 1" in {
    val user          = "user:user_agilelab.it"
    val snowflakeUser = snowflakePrincipalsMapper.map(Set(user))
    val expectedRef   = SnowflakeUser("USER@AGILELAB.IT")

    snowflakeUser.foreach { case (_, mappedRef) =>
      mappedRef shouldBe a[Right[_, _]]
      mappedRef.foreach(_ shouldEqual expectedRef)
    }
  }

  "map method" should "return the correct Snowflake user. Use case 2" in {
    val user          = "user:user_name_agilelab.it"
    val snowflakeUser = snowflakePrincipalsMapper.map(Set(user))
    val expectedRef   = SnowflakeUser("USER_NAME@AGILELAB.IT")

    snowflakeUser.foreach { case (_, mappedRef) =>
      mappedRef shouldBe a[Right[_, _]]
      mappedRef.foreach(_ shouldEqual expectedRef)
    }
  }

  "map method" should "return right on group mapping" in {
    val group          = "group:witboost"
    val snowflakeGroup = snowflakePrincipalsMapper.map(Set(group))
    val expectedRef    = SnowflakeGroup("WITBOOST")

    snowflakeGroup.foreach { case (_, mappedRef) =>
      mappedRef shouldBe a[Right[_, _]]
      mappedRef.foreach(_ shouldEqual expectedRef)
    }
  }

  "map method" should "return the left on wrong Snowflake refs" in {
    val wrong         = Set("user@agilelab.it", "owner:bigData")
    val snowflakeUser = snowflakePrincipalsMapper.map(wrong)

    snowflakeUser.foreach { case (_, mappedRef) => mappedRef shouldBe a[Left[_, _]] }
  }

}
