package it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper

import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.{QueryExecutor, QueryHelper}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PrincipalsMapperFactorySpec extends AnyFlatSpec with Matchers with MockFactory {

  "create method" should "return the identity based mapping" in {
    val queryExecutor = mock[QueryExecutor]
    val queryHelper   = mock[QueryHelper]

    val result = PrincipalsMapperFactory.create(queryExecutor, queryHelper)

    result shouldBe a[Right[_, SnowflakePrincipalsMapper]]
  }

  // TODO to add other tests, configuration refactor is needed

}
