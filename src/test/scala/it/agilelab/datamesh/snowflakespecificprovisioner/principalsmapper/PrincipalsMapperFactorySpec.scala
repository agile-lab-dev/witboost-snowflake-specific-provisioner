package it.agilelab.datamesh.snowflakespecificprovisioner.principalsmapper

import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.QueryExecutor
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PrincipalsMapperFactorySpec extends AnyFlatSpec with Matchers with MockFactory {

  "create method" should "return the identity based mapping" in {
    val queryExecutor = mock[QueryExecutor]

    val result = PrincipalsMapperFactory.create(queryExecutor)

    result shouldBe a[Right[_, SnowflakePrincipalsMapper]]
  }

  // TODO to add other tests, configuration refactor is needed

}
