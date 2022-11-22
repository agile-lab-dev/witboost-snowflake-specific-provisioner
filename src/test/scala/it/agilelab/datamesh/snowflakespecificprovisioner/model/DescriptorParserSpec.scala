package it.agilelab.datamesh.snowflakespecificprovisioner.model

import org.scalatest.flatspec.AnyFlatSpec
import cats.data.{EitherNel, NonEmptyList}
import org.scalatest.EitherValues._
import org.scalatest.matchers.should.Matchers._
import it.agilelab.datamesh.snowflakespecificprovisioner.common.test.getTestResourceAsString

class DescriptorParserSpec extends AnyFlatSpec {

  "Parsing a well formed descriptor" should "return a correct DataProductDescriptor" in {
    val yaml = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1.yml")

    val prDescr = ProvisioningRequestDescriptor(yaml)

    val dpDescr = prDescr.toOption.get.dataProduct

    val backYaml = dpDescr.toString

    val backDpDescr = DataProductDescriptor(backYaml).toOption.get

    dpDescr should be(backDpDescr)
  }

  "Parsing a well formed descriptor" should "return a correct ComponentToProvision" in {
    val yaml = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1.yml")

    val prDescr = ProvisioningRequestDescriptor(yaml)

    val component = prDescr.toOption.get.getComponentToProvision

    component should be(Symbol("defined"))
  }

  "Parsing a wrongly formed descriptor with missing component id field" should "return a Left with a Exception" in {

    val yaml = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1_missing_component_id.yml")

    val dp: EitherNel[String, ProvisioningRequestDescriptor] = ProvisioningRequestDescriptor(yaml)

    dp.left.value.head should
      startWith("The yaml is not a correct Provisioning Request: cannot parse ComponentIdToProvision for")

  }

  "Parsing a wrongly formed descriptor with missing components section" should "return a Left with a Exception" in {

    val yaml = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1_missing_components.yml")

    val dp: EitherNel[String, ProvisioningRequestDescriptor] = ProvisioningRequestDescriptor(yaml)

    dp.left.value.head should
      startWith("The yaml is not a correct Provisioning Request: cannot parse Data Product components for")

  }

  "Parsing a wrongly formed descriptor with missing specific section in component" should
    "return a Left with a Exception" in {

      val yaml = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1_missing_specific.yml")

      val dp: EitherNel[String, ProvisioningRequestDescriptor] = ProvisioningRequestDescriptor(yaml)

      dp.left.value.head should
        startWith("The yaml is not a correct Provisioning Request: cannot parse Component specific for ")

    }

  "Parsing a totally wrongly formed descriptor" should "return a Right with a ParsingFailure" in {

    val yaml = """name: Marketing-Invoice-1
                 |[]
                 |""".stripMargin

    val dp: EitherNel[String, ProvisioningRequestDescriptor] = ProvisioningRequestDescriptor(yaml)

    dp.left.value should be(a[NonEmptyList[_]])
  }

}
