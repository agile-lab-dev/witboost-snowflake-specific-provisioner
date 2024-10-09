package it.agilelab.datamesh.snowflakespecificprovisioner.model

import it.agilelab.datamesh.snowflakespecificprovisioner.common.test.getTestResourceAsString
import it.agilelab.datamesh.snowflakespecificprovisioner.snowflakeconnector.ParseError
import org.scalatest.EitherValues._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class DescriptorParserSpec extends AnyFlatSpec {

  "Parsing a well formed descriptor" should "return a correct DataProductDescriptor" in {
    val yaml = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1.yml")

    val prDescr = ProvisioningRequestDescriptor(yaml)

    val dpDescr = prDescr.toOption.get.dataProduct

    val backYaml = dpDescr.toString

    val backDpDescr = DataProductDescriptor(backYaml)

    backDpDescr shouldBe a[Right[_, _]]
    backDpDescr.foreach(backDpDescr => dpDescr should be(backDpDescr))
  }

  "Parsing a well formed descriptor" should "return a correct ComponentToProvision" in {
    val yaml = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1.yml")

    val prDescr = ProvisioningRequestDescriptor(yaml)

    val component = prDescr.toOption.get.getComponentToProvision

    component should be(Symbol("defined"))
  }

  "Parsing a wrongly formed descriptor with missing component id field" should "return a Left with a Exception" in {

    val yaml = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1_missing_component_id.yml")

    val dp = ProvisioningRequestDescriptor(yaml)

    dp.left.value.problems should contain("cannot parse ComponentIdToProvision")

  }

  "Parsing a wrongly formed descriptor with missing components section" should "return a Left with a Exception" in {

    val yaml = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1_missing_components.yml")

    val dp = ProvisioningRequestDescriptor(yaml)

    dp.left.value.problems should contain("cannot parse Data Product components")

  }

  "Parsing a wrongly formed descriptor with missing specific section in component" should
    "return a Left with a Exception" in {

      val yaml = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_1_missing_specific.yml")

      val dp = ProvisioningRequestDescriptor(yaml).toOption.get.dataProduct
        .getComponentToProvision("domain:marketing.Marketing-Invoice-S3-Ingestion-Data-1.1")

      dp.left.value.problems should
        contain("Failed to decode OutputPort: Missing required field: DownField(specific): DownField(specific)")

    }

  "Parsing a totally wrongly formed descriptor" should "return a Right with a ParsingFailure" in {

    val yaml = """name: Marketing-Invoice-1
                 |[]
                 |""".stripMargin

    val dp = ProvisioningRequestDescriptor(yaml)

    dp.left.value shouldBe a[ParseError]
  }

  "Parsing a well formed storage component descriptor" should "return a correct DataProductDescriptor" in {
    val yaml = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_6.yml")

    val prDescr = ProvisioningRequestDescriptor(yaml)

    val dpDescr = prDescr.toOption.get.dataProduct

    println(dpDescr.components)

    val backYaml = dpDescr.toString

    val backDpDescr = DataProductDescriptor(backYaml)

    backDpDescr shouldBe a[Right[_, _]]
    backDpDescr.foreach(backDpDescr => dpDescr should be(backDpDescr))
  }

  "Parsing a descriptor which contains an inappropriate field in Storage Area Descriptor" should "return a Left" in {

    val yaml = getTestResourceAsString("pr_descriptors/storage/pr_descriptor_12_sql_injected_field.yml")

    val prDescr = ProvisioningRequestDescriptor(yaml).toOption.get.dataProduct
      .getComponentToProvision("domain:marketing.Marketing-Invoice-S3-Ingestion-Data-1.1")

    prDescr.left.value.problems should contain(
      "The inputs provided as part of domain:marketing.Marketing-Invoice-S3-Ingestion-Data-1.1 are not conforming to the required pattern!"
    )
  }

  "Parsing a descriptor which contains an inappropriate field in Output Port Descriptor" should "return a Left" in {

    val yaml = getTestResourceAsString("pr_descriptors/outputport/pr_descriptor_12_sql_injected_table_name.yml")

    val prDescr = ProvisioningRequestDescriptor(yaml).toOption.get.dataProduct
      .getComponentToProvision("domain:marketing.Marketing-Invoice-S3-Ingestion-Data-1.1")

    prDescr.left.value.problems should contain(
      "The inputs provided as part of domain:marketing.Marketing-Invoice-S3-Ingestion-Data-1.1 are not conforming to the required pattern!"
    )
  }

}
