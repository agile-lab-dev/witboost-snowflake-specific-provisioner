<p align="center">
    <a href="https://www.agilelab.it/witboost">
        <img src="docs/img/witboost_logo.svg" alt="witboost" width=600 >
    </a>
</p>

Designed by [Agile Lab](https://www.agilelab.it/), witboost is a versatile platform that addresses a wide range of sophisticated data engineering challenges. It enables businesses to discover, enhance, and productize their data, fostering the creation of automated data platforms that adhere to the highest standards of data governance. Want to know more about witboost? Check it out [here](https://www.agilelab.it/witboost) or [contact us!](https://www.agilelab.it/contacts)

This repository is part of our [Starter Kit](https://github.com/agile-lab-dev/witboost-starter-kit) meant to showcase witboost's integration capabilities and provide a "batteries-included" product.

# Snowflake Specific Provisioner

- [Overview](#overview)
- [Building](#building)
- [Running](#running)
- [Configuring](#configuring)
- [Deploying](#deploying)
- [HLD](docs/HLD.md)
- [API specification](docs/API.md)

## Overview

This project implements a simple Specific Provisioner for Snowflake. After deploying this microservice and configuring witboost to use it, the platform can deploy components of a Data Product that use Snowflake. Supported components are: Output Port, Storage Area.

Refer to the [witboost Starter Kit repository](https://github.com/agile-lab-dev/witboost-starter-kit) for information on the templates that can be used with this Specific Provisioner.

### What's a Specific Provisioner?

A Specific Provisioner is a microservice which is in charge of deploying components that use a specific technology. When the deployment of a Data Product is triggered, the platform generates it descriptor and orchestrates the deployment of every component contained in the Data Product. For every such component the platform knows which Specific Provisioner is responsible for its deployment, and can thus send a provisioning request with the descriptor to it so that the Specific Provisioner can perform whatever operation is required to fulfill this request and report back the outcome to the platform.

You can learn more about how the Specific Provisioners fit in the broader picture [here](https://docs.witboost.agilelab.it/docs/p2_arch/p1_intro/#deploy-flow).

### Snowflake

Snowflake is a cloud-based data warehousing platform that provides scalable storage, easy data sharing, and on-demand, near-infinite computing power for processing complex, analytical queries on structured and semi-structured data. It separates compute and storage resources, allowing users to scale and pay for each independently, maximizing efficiency and cost-effectiveness. Learn more about it on the [official website](https://www.snowflake.com/en/).

### Software stack

This microservice is written in Scala 2.13, using Akka HTTP (pre-license change) for the HTTP layer. Communication with Snowflake is handled via the official JDBC driver. Project is built with SBT and supports packaging as JAR, fat-JAR and Docker image, ideal for Kubernetes deployments (which is the preferred option).

## Building

**Requirements:**

- Java 17 (11 works as well)
- SBT
- Node
- Docker (for building images only)

**Generating sources:** this project uses OpenAPI as standard API specification and the [OpenAPI Generator](https://openapi-generator.tech) CLI to generate server code from the specification.

In a terminal, install the OpenAPI Generator CLI and run the `generateCode` SBT task:

```bash
npm install @openapitools/openapi-generator-cli -g
sbt generateCode
```

*Note:* the `generateCode` SBT task needs to be run again if `clean` or similar tasks are executed.

**Compiling:** is handled by the standard task:

```bash
sbt compile
```

**Tests:** are handled by the standard task as well:

```bash
sbt test
```

**Artifacts & Docker image:** the project uses SBT Native Packager for packaging. Build artifacts with:

```
sbt package
```

The Docker image can be built with:

```
sbt docker:publishLocal
```

*Note:* the version for the project is automatically computed using information gathered from Git, using branch name and tags. Unless you are on a release branch `1.2.x` or a tag `v1.2.3` it will end up being `0.0.0`. You can follow this branch/tag convention or update the version computation to match your preferred strategy.

**CI/CD:** the pipeline is based on GitLab CI as that's what we use internally. It's configured by the `.gitlab-ci.yaml` file in the root of the repository. You can use that as a starting point for your customizations.

## Running

To run the server locally, use:

```bash
sbt generateCode compile run
```

By default, the server binds to port 8093 on localhost. After it's up and running you can make provisioning requests to this address. You can also check the API documentation served [here](http://127.0.0.1:8093/swagger/docs/index.html).

## Configuring

Most application configurations are handled with the Typesafe Config library. You can find the default settings in the `reference.conf` and some `application.conf` examples in the Helm chart (see below). Customize them and use the `config.file` system property or the other options provided by Typesafe Config according to your needs.

Snowflake connection information can be passed in via environment variables or configuration file; the mapping done by default resides in the `reference.conf` and is the following:

| Setting                       | Environment Variable          |
|-------------------------------|-------------------------------|
| snowflake.user                | SNOWFLAKE_USER                |
| snowflake.password            | SNOWFLAKE_PASSWORD            |
| snowflake.role                | SNOWFLAKE_ROLE                |
| snowflake.account             | SNOWFLAKE_ACCOUNT             |
| snowflake.warehouse           | SNOWFLAKE_WAREHOUSE           |
| snowflake.jdbc-url            | JDBC_URL                      |
| snowflake.account-locator-url | SNOWFLAKE_ACCOUNT_LOCATOR_URL |

Logging is handled with Logback, you can find an example `logback.xml` in the Helm chart. Customize it and pass it using the `logback.configurationFile` system property.

### Principals Mapper

The default configuration uses an `identity` mapping strategy.

| Setting                              | Environment Variable                 | Default value | Allowed values       |
|--------------------------------------|--------------------------------------|---------------|----------------------|
| snowflake.principals-mapper.strategy | SNOWFLAKE_PRINCIPALS_MAPPER_STRATEGY | identity      | identity,table-based |

It is possible to use a `table-based` mapping if, for example, Snowflake is not configured to use SSO. In this case the mapping table is used and is expected to be already present and filled with all the required mapping entries.

The following options allow you to customize the database, schema and name of the mapping table:

| Setting                                          | Environment Variable                             | Default value      |
|--------------------------------------------------|--------------------------------------------------|--------------------|
| snowflake.principals-mapper.table-based.database | SNOWFLAKE_PRINCIPALS_MAPPER_TABLE_BASED_DATABASE | WITBOOST           |
| snowflake.principals-mapper.table-based.schema   | SNOWFLAKE_PRINCIPALS_MAPPER_TABLE_BASED_SCHEMA   | CONFIGURATIONS     |
| snowflake.principals-mapper.table-based.table    | SNOWFLAKE_PRINCIPALS_MAPPER_TABLE_BASED_TABLE    | PRINCIPALS_MAPPING |

You can find a sample SQL script to create required objects [here](docs/create_mapping_table.sql). Update it before executing, if you decide to use different default values for database, schema or table name.

The role used  by the provisioner must have SELECT privileges on this table.

## Deploying

This microservice is meant to be deployed to a Kubernetes cluster with the included Helm chart and the scripts that can be found in the `k8s` subdirectory.

## License

This project is available under the [Apache License, Version 2.0](https://opensource.org/licenses/Apache-2.0); see [LICENSE](LICENSE) for full details.

## About us

<p align="center">
    <a href="https://www.agilelab.it">
        <img src="docs/img/agilelab_logo.jpg" alt="Agile Lab" width=600>
    </a>
</p>

Agile Lab creates value for its Clients in data-intensive environments through customizable solutions to establish performance driven processes, sustainable architectures, and automated platforms driven by data governance best practices.

Since 2014 we have implemented 100+ successful Elite Data Engineering initiatives and used that experience to create Witboost: a technology agnostic, modular platform, that empowers modern enterprises to discover, elevate and productize their data both in traditional environments and on fully compliant Data mesh architectures.

[Contact us](https://www.agilelab.it/contacts) or follow us on:
- [LinkedIn](https://www.linkedin.com/company/agile-lab/)
- [Instagram](https://www.instagram.com/agilelab_official/)
- [YouTube](https://www.youtube.com/channel/UCTWdhr7_4JmZIpZFhMdLzAA)
- [Twitter](https://twitter.com/agile__lab)


