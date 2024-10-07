package it.agilelab.datamesh.snowflakespecificprovisioner.system

import com.typesafe.config.{Config, ConfigFactory}

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

trait ApplicationConfigurationWrapper {
  def jdbcUrl: String
  def accountLocatorUrl: String
}

object RealApplicationConfiguration extends ApplicationConfigurationWrapper {
  override def jdbcUrl: String = ApplicationConfiguration.jdbcUrl

  override def accountLocatorUrl: String = ApplicationConfiguration.accountLocatorUrl
}

object ApplicationConfiguration {

  val config: AtomicReference[Config]      = new AtomicReference(ConfigFactory.load())
  def httpPort: Int                        = config.get.getInt("specific-provisioner.http-port")
  def ignoreSnowflakeTags: Boolean         = config.get.getBoolean("specific-provisioner.ignore-snowflake-tags")
  def snowflakeTagNameField: String        = config.get.getString("specific-provisioner.snowflake-tag-name-field")
  def snowflakeTagValueField: String       = config.get.getString("specific-provisioner.snowflake-tag-value-field")
  def user: String                         = config.get.getString("snowflake.user")
  def password: String                     = config.get.getString("snowflake.password")
  def role: String                         = config.get.getString("snowflake.role")
  def account: String                      = config.get.getString("snowflake.account")
  def warehouse: String                    = config.get.getString("snowflake.warehouse")
  def jdbcUrl: String                      = config.get.getString("snowflake.jdbc-url")
  def accountLocatorUrl: String            = config.get.getString("snowflake.account-locator-url")
  def snowflakeConnectionTimeout: Duration = config.get.getDuration("snowflake.connection-timeout")
  def principalsMapperStrategy: String     = config.get.getString("snowflake.principals-mapper.strategy")

  def principalsMapperTableBasedDatabase: String = config.get
    .getString("snowflake.principals-mapper.table-based.database")

  def principalsMapperTableBasedSchema: String = config.get.getString("snowflake.principals-mapper.table-based.schema")
  def principalsMapperTableBasedTable: String  = config.get.getString("snowflake.principals-mapper.table-based.table")
  def tagReferencesDatabase: String            = config.get.getString("snowflake.tag-references.database")
  def tagReferencesSchema: String              = config.get.getString("snowflake.tag-references.schema")
  def tagReferencesView: String                = config.get.getString("snowflake.tag-references.view")

  val storageAreaUseCaseTemplateId: String = config.get.getString("specific-provisioner.storage-useCaseTemplateId")
  val outputPortUseCaseTemplateId: String  = config.get.getString("specific-provisioner.outputport-useCaseTemplateId")
}
