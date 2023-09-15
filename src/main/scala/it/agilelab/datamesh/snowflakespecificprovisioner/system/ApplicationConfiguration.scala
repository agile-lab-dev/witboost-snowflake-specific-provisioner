package it.agilelab.datamesh.snowflakespecificprovisioner.system

import com.typesafe.config.{Config, ConfigFactory}

import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

object ApplicationConfiguration {

  val config: AtomicReference[Config] = new AtomicReference(ConfigFactory.load())
  def httpPort: Int                   = config.get.getInt("specific-provisioner.http-port")
  def user: String                    = config.get.getString("snowflake.user")
  def password: String                = config.get.getString("snowflake.password")
  def role: String                    = config.get.getString("snowflake.role")
  def account: String                 = config.get.getString("snowflake.account")
  def warehouse: String               = config.get.getString("snowflake.warehouse")
  def db: String                      = config.get.getString("snowflake.db")
  def jdbcUrl: String                 = config.get.getString("snowflake.jdbc-url")
  def schema: String                  = config.get.getString("snowflake.schema")

  def snowflakeConnectionTimeout: Duration = config.get.getDuration("snowflake.connection-timeout")
}
