package it.agilelab.datamesh.snowflakespecificprovisioner.system

import com.typesafe.config.{Config, ConfigFactory}

import java.util.concurrent.atomic.AtomicReference

object ApplicationConfiguration {

  val config: AtomicReference[Config] = new AtomicReference(ConfigFactory.load())
  def httpPort: Int                   = config.get.getInt("specific-provisioner.http-port")

  def isMocked =
    if (config.get.hasPath("specific-provisioner.is-mock")) config.get.getBoolean("specific-provisioner.is-mock")
    else false
}
