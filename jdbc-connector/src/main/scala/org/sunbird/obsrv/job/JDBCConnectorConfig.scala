package org.sunbird.obsrv.job

import com.typesafe.config.Config

class JDBCConnectorConfig (config: Config, args: Array[String]) extends Serializable {

  val kafkaServerUrl: String = config.getString("kafka.bootstrap-servers")
  val sparkMasterUrl: String = config.getString("spark.master")
  val jdbcConnectionRetry: Int = config.getInt("jdbc.connection.retry")
  val jdbcConnectionRetryDelay: Int = config.getInt("jdbc.connection.retryDelay")
  val connectorVersion: String = config.getString("connector.version")
  val postgresqlDriver: String = config.getString("drivers.postgresql")
  val mysqlDriver: String = config.getString("drivers.mysql")
  val cipherSecret: String = config.getString("cipher.secret")
  val eventMaxLimit: Long = config.getLong("event.max.limit")
  val env: String = config.getString("env")
  val metricsTopic: String = config.getString("metrics.topicName")
  val metricsVersion: String = config.getString("metrics.version")
  val jobName: String = "JDBCConnectorJob"

}
