package org.sunbird.obsrv.helper

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.logging.log4j.{LogManager, Logger}
import org.sunbird.obsrv.job.JDBCConnectorConfig

import java.util.Properties

case class KafkaMessageProducer(config: JDBCConnectorConfig) {

  private final val logger: Logger = LogManager.getLogger(getClass)

  private val kafkaProperties = new Properties();
  private val defaultTopicName = config.metricsTopic
  private val defaultKey = null

  kafkaProperties.put("bootstrap.servers", config.kafkaServerUrl)
  kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](kafkaProperties)

  def sendMessage(topic: String = defaultTopicName, key: String = defaultKey, message: String): Unit = {
    try {
      val record = new ProducerRecord[String, String](topic, key, message)
      producer.send(record)
    } catch {
      case ex: Exception =>
        logger.error("Error while sending metrics data to Kafka: ", ex.getMessage)
        throw new Exception(s"Error while sending metrics data to Kafka: ${ex.getMessage}")
    }
  }
}
