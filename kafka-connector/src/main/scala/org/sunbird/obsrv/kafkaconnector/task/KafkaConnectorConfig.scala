package org.sunbird.obsrv.kafkaconnector.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.streaming.BaseJobConfig

import scala.collection.mutable.{Map => MMap}

class KafkaConnectorConfig(override val config: Config) extends BaseJobConfig[String](config, "KafkaConnectorJob") {

  private val serialVersionUID = 2905979435603791379L

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val kafkaDefaultInputTopic: String = config.getString("kafka.input.topic")
  val kafkaDefaultOutputTopic: String = config.getString("kafka.output.topic")
  override def inputTopic(): String = kafkaDefaultInputTopic
  override def inputConsumer(): String = ""
  private val SUCCESS_OUTPUT_TAG = "success-events"
  override def successTag(): OutputTag[String] = OutputTag[String](SUCCESS_OUTPUT_TAG)

  val connectorVersion: String = config.getString("connector.version")
  override def failedEventsOutputTag(): OutputTag[String] = OutputTag[String]("failed-events")

  val successfulDebeziumTransformedCount = "debezium-success-count"

}
