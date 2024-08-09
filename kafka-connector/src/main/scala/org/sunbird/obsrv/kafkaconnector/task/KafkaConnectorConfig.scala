package org.sunbird.obsrv.kafkaconnector.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.obsrv.core.streaming.BaseJobConfig

class KafkaConnectorConfig(override val config: Config) extends BaseJobConfig[String](config, "KafkaConnectorJob") {

  private val serialVersionUID = 2905979435603791379L

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val kafkaDefaultOutputTopic: String = config.getString("kafka.output.topic")

  override def inputTopic(): String = config.getString("kafka.input.topic")

  override def inputConsumer(): String = ""

  private val SUCCESS_OUTPUT_TAG = "success-events"

  override def successTag(): OutputTag[String] = OutputTag[String](SUCCESS_OUTPUT_TAG)

  override def failedEventsOutputTag(): OutputTag[String] = OutputTag[String]("failed-events")

}