package org.sunbird.obsrv.kafkaconnector.task

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.joda.time.{DateTime, DateTimeZone}
import org.sunbird.obsrv.core.streaming.{BaseStreamTask, FlinkKafkaConnector}
import org.sunbird.obsrv.core.util.{FlinkUtil, JSONUtil}
import org.sunbird.obsrv.model.DatasetModels.DatasetSourceConfig
import org.sunbird.obsrv.registry.DatasetRegistry

import java.io.File
import java.util.UUID
import scala.collection.mutable


class KafkaConnectorStreamTask(config: KafkaConnectorConfig, kafkaConnector: FlinkKafkaConnector) extends BaseStreamTask[String] {

  private val serialVersionUID = -7729362727131516112L

  // $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)

    val datasetSourceConfig = DatasetRegistry.getDatasetSourceConfig()
    datasetSourceConfig.map { configList =>
      configList.filter(_.connectorType.equalsIgnoreCase("kafka")).map {
        dataSourceConfig =>
          val dataStream: DataStream[String] =
            getStringDataStream(env, config, List(dataSourceConfig.connectorConfig.topic),
            config.kafkaConsumerProperties(kafkaBrokerServers = Some(dataSourceConfig.connectorConfig.kafkaBrokers),
              kafkaConsumerGroup = Some(s"kafka-${dataSourceConfig.connectorConfig.topic}-consumer")),
              consumerSourceName = s"kafka-${dataSourceConfig.connectorConfig.topic}", kafkaConnector)
          val datasetId = dataSourceConfig.datasetId
          val kafkaOutputTopic = DatasetRegistry.getDataset(datasetId).get.datasetConfig.entryTopic
          val resultMapStream: DataStream[String] = dataStream
            .filter{msg: String => JSONUtil.isJSON(msg)}.returns(classOf[String]) // TODO: Add a metric to capture invalid JSON messages
            .map { streamMap: String => {
              val mutableMap = JSONUtil.deserialize[mutable.Map[String, AnyRef]](streamMap)
              mutableMap.put("dataset", datasetId)
              mutableMap.put("syncts", java.lang.Long.valueOf(new DateTime(DateTimeZone.UTC).getMillis))
              addObsrvMeta(mutableMap, dataSourceConfig)
              JSONUtil.serialize(mutableMap)
            }
          }.returns(classOf[String])
          resultMapStream.sinkTo(kafkaConnector.kafkaStringSink(kafkaOutputTopic))
            .name(s"$datasetId-kafka-connector-sink").uid(s"$datasetId-kafka-connector-sink")
            .setParallelism(config.downstreamOperatorsParallelism)
      }
      env.execute(config.jobName)
    }
  }

  private def addObsrvMeta(event: mutable.Map[String, AnyRef], dataSourceConfig: DatasetSourceConfig) : Unit ={
    event.put("obsrv_meta", Map(
      "syncts" -> System.currentTimeMillis(),
      "processingStartTime" -> System.currentTimeMillis(),
      "flags" -> Map(),
      "timespans" -> Map(),
      "error" -> Map(),
      "source" -> Map(
        "meta" -> Map(
          "id" -> dataSourceConfig.id,
          "connector_type" -> "kafka",
          "version" -> config.connectorVersion,
          "entry_source" -> dataSourceConfig.connectorConfig.topic
        ),
        "trace_id" -> UUID.randomUUID().toString
      )
    ))
  }

  override def processStream(dataStream: DataStream[String]): DataStream[String] = {
    null
  }
  // $COVERAGE-ON$
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object KafkaConnectorStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("kafka-connector.conf").withFallback(ConfigFactory.systemEnvironment()))
    val kafkaConnectorConfig = new KafkaConnectorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(kafkaConnectorConfig)
    val task = new KafkaConnectorStreamTask(kafkaConnectorConfig, kafkaUtil)
    task.process()
  }
}
// $COVERAGE-ON$
