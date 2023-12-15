package org.sunbird.obsrv.helper

import org.sunbird.obsrv.core.util.JSONUtil
import org.sunbird.obsrv.job.JDBCConnectorConfig
import org.sunbird.obsrv.model.{IJobMetric, IMetricsHelper}

abstract class BaseMetricHelper(config: JDBCConnectorConfig) extends IMetricsHelper {

  override val metrics: Map[String, String] = Map(
    "batch_count" -> "batch_count",
    "failure_count" -> "failure_count",
    "fetched_event_count" -> "fetched_event_count",
    "fetched_time_in_ms" -> "fetched_time_in_ms",
    "processed_event_count" -> "processed_event_count",
    "processing_time_in_ms" -> "processing_time_in_ms"
  )

  val metricsProducer = KafkaMessageProducer(config)

  def sync(metric: IJobMetric): Unit = {
    val metricStr = JSONUtil.serialize(metric)
    metricsProducer.sendMessage(message = metricStr)
  }
}
