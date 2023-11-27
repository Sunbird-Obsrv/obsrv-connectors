package org.sunbird.obsrv.model

import org.sunbird.obsrv.helper.KafkaMessageProducer

trait IMetricsHelper {
  val metricsProducer: KafkaMessageProducer
  val metrics: Map[String, String]
  def sync(metric: IJobMetric): Unit

  def generate(datasetId: String, dsVersion: Int, edata: Edata): Unit
  def getMetricName(name: String) = {
    metrics.get(name).getOrElse("")
  }
}
