package org.sunbird.obsrv.helper

import org.sunbird.obsrv.model.DatasetModels.{Dataset, DatasetSourceConfig}
import org.sunbird.obsrv.model.{Edata, MetricLabel}
import org.sunbird.obsrv.job.JDBCConnectorConfig

import java.util.UUID
import scala.collection.mutable


object EventGenerator {

  def getObsrvMeta(dsSourceConfig: DatasetSourceConfig, config: JDBCConnectorConfig): mutable.Map[String,Any] = {
    val obsrvMeta = mutable.Map[String,Any]()
    obsrvMeta.put("syncts", System.currentTimeMillis())
    obsrvMeta.put("flags", Map())
    obsrvMeta.put("timespans", Map())
    obsrvMeta.put("error", Map())
    obsrvMeta.put("processingStartTime", System.currentTimeMillis())
    obsrvMeta.put("source", Map(
      "meta" -> Map(
        "id" -> dsSourceConfig.id,
        "connector_type" -> "jdbc",
        "version" -> config.connectorVersion,
        "entry_source" -> dsSourceConfig.connectorConfig.tableName
      ),
      "trace_id" -> UUID.randomUUID().toString
    ))
    obsrvMeta
  }


  def generateProcessingMetric(config: JDBCConnectorConfig, dataset: Dataset, batch: Int, eventCount: Long, dsSourceConfig: DatasetSourceConfig, metrics: MetricsHelper, eventProcessingTime: Long): Unit = {
    metrics.generate(
      dataset.id,
      dataset.dataVersion.getOrElse(1),
      edata = Edata(
        metric = Map(
          metrics.getMetricName("batch_count") -> batch,
          metrics.getMetricName("processed_event_count") -> eventCount,
          metrics.getMetricName("processing_time_in_ms") -> eventProcessingTime
        ),
        labels = getMetricLabels(config, dsSourceConfig)
      )
    )
  }

  private def getMetricLabels(config: JDBCConnectorConfig, dsSourceConfig: DatasetSourceConfig) = {
    List(
      MetricLabel("job", config.jobName),
      MetricLabel("databaseType", dsSourceConfig.connectorConfig.databaseType),
      MetricLabel("databaseName", dsSourceConfig.connectorConfig.databaseName),
      MetricLabel("tableName", dsSourceConfig.connectorConfig.tableName),
      MetricLabel("batchSize", String.valueOf(dsSourceConfig.connectorConfig.batchSize)),
      MetricLabel("metricsVersion", config.metricsVersion),
      MetricLabel("connectorVersion", config.connectorVersion),
      MetricLabel("datasetId", dsSourceConfig.datasetId),
    )
  }

  def generateFetchMetric(config: JDBCConnectorConfig, dataset: Dataset, batch: Int, eventCount: Long, dsSourceConfig: DatasetSourceConfig, metrics: MetricsHelper, eventProcessingTime: Long): Unit = {
    metrics.generate(
      dataset.id,
      dataset.dataVersion.getOrElse(1),
      edata = Edata(
        metric = Map(
          metrics.getMetricName("batch_count") -> batch,
          metrics.getMetricName("fetched_event_count") -> eventCount,
          metrics.getMetricName("fetched_time_in_ms") -> eventProcessingTime
        ),
        labels = getMetricLabels(config, dsSourceConfig)
      )
    )
  }

  def generateErrorMetric(config: JDBCConnectorConfig, dsSourceConfig: DatasetSourceConfig, metrics: MetricsHelper, failureCount: Int, batch: Int, error: String, errorMessage: String, dsVersion: Option[Int]): Unit = {
    metrics.generate(
      dsSourceConfig.datasetId,
      dsVersion.getOrElse(1),
      edata = Edata(
        metric = Map(
          metrics.getMetricName("batch_count") -> batch,
          metrics.getMetricName("failure_count") -> failureCount
        ),
        labels = getMetricLabels(config, dsSourceConfig),
        err = error,
        errMsg = errorMessage
      )
    )
  }

  def generateErrorMetric(config: JDBCConnectorConfig, dsSourceConfig: DatasetSourceConfig, metrics: MetricsHelper, error: String, errorMessage: String, dsVersion: Option[Int]): Unit = {
    metrics.generate(
      dsSourceConfig.datasetId,
      dsVersion.getOrElse(1),
      edata = Edata(
        metric = Map(),
        labels = getMetricLabels(config, dsSourceConfig),
        err = error,
        errMsg = errorMessage
      )
    )
  }

}
