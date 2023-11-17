package org.sunbird.obsrv.helper

import org.sunbird.obsrv.job.JDBCConnectorConfig
import org.sunbird.obsrv.model.DatasetModels.DatasetSourceConfig
import org.sunbird.obsrv.util.JSONUtil

import java.util.UUID
import scala.collection.mutable

case class SingleEvent(dataset: String, event: Map[String, Any], syncts: Long, obsrv_meta: mutable.Map[String,Any])

object EventGenerator {

  def getBatchEvent(datasetId: String, record: String, dsSourceConfig: DatasetSourceConfig, config: JDBCConnectorConfig, extractionKey: String): String = {
    val event = Map(
      "id" -> UUID.randomUUID().toString,
      "dataset" -> datasetId,
      extractionKey -> List(JSONUtil.deserialize(record, classOf[Map[String, Any]])),
      "syncts" -> System.currentTimeMillis(),
      "obsrv_meta" -> getObsrvMeta(dsSourceConfig, config)
    )
    JSONUtil.serialize(event)
  }


  def getSingleEvent(datasetId: String, record: String, dsSourceConfig: DatasetSourceConfig, config: JDBCConnectorConfig): String = {
    val event = SingleEvent(
      datasetId,
      JSONUtil.deserialize(record, classOf[Map[String, Any]]),
      System.currentTimeMillis(),
      getObsrvMeta(dsSourceConfig, config)
    )
    JSONUtil.serialize(event)
  }

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

}
