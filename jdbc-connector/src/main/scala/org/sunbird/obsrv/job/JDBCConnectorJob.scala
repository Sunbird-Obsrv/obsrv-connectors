package org.sunbird.obsrv.job

import com.typesafe.config.ConfigFactory
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sunbird.obsrv.helper.{ConnectorHelper, EventGenerator, MetricsHelper}
import org.sunbird.obsrv.model.DatasetModels
import org.sunbird.obsrv.registry.DatasetRegistry

import scala.util.control.Breaks.{break, breakable}

object JDBCConnectorJob extends Serializable {

  private final val logger: Logger = LogManager.getLogger(JDBCConnectorJob.getClass)

  def main(args: Array[String]): Unit = {
    val appConfig = ConfigFactory.load("jdbc-connector.conf").withFallback(ConfigFactory.systemEnvironment())
    val config = new JDBCConnectorConfig(appConfig, args)
    val helper = new ConnectorHelper(config)
    val metrics = MetricsHelper(config)
    val dsSourceConfigList =  DatasetRegistry.getDatasetSourceConfig()
    val datasetList = DatasetRegistry.getAllDatasets()

     val spark = SparkSession.builder()
      .appName("JDBC Connector Batch Job")
      .master(config.sparkMasterUrl)
      .getOrCreate()

    val filteredDSSourceConfigList = getActiveDataSetsSourceConfig(dsSourceConfigList, datasetList)
    logger.info(s"Total no of datasets to be processed: ${filteredDSSourceConfigList.size}")

    filteredDSSourceConfigList.map {
        dataSourceConfig =>
          processTask(config, helper, spark, dataSourceConfig, metrics)
    }

    spark.stop()
  }


  private def processTask(config: JDBCConnectorConfig, helper: ConnectorHelper, spark: SparkSession, dataSourceConfig: DatasetModels.DatasetSourceConfig, metrics: MetricsHelper) = {
    val dataset = DatasetRegistry.getDataset(dataSourceConfig.datasetId).get
    try {
      logger.info(s"Started processing dataset: ${dataSourceConfig.datasetId}")
      var batch: Int = 0
      var eventCount: Long = 0
      breakable {
        while (true) {
          val data: DataFrame = helper.pullRecords(spark, dataSourceConfig, dataset, batch, metrics)
          batch += 1
          if (data.count == 0 || validateMaxSize(eventCount, config.eventMaxLimit)) {
            break
          } else {
            helper.processRecords(config, dataset, batch, data, dataSourceConfig, metrics)
            eventCount += data.count()
          }
        }
      }
      logger.info(s"Completed processing dataset: ${dataSourceConfig.datasetId} :: Total number of records are pulled: $eventCount")
      dataSourceConfig
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        EventGenerator.generateErrorMetric(config, dataSourceConfig, metrics, "Error while processing the JDBC Connector Job", ex.getMessage, dataset.dataVersion)
    }
  }

  private def getActiveDataSetsSourceConfig(dsSourceConfigList: Option[List[DatasetModels.DatasetSourceConfig]], datasetList: Map[String, DatasetModels.Dataset]) = {
    val activeDatasets = datasetList.filter(dataset => dataset._2.status.equalsIgnoreCase("active"))
    val filteredDSSourceConfigList = dsSourceConfigList.map { configList =>
      configList.filter(config => config.connectorType.equalsIgnoreCase("jdbc") &&
        config.status.equalsIgnoreCase("active") && activeDatasets.contains(config.datasetId))
    }.get
    filteredDSSourceConfigList
  }

  private def validateMaxSize(eventCount: Long, maxLimit: Long): Boolean = {
     if (maxLimit == -1) {
       false
     } else if (eventCount > maxLimit) {
       logger.info(s"Max fetch limit is reached, stopped fetching :: event count: ${eventCount} :: max limit: ${maxLimit}")
       true
     } else false
  }
}

