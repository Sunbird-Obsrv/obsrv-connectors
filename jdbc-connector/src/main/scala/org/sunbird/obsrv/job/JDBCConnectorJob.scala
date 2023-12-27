package org.sunbird.obsrv.job

import com.typesafe.config.ConfigFactory
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sunbird.obsrv.helper.{ConnectorHelper, EventGenerator, MetricsHelper}
import org.sunbird.obsrv.model.DatasetStatus
import org.sunbird.obsrv.model.DatasetModels.{DatasetSourceConfig, Dataset}
import org.sunbird.obsrv.registry.DatasetRegistry

import scala.util.control.Breaks.{break, breakable}

object JDBCConnectorJob extends Serializable {

  private final val logger: Logger = LogManager.getLogger(JDBCConnectorJob.getClass)

  private var datasetList: List[Dataset] = List[Dataset]()

  def main(args: Array[String]): Unit = {
    val appConfig = ConfigFactory.load("jdbc-connector.conf").withFallback(ConfigFactory.systemEnvironment())
    val config = new JDBCConnectorConfig(appConfig, args)
    val helper = new ConnectorHelper(config)
    val metrics = MetricsHelper(config)
    val dsSourceConfigList =  DatasetRegistry.getAllDatasetSourceConfig()
    datasetList = DatasetRegistry.getAllDatasets("dataset") ++ DatasetRegistry.getAllDatasets("master-dataset")

     val spark = SparkSession.builder()
      .appName("JDBC Connector Batch Job")
      .master(config.sparkMasterUrl)
      .getOrCreate()

    val filteredDSSourceConfigList = getActiveDataSetsSourceConfig(dsSourceConfigList)
    logger.info(s"Total no of datasets to be processed: ${filteredDSSourceConfigList.size}")

    filteredDSSourceConfigList.map {
        dataSourceConfig =>
          processTask(config, helper, spark, dataSourceConfig, metrics)
    }

    spark.stop()
  }


  private def processTask(config: JDBCConnectorConfig, helper: ConnectorHelper, spark: SparkSession, dataSourceConfig: DatasetSourceConfig, metrics: MetricsHelper) = {
    val dataset = datasetList.filter(dataset => dataset.id == dataSourceConfig.datasetId).head
    try {
      logger.info(s"Started processing dataset: ${dataSourceConfig.datasetId}")
      var batch: Int = 0
      var eventCount: Long = 0
      breakable {
        while (true) {
          val data: DataFrame = helper.pullRecords(spark, dataSourceConfig, dataset, batch, metrics)
          val recordCount = data.count()
          batch += 1
          if (recordCount == 0 || validateMaxSize(eventCount, config.eventMaxLimit)) {
            break
          } else {
            helper.processRecords(config, dataset, batch, data, recordCount, dataSourceConfig, metrics)
            eventCount += recordCount
          }
        }
      }
      logger.info(s"Completed processing dataset: ${dataSourceConfig.datasetId} :: Total number of records are pulled: $eventCount")
      dataSourceConfig
    } catch {
      // $COVERAGE-OFF$
      case ex: Exception =>
        ex.printStackTrace()
        EventGenerator.generateErrorMetric(config, dataSourceConfig, metrics, "Error while processing the JDBC Connector Job", ex.getMessage, dataset.dataVersion)
      // $COVERAGE-ON$
    }
  }

  private def getActiveDataSetsSourceConfig(dsSourceConfigList: Option[List[DatasetSourceConfig]]) = {
    val filteredDSSourceConfigList = dsSourceConfigList.map { configList =>
      configList.filter(config => {
          val dataset = datasetList.filter(dataset => dataset.id == config.datasetId && dataset.status.equals(DatasetStatus.Live))
          config.connectorType.equalsIgnoreCase("jdbc") &&
            config.status.equals(DatasetStatus.Live.toString) &&
            dataset.nonEmpty
        })
      }.getOrElse(List[DatasetSourceConfig]())
    filteredDSSourceConfigList
  }

  private def validateMaxSize(eventCount: Long, maxLimit: Long): Boolean = {
     if (maxLimit != -1 && eventCount > maxLimit) {
       logger.info(s"Max fetch limit is reached, stopped fetching :: event count: ${eventCount} :: max limit: ${maxLimit}")
       true
     } else false
  }
}

