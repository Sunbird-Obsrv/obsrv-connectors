package org.sunbird.obsrv.helper

import org.apache.kafka.common.KafkaException
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.sunbird.obsrv.model.DatasetModels
import org.sunbird.obsrv.model.DatasetModels.{ConnectorConfig, ConnectorStats, Dataset, DatasetSourceConfig}
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.util.{CipherUtil, JSONUtil}
import org.sunbird.obsrv.job.JDBCConnectorConfig

import java.net.UnknownHostException
import java.sql.Timestamp
import scala.util.control.Breaks.break

class ConnectorHelper(config: JDBCConnectorConfig) extends Serializable {

 private final val logger: Logger = LogManager.getLogger(getClass)

  def pullRecords(spark: SparkSession, dsSourceConfig: DatasetSourceConfig, dataset: Dataset, batch: Int, metrics: MetricsHelper): DataFrame = {
    val cipherUtil = new CipherUtil(config)
    val connectorConfig = dsSourceConfig.connectorConfig
    val connectorStats = dsSourceConfig.connectorStats.getOrElse(
      ConnectorStats(records = 0, lastFetchTimestamp = null, disconnections = 0, avgBatchReadTime = 0)
    )
    val jdbcUrl = s"jdbc:${connectorConfig.databaseType}://${connectorConfig.connection.host}:${connectorConfig.connection.port}/${connectorConfig.databaseName}"
    val offset = batch * connectorConfig.batchSize
    val query: String = getQuery(connectorStats, connectorConfig, dataset, offset)
    var data: DataFrame = null
    var batchReadTime: Long = 0
    var retryCount = 0

    logger.info(s"Started pulling batch ${batch + 1}")

    while (retryCount < config.jdbcConnectionRetry && data == null) {
      try {
        val readStartTime = System.currentTimeMillis()
        val authenticationData: Map[String, String] =  JSONUtil.deserialize(cipherUtil.decrypt(connectorConfig.authenticationMechanism.encryptedValues), classOf[Map[String, String]])
        val result =  spark.read.format("jdbc")
            .option("driver", getDriver(connectorConfig.databaseType))
            .option("url", jdbcUrl)
            .option("user", authenticationData("username"))
            .option("password", authenticationData("password"))
            .option("query", query)
            .load()

        batchReadTime = System.currentTimeMillis() - readStartTime
        EventGenerator.generateFetchMetric(config, dataset, batch + 1, result.count(), dsSourceConfig, metrics, batchReadTime)
        data = result
      } catch {
        case exception: Exception =>
          retryCount += 1
          exceptionHandler(dsSourceConfig, retryCount, exception, metrics, batch + 1, dataset.dataVersion)
      }
    }
    data
  }

  private def exceptionHandler(dsSourceConfig: DatasetSourceConfig, retryCount: Int, ex: Throwable, metrics: MetricsHelper, batch: Int, dsVersion: Option[Int]): Unit = {
    val error = getExceptionMessage(ex)
    logger.error(s"$error :: Retrying (${retryCount}/${config.jdbcConnectionRetry}) :: Exception: ${ex.getMessage}")
    ex.printStackTrace()
    DatasetRegistry.updateConnectorDisconnections(dsSourceConfig.datasetId, retryCount)
    EventGenerator.generateErrorMetric(config, dsSourceConfig, metrics, retryCount, batch, error, ex.getMessage, dsVersion)
    if (retryCount == config.jdbcConnectionRetry) break
    Thread.sleep(config.jdbcConnectionRetryDelay)
  }

  private def getExceptionMessage(exception: Throwable): String = {
    exception.getMessage match {
      case msg if msg.contains("authentication failed") => "Invalid authentication details"
      case _ =>
        exception.getClass.toString match {
          case cls if cls.contains("javax.crypto") => "Error while decrypting the authentication details"
          case _ =>
            exception match {
              case _: UnknownHostException => "Database Connection failed"
              case _: Exception => "Error while fetching data from database"
            }
        }
    }
  }

  def processRecords(config: JDBCConnectorConfig, dataset: DatasetModels.Dataset, batch: Int, data: DataFrame, eventCount: Long, dsSourceConfig: DatasetSourceConfig, metrics: MetricsHelper): Unit = {
    val processStartTime = System.currentTimeMillis()
    val lastRowTimestamp = data.orderBy(data(dsSourceConfig.connectorConfig.timestampColumn).desc).first().getAs[Timestamp](dsSourceConfig.connectorConfig.timestampColumn)
    pushToKafka(config, dataset, dsSourceConfig, data)
    val eventProcessingTime = System.currentTimeMillis() - processStartTime
    DatasetRegistry.updateConnectorStats(dsSourceConfig.datasetId, lastRowTimestamp, eventCount)
    EventGenerator.generateProcessingMetric(config, dataset, batch, eventCount, dsSourceConfig, metrics, eventProcessingTime)
    logger.info(s"Batch $batch is processed successfully :: Number of records pulled: $eventCount")
  }

  private def pushToKafka(config: JDBCConnectorConfig, dataset: DatasetModels.Dataset, dsSourceConfig: DatasetSourceConfig, df: DataFrame): Unit = {
    try {
      val transformedDF: DataFrame = transformDF(df, dsSourceConfig, dataset)
      transformedDF
        .selectExpr("to_json(struct(*)) AS value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafkaServerUrl)
        .option("topic", dataset.datasetConfig.entryTopic)
        .save()
    } catch {
      case ex: KafkaException =>
        throw new Exception(s"Error while sending data to kafka: ${ex.getMessage}")
    }
  }

  private def transformDF(df: DataFrame, dsSourceConfig: DatasetSourceConfig, dataset: DatasetModels.Dataset): DataFrame = {
    try {
      val fieldNames = df.columns
      val structExpr = struct(fieldNames.map(col): _*)
      val getObsrvMeta = udf(() => JSONUtil.serialize(EventGenerator.getObsrvMeta(dsSourceConfig, config)))
      val generateUUID = udf(() => java.util.UUID.randomUUID().toString)
      var resultDF: DataFrame = null

      val jsonColumn = df.toJSON
      val value = jsonColumn.select("value")

      if (dataset.extractionConfig.get.isBatchEvent.get) {
        val updatedValue = value.withColumnRenamed("value", dataset.extractionConfig.get.extractionKey.get)
        resultDF = updatedValue
          .withColumn(dataset.extractionConfig.get.extractionKey.get, array(from_json(col(dataset.extractionConfig.get.extractionKey.get), df.schema)))
          .withColumn(dataset.extractionConfig.get.dedupConfig.get.dedupKey.get, generateUUID())
      } else {
        resultDF = df
          .withColumn("event", structExpr)
          .withColumn("id", generateUUID())
      }

      resultDF = resultDF
        .withColumn("dataset", lit(dsSourceConfig.datasetId))
        .withColumn("syncts", expr("cast(current_timestamp() as long)"))
        .withColumn("obsrv_meta_str", getObsrvMeta())
        .withColumn("obsrv_meta", from_json(col("obsrv_meta_str"), getObsrvMetaSchema))

      val columnsToRemove = fieldNames.toSeq :+ "obsrv_meta_str"
      resultDF.drop(columnsToRemove: _*)
    } catch {
      // $COVERAGE-OFF$
      case ex: Exception =>
        throw new Exception(s"Error while transforming the data: ${ex.getMessage}")
      // $COVERAGE-ON$
    }
  }

  private def getQuery(connectorStats: ConnectorStats, connectorConfig: ConnectorConfig, dataset: Dataset, offset: Int): String = {
    if (connectorStats.lastFetchTimestamp == null) {
      s"SELECT * FROM ${connectorConfig.tableName} ORDER BY ${connectorConfig.timestampColumn} LIMIT ${connectorConfig.batchSize} OFFSET $offset"
    } else {
      s"SELECT * FROM ${connectorConfig.tableName} WHERE ${connectorConfig.timestampColumn} >= '${connectorStats.lastFetchTimestamp}' ORDER BY ${connectorConfig.timestampColumn} LIMIT ${connectorConfig.batchSize} OFFSET $offset"
    }
  }

  private def getDriver(databaseType: String): String = {
     databaseType match {
      case "postgresql" => config.postgresqlDriver
      // $COVERAGE-OFF$
      case "mysql" => config.mysqlDriver
      // $COVERAGE-ON$
    }
  }

  private def getObsrvMetaSchema = {
    val obsrvMetaSchema = StructType(
      Array(
        StructField("timespans", MapType(StringType, StringType), nullable = true),
        StructField("error", MapType(StringType, StringType), nullable = true),
        StructField("source", StructType(
          Array(
            StructField("meta", StructType(
              Array(
                StructField("id", StringType),
                StructField("connector_type", StringType),
                StructField("version", StringType),
                StructField("entry_source", StringType)
              )
            )),
            StructField("trace_id", StringType)
          )
        )),
        StructField("processingStartTime", LongType),
        StructField("syncts", LongType),
        StructField("flags", MapType(StringType, StringType), nullable = true)
      )
    )
    obsrvMetaSchema
  }

}
