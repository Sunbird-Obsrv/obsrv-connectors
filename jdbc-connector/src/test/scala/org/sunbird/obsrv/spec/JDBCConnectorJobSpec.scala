package org.sunbird.obsrv.spec

import com.typesafe.config.{Config, ConfigFactory}
import io.github.embeddedkafka.Codecs.stringDeserializer
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig, duration2JavaDuration}
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.sunbird.obsrv.core.util.{PostgresConnect, PostgresConnectionConfig}
import org.sunbird.obsrv.fixture.EventFixture
import org.sunbird.obsrv.job.{JDBCConnectorConfig, JDBCConnectorJob}
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.util.{CipherUtil, JSONUtil}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.TimeZone
import scala.collection.JavaConverters._
import scala.collection.compat._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._


class JDBCConnectorJobSpec extends FlatSpec with EmbeddedKafka with BeforeAndAfterAll {
  val config: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())

  val customKafkaConsumerProperties: Map[String, String] = Map("group.id" -> "test-consumer-group", "auto.offset.reset" -> "earliest")
  var embeddedPostgres: EmbeddedPostgres = _
  val postgresConfig: PostgresConnectionConfig = PostgresConnectionConfig(
    user = config.getString("postgres.user"),
    password = config.getString("postgres.password"),
    database = "obsrv",
    host = config.getString("postgres.host"),
    port = config.getInt("postgres.port"),
    maxConnections = config.getInt("postgres.maxConnections")
  )
  val postgresConnect = new PostgresConnect(postgresConfig)
  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181, customConsumerProperties = customKafkaConsumerProperties)
  val jdbcConfig = new JDBCConnectorConfig(config, Array())

  def init(): Unit = {
    EmbeddedKafka.start()(embeddedKafkaConfig)
    createTestTopics()
    createTableSchema()
    insertTestData()
    JDBCConnectorJob.main(Array())
    checkTestTopicsOffset()
  }

  def cleanup(): Unit = {
    EmbeddedKafka.stop()
  }

  "JDBCConnectorJob" should "pull records successfully from postgresql database" in {
    init()
    cleanup()
  }

  "Decryption" should "succeed" in {
    val cipherUtil = new CipherUtil(jdbcConfig)
    val authenticationData: Map[String, String] = JSONUtil.deserialize(cipherUtil.decrypt(EventFixture.validEncryptedAuthenticationValues), classOf[Map[String, String]])
    assert(authenticationData.contains("username"))
    assert(authenticationData.contains("password"))
  }

  "Decryption" should "throw exception" in {
    //Invalid Authentication details
    val cipherUtil = new CipherUtil(jdbcConfig)
    intercept[Exception] {
      val authenticationData: Map[String, String] = JSONUtil.deserialize(cipherUtil.decrypt(EventFixture.invalidEncryptedAuthenticationvalues), classOf[Map[String, String]])
    }
    //Decryption failure
  }

  "JDBCConectorJob" should "check lastRowTimestamp" in {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    val datasetSourceConfig = DatasetRegistry.getDatasetSourceConfigById("jdbc_test")
    datasetSourceConfig.map { configList =>
      configList.map(config => {
        val connectorStats = config.connectorStats.map(_.lastFetchTimestamp)
        connectorStats should not be empty
        //Check value of lastFetchTimestamp
        connectorStats.map(timestamp => {
          timestamp shouldBe Timestamp.valueOf("2023-11-26 11:53:50")
        })
      })
    }
  }

  it should "validate fetch metric format" in {
    val message = EmbeddedKafka.consumeFirstMessageFrom("spark.stats")
    val metric = JSONUtil.deserialize(message, classOf[Map[String, Any]])
    metric should not be empty

    val mapValue = metric("edata").asInstanceOf[Map[String, Any]]
    mapValue.get("metric").toList.length shouldBe 1
  }

  ""
  def createTableSchema(): Unit = {
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS datasets ( id text PRIMARY KEY, type text NOT NULL, validation_config json, extraction_config json, dedup_config json, data_schema json, denorm_config json, router_config json NOT NULL, dataset_config json NOT NULL, status text NOT NULL, tags text[], data_version INT, created_by text NOT NULL, updated_by text NOT NULL, created_date timestamp NOT NULL, updated_date timestamp NOT NULL );")
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS datasources ( id text PRIMARY KEY, dataset_id text REFERENCES datasets (id), ingestion_spec json NOT NULL, datasource text NOT NULL, datasource_ref text NOT NULL, retention_period json, archival_policy json, purge_policy json, backup_config json NOT NULL, status text NOT NULL, created_by text NOT NULL, updated_by text NOT NULL, created_date Date NOT NULL, updated_date Date NOT NULL );")
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS dataset_transformations ( id text PRIMARY KEY, dataset_id text REFERENCES datasets (id), field_key text NOT NULL, transformation_function json NOT NULL, status text NOT NULL, mode text, created_by text NOT NULL, updated_by text NOT NULL, created_date Date NOT NULL, updated_date Date NOT NULL, UNIQUE(field_key, dataset_id) );")
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS dataset_source_config ( id text PRIMARY KEY, dataset_id text NOT NULL REFERENCES datasets (id), connector_type text NOT NULL, connector_config json NOT NULL, status text NOT NULL, connector_stats json, created_by text NOT NULL, updated_by text NOT NULL, created_date Date NOT NULL, updated_date Date NOT NULL, UNIQUE(connector_type, dataset_id) );")
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS emp_data (id INT, first_name text, last_name text, email text, gender text, time timestamp);")
  }

  def insertTestData(): Unit = {
    postgresConnect.execute("INSERT INTO \"public\".\"datasets\" (\"id\", \"dataset_id\", \"type\", \"name\", \"validation_config\", \"extraction_config\", \"dedup_config\", \"data_schema\", \"denorm_config\", \"router_config\", \"dataset_config\", \"tags\", \"status\", \"created_by\", \"updated_by\", \"created_date\", \"updated_date\", \"published_date\", \"data_version\") VALUES\n('jdbc_test', 'jdbc_test', 'dataset', 'jdbc_test', '{\"validate\": true, \"mode\": \"Strict\", \"validation_mode\": \"Strict\"}', '{\"is_batch_event\": true, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"eid\", \"dedup_period\": 1036800}, \"batch_id\": \"eid\"}', '{\"drop_duplicates\": false, \"dedup_key\": \"\", \"dedup_period\": 1036800}', '{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": {\"id\": {\"type\": \"integer\", \"arrival_format\": \"number\", \"data_type\": \"integer\"}, \"first_name\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"last_name\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"email\": {\"type\": \"string\", \"suggestions\": [{\"message\": \"The Property ''email'' appears to be ''email'' format type.\", \"advice\": \"Suggest to Mask the Personal Information\", \"resolutionType\": \"TRANSFORMATION\", \"severity\": \"LOW\", \"path\": \"properties.email\"}], \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"gender\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"time\": {\"type\": \"string\", \"format\": \"date-time\", \"suggestions\": [{\"message\": \"The Property ''time'' appears to be ''date-time'' format type.\", \"advice\": \"The System can index all data on this column\", \"resolutionType\": \"INDEX\", \"severity\": \"LOW\", \"path\": \"properties.time\"}], \"arrival_format\": \"text\", \"data_type\": \"date-time\"}}, \"addtionalProperties\": false, \"required\": [\"id\"]}', '{\"redis_db_host\": \"localhost\", \"redis_db_port\": 6379, \"denorm_fields\": []}', '{\"topic\": \"jdbc_test\"}', '{\"data_key\": \"\", \"timestamp_key\": \"time\", \"exclude_fields\": [], \"entry_topic\": \"local.ingest\", \"redis_db_host\": \"localhost\", \"redis_db_port\": 6379, \"index_data\": true, \"redis_db\": 0}', '{}', 'Live', 'SYSTEM', 'SYSTEM', '2023-12-11 11:11:53.803136', '2023-12-11 11:11:53.803136', '2023-12-11 11:11:53.803136', 1);")
    postgresConnect.execute("INSERT INTO \"public\".\"dataset_source_config\" (\"id\", \"dataset_id\", \"connector_type\", \"connector_config\", \"status\", \"connector_stats\", \"created_by\", \"updated_by\", \"created_date\", \"updated_date\", \"published_date\") VALUES\n('jdbc_test.1_jdbc', 'jdbc_test', 'jdbc', '{\"type\": \"postgresql\", \"connection\": {\"host\": \"localhost\", \"port\": \"5432\"}, \"databaseName\": \"obsrv\", \"tableName\": \"emp_data\", \"pollingInterval\": {\"type\": \"periodic\", \"schedule\": \"hourly\"}, \"authenticationMechanism\": {\"encrypted\": true, \"encryptedValues\": \"xfXQl058Kq4Js78+hile8N5DlVaXATPVjYrvUR5weHp/RUSnqIFQVUE0ulGMawtS\"}, \"batchSize\": 1000, \"timestampColumn\": \"time\"}', 'Live', '{\"records\": 490, \"last_fetch_timestamp\": \"2023-12-09T23:43:27\"}', 'SYSTEM', 'SYSTEM', '2023-12-11 11:11:53.923346', '2023-12-11 11:11:53.923346', '2023-12-11 11:11:53.923346');")
    postgresConnect.execute("INSERT INTO \"public\".\"emp_data\" (\"id\", \"first_name\", \"last_name\", \"email\", \"gender\", \"time\") VALUES\n(1, 'Merry', 'Bister', 'mbister0@a8.net', 'Agender', '2023-11-26 11:53:50');")
    postgresConnect.execute("INSERT INTO \"public\".\"emp_data\" (\"id\", \"first_name\", \"last_name\", \"email\", \"gender\", \"time\") VALUES\n(2, 'Cherie', 'Fawltey', 'cfawltey1@phpbb.com', 'Female', '2023-03-05 06:45:40');")
    postgresConnect.execute("INSERT INTO \"public\".\"emp_data\" (\"id\", \"first_name\", \"last_name\", \"email\", \"gender\", \"time\") VALUES\n(3, 'Gardener', 'Prestwich', 'gprestwich2@mayoclinic.com', 'Male', '2023-02-11 10:58:25');")
    postgresConnect.execute("INSERT INTO \"public\".\"emp_data\" (\"id\", \"first_name\", \"last_name\", \"email\", \"gender\", \"time\") VALUES\n(4, 'Yance', 'Scullin', 'yscullin3@hhs.gov', 'Male', '2023-01-12 10:05:39');")
  }


  def createTestTopics(): Unit = {
    EmbeddedKafka.createCustomTopic(topic = "local.ingest")
    EmbeddedKafka.createCustomTopic("spark.stats")
  }

  def checkTestTopicsOffset(): Unit = {
    val topics: util.Collection[String] = new util.ArrayList[String]()
    topics.add("spark.stats")
    EmbeddedKafka.withConsumer[String, String, Unit] {
      val messagesBuffers = topics.asScala.map(_ -> ListBuffer.empty[(String, String)]).toMap
      consumer =>
        consumer.subscribe(topics)
        val recordIterator = consumer.poll(duration2JavaDuration(60.second)).iterator()
        while (recordIterator.hasNext) {
          val record = recordIterator.next
          messagesBuffers(record.topic) += (record.key() -> record.value())
          val tp = new TopicPartition(record.topic, record.partition)
          val om = new OffsetAndMetadata(record.offset + 1)
          consumer.commitSync()
        }
        consumer.close()
        val messages = messagesBuffers.view.mapValues(_.toList).toMap
        messages("spark.stats").length shouldBe >= (0)
        messages("spark.stats").length shouldBe 3
    }
  }
}
