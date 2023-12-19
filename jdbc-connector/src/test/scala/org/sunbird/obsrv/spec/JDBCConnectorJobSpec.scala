package org.sunbird.obsrv.spec

import com.typesafe.config.{Config, ConfigFactory}
import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig, duration2JavaDuration}
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.sunbird.obsrv.core.util.{PostgresConnect, PostgresConnectionConfig}
import org.sunbird.obsrv.fixture.EventFixture
import org.sunbird.obsrv.job.{JDBCConnectorConfig, JDBCConnectorJob}
import org.sunbird.obsrv.model.JobMetric
import org.sunbird.obsrv.registry.DatasetRegistry
import org.sunbird.obsrv.util.{CipherUtil, JSONUtil}

import scala.collection.JavaConverters._
import scala.collection.compat._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._


class JDBCConnectorJobSpec extends FlatSpec with EmbeddedKafka with BeforeAndAfterAll {
  val appConfig: Config = ConfigFactory.load("test.conf").withFallback(ConfigFactory.systemEnvironment())
  val config = new JDBCConnectorConfig(appConfig, Array())

  val postgresConfig: PostgresConnectionConfig = PostgresConnectionConfig(
    user = appConfig.getString("postgres.user"),
    password = appConfig.getString("postgres.password"),
    database = "postgres",
    host = appConfig.getString("postgres.host"),
    port = appConfig.getInt("postgres.port"),
    maxConnections = appConfig.getInt("postgres.maxConnections")
  )

  val externalDbConfig: PostgresConnectionConfig = PostgresConnectionConfig(
    user = appConfig.getString("postgres.user"),
    password = appConfig.getString("postgres.password"),
    database = "postgres",
    host = appConfig.getString("postgres.host"),
    port = 5452,
    maxConnections = appConfig.getInt("postgres.maxConnections")
  )

  val externalDbConfigAdmin: PostgresConnectionConfig = PostgresConnectionConfig(
    user = "admin",
    password = "admin",
    database = "postgres",
    host = appConfig.getString("postgres.host"),
    port = 5452,
    maxConnections = appConfig.getInt("postgres.maxConnections")
  )

  var embeddedPostgres: EmbeddedPostgres = _
  var externalDb: EmbeddedPostgres = _
  val customKafkaConsumerProperties: Map[String, String] = Map[String, String]("group.id" -> "test-consumer-group", "auto.offset.reset" -> "earliest")
  implicit val embeddedKafkaConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(
      kafkaPort = 9092,
      zooKeeperPort = 2183,
      customConsumerProperties = customKafkaConsumerProperties
    )
  implicit val deserializer: StringDeserializer = new StringDeserializer()

  override def beforeAll(): Unit = {
    super.beforeAll()
    embeddedPostgres = EmbeddedPostgres.builder.setPort(5432).start()
    externalDb = EmbeddedPostgres.builder.setPort(5452).start()
    var extPostgresConnect = new PostgresConnect(externalDbConfig)
    val postgresConnect = new PostgresConnect(postgresConfig)
    EmbeddedKafka.start()(embeddedKafkaConfig)
    createTestTopics()
    createTableSchema(postgresConnect)
    createExtUser(extPostgresConnect)
    extPostgresConnect = new PostgresConnect(externalDbConfigAdmin)
    insertTestData(postgresConnect)
    insertMockData(extPostgresConnect)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    embeddedPostgres.close()
    externalDb.close()
    EmbeddedKafka.stop()
  }

  def checkTestTopicsOffset(topicName: String, count: Int): List[String] = {
    var msg = List[String]()
    val topics: java.util.Collection[String] = new java.util.ArrayList[String]()
    topics.add(topicName)
    val consumerPollingTimeout: FiniteDuration = FiniteDuration(1, "minute")
    EmbeddedKafka.withConsumer[String, String, Unit] {
      val messagesBuffers = topics.asScala.map(_ -> ListBuffer.empty[(String, String)]).toMap
      consumer =>
        consumer.subscribe(topics)
        val recordIterator = consumer.poll(duration2JavaDuration(consumerPollingTimeout)).iterator()
        while (recordIterator.hasNext) {
          val record = recordIterator.next
          messagesBuffers(record.topic) += (record.key() -> record.value())
          consumer.commitSync()
        }
        consumer.close()
        val messages = messagesBuffers.mapValues(_.toList).mapValues(_.map(_._2))
        messages(topicName).length shouldBe count
        msg = messages(topicName)
    }
    msg
  }

  def resetTables(postgresConnect: PostgresConnect) : Unit = {
    postgresConnect.execute("TRUNCATE TABLE datasets;")
    postgresConnect.execute("TRUNCATE TABLE datasources;")
    postgresConnect.execute("TRUNCATE TABLE dataset_transformations;")
    postgresConnect.execute("TRUNCATE TABLE dataset_source_config;")
  }

  def createTableSchema(postgresConnect: PostgresConnect): Unit = {
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS datasets (id TEXT PRIMARY KEY,dataset_id TEXT,type TEXT NOT NULL,name TEXT,validation_config JSON,extraction_config JSON,dedup_config JSON,data_schema JSON,denorm_config JSON,router_config JSON,dataset_config JSON,tags TEXT[],data_version INT,status TEXT,created_by TEXT,updated_by TEXT,created_date TIMESTAMP NOT NULL DEFAULT now(),updated_date TIMESTAMP NOT NULL,published_date TIMESTAMP NOT NULL DEFAULT now());")
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS datasources (id TEXT PRIMARY KEY,datasource text NOT NULL,dataset_id TEXT NOT NULL ,ingestion_spec json NOT NULL,datasource_ref text NOT NULL,retention_period json,archival_policy json,purge_policy json,backup_config json NOT NULL,metadata json,status text NOT NULL,created_by text NOT NULL,updated_by text NOT NULL,created_date TIMESTAMP NOT NULL DEFAULT now(),updated_date TIMESTAMP NOT NULL,published_date TIMESTAMP NOT NULL DEFAULT now(),UNIQUE (dataset_id, datasource));")
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS dataset_transformations (id TEXT PRIMARY KEY,dataset_id TEXT NOT NULL,field_key TEXT NOT NULL,transformation_function JSON,mode TEXT,metadata JSON,status TEXT NOT NULL,created_by TEXT NOT NULL,updated_by TEXT NOT NULL,created_date TIMESTAMP NOT NULL DEFAULT now(),updated_date TIMESTAMP NOT NULL,published_date TIMESTAMP NOT NULL DEFAULT now(),UNIQUE (dataset_id, field_key));")
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS dataset_source_config (id TEXT PRIMARY KEY,dataset_id TEXT NOT NULL,connector_type text NOT NULL,connector_config json NOT NULL,status text NOT NULL,connector_stats json,created_by text NOT NULL,updated_by text NOT NULL,created_date TIMESTAMP NOT NULL DEFAULT now(),updated_date TIMESTAMP NOT NULL,published_date TIMESTAMP NOT NULL DEFAULT now(),UNIQUE(connector_type, dataset_id));")
  }

  def createExtUser(connect: PostgresConnect): Unit = {
    connect.execute("CREATE TABLE IF NOT EXISTS emp_data (id INT, first_name text, last_name text, email text, gender text, time timestamp);")
    connect.execute("CREATE USER admin WITH PASSWORD 'admin';")
    connect.execute("GRANT ALL PRIVILEGES ON DATABASE postgres TO admin;")
    connect.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;")
  }

  def insertTestData(postgresConnect: PostgresConnect): Unit = {
    postgresConnect.execute("INSERT INTO \"public\".\"datasets\" (\"id\", \"dataset_id\", \"type\", \"name\", \"validation_config\", \"extraction_config\", \"dedup_config\", \"data_schema\", \"denorm_config\", \"router_config\", \"dataset_config\", \"tags\", \"status\", \"created_by\", \"updated_by\", \"created_date\", \"updated_date\", \"published_date\", \"data_version\") VALUES\n('jdbc_test', 'jdbc_test', 'dataset', 'jdbc_test', '{\"validate\": true, \"mode\": \"Strict\", \"validation_mode\": \"Strict\"}', '{\"is_batch_event\": true, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"eid\", \"dedup_period\": 1036800}, \"batch_id\": \"eid\"}', '{\"drop_duplicates\": false, \"dedup_key\": \"\", \"dedup_period\": 1036800}', '{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": {\"id\": {\"type\": \"integer\", \"arrival_format\": \"number\", \"data_type\": \"integer\"}, \"first_name\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"last_name\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"email\": {\"type\": \"string\", \"suggestions\": [{\"message\": \"The Property ''email'' appears to be ''email'' format type.\", \"advice\": \"Suggest to Mask the Personal Information\", \"resolutionType\": \"TRANSFORMATION\", \"severity\": \"LOW\", \"path\": \"properties.email\"}], \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"gender\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"time\": {\"type\": \"string\", \"format\": \"date-time\", \"suggestions\": [{\"message\": \"The Property ''time'' appears to be ''date-time'' format type.\", \"advice\": \"The System can index all data on this column\", \"resolutionType\": \"INDEX\", \"severity\": \"LOW\", \"path\": \"properties.time\"}], \"arrival_format\": \"text\", \"data_type\": \"date-time\"}}, \"addtionalProperties\": false, \"required\": [\"id\"]}', '{\"redis_db_host\": \"localhost\", \"redis_db_port\": 6379, \"denorm_fields\": []}', '{\"topic\": \"jdbc_test\"}', '{\"data_key\": \"\", \"timestamp_key\": \"time\", \"exclude_fields\": [], \"entry_topic\": \"local.ingest\", \"redis_db_host\": \"localhost\", \"redis_db_port\": 6379, \"index_data\": true, \"redis_db\": 0}', '{}', 'Live', 'SYSTEM', 'SYSTEM', '2023-12-11 11:11:53.803136', '2023-12-11 11:11:53.803136', '2023-12-11 11:11:53.803136', 1);")
    postgresConnect.execute("INSERT INTO \"public\".\"dataset_source_config\" (\"id\", \"dataset_id\", \"connector_type\", \"connector_config\", \"status\", \"connector_stats\", \"created_by\", \"updated_by\", \"created_date\", \"updated_date\", \"published_date\") VALUES\n('jdbc_test.1_jdbc', 'jdbc_test', 'jdbc', '{\"type\": \"postgresql\", \"connection\": {\"host\": \"localhost\", \"port\": \"5452\"}, \"databaseName\": \"postgres\", \"tableName\": \"emp_data\", \"pollingInterval\": {\"type\": \"periodic\", \"schedule\": \"hourly\"}, \"authenticationMechanism\": {\"encrypted\": true, \"encryptedValues\": \"xfXQl058Kq4Js78+hile8N5DlVaXATPVjYrvUR5weHp/RUSnqIFQVUE0ulGMawtS\"}, \"batchSize\": 1, \"timestampColumn\": \"time\"}', 'Live', '{\"records\": 490, \"last_fetch_timestamp\": \"2023-09-09T23:43:27\"}', 'SYSTEM', 'SYSTEM', '2023-12-11 11:11:53.923346', '2023-12-11 11:11:53.923346', '2023-12-11 11:11:53.923346');")

    postgresConnect.execute("INSERT INTO \"public\".\"datasets\" (\"id\", \"dataset_id\", \"type\", \"name\", \"validation_config\", \"extraction_config\", \"dedup_config\", \"data_schema\", \"denorm_config\", \"router_config\", \"dataset_config\", \"tags\", \"status\", \"created_by\", \"updated_by\", \"created_date\", \"updated_date\", \"published_date\", \"data_version\") VALUES\n('jdbc_test_1', 'jdbc_test_1', 'dataset', 'jdbc_test_1', '{\"validate\": true, \"mode\": \"Strict\", \"validation_mode\": \"Strict\"}', '{\"is_batch_event\": false, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"eid\", \"dedup_period\": 1036800}, \"batch_id\": \"eid\"}', '{\"drop_duplicates\": false, \"dedup_key\": \"\", \"dedup_period\": 1036800}', '{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": {\"id\": {\"type\": \"integer\", \"arrival_format\": \"number\", \"data_type\": \"integer\"}, \"first_name\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"last_name\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"email\": {\"type\": \"string\", \"suggestions\": [{\"message\": \"The Property ''email'' appears to be ''email'' format type.\", \"advice\": \"Suggest to Mask the Personal Information\", \"resolutionType\": \"TRANSFORMATION\", \"severity\": \"LOW\", \"path\": \"properties.email\"}], \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"gender\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"time\": {\"type\": \"string\", \"format\": \"date-time\", \"suggestions\": [{\"message\": \"The Property ''time'' appears to be ''date-time'' format type.\", \"advice\": \"The System can index all data on this column\", \"resolutionType\": \"INDEX\", \"severity\": \"LOW\", \"path\": \"properties.time\"}], \"arrival_format\": \"text\", \"data_type\": \"date-time\"}}, \"addtionalProperties\": false, \"required\": [\"id\"]}', '{\"redis_db_host\": \"localhost\", \"redis_db_port\": 6379, \"denorm_fields\": []}', '{\"topic\": \"jdbc_test\"}', '{\"data_key\": \"\", \"timestamp_key\": \"time\", \"exclude_fields\": [], \"entry_topic\": \"local.ingest\", \"redis_db_host\": \"localhost\", \"redis_db_port\": 6379, \"index_data\": true, \"redis_db\": 0}', '{}', 'Live', 'SYSTEM', 'SYSTEM', '2023-12-11 11:11:53.803136', '2023-12-11 11:11:53.803136', '2023-12-11 11:11:53.803136', 1);")
    postgresConnect.execute("INSERT INTO \"public\".\"dataset_source_config\" (\"id\", \"dataset_id\", \"connector_type\", \"connector_config\", \"status\", \"connector_stats\", \"created_by\", \"updated_by\", \"created_date\", \"updated_date\", \"published_date\") VALUES\n('jdbc_test_1.1_jdbc', 'jdbc_test_1', 'jdbc', '{\"type\": \"postgresql\", \"connection\": {\"host\": \"localhost\", \"port\": \"5452\"}, \"databaseName\": \"postgres\", \"tableName\": \"emp_data\", \"pollingInterval\": {\"type\": \"periodic\", \"schedule\": \"hourly\"}, \"authenticationMechanism\": {\"encrypted\": true, \"encryptedValues\": \"xfXQl058Kq4Js78+hile8N5DlVaXATPVjYrvUR5weHp/RUSnqIFQVUE0ulGMawtS\"}, \"batchSize\": 1000, \"timestampColumn\": \"time\"}', 'Live', null, 'SYSTEM', 'SYSTEM', '2023-12-11 11:11:53.923346', '2023-12-11 11:11:53.923346', '2023-12-11 11:11:53.923346');")
  }

  def insertAuthErrors(postgresConnect: PostgresConnect): Unit = {
    postgresConnect.execute("INSERT INTO \"public\".\"datasets\" (\"id\", \"dataset_id\", \"type\", \"name\", \"validation_config\", \"extraction_config\", \"dedup_config\", \"data_schema\", \"denorm_config\", \"router_config\", \"dataset_config\", \"tags\", \"status\", \"created_by\", \"updated_by\", \"created_date\", \"updated_date\", \"published_date\", \"data_version\") VALUES\n('jdbc_test_decrypt_failure', 'jdbc_test_decrypt_failure', 'dataset', 'jdbc_test_decrypt_failure', '{\"validate\": true, \"mode\": \"Strict\", \"validation_mode\": \"Strict\"}', '{\"is_batch_event\": true, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"eid\", \"dedup_period\": 1036800}, \"batch_id\": \"eid\"}', '{\"drop_duplicates\": false, \"dedup_key\": \"\", \"dedup_period\": 1036800}', '{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": {\"id\": {\"type\": \"integer\", \"arrival_format\": \"number\", \"data_type\": \"integer\"}, \"first_name\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"last_name\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"email\": {\"type\": \"string\", \"suggestions\": [{\"message\": \"The Property ''email'' appears to be ''email'' format type.\", \"advice\": \"Suggest to Mask the Personal Information\", \"resolutionType\": \"TRANSFORMATION\", \"severity\": \"LOW\", \"path\": \"properties.email\"}], \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"gender\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"time\": {\"type\": \"string\", \"format\": \"date-time\", \"suggestions\": [{\"message\": \"The Property ''time'' appears to be ''date-time'' format type.\", \"advice\": \"The System can index all data on this column\", \"resolutionType\": \"INDEX\", \"severity\": \"LOW\", \"path\": \"properties.time\"}], \"arrival_format\": \"text\", \"data_type\": \"date-time\"}}, \"addtionalProperties\": false, \"required\": [\"id\"]}', '{\"redis_db_host\": \"localhost\", \"redis_db_port\": 6379, \"denorm_fields\": []}', '{\"topic\": \"jdbc_test\"}', '{\"data_key\": \"\", \"timestamp_key\": \"time\", \"exclude_fields\": [], \"entry_topic\": \"local.ingest\", \"redis_db_host\": \"localhost\", \"redis_db_port\": 6379, \"index_data\": true, \"redis_db\": 0}', '{}', 'Live', 'SYSTEM', 'SYSTEM', '2023-12-11 11:11:53.803136', '2023-12-11 11:11:53.803136', '2023-12-11 11:11:53.803136', 1);")
    postgresConnect.execute("INSERT INTO \"public\".\"dataset_source_config\" (\"id\", \"dataset_id\", \"connector_type\", \"connector_config\", \"status\", \"connector_stats\", \"created_by\", \"updated_by\", \"created_date\", \"updated_date\", \"published_date\") VALUES\n('jdbc_test_decrypt_failure.1_jdbc', 'jdbc_test_decrypt_failure', 'jdbc', '{\"type\": \"postgresql\", \"connection\": {\"host\": \"localhost\", \"port\": \"5452\"}, \"databaseName\": \"postgres\", \"tableName\": \"emp_data\", \"pollingInterval\": {\"type\": \"periodic\", \"schedule\": \"hourly\"}, \"authenticationMechanism\": {\"encrypted\": true, \"encryptedValues\": \"xfXQl058Kq4Js78+hile8N5DlVaXATPVjYrvUR5weHp/RUSnqIFQVUE0ulGMatS\"}, \"batchSize\": 1000, \"timestampColumn\": \"time\"}', 'Live', '{\"records\": 490, \"last_fetch_timestamp\": \"2023-09-09T23:43:27\"}', 'SYSTEM', 'SYSTEM', '2023-12-11 11:11:53.923346', '2023-12-11 11:11:53.923346', '2023-12-11 11:11:53.923346');")

    postgresConnect.execute("INSERT INTO \"public\".\"datasets\" (\"id\", \"dataset_id\", \"type\", \"name\", \"validation_config\", \"extraction_config\", \"dedup_config\", \"data_schema\", \"denorm_config\", \"router_config\", \"dataset_config\", \"tags\", \"status\", \"created_by\", \"updated_by\", \"created_date\", \"updated_date\", \"published_date\", \"data_version\") VALUES\n('jdbc_no_role', 'jdbc_no_role', 'dataset', 'jdbc_no_role', '{\"validate\": true, \"mode\": \"Strict\", \"validation_mode\": \"Strict\"}', '{\"is_batch_event\": true, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"eid\", \"dedup_period\": 1036800}, \"batch_id\": \"eid\"}', '{\"drop_duplicates\": false, \"dedup_key\": \"\", \"dedup_period\": 1036800}', '{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": {\"id\": {\"type\": \"integer\", \"arrival_format\": \"number\", \"data_type\": \"integer\"}, \"first_name\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"last_name\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"email\": {\"type\": \"string\", \"suggestions\": [{\"message\": \"The Property ''email'' appears to be ''email'' format type.\", \"advice\": \"Suggest to Mask the Personal Information\", \"resolutionType\": \"TRANSFORMATION\", \"severity\": \"LOW\", \"path\": \"properties.email\"}], \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"gender\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"time\": {\"type\": \"string\", \"format\": \"date-time\", \"suggestions\": [{\"message\": \"The Property ''time'' appears to be ''date-time'' format type.\", \"advice\": \"The System can index all data on this column\", \"resolutionType\": \"INDEX\", \"severity\": \"LOW\", \"path\": \"properties.time\"}], \"arrival_format\": \"text\", \"data_type\": \"date-time\"}}, \"addtionalProperties\": false, \"required\": [\"id\"]}', '{\"redis_db_host\": \"localhost\", \"redis_db_port\": 6379, \"denorm_fields\": []}', '{\"topic\": \"jdbc_test\"}', '{\"data_key\": \"\", \"timestamp_key\": \"time\", \"exclude_fields\": [], \"entry_topic\": \"local.ingest\", \"redis_db_host\": \"localhost\", \"redis_db_port\": 6379, \"index_data\": true, \"redis_db\": 0}', '{}', 'Live', 'SYSTEM', 'SYSTEM', '2023-12-11 11:11:53.803136', '2023-12-11 11:11:53.803136', '2023-12-11 11:11:53.803136', 1);")
    postgresConnect.execute("INSERT INTO \"public\".\"dataset_source_config\" (\"id\", \"dataset_id\", \"connector_type\", \"connector_config\", \"status\", \"connector_stats\", \"created_by\", \"updated_by\", \"created_date\", \"updated_date\", \"published_date\") VALUES\n('jdbc_no_role.1_jdbc', 'jdbc_no_role', 'jdbc', '{\"type\": \"postgresql\", \"connection\": {\"host\": \"localhost\", \"port\": \"5452\"}, \"databaseName\": \"postgres\", \"tableName\": \"emp_data\", \"pollingInterval\": {\"type\": \"periodic\", \"schedule\": \"hourly\"}, \"authenticationMechanism\": {\"encrypted\": true, \"encryptedValues\": \"aPhIhcW/M9eZh2gNe9CNzqj6q+XtsbxeTvDmcSCojLkkoEbYf7jrgMxGpv8zybBfznxRO6YEwRXE514tfzZaew==\"}, \"batchSize\": 1000, \"timestampColumn\": \"time\"}', 'Live', '{\"records\": 490, \"last_fetch_timestamp\": \"2023-09-09T23:43:27\"}', 'SYSTEM', 'SYSTEM', '2023-12-11 11:11:53.923346', '2023-12-11 11:11:53.923346', '2023-12-11 11:11:53.923346');")

    postgresConnect.execute("INSERT INTO \"public\".\"datasets\" (\"id\", \"dataset_id\", \"type\", \"name\", \"validation_config\", \"extraction_config\", \"dedup_config\", \"data_schema\", \"denorm_config\", \"router_config\", \"dataset_config\", \"tags\", \"status\", \"created_by\", \"updated_by\", \"created_date\", \"updated_date\", \"published_date\", \"data_version\") VALUES\n('jdbc_test_auth_failure', 'jdbc_test_auth_failure', 'dataset', 'jdbc_test_auth_failure', '{\"validate\": true, \"mode\": \"Strict\", \"validation_mode\": \"Strict\"}', '{\"is_batch_event\": true, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"eid\", \"dedup_period\": 1036800}, \"batch_id\": \"eid\"}', '{\"drop_duplicates\": false, \"dedup_key\": \"\", \"dedup_period\": 1036800}', '{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": {\"id\": {\"type\": \"integer\", \"arrival_format\": \"number\", \"data_type\": \"integer\"}, \"first_name\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"last_name\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"email\": {\"type\": \"string\", \"suggestions\": [{\"message\": \"The Property ''email'' appears to be ''email'' format type.\", \"advice\": \"Suggest to Mask the Personal Information\", \"resolutionType\": \"TRANSFORMATION\", \"severity\": \"LOW\", \"path\": \"properties.email\"}], \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"gender\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"time\": {\"type\": \"string\", \"format\": \"date-time\", \"suggestions\": [{\"message\": \"The Property ''time'' appears to be ''date-time'' format type.\", \"advice\": \"The System can index all data on this column\", \"resolutionType\": \"INDEX\", \"severity\": \"LOW\", \"path\": \"properties.time\"}], \"arrival_format\": \"text\", \"data_type\": \"date-time\"}}, \"addtionalProperties\": false, \"required\": [\"id\"]}', '{\"redis_db_host\": \"localhost\", \"redis_db_port\": 6379, \"denorm_fields\": []}', '{\"topic\": \"jdbc_test\"}', '{\"data_key\": \"\", \"timestamp_key\": \"time\", \"exclude_fields\": [], \"entry_topic\": \"local.ingest\", \"redis_db_host\": \"localhost\", \"redis_db_port\": 6379, \"index_data\": true, \"redis_db\": 0}', '{}', 'Live', 'SYSTEM', 'SYSTEM', '2023-12-11 11:11:53.803136', '2023-12-11 11:11:53.803136', '2023-12-11 11:11:53.803136', 1);")
    postgresConnect.execute("INSERT INTO \"public\".\"dataset_source_config\" (\"id\", \"dataset_id\", \"connector_type\", \"connector_config\", \"status\", \"connector_stats\", \"created_by\", \"updated_by\", \"created_date\", \"updated_date\", \"published_date\") VALUES\n('jdbc_test_auth_failure.1_jdbc', 'jdbc_test_auth_failure', 'jdbc', '{\"type\": \"postgresql\", \"connection\": {\"host\": \"localhost\", \"port\": \"5452\"}, \"databaseName\": \"postgres\", \"tableName\": \"emp_data\", \"pollingInterval\": {\"type\": \"periodic\", \"schedule\": \"hourly\"}, \"authenticationMechanism\": {\"encrypted\": true, \"encryptedValues\": \"aPhIhcW/M9eZh2gNe9CNzqKh+tzz5ToEHYIiJFwIqiozID9VPRf3PX2o4W/nGnUa4e0yagYTE0ojHqzKcYEwGg==\"}, \"batchSize\": 1000, \"timestampColumn\": \"time\"}', 'Live', '{\"records\": 490, \"last_fetch_timestamp\": \"2023-09-09T23:43:27\"}', 'SYSTEM', 'SYSTEM', '2023-12-11 11:11:53.923346', '2023-12-11 11:11:53.923346', '2023-12-11 11:11:53.923346');")

    postgresConnect.execute("INSERT INTO \"public\".\"datasets\" (\"id\", \"dataset_id\", \"type\", \"name\", \"validation_config\", \"extraction_config\", \"dedup_config\", \"data_schema\", \"denorm_config\", \"router_config\", \"dataset_config\", \"tags\", \"status\", \"created_by\", \"updated_by\", \"created_date\", \"updated_date\", \"published_date\", \"data_version\") VALUES\n('jdbc_test_invalid_host', 'jdbc_test_invalid_host', 'dataset', 'jdbc_test_invalid_host', '{\"validate\": true, \"mode\": \"Strict\", \"validation_mode\": \"Strict\"}', '{\"is_batch_event\": true, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"eid\", \"dedup_period\": 1036800}, \"batch_id\": \"eid\"}', '{\"drop_duplicates\": false, \"dedup_key\": \"\", \"dedup_period\": 1036800}', '{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": {\"id\": {\"type\": \"integer\", \"arrival_format\": \"number\", \"data_type\": \"integer\"}, \"first_name\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"last_name\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"email\": {\"type\": \"string\", \"suggestions\": [{\"message\": \"The Property ''email'' appears to be ''email'' format type.\", \"advice\": \"Suggest to Mask the Personal Information\", \"resolutionType\": \"TRANSFORMATION\", \"severity\": \"LOW\", \"path\": \"properties.email\"}], \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"gender\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"time\": {\"type\": \"string\", \"format\": \"date-time\", \"suggestions\": [{\"message\": \"The Property ''time'' appears to be ''date-time'' format type.\", \"advice\": \"The System can index all data on this column\", \"resolutionType\": \"INDEX\", \"severity\": \"LOW\", \"path\": \"properties.time\"}], \"arrival_format\": \"text\", \"data_type\": \"date-time\"}}, \"addtionalProperties\": false, \"required\": [\"id\"]}', '{\"redis_db_host\": \"localhost\", \"redis_db_port\": 6379, \"denorm_fields\": []}', '{\"topic\": \"jdbc_test\"}', '{\"data_key\": \"\", \"timestamp_key\": \"time\", \"exclude_fields\": [], \"entry_topic\": \"local.ingest\", \"redis_db_host\": \"localhost\", \"redis_db_port\": 6379, \"index_data\": true, \"redis_db\": 0}', '{}', 'Live', 'SYSTEM', 'SYSTEM', '2023-12-11 11:11:53.803136', '2023-12-11 11:11:53.803136', '2023-12-11 11:11:53.803136', 1);")
    postgresConnect.execute("INSERT INTO \"public\".\"dataset_source_config\" (\"id\", \"dataset_id\", \"connector_type\", \"connector_config\", \"status\", \"connector_stats\", \"created_by\", \"updated_by\", \"created_date\", \"updated_date\", \"published_date\") VALUES\n('jdbc_test_invalid_host.1_jdbc', 'jdbc_test_invalid_host', 'jdbc', '{\"type\": \"postgresql\", \"connection\": {\"host\": \"127.0.0.9\", \"port\": \"5452\"}, \"databaseName\": \"postgres\", \"tableName\": \"emp_data\", \"pollingInterval\": {\"type\": \"periodic\", \"schedule\": \"hourly\"}, \"authenticationMechanism\": {\"encrypted\": true, \"encryptedValues\": \"aPhIhcW/M9eZh2gNe9CNzqKh+tzz5ToEHYIiJFwIqiozID9VPRf3PX2o4W/nGnUa4e0yagYTE0ojHqzKcYEwGg==\"}, \"batchSize\": 1000, \"timestampColumn\": \"time\"}', 'Live', '{\"records\": 490, \"last_fetch_timestamp\": \"2023-09-09T23:43:27\"}', 'SYSTEM', 'SYSTEM', '2023-12-11 11:11:53.923346', '2023-12-11 11:11:53.923346', '2023-12-11 11:11:53.923346');")

    postgresConnect.execute("INSERT INTO \"public\".\"datasets\" (\"id\", \"dataset_id\", \"type\", \"name\", \"validation_config\", \"extraction_config\", \"dedup_config\", \"data_schema\", \"denorm_config\", \"router_config\", \"dataset_config\", \"tags\", \"status\", \"created_by\", \"updated_by\", \"created_date\", \"updated_date\", \"published_date\", \"data_version\") VALUES\n('jdbc_test', 'jdbc_test', 'dataset', 'jdbc_test', '{\"validate\": true, \"mode\": \"Strict\", \"validation_mode\": \"Strict\"}', '{\"is_batch_event\": true, \"extraction_key\": \"events\", \"dedup_config\": {\"drop_duplicates\": true, \"dedup_key\": \"eid\", \"dedup_period\": 1036800}, \"batch_id\": \"eid\"}', '{\"drop_duplicates\": false, \"dedup_key\": \"\", \"dedup_period\": 1036800}', '{\"$schema\": \"https://json-schema.org/draft/2020-12/schema\", \"type\": \"object\", \"properties\": {\"id\": {\"type\": \"integer\", \"arrival_format\": \"number\", \"data_type\": \"integer\"}, \"first_name\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"last_name\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"email\": {\"type\": \"string\", \"suggestions\": [{\"message\": \"The Property ''email'' appears to be ''email'' format type.\", \"advice\": \"Suggest to Mask the Personal Information\", \"resolutionType\": \"TRANSFORMATION\", \"severity\": \"LOW\", \"path\": \"properties.email\"}], \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"gender\": {\"type\": \"string\", \"arrival_format\": \"text\", \"data_type\": \"string\"}, \"time\": {\"type\": \"string\", \"format\": \"date-time\", \"suggestions\": [{\"message\": \"The Property ''time'' appears to be ''date-time'' format type.\", \"advice\": \"The System can index all data on this column\", \"resolutionType\": \"INDEX\", \"severity\": \"LOW\", \"path\": \"properties.time\"}], \"arrival_format\": \"text\", \"data_type\": \"date-time\"}}, \"addtionalProperties\": false, \"required\": [\"id\"]}', '{\"redis_db_host\": \"localhost\", \"redis_db_port\": 6379, \"denorm_fields\": []}', '{\"topic\": \"jdbc_test\"}', '{\"data_key\": \"\", \"timestamp_key\": \"time\", \"exclude_fields\": [], \"entry_topic\": \"local.ingest\", \"redis_db_host\": \"localhost\", \"redis_db_port\": 6379, \"index_data\": true, \"redis_db\": 0}', '{}', 'Live', 'SYSTEM', 'SYSTEM', '2023-12-11 11:11:53.803136', '2023-12-11 11:11:53.803136', '2023-12-11 11:11:53.803136', 1);")
    postgresConnect.execute("INSERT INTO \"public\".\"dataset_source_config\" (\"id\", \"dataset_id\", \"connector_type\", \"connector_config\", \"status\", \"connector_stats\", \"created_by\", \"updated_by\", \"created_date\", \"updated_date\", \"published_date\") VALUES\n('jdbc_test.1_jdbc', 'jdbc_test', 'jdbc', '{\"type\": \"postgresql\", \"connection\": {\"host\": \"localhost\", \"port\": \"5452\"}, \"databaseName\": \"postgres\", \"tableName\": \"emp_data\", \"pollingInterval\": {\"type\": \"periodic\", \"schedule\": \"hourly\"}, \"authenticationMechanism\": {\"encrypted\": true, \"encryptedValues\": \"xfXQl058Kq4Js78+hile8N5DlVaXATPVjYrvUR5weHp/RUSnqIFQVUE0ulGMawtS\"}, \"batchSize\": 1, \"timestampColumn\": \"time\"}', 'Live', '{}', 'SYSTEM', 'SYSTEM', '2023-12-11 11:11:53.923346', '2023-12-11 11:11:53.923346', '2023-12-11 11:11:53.923346');")
  }

  def insertMockData(postgresConnect: PostgresConnect): Unit = {
    postgresConnect.execute("CREATE TABLE IF NOT EXISTS emp_data (id INT, first_name text, last_name text, email text, gender text, time timestamp);")
    postgresConnect.execute("INSERT INTO \"public\".\"emp_data\" (\"id\", \"first_name\", \"last_name\", \"email\", \"gender\", \"time\") VALUES\n(1, 'Merry', 'Bister', 'mbister0@a8.net', 'Agender', '2023-11-26 11:53:50');")
    postgresConnect.execute("INSERT INTO \"public\".\"emp_data\" (\"id\", \"first_name\", \"last_name\", \"email\", \"gender\", \"time\") VALUES\n(2, 'Cherie', 'Fawltey', 'cfawltey1@phpbb.com', 'Female', '2023-11-05 06:45:40');")
    postgresConnect.execute("INSERT INTO \"public\".\"emp_data\" (\"id\", \"first_name\", \"last_name\", \"email\", \"gender\", \"time\") VALUES\n(3, 'Gardener', 'Prestwich', 'gprestwich2@mayoclinic.com', 'Male', '2023-11-11 10:58:25');")
    postgresConnect.execute("INSERT INTO \"public\".\"emp_data\" (\"id\", \"first_name\", \"last_name\", \"email\", \"gender\", \"time\") VALUES\n(4, 'Yance', 'Scullin', 'yscullin3@hhs.gov', 'Male', '2023-11-12 10:05:39');")
    val rs = postgresConnect.executeQuery("SELECT COUNT(*) FROM emp_data;")
    rs.next()
    assert(rs.getInt(1) equals 4)
  }


  def createTestTopics(): Unit = {
    EmbeddedKafka.createCustomTopic(topic = "local.ingest")
    EmbeddedKafka.createCustomTopic("spark.stats")
  }

  "JDBCConnectorJob" should "run successfully generating metrics" in {
    val datasets = DatasetRegistry.getAllDatasets("dataset")
    val datasetSourceConfigs = DatasetRegistry.getAllDatasetSourceConfig()
    JDBCConnectorJob.main(Array())
    assert(datasetSourceConfigs.get.size equals 2)
    assert(datasets.size equals 2)
    val messages = checkTestTopicsOffset("spark.stats", 12)
    var metric = JSONUtil.deserialize(messages.head, classOf[JobMetric])

    // Validate fetch metric
    assert(metric.edata.metric("batch_count").asInstanceOf[Int] equals 1)
    assert(metric.edata.metric("fetched_event_count").asInstanceOf[Number].longValue() equals 1L)
    assert(metric.edata.metric("fetched_time_in_ms").asInstanceOf[Number].longValue() > 0L)
    // Validate processing metric
    metric = JSONUtil.deserialize(messages(1), classOf[JobMetric])
    assert(metric.edata.metric("batch_count").asInstanceOf[Int] equals 1)
    assert(metric.edata.metric("processed_event_count").asInstanceOf[Number].longValue() equals 1L)
    assert(metric.edata.metric("processing_time_in_ms").asInstanceOf[Number].longValue() > 0L)
    // Validate fetch metric
    metric = JSONUtil.deserialize(messages(11), classOf[JobMetric])
    assert(metric.edata.metric("batch_count").asInstanceOf[Int] equals 2)
    assert(metric.edata.metric("fetched_event_count").asInstanceOf[Number].longValue() equals 0L)
    assert(metric.edata.metric("fetched_time_in_ms").asInstanceOf[Number].longValue() > 0L)
  }

  "JDBCConnectorJob" should "Generate failed metrics" in {
    val postgresConnect = new PostgresConnect(postgresConfig)
    resetTables(postgresConnect)
    insertAuthErrors(postgresConnect = postgresConnect)
    val datasets = DatasetRegistry.getAllDatasets("dataset")
    val datasetSourceConfigs = DatasetRegistry.getAllDatasetSourceConfig()
    JDBCConnectorJob.main(Array())
    assert(datasetSourceConfigs.get.size equals 5)
    assert(datasets.size equals 5)
    val messages = checkTestTopicsOffset("spark.stats", 15)
  }
}
