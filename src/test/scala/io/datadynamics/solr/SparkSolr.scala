package io.datadynamics.solr

import io.datadynamics.ChatLog
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.convertScanToString
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ColumnFamilySchema
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.request.CollectionAdminRequest
import org.apache.solr.common.cloud.ZkConfigManager
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.{Before, Test}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{ByteArrayInputStream, DataInputStream}
import java.util
import java.util.Optional
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * User
 */
class SparkSolr {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val sparkConfig: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Solr Test with Spark")
  private val spark: SparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()

  @Before
  def setup(): Unit = {
  }

  @Test
  def solrTest(): Unit = {
    val options = Map(
      "collection" -> "collection1_config1_shards3",
      "zkhost" -> "tt05cn001.hdp.local:2181,tt05nn001.hdp.local:2181,tt05nn002.hdp.local:2181"
    )

    val df: DataFrame = spark.read.format("solr").options(options).load()

    df.foreach(row => logger.info(s"row >> ${row}"))
  }

  @Test
  def connectSolrJTest(): Unit = {
    val zkHost = "tt05cn001.hdp.local:2181,tt05nn001.hdp.local:2181,tt05nn002.hdp.local:2181"
    val collectionName = "collection1_config1_shards3"

    val client: CloudSolrClient = new CloudSolrClient.Builder(
      List("tt05cn001.hdp.local:2181", "tt05nn001.hdp.local:2181", "tt05nn002.hdp.local:2181").asJava,
      Optional.of("/solrtest")
    ).build()

    client.connect()

    //val solrRDD = new SelectSolrRDD(zkHost, collectionName, spark.sparkContext)
    //logger.info(s"solrRDD.count() >> ${solrRDD.count()}")

    val collections: util.List[String] = CollectionAdminRequest.listCollections(client)
    logger.info(s"collections >> ${collections}")

    client.close()
  }

  @Test
  def connectTest(): Unit = {
    val options = Map(
      "collection" -> "films",
      "zkhost" -> "tt05cn001.hdp.local:2181,tt05nn001.hdp.local:2181,tt05nn002.hdp.local:2181/solrtest"
      //"zkhost" -> "172.30.1.241:2181,172.30.1.243:2181,172.30.1.244:2181/solrtest"
    )
    val df: DataFrame = spark.read.format("solr")
      .options(options)
      .option("filters", "name:Big")
      .load
    df.show()
  }

  @Test
  def csvWriteTest(): Unit = {
    val csvPath = "hdfs://172.30.1.243:8020/download/input/films.csv"

    val csvDF: DataFrame = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(csvPath)

    logger.info(s"csvDF.count() >> ${csvDF.count()}")

    val options = Map(
      "zkhost" -> "172.30.1.241:2181,172.30.1.243:2181,172.30.1.244:2181/solrtest",
      "collection" -> "films",
      "commit_within" -> "2000",
      "gen_uniq_key" -> "true"
    )

    csvDF.write.format("solr").options(options).mode(org.apache.spark.sql.SaveMode.Overwrite).save
  }

  @Test
  def chatWriteTest(): Unit = {

    val chatPath = "hdfs://172.30.1.243:8020/download/input/*"
    val partitions = 5

    val baseRdd = spark.sparkContext.textFile(chatPath, partitions).map(line => {
      ChatLog.create(line)
    })

    val valueRdd: RDD[Row] = baseRdd.groupBy(chat => {
      (chat.userId, chat.userNick, chat.bStartTime)
    }).map(kv => {
      val result: ((String, String, Long), Iterable[ChatLog]) = kv
      val (userId: String, userNick:String, bStartTime:Long) = result._1
      //Row(userId, userNick, bStartTime)

      val chatLogs: Iterable[ChatLog] = result._2
      var changeTime: Long = chatLogs.head.chatNow
      chatLogs.map(chatLog => {
        if (changeTime > chatLog.chatNow) {
          changeTime = chatLog.chatNow
        }
      })
      Row(userId, userNick, bStartTime, changeTime)
    })

    logger.info(s"valueRdd.count() >> ${valueRdd.count()}")

    val options = Map(
      "zkhost" -> "172.30.1.241:2181,172.30.1.243:2181,172.30.1.244:2181/solrtest",
      "collection" -> "chat_nick_change",
      "commit_within" -> "2000",
      "gen_uniq_key" -> "true"
    )

    val schema: StructType = StructType(Array(
      StructField("userId", DataTypes.StringType),
      StructField("userNick", DataTypes.StringType),
      StructField("bStartTime", LongType),
      StructField("nickChangeTime", LongType)
    ))

    val valueDF: DataFrame = spark.createDataFrame(valueRdd, schema)

    logger.info(s"valueDF.count() >> ${valueDF.count()}")

    valueDF.write.format("solr").options(options).mode(org.apache.spark.sql.SaveMode.Overwrite).save
  }

  @Test
  def readChatTest(): Unit = {
    val userNick = "HoneyBread"

    val options = Map(
      "collection" -> "chats",
      "zkhost" -> "tt05cn001.hdp.local:2181,tt05nn001.hdp.local:2181,tt05nn002.hdp.local:2181/solrtest"
    )
    val df: DataFrame = spark.read.format("solr")
      .options(options)
      .option("filters", s"userNick:${userNick}")
      .load

    val cfqs: Array[(String, String)] = df.collect().map(row => {
      val cf: String = row.getAs[Long](0).toString
      val q: String = row.getAs[String]("userId")
      (cf, q)
    })

    // hbase setting
    val hbaseConfig: Configuration = HBaseConfiguration.create(spark.sparkContext.hadoopConfiguration)
    hbaseConfig.set("hbase.zookeeper.quorum", "tt05gn001.hdp.local,tt05nn001.hdp.local,tt05nn002.hdp.local")
    hbaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure")
    hbaseConfig.setInt("hbase.zookeeper.property.clientPort", 2181)

    val scan = new Scan()
    cfqs.foreach(cfq => {
      scan.addColumn(cfq._1.getBytes, cfq._2.getBytes)
    })
    hbaseConfig.set(TableInputFormat.INPUT_TABLE, "chat")
    hbaseConfig.set(TableInputFormat.SCAN, convertScanToString(scan))

    val resultRdd: RDD[(ImmutableBytesWritable, Result)] =
      spark.sparkContext.newAPIHadoopRDD(hbaseConfig, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val values: RDD[(String, String)] = resultRdd.flatMap(kv => {
      val result: Result = kv._2
      //result.advance()
      val buf = new ArrayBuffer[(String, String)]()
      while (result.advance()) {
        val cell: Cell = result.current()
        val qualifier: Array[Byte] = CellUtil.cloneQualifier(cell)
        val value: Array[Byte] = CellUtil.cloneValue(cell)

        val dis = new DataInputStream(new ByteArrayInputStream(value))
        val size: Int = dis.readInt()
        val chatMessage: String = dis.readUTF()
        buf += ((new String(qualifier), chatMessage))
      }
      buf
    })

    //values.take(3).map(s => logger.info(s"[${s._1}] ${s._2}"))

    val sortedRdd: RDD[(String, String)] = values.sortByKey()

    sortedRdd.take(3).map(s => logger.info(s"[${s._1}] ${s._2}"))

    /*
    df.collect().foreach(row => {

      //val cf = row.get(0).toString
      //val q = row.get(2).toString
      val cf: String = row.getAs[String](0)
      val q: String = row.getAs[String]("userId")

      logger.info(s"cf, q >>>> ${cf}, ${q}")

      val scan = new Scan()
      scan.addColumn(cf.getBytes, q.getBytes)

      hbaseConfig.set(TableInputFormat.INPUT_TABLE, "create_test")
      hbaseConfig.set(TableInputFormat.SCAN, convertScanToString(scan))

      val resultRdd: RDD[(ImmutableBytesWritable, Result)] =
        spark.sparkContext.newAPIHadoopRDD(hbaseConfig, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

      val values: RDD[(String, String)] = resultRdd.map(kv => {
        val result: Result = kv._2
        result.advance()
        val cell: Cell = result.current()
        val qualifier: Array[Byte] = CellUtil.cloneQualifier(cell)
        val value: Array[Byte] = CellUtil.cloneValue(cell)

        val dis = new DataInputStream(new ByteArrayInputStream(value))
        val size: Int = dis.readInt()
        val chatMessage: String = dis.readUTF()
        (new String(qualifier), chatMessage)
      })

      values.take(3).map(s => logger.info(s"[${s._1}] ${s._2}"))
    })
    */

  }

  @Test
  def chatWriteTest2(): Unit = {

    val chatPath = "hdfs://172.30.1.243:8020/download/input/chat_1582635623000.log"
    val partitions = 5

    val baseRdd = spark.sparkContext.textFile(chatPath, partitions).map(line => {
      ChatLog.create(line)
    })

    val valueRdd: RDD[Row] = baseRdd.groupBy(chat => {
      (chat.userId, chat.userNick, chat.bStartTime)
    }).map(kv => {
      val result: ((String, String, Long), Iterable[ChatLog]) = kv
      val (userId: String, userNick: String, bStartTime: Long) = result._1
      val chatLogs: Iterable[ChatLog] = result._2
      var changeTime: Long = chatLogs.head.chatNow
      chatLogs.map(chatLog => {
        if (changeTime < chatLog.chatNow) {
          changeTime = chatLog.chatNow
        }
      })
      Row(userId, userNick, bStartTime, changeTime)
    })

    valueRdd.take(3).map(s => logger.info(s"s >> ${s}"))
  }

  @Test
  def createCollectionTest(): Unit = {
    val shards = 5
    val replicas = 2
    val collection = "chat_nick_change"
    val client: CloudSolrClient = new CloudSolrClient.Builder(
      List("tt05cn001.hdp.local:2181", "tt05nn001.hdp.local:2181", "tt05nn002.hdp.local:2181").asJava,
      Optional.of("/solrtest")
    ).build()

    val create = CollectionAdminRequest.createCollection(collection, shards, replicas)
    create.setMaxShardsPerNode(replicas)
    logger.debug(s"create = ${create}")
    val response = create.process(client)
    logger.debug(s"response = ${response}")
  }

  @Test
  def deleteDocument(): Unit = {
    val collection = "chat_nick_change"
    val client: CloudSolrClient = new CloudSolrClient.Builder(
      List("tt05cn001.hdp.local:2181", "tt05nn001.hdp.local:2181", "tt05nn002.hdp.local:2181").asJava,
      Optional.of("/solrtest")
    ).build()

    val query = "*:*"
    val deleteResponse = client.deleteByQuery(collection, query)
    logger.debug(s"deleteResponse = ${deleteResponse}")
    val commitResponse = client.commit(collection, false, false)
    logger.debug(s"commitResponse = ${commitResponse}")
  }

  @Test
  def deleteCollectionTest(): Unit = {
    val collection = "chat_nick_change"
    val client: CloudSolrClient = new CloudSolrClient.Builder(
      List("tt05cn001.hdp.local:2181", "tt05nn001.hdp.local:2181", "tt05nn002.hdp.local:2181").asJava,
      Optional.of("/solrtest")
    ).build()

    val delete = CollectionAdminRequest.deleteCollection(collection)
    logger.debug(s"delete = ${delete}")
    val deleteResponse = delete.process(client)
    logger.debug(s"deleteResponse = ${deleteResponse}")

    val deleteConfigUrl = s"http://tt05dn003.hdp.local:8983/solr/admin/configs?action=DELETE&name=${collection}&omitHeader=true"
    Source.fromURL(deleteConfigUrl).close()
  }
}
