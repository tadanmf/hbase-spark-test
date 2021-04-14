package io.datadynamics.solr

import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.request.CollectionAdminRequest
import org.apache.solr.common.cloud.ZkConfigManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.{Before, Test}
import org.slf4j.{Logger, LoggerFactory}

import java.util
import java.util.Optional
import scala.collection.JavaConverters._

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
    //val zkClient = cloudClient.getZkStateReader.getZkClient
    //val zkConfigManager = new ZkConfigManager(zkClient)
    //zkConfigManager.uploadConfigDir(confDir.toPath, confName)
    val options = Map(
      "collection" -> "films",
      //"zkhost" -> "tt05cn001.hdp.local:2181,tt05nn001.hdp.local:2181,tt05nn002.hdp.local:2181/solrtest"
      "zkhost" -> "172.30.1.221:2181,172.30.1.223:2181,172.30.1.224:2181/solrtest"
    )
    val df: DataFrame = spark.read.format("solr")
      .options(options)
      .load
    df.show()
  }

}
