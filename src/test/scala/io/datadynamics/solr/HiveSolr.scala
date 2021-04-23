package io.datadynamics.solr

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}
import com.hortonworks.spark.sql.hive.llap.{HiveWarehouseBuilder, HiveWarehouseSessionImpl}

import java.util.Properties

/**
 * User
 */
class HiveSolr {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  //private val sparkConfig: SparkConf = new SparkConf().setMaster("yarn-cluster").setAppName("Solr-Hive Test with Spark")
  //private val spark: SparkSession = SparkSession.builder().config(sparkConfig).enableHiveSupport().getOrCreate()

  @Test
  def connectHiveTest(): Unit = {
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Solr-Hive Test with Spark")
    sparkConfig.set("spark.sql.warehouse.dir", "warehouse")
    val spark: SparkSession = SparkSession.builder().config(sparkConfig).enableHiveSupport().getOrCreate()

    // Turn on flag for Hive Dynamic Partitioning
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")


    //spark.sqlContext.setConf("hive.hiveserver2.jdbc.url", "jdbc:hive2://tt05nn001.hdp.local:2181,tt05cn001.hdp.local:2181,tt05nn002.hdp.local:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;user=hive;password=hive")

    //spark.sparkContext.getConf.set("spark.datasource.hive.warehouse.load.staging.dir", "/tmp")
    //spark.sparkContext.getConf.set("spark.datasource.hive.warehouse.metastoreUri", "thrift://tt05cn001.hdp.local:9083")
    //spark.sparkContext.getConf.set("spark.hadoop.hive.llap.daemon.service.hosts", "@lldap0")
    //spark.sparkContext.getConf.set("spark.hadoop.hive.zookeeper.quorum", "tt05nn001.hdp.local:2181,tt05cn001.hdp.local:2181,tt05nn002.hdp.local:2181")
    //spark.sparkContext.getConf.set("spark.sql.hive.hiveserver2.jdbc.url", "jdbc:hive2://tt05nn001.hdp.local:2181,tt05cn001.hdp.local:2181,tt05nn002.hdp.local:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;user=hive;password=hive")

    spark.conf.set("spark.sql.hive.hiveserver2.jdbc.url", "jdbc:hive2://tt05nn001.hdp.local:2181,tt05cn001.hdp.local:2181,tt05nn002.hdp.local:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;user=hive;password=hive")

    logger.info(s"spark conf >> ${spark.conf.get("spark.sql.hive.hiveserver2.jdbc.url")}")

    import spark.sql

    val sqlStr = "SELECT chat['maxxxxkr'] FROM hbase_hive_table3 WHERE chat['maxxxxkr'] IS NOT NULL LIMIT 5"
    //sql(sqlStr).show()

    val hive: HiveWarehouseSessionImpl = HiveWarehouseBuilder.session(spark).build()
    hive.showTables().show()

    hive.execute(sqlStr).show()
  }

  @Test
  def selectChatsTest(): Unit = {
    // 검색어
    val userNick = ""

    // get solr result

    // get hive result

    // output: [nick] chat text
  }
}
