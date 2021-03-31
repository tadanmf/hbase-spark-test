package io.datadynamics

import io.datadynamics.ConfigUtils.RichConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

class ParseTest {

  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val conf = new Configuration()
  private val sparkConfig: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Spark Test Hbase")
  private val spark: SparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()
  private val sc: SparkContext = spark.sparkContext

  @Test
  def test() {
    val path = new Path("hdfs://172.30.1.243:8020/download/input/")
    conf.set("fs.defaultFS", "hdfs://172.30.1.243")
    conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    //    conf.addResource(new Path("file:///test/resource/core-site.xml"))
    val fs: FileSystem = path.getFileSystem(conf)

    val length: Int = fs.listStatus(path).length
    println(s"length = ${length}")

    val baseRdd: RDD[ChatLog] = sc.textFile("hdfs://172.30.1.243:8020/download/input/").map(line => {
      logger.info(s"${line}")
      val chat = new ChatLog
      chat
    })
    baseRdd.map(line => {
      logger.info(s"${line}")
    })
  }
}
