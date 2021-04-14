package io.datadynamics

import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
 * User
 */
object GetHbaseSpark {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val sparkConfig: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Spark Test Hbase")
  private val spark: SparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()

  def main(args: Array[String]): Unit = {
    sparkConfig.set("spark.hadoop.fs.defaultFS", "hdfs://172.30.1.243")
    sparkConfig.set("spark.hadoop.fs.hdfs.impl", classOf[DistributedFileSystem].getName)


  }
}
