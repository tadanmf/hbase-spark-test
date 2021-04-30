package io.datadynamics

import io.datadynamics.ParseToParquet.{getClass, logger, spark}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

class HdfsTest {

  @Test
  def connectTest(): Unit = {
    val path = new Path("hdfs://nn/download/input/")
    //val path = new Path("hdfs://172.30.1.243:8020/download/input/")
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://nn")
    //conf.set("fs.defaultFS", "hdfs://172.30.1.243")
    conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    //    conf.addResource(new Path("file:///test/resource/core-site.xml"))
    val fs: FileSystem = path.getFileSystem(conf)

    val length: Int = fs.listStatus(path).length
    println(s"length = ${length}")
  }

  @Test
  def deleteTest(): Unit = {
    val logger: Logger = LoggerFactory.getLogger(getClass)
    val sparkConfig: SparkConf = new SparkConf().setMaster("local[2]").setAppName("delete directory")
    val spark: SparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()

    val outputDir = "hdfs://nn/download/output_star_parquet"

    val outputPath = new Path(outputDir)
    val fs: FileSystem = outputPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
      logger.warn(s"delete ${outputPath}")
    }
  }
}
