package io.datadynamics

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

object HbaseSpark {

  private val logger = LoggerFactory.getLogger("HbaseSpark")

  def main(args: Array[String]): Unit = {
    val usage: String = s"""${getClass.getSimpleName} <CONF_FILE_PATH>([bj_ids:Seq[String]],from_date.format,to_date.format,[collection],[partitions])""".stripMargin

    if (args.length < 1) {
      println(usage)
      System.exit(1)
    }

    val conf = new Configuration
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set("hbase.zookeeper.quorum", "tt05nn001.hdp.local,tt05cn001.hdp.local,tt05nn002.hdp.local")

    val sparkConf: SparkConf = new SparkConf()
//    sparkConf.setMaster("yarn").setAppName("asdfadfsgadfhg")
    sparkConf.setMaster("local[2]").setAppName("asdfadfsgadfhg")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val fs: FileSystem = FileSystem.get(conf)
    val chatFiles = new Path("/download/input/")

    val partitions: Int = 6

    val sc: SparkContext = spark.sparkContext

    val df = spark.read.option("header", "false").csv(args(0))
    df.show()
  }
}
