package io.datadynamics.format

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.convertScanToString
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

import java.io.{ByteArrayInputStream, DataInputStream}
import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer

/**
 * User
 */
class HbaseToParquet {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val sparkConfig: SparkConf = new SparkConf().setMaster("local[2]").setAppName("export hbase to parquet")
  private val spark: SparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()

  @Test
  def parseChatToParquet(): Unit = {
    // input
    val table = "chat"
    val cf = "1582635623000"
    val qualifier = "jeondaehun"
    val outputDir = "hdfs://172.30.1.243:8020/download/output"

    // hbase setting
    val hbaseConfig: Configuration = HBaseConfiguration.create(spark.sparkContext.hadoopConfiguration)
    hbaseConfig.set("hbase.zookeeper.quorum", "tt05gn001.hdp.local,tt05nn001.hdp.local,tt05nn002.hdp.local")
    hbaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure")
    hbaseConfig.setInt("hbase.zookeeper.property.clientPort", 2181)

    // scan setting
    val scan = new Scan()
    scan.addColumn(cf.getBytes(), qualifier.getBytes())
    hbaseConfig.set(TableInputFormat.INPUT_TABLE, table)
    hbaseConfig.set(TableInputFormat.SCAN, convertScanToString(scan))

    // get data from hbase
    val resultRdd: RDD[(ImmutableBytesWritable, Result)] =
      spark.sparkContext.newAPIHadoopRDD(hbaseConfig, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val values: RDD[Row] = resultRdd.flatMap(kv => {
      val result: Result = kv._2
      val buf = new ArrayBuffer[Row]()
      while (result.advance()) {
        val cell: Cell = result.current()
        val columnFamily: Array[Byte] = CellUtil.cloneFamily(cell)
        val qualifier: Array[Byte] = CellUtil.cloneQualifier(cell)
        val value: Array[Byte] = CellUtil.cloneValue(cell)
        val timestamp: Long = cell.getTimestamp

        val dis = new DataInputStream(new ByteArrayInputStream(value))
        val size: Int = dis.readInt()
        val chatMessage: String = dis.readUTF()
        buf += Row(Bytes.toString(kv._1.get()), ByteBuffer.wrap(columnFamily).getLong, new String(qualifier), chatMessage, timestamp)
      }
      buf
    })

    // key, cf, q, value, ts
    // (String, Long, String, String, Long)

    // rdd to df
    val schema: StructType = StructType(Array(
      StructField("key", DataTypes.StringType),
      StructField("bStartTime", LongType),
      StructField("userId", DataTypes.StringType),
      StructField("chatText", DataTypes.StringType),
      StructField("timestamp", LongType)
    ))
    val df: DataFrame = spark.createDataFrame(values, schema)

    df.show(5)

    // delete output path
    val outputPath = new Path(outputDir)
    val fs: FileSystem = outputPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
      logger.warn(s"delete ${outputPath}")
    }

    df.sort("key").write.parquet(outputDir)
  }

  @Test
  def parseStarToParquet(): Unit = {
    // input
    val table = "star"
    val cf = "1582635623000"
    val qualifier = "jeondaehun"
    val outputDir = "hdfs://172.30.1.243:8020/download/output_star_parquet"

    // hbase setting
    val hbaseConfig: Configuration = HBaseConfiguration.create(spark.sparkContext.hadoopConfiguration)
    hbaseConfig.set("hbase.zookeeper.quorum", "tt05gn001.hdp.local,tt05nn001.hdp.local,tt05nn002.hdp.local")
    hbaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure")
    hbaseConfig.setInt("hbase.zookeeper.property.clientPort", 2181)

    // scan setting
    val scan = new Scan()
    scan.addFamily(cf.getBytes)
    //scan.addColumn(cf.getBytes(), qualifier.getBytes())
    hbaseConfig.set(TableInputFormat.INPUT_TABLE, table)
    hbaseConfig.set(TableInputFormat.SCAN, convertScanToString(scan))

    // get data from hbase
    val resultRdd: RDD[(ImmutableBytesWritable, Result)] =
      spark.sparkContext.newAPIHadoopRDD(hbaseConfig, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val values: RDD[Row] = resultRdd.flatMap(kv => {
      val result: Result = kv._2
      val buf = new ArrayBuffer[Row]()
      while (result.advance()) {
        val cell: Cell = result.current()
        val columnFamily: Array[Byte] = CellUtil.cloneFamily(cell)
        val qualifier: Array[Byte] = CellUtil.cloneQualifier(cell)
        val value: Array[Byte] = CellUtil.cloneValue(cell)
        val timestamp: Long = cell.getTimestamp

        buf += Row(Bytes.toString(kv._1.get()), ByteBuffer.wrap(columnFamily).getLong, new String(qualifier), ByteBuffer.wrap(value).getInt, timestamp)
      }
      buf
    })

    // key, cf, q, value, ts
    // (String, Long, String, String, Long)

    // rdd to df
    val schema: StructType = StructType(Array(
      StructField("key", DataTypes.StringType),
      StructField("bStartTime", LongType),
      StructField("userId", DataTypes.StringType),
      StructField("balloonNum", DataTypes.IntegerType),
      StructField("timestamp", LongType)
    ))
    val df: DataFrame = spark.createDataFrame(values, schema)

    //df.show(30)

    logger.info(s"df.rdd.getNumPartitions >> ${df.rdd.getNumPartitions}")

    // delete output path
    /*val outputPath = new Path(outputDir)
    val fs: FileSystem = outputPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
      logger.warn(s"delete ${outputPath}")
    }

    df.
      sort("key").
      write.
      options(Map(
        ("compression", "gzip"),
        ("parquet.block.size", s"${256 * 1024 * 1024}"),
        ("parquet.page.size", s"${2 * 1024 * 1024}")
      )).
      parquet(outputDir)
    */
  }
}
