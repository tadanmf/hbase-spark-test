package io.datadynamics

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.junit.{Before, Test}
import org.slf4j.{Logger, LoggerFactory}

class ParseTest extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val sparkConfig: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Spark Test Hbase")
  private val spark: SparkSession = SparkSession.builder().config(sparkConfig).getOrCreate()

  @Before
  def init() {
    //    conf.set("fs.defaultFS", "hdfs://172.30.1.243")
    //    conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    sparkConfig.set("spark.hadoop.fs.defaultFS", "hdfs://172.30.1.243")
    sparkConfig.set("spark.hadoop.fs.hdfs.impl", classOf[DistributedFileSystem].getName)
  }

  @Test
  def fsTest() {
    val path = new Path("hdfs://172.30.1.243:8020/download/input/")
    val fs: FileSystem = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val length: Int = fs.listStatus(path).length
    println(s"length = ${length}")

    //    val files= sc.textFile(chatPath)
    //    logger.info(s"${files.count()}")
    //    logger.info(s"${files.take(5)}")

    //    files.take(5).foreach(s => logger.info(s"${s}"))
    //    files.map(line => line.map(unwrap => logger.info(s"${unwrap}")))

    //    val counts: RDD[(String, Int)] = files.flatMap(line => line.split("\t")).map(word => (word, 1)).reduceByKey(_ + _)
    //    logger.info(s"count > ${counts}")
  }

  @Test
  def parse() {
    val chatPath = "hdfs://172.30.1.243:8020/download/input/chat_1582635623000.log"

    // hbase setting
    /*val hbaseConfig: Configuration = HBaseConfiguration.create(conf)
    val connection: Connection = ConnectionFactory.createConnection(hbaseConfig)
    val admin: Admin = connection.getAdmin
    */

    // create table
    /*val table = TableDescriptorBuilder.newBuilder(TableName.valueOf("create_test")).setColumnFamily(ColumnFamilyDescriptorBuilder.of("create_cf"))
    admin.createTable(table.build())
    logger.info(s"create table")

    val tableNames: Array[TableName] = admin.listTableNames()
    tableNames.foreach(tableName => logger.info(s"table name > ${tableName}"))
    */

    // scan log file
    val partitions = 6
    val baseRdd: RDD[ChatLog] = spark.sparkContext.textFile(chatPath, partitions).map(line => {
      ChatLog.create(line)
    })
    baseRdd.take(5).map(log => logger.info(s"baseRdd > ${log}"))

    val cellRdd: RDD[((String, Long, String), Iterable[ChatLog])] = baseRdd.groupBy(chatLog => {
      (chatLog.bjId, chatLog.bStartTime, chatLog.userId)
    })

    logger.info(s"${cellRdd.count()}")
    cellRdd.take(3).map(log => logger.info(s"cellRdd > ${log._1}"))

    // create cell
    /*val toCellRdd: RDD[(ImmutableBytesWritable, KeyValue)] = baseRdd.map(log => {
      logger.info(s"${log}")
      val rowkey: Array[Byte] = Bytes.toBytes(log.chatNow)
      val family: Array[Byte] = Bytes.toBytes(log.bStartTime)
      val qualifier: Array[Byte] = log.userId.getBytes()
      val value: Array[Byte] = log.chatText.getBytes()

      val cell = new KeyValue(rowkey, family, qualifier, value)
      (new ImmutableBytesWritable(rowkey), cell)
    })
    toCellRdd.saveAsNewAPIHadoopFile()
    */

//    toCellRdd.take(5).map(rdd => logger.info(s"${rdd}"))


    // create hfile
    //    val outputDir = "hdfs://172.30.1.243:8020/download/output_hfile"
    //    val outputPath = new Path(outputDir)

  }

  @Test
  def toCellTest(): Unit = {
    val rowkey: Array[Byte] = Bytes.toBytes(102493867)
    val family: Array[Byte] = Bytes.toBytes("create_test")
    val qualifier: Array[Byte] = "log.userId".getBytes()
    val value: Array[Byte] = "log.chatText".getBytes()

    val cell = new KeyValue(rowkey, family, qualifier, value)
    val tuple: (ImmutableBytesWritable, KeyValue) = (new ImmutableBytesWritable(rowkey), cell)
    logger.info(s"tuple = ${tuple}")
  }
}
