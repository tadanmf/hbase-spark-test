package io.datadynamics

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partitioner, SparkConf}
import org.junit.{Before, Test}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.text.SimpleDateFormat

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
    val hbaseConfig: Configuration = HBaseConfiguration.create(spark.sparkContext.hadoopConfiguration)
    hbaseConfig.set("hbase.zookeeper.quorum", "tt05gn001.hdp.local,tt05nn001.hdp.local,tt05nn002.hdp.local")
    hbaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure")
    hbaseConfig.setInt("hbase.zookeeper.property.clientPort", 2181)

    // scan log file
    val partitions = 6
    val baseRdd: RDD[ChatLog] = spark.sparkContext.textFile(chatPath, partitions).map(line => {
      ChatLog.create(line)
    })
    //    baseRdd.take(5).map(log => logger.info(s"baseRdd > ${log}"))

    // set input format
    val rowKeySdf = new SimpleDateFormat("yyyyMMdd-HHmmss.SSS")
    val cellRdd: RDD[((String, String, Long), Array[Byte])] = baseRdd.groupBy(chatLog => {
      (chatLog.bjId, chatLog.bStartTime, chatLog.userId)
    }).map(kv => {
      val (bjId: String, startTime: Long, userId: String) = kv._1
      val chatLogs: Iterable[ChatLog] = kv._2
      val chats: Array[Byte] = Bytes.toBytes(chatLogs.size) ++ chatLogs.map(chatLog => {
        val bos = new ByteArrayOutputStream(128)
        val dos = new DataOutputStream(bos)
        bos.toByteArray
      }).reduce(_ ++ _)

      val bStartTime: String = rowKeySdf.format(startTime)
      val bucket: Int = (s"${bjId}^${startTime}".hashCode & Int.MaxValue) % partitions
      ((s"${bucket}^${bjId}^${bStartTime}", userId, startTime), chats)
    })
    logger.info(s"${cellRdd.count()}")
    //cellRdd.take(3).map(log => logger.info(s"cellRdd > ${log._1}"))
    //cellRdd.take(3).map(log => logger.info(s"cellRdd > ${log._2}"))

    class CellPartitioner(partitions: Int) extends Partitioner {
      override def numPartitions: Int = partitions

      override def getPartition(key: Any): Int = key.asInstanceOf[(String, String, Long)]._1.split("\\^")(0).toInt
    }

    val connection: Connection = ConnectionFactory.createConnection(hbaseConfig)
    val tableName: TableName = TableName.valueOf("create_test")

    // broadcast
    // transformation, action

    //val cfs = spark.sparkContext.broadcast(new util.HashSet[ColumnFamilyDescriptor]())
    var cfs = spark.sparkContext.broadcast(List("one"))

    // create cell
    val toCellRdd: RDD[(ImmutableBytesWritable, KeyValue)] = cellRdd.repartitionAndSortWithinPartitions(new CellPartitioner(partitions)).map(tempCell => {
      val (rowKeyQualifier: (String, String, Long), value: Array[Byte]) = tempCell
      val (rowkeyString: String, qualifierString: String, startTime: Long) = rowKeyQualifier
      val ts: Long = rowKeySdf.parse(rowkeyString.split("\\^", -1)(2)).getTime
      val startTimeStr: String = startTime.toString

      //      cfs = (cfs.value :+ startTimeStr).asInstanceOf[Broadcast[List[String]]]
      cfs.value
      cfs

      val rowkey: Array[Byte] = rowkeyString.getBytes
      val family: Array[Byte] = Bytes.toBytes(startTimeStr)
      val qualifier: Array[Byte] = qualifierString.getBytes()

      val cell = new KeyValue(rowkey, family, qualifier, ts, value)
      (new ImmutableBytesWritable(rowkey), cell)
    })

    logger.info(s"${cfs.value.size}")
    logger.info(s"${cfs.value(0)}")
    logger.info(s"${cfs.value}")

    // job, connection, admin
    val job: Job = Job.getInstance(hbaseConfig, "toCellJob")
    val admin: Admin = connection.getAdmin

    // create table
    //    admin.createTable(TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(ColumnFamilyDescriptorBuilder.of()).build())
    //    admin.createTable(TableDescriptorBuilder.newBuilder(tableName).setColumnFamilies(cfs).build())
    logger.info(s"create table")

    /*val tableNames: Array[TableName] = admin.listTableNames()
    tableNames.foreach(name => logger.info(s"table name > ${name}"))

    val regionLocator: RegionLocator = connection.getRegionLocator(tableName)
    val table: Table = connection.getTable(tableName)

    // create hfile
    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)
    val toCellConf: Configuration = job.getConfiguration

    val outputDir = "hdfs://172.30.1.243:8020/download/output_hfile"
    val outputPath = new Path(outputDir)
    val fs: FileSystem = outputPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true)
      logger.warn(s"delete ${outputPath}")
    }
    toCellRdd.saveAsNewAPIHadoopFile(outputDir, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], toCellConf)

    val loadIncrementalHFiles = new LoadIncrementalHFiles(hbaseConfig)
    logger.info("bulk loading ...")
    loadIncrementalHFiles.doBulkLoad(outputPath, admin, table, regionLocator, false, true)
    logger.info("bulk load success")
    */
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
