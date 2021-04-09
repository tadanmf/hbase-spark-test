package io.datadynamics

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.convertScanToString
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellScanner, CellUtil, HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.api.java.JavaSparkContext.fromSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.junit.{Before, Test}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.text.SimpleDateFormat
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

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
      (chatLog.bjId, chatLog.bStartTime, chatLog.userId, chatLog.chatNow)
    }).map(kv => {
      //println(s"${kv._2}")
      val (bjId: String, startTime: Long, userId: String, chatNow: Long) = kv._1
      val chatLogs: Iterable[ChatLog] = kv._2
      val chat: String = chatLogs.map(chatLog => {
        chatLog.chatText
      }).mkString
      val chats: Array[Byte] = Bytes.toBytes(chatLogs.size) ++ chatLogs.map(chatLog => {
        val bos = new ByteArrayOutputStream(128)
        val dos = new DataOutputStream(bos)
        dos.writeUTF(chatLog.chatText)
        bos.toByteArray
      }).reduce(_ ++ _)

      val chatTime: String = rowKeySdf.format(chatNow)
      val bucket: Int = (s"${bjId}^${startTime}".hashCode & Int.MaxValue) % partitions
      //((s"${bucket}^${bjId}^${bStartTime}", userId, startTime), chats)
      ((s"${bucket}^${bjId}^${chatTime}", userId, startTime), chats)
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

    // data에서 column family 추출하여 저장할 변수
    //val cfs = spark.sparkContext.broadcast(mutable.Set[ColumnFamilyDescriptor]())

    // create cell
    val toCellRdd: RDD[(ImmutableBytesWritable, KeyValue)] = cellRdd.repartitionAndSortWithinPartitions(new CellPartitioner(partitions)).map(tempCell => {
      val (rowKeyQualifier: (String, String, Long), value: Array[Byte]) = tempCell
      val (rowkeyString: String, qualifierString: String, startTime: Long) = rowKeyQualifier
      val ts: Long = rowKeySdf.parse(rowkeyString.split("\\^", -1)(2)).getTime
      val startTimeStr: String = startTime.toString

      //cfs.value += ColumnFamilyDescriptorBuilder.of(startTimeStr)

      val rowkey: Array[Byte] = rowkeyString.getBytes
      val family: Array[Byte] = Bytes.toBytes("snappy_cf")
      //val family: Array[Byte] = Bytes.toBytes(startTimeStr)
      val qualifier: Array[Byte] = qualifierString.getBytes()
      //val value = Bytes.toBytes(chat)

      val cell = new KeyValue(rowkey, family, qualifier, ts, value)
      (new ImmutableBytesWritable(rowkey), cell)
    })

    // cfs 값 세팅을 위해 RDD action
    toCellRdd.count()
    //logger.info(s"${cfs.value}")

    // job, connection, admin
    val job: Job = Job.getInstance(hbaseConfig, "toCellJob")
    val admin: Admin = connection.getAdmin

    // create table
    //admin.createTable(TableDescriptorBuilder.newBuilder(tableName).setColumnFamilies(cfs.value.asJava).build())
    //logger.info(s"create table")

    //val tableNames: Array[TableName] = admin.listTableNames()
    //tableNames.foreach(name => logger.info(s"table name > ${name}"))

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
  }

  @Test
  def getDataMapreduceTest(): Unit = {
    // hbase setting
    val hbaseConfig: Configuration = HBaseConfiguration.create(spark.sparkContext.hadoopConfiguration)
    hbaseConfig.set("hbase.zookeeper.quorum", "tt05gn001.hdp.local,tt05nn001.hdp.local,tt05nn002.hdp.local")
    hbaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure")
    hbaseConfig.setInt("hbase.zookeeper.property.clientPort", 2181)

    val cf = "gzip_cf"        // column family
    val q = "jjjuuu"          // qualifier

    val scan = new Scan()
    scan.addColumn(cf.getBytes, q.getBytes)
    //scan.setLimit(4)

    hbaseConfig.set(TableInputFormat.INPUT_TABLE, "create_test")
    hbaseConfig.set(TableInputFormat.SCAN, convertScanToString(scan))

    val resultRdd: RDD[(ImmutableBytesWritable, Result)] =
      spark.sparkContext.newAPIHadoopRDD(hbaseConfig, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    logger.info(s"resultRdd.count() >> ${resultRdd.count()}")

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
  }

  @Test
  def getDataTest(): Unit = {
    // hbase setting
    val hbaseConfig: Configuration = HBaseConfiguration.create(spark.sparkContext.hadoopConfiguration)
    hbaseConfig.set("hbase.zookeeper.quorum", "tt05gn001.hdp.local,tt05nn001.hdp.local,tt05nn002.hdp.local")
    hbaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure")
    hbaseConfig.setInt("hbase.zookeeper.property.clientPort", 2181)

    val connection: Connection = ConnectionFactory.createConnection(hbaseConfig)
    val tableName: TableName = TableName.valueOf("create_test")
    val table: Table = connection.getTable(tableName)

    val userId = "jjjuuu"

    val scan = new Scan()
    scan.addColumn("1582635623000".getBytes(), userId.getBytes())
    scan.setLimit(4)

    val scanner: ResultScanner = table.getScanner(scan)
    val it: util.Iterator[Result] = scanner.iterator()
    while (it.hasNext) {
      val cb: CellScanner = it.next().cellScanner()
      while (cb.advance()) {
        val cell: Cell = cb.current()
        val value: Array[Byte] = CellUtil.cloneValue(cell)
        //logger.info(s"${new String(value)}")

        val dis = new DataInputStream(new ByteArrayInputStream(value))
        val size: Int = dis.readInt()
        val chatMessage: String = dis.readUTF()
        logger.info(s"[${userId}] ${chatMessage}")
      }
    }
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
