package io.datadynamics

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.junit.Test

class HdfsTest {

  @Test
  def connectTest(): Unit = {
    //    val path = new Path("hdfs://nn/download/input/")
    val path = new Path("hdfs://172.30.1.243:8020/download/input/")
    val conf = new Configuration()
    //    conf.set("fs.defaultFS", "hdfs://nn")
    conf.set("fs.defaultFS", "hdfs://172.30.1.243")
    conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    //    conf.addResource(new Path("file:///test/resource/core-site.xml"))
    val fs: FileSystem = path.getFileSystem(conf)

    val length: Int = fs.listStatus(path).length
    println(s"length = ${length}")
  }
}
