package io.datadynamics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class SparkTest {

  private val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Spark Local Test")
  /*lazy*/ val spark: SparkSession = SparkSession.builder().config(sparkConf). /*enableHiveSupport().*/ getOrCreate()
  /*lazy*/ val sc: SparkContext = spark.sparkContext

  @Test
  def sparkTest(): Unit = {
    val rdd: RDD[String] = sc.textFile("/download/input")
    // map, filter, keyBy, flatMap, groupBy, reduce, reduceByKey
    rdd.map(s => {

    })
  }
}
