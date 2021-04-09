package io.datadynamics

import com.google.common.collect.{ArrayListMultimap, HashBasedTable}
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

import java.util
import java.util.Calendar
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{immutable, mutable}

class ScalaCollectionTest {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  @Test
  def listTest(): Unit = {
    // map, filter, flatMap, groupBy, reduce
    val l = List(1, 2, 3)
    val sl = List("str1", "str2", "str3", "str2", "sTr2")

    // map
    val mapPlus: List[Int] = l.map(x => x + 1)
    logger.info(s"mapPlus = ${mapPlus}")

    // filter
    val filterEven: List[Int] = l.filter(x => x % 2 == 0)
    logger.info(s"filterEven = ${filterEven}")

    // flatMap
    val flatToString: List[Char] = l.flatMap(x => x.toString)
    logger.info(s"flatToString = ${flatToString}")

    val flatX: List[Char] = sl.flatMap(x => x)
    logger.info(s"flatX = ${flatX}")

    val flatSplit: List[String] = sl.flatMap(x => x.split("s"))
    logger.info(s"flatSplit = ${flatSplit}")

    val mapSplit: List[Array[String]] = sl.map(x => x.split("s"))
    logger.info(s"mapSplit = ${mapSplit}")

    val flatUpper: List[Char] = sl.flatMap(_.toUpperCase)
    logger.info(s"flatUpper = ${flatUpper}")
    logger.info(s"mapUpper = ${sl.map(_.toUpperCase)}")

    // groupBy
    logger.info(s"groupBy length = ${sl.groupBy(_.length)}")
    logger.info(s"disct groupBy length = ${sl.distinct.groupBy(_.length)}")
    logger.info(s"groupBy string = ${sl.groupBy(_.toString)}")

    // reduce
    logger.info(s"reduce plus = ${l.reduce((x, y) => x + y)}")
    logger.info(s"reduce concat = ${sl.reduce((x, y) => x.concat(y))}")
    logger.info(s"map concat = ${sl.map(x => x.concat(x))}")
  }

  @Test
  def mapTest(): Unit = {
    //    val l2: List[(Int, String)] = List((1, "one"))
    //    l2.toMap
    val intMap = Map(1 -> "one", 2 -> "two", (3, "three"))

    // map
    intMap.map((x) => {
      logger.info(s"value = ${x._2}")
    })

    logger.info(s"map = ${intMap.map(x => x)}")

    // filter
    logger.info(s"filter = ${intMap.filter(x => x._1 > 2)}")

    // flatMap
    val flatUpper: immutable.Iterable[Char] = intMap.flatMap(x => x._2.toUpperCase)
    logger.info(s"flatUpper > ${flatUpper}")
    intMap.map(x => {
      logger.info(s"mapUpper(${x._1}) > ${x._2.toUpperCase}")
    })

    // groupBy
    logger.info(s"groupBy > ${intMap.groupBy(x => x._2.length)}")

    // reduce
    val sumConcat: (Int, String) = intMap.reduce((x, y) => {
      (x._1 + y._1, x._2.concat(y._2))
    })
    logger.info(s"sumConcat = ${sumConcat}")
  }

  @Test
  def zipTest(): Unit = {
    val seq1 = Seq(1, 2, 3)
    val seq2 = Seq("one", "two", "three")
    val seq3 = Seq("one1", "two2", "three3")
    logger.info(s"${seq1.zip(seq2).zip(seq3).toMap}")
    logger.info(s"${seq1.zip(seq2.zip(seq3)).toMap}")
    val map: Map[Int, String] = seq1.zip(seq2).toMap
    logger.info(s"map = ${map}")
    logger.info(s"zipWithIndex > ${seq2.zipWithIndex}")
  }

  @Test
  def plusTest(): Unit = {
    // append(element + list, list + element, elem + elem, list + list)
    // prepend(element + list, list + element, elem + elem, list + list)
    // :+ +: :++ ++: ++
    var strList = List("t1", "t2")
    val intList = List(1, 2, 3)
    val text3: String = "t3"
    val strVector = Vector(9, 8)

    logger.info(s"strList :+ text3 >> ${strList :+ text3}")
    logger.info(s"strList :+ intList >> ${strList :+ intList}")
    logger.info(s"strList +: text3 >> ${strList +: text3}")
    logger.info(s"strList +: intList >> ${strList +: intList}")
    //    logger.info(s":++ >> ${strList :++ text3}") // 2.13부터 가능
    logger.info(s"strList ++: intList >> ${strList ++: intList}")
    logger.info(s"strList ++ intList >> ${strList ++ intList}")
    logger.info(s"strList ++ strVector >> ${strList ++ strVector}")
    logger.info(s"text3 ++ strVector >> ${text3 ++ strVector}")
    logger.info(s"strList += text3 >> ${strList = strList :+ text3}")
    logger.info(s"strList >> ${strList}")
  }

  @Test
  def multimapTest(): Unit = {
    // google guava
    // ArrayListMultimap: ArrayList를 value로 담을 수 있는 map. 동일 key에 put 할 경우 append.
    // HashBasedTable

    val multiMap: ArrayListMultimap[String, String] = ArrayListMultimap.create() // Map<String, ArrayList<String>>
    multiMap.put("key1", "value1")
    multiMap.put("key2", "value2")
    multiMap.put("key2", "value2")
    multiMap.put("key3", "value3")
    logger.info(s"${multiMap}")
    //    logger.info(s"${multiMap.get("key2")}")
    multiMap.put("key2", "value2_1")
    logger.info(s"${multiMap}")

    val listMap: ArrayListMultimap[String, List[Int]] = ArrayListMultimap.create()
    val strList = List("t1", "t2")
    val intList = List(1, 2, 3)
    val intList2 = List(4, 5)
    listMap.put(strList(0), intList)
    logger.info(s"${listMap}")
    listMap.put(strList(0), intList2)
    logger.info(s"${listMap}")

    val testTable: HashBasedTable[Int, String, String] = HashBasedTable.create()
    testTable.put(1, "column1", "value1")
    logger.info(s"${testTable}")
    val columnMap: mutable.Map[String, String] = testTable.row(1).asScala
    logger.info(s"columnMap = ${columnMap}")
    val rowMap: mutable.Map[Int, String] = testTable.column("column1").asScala
    logger.info(s"rowMap = ${rowMap}")
  }

  @Test
  def foldTest(): Unit = {
    val keys = Seq(1, 2, 3)
    val values1 = Seq("one", "two", "three")
    val values2 = Seq("하나", "둘", "셋")
    // 1 -> ("one", "하나"), ...

    // 6
    //    logger.info(s"${Seq(1, 2, 3).fold(0)((x, i) => x+i)}")

    //    val multiMap: ArrayListMultimap[Any, Any] = ArrayListMultimap.create()

    //    val result = keys.fold() {(x, i) =>
    //      multiMap.put
    //    }

    // List(((one,0),(하나,0)), ((two,1),(둘,1)), ((three,2),(셋,2)))
    logger.info(s"${values1.zipWithIndex.zip(values2.zipWithIndex)}")
    // List(((one,하나),0), ((two,둘),1), ((three,셋),2))
    logger.info(s"${values1.zip(values2).zipWithIndex}")

    val result = values1.zip(values2).zipWithIndex.fold(ArrayListMultimap.create()) { (x, i) =>
      //      val test: ArrayListMultimap[Any, Any] = x
      //      x ++ i
      val temp = i.asInstanceOf[(Any, Any)]
      x.asInstanceOf[ArrayListMultimap[Any, Any]].put(temp._2, temp._1)
      x
    }
    logger.info(s"result >> ${result}")

    /*values1.zipWithIndex.foreach {
      case(value, count) => multiMap.put(count+1, value)
    }
    values2.zipWithIndex.foreach{
      case(value, count) => multiMap.put(count+1, value)
    }
    logger.info(s"${multiMap}")
    */

    /*val lists = Seq(keys, values1, values2)
    val result = lists.fold(Seq[String]()) {(x, i) =>
      // List(1, 2, 3, one, two, three, 하나, 둘, 셋)
//      x++i
      // 하나
      i(0).toString
    }*/


    /*val result = values1.fold((values2, Seq[String]())) {
      case ((a, b), c) => (a, b, c)
    }*/

    /*keys.fold(values1, values2) { (a, b) =>
      // {1=[(List(one, two, three),List(하나, 둘, 셋))], 2=[true], 3=[true]}
      multiMap.put(b, a)
    }
    logger.info(s"${multiMap}")
    */

    /*val lists = Seq(values1, values2)
    val result = keys.fold(values1, values2) {(x, i) =>
//      ((Seq)x)(0)
    }
    logger.info(s"${result}")
    */

    logger.info(s"${keys.zip(values1.zip(values2).toList)}")
    logger.info(s"${keys.zip(values1.zip(values2).toList).toMap}")
    val values: Seq[(Int, (String, String))] = keys.zip(values1.zip(values2))
    val result2 = values.fold(ArrayListMultimap.create[Int, String]()) { (x, i) =>
      i
      //      x.asInstanceOf[ArrayListMultimap[Any, Any]].put()
    }
    logger.info(s"result2 >> ${result2}")

    val v: ArrayListMultimap[Int, String] = values.foldLeft(ArrayListMultimap.create[Int, String]())(
      //    val v: ArrayListMultimap[Int, String] = values./:(ArrayListMultimap.create[Int, String]())(
      (accum, elem) => {
        accum.put(elem._1, elem._2._1)
        accum.put(elem._1, elem._2._2)
        accum
      }
    )
    logger.info(s"v = ${v}")
  }

  @Test
  def tupleTest(): Unit = {
    val t1: (String, String) = ("one", "하나")
    val t2: Tuple2[String, String] = "two" -> "둘"
    t1._1
    t1._2
//    Tuple22
  }

  @Test
  def functionTest() {
    val op1: Int => Int = a => a + 1

    def op2(a: Int): Int = a + 1

    val op3: Int => Int = _ + 1

    val l = List(1, 2, 3)
    l.map(x => {
      x + 1
    })
    l.map(x => x + 1)
    l.map(_ + 1)
    l.map(op1)
    l.map(op2)
    l.map(op3)
  }

  @Test
  def mutableListTest(): Unit = {
    // mutable list
    logger.info(s"start first test")
    val list = new mutable.MutableList[(String, String)]()
    for (x <- 1 to 10000000) {
      val tuple: (String, String) = (x.toString, x.toString)
      list += tuple
    }
//    list += 4
//    list ++= List(11, 12, 13, 14, 15, 16)
//    val strings = new ListBuffer[String]()
//    val arraySeq = new mutable.ArraySeq[String](5)
    //val strings1: mutable.ArraySeq[String] = arraySeq :+ "Asdf"
    //logger.info(s"arraySeq = ${arraySeq}")
    //logger.info(s"${list}")
    logger.info(s"complete")
  }

  @Test
  def arrayBufferTest(): Unit = {
    // ArrayBuffer
    logger.info(s"start second test")
//    val arrayBuffer = ArrayBuffer(1)
    val arrayBuffer:ArrayBuffer[(String, String)] = new ArrayBuffer[(String, String)]()
    for (x <- 1 to 10000000) {
      val tuple: (String, String) = (x.toString, x.toString)
      arrayBuffer += tuple
    }
//    arrayBuffer += 4
//    arrayBuffer -= 3
//    arrayBuffer ++= List(11, 12, 13, 14, 15, 16)
//    logger.info(s"${arrayBuffer}")
    logger.info(s"complete")
  }

  @Test
  def mutableListVersusArrayBufferTest(): Unit = {
    // 속도 차이
    val startTime: Long = System.nanoTime()
    mutableListTest()     // 19 sec
    val middleTime = System.nanoTime()
    logger.info(s"${TimeUnit.SECONDS.convert((middleTime - startTime), TimeUnit.NANOSECONDS)} seconds")
    arrayBufferTest()     // 5 sec
    val endTime: Long = System.nanoTime()
    logger.info(s"${TimeUnit.SECONDS.convert((endTime - middleTime), TimeUnit.NANOSECONDS)} seconds")
  }

  @Test
  def mutableMapTest(): Unit = {
    // mutable map
    val map = mutable.Map[Int, String]()
    map += (1 -> "one")
    map += (2 -> "two")
    map.put(3, "three")
    map.put(4, "four")
    map += (5 -> "five")
    map -= 4
    map.put(1, "one_2")
    map.foreach(m => logger.info(s"(${m._1}, ${m._2})"))
    map.retain((k, v) => k < 4)
    map.foreach(m => logger.info(s"k < 4: (${m._1}, ${m._2})"))
    map.transform((k, v) => v + "_test")
    map.foreach(m => logger.info(s"v + test: (${m._1}, ${m._2})"))
    val map2 = map.filterKeys(_ > 1)
    map2.foreach(m => logger.info(s"filter map: (${m._1}, ${m._2})"))
  }

  @Test
  def mutableSetTest(): Unit = {
    // mutable set
    val set = mutable.Set[Int]()
    set += 2
    set ++= mutable.Set(2, 3, 4, 5, 6, 7, 8, 9)
    set -= 3
    set --= mutable.Set(7, 8)
    logger.info(s"${set}")
    logger.info(s"set.slice(2, 4) >> ${set.slice(2, 4)}")
    set.retain(s => s > 5)
    logger.info(s"${set}")
    logger.info(s"set.sum >> ${set.sum}") // Numeric
    val set2 = mutable.Set[String]("one", "two")
    //logger.info(s"${set2.sum}")
    logger.info(s"set.toList >> ${set.toList}")
  }
}
