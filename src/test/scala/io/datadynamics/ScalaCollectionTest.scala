package io.datadynamics

import com.google.common.collect.{ArrayListMultimap, HashBasedTable}
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{immutable, mutable}
import scala.collection.JavaConverters._
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
    val map: Map[Int, String] = seq1.zip(seq2).toMap
    logger.info(s"map = ${map}")
    logger.info(s"zipWithIndex > ${seq2.zipWithIndex}")
  }

  @Test
  def plusTest(): Unit = {
    // append(element + list, list + element, elem + elem, list + list)
    // prepend(element + list, list + element, elem + elem, list + list)
    // :+ +: :++ ++: ++
    val strList = List("t1", "t2")
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
  }

  @Test
  def multimapTest(): Unit = {
    // google guava
    // ArrayListMultimap: ArrayList를 value로 담을 수 있는 map. 동일 key에 put 할 경우 append.
    // HashBasedTable

    val multiMap: ArrayListMultimap[String, String] = ArrayListMultimap.create()// Map<String, ArrayList<String>>
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
  }
}
