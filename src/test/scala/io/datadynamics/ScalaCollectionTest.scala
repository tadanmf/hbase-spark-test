package io.datadynamics

import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable

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
  }

  @Test
  def plusTest(): Unit = {
    // append(element + list, list + element, elem + elem, list + list)
    // prepend(element + list, list + element, elem + elem, list + list)
    // :+ +: :++ ++: ++
  }

  @Test
  def multimapTest(): Unit = {
    // google guava
    // ArrayListMultimap
    // HashBasedTable
  }
}
