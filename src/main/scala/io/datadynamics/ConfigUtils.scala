package io.datadynamics

import com.typesafe.config.Config
import java.lang.reflect.{Field, Method}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.JavaConverters._
/**
 * description
 *
 * @author Haneul, Kim
 */
object ConfigUtils {
  implicit class RichConfig(config: Config) {
    //def toSource: BaseSource = invokeApply.asInstanceOf[BaseSource]
    //def toProcessor: BaseProcessor = invokeApply.asInstanceOf[BaseProcessor]
    //def toSink: BaseSink = invokeApply.asInstanceOf[BaseSink]
    def invokeApply: AnyRef = {
      val objectString: String = config.getString("object")
      val objectField: Field = Class.forName(s"${objectString}$$").getField("MODULE$")
      val objectClass: Class[_ <: AnyRef] = objectField.get(null).getClass
      val applyMethod: Method = objectClass.getDeclaredMethod("apply", classOf[Config])
      applyMethod.invoke(objectField.get(null), config)
    }
    def getBoolean(path: String, defaultValue: Boolean): Boolean = {
      if (config.hasPath(path)) config.getBoolean(path) else defaultValue
    }
    def getBooleanList(path: String, defaultValue: Seq[Boolean]): Seq[Boolean] = {
      if (config.hasPath(path)) config.getBooleanList(path).asScala.map(_.booleanValue()) else defaultValue
    }
    def getBytes(path: String, defaultValue: Long): Long = {
      if (config.hasPath(path)) config.getBytes(path) else defaultValue
    }
    def getBytesList(path: String, defaultValue: Seq[Long]): Seq[Long] = {
      if (config.hasPath(path)) config.getBytesList(path).asScala.map(_.longValue()) else defaultValue
    }
    def getDate(path: String): Date = getDate(s"${path}.date", s"${path}.format")
    def getDate(datePath: String, formatPath: String): Date = {
      val format: String = config.getString(formatPath)
      val sdf = new SimpleDateFormat(format)
      sdf.parse(config.getString(datePath))
    }
    def getDate(path: String, defaultValue: Date): Date = {
      getDate(s"${path}.date", s"${path}.format", defaultValue)
    }
    def getDate(datePath: String, formatPath: String, defaultValue: Date): Date = {
      if (config.hasPath(datePath) && config.hasPath(formatPath)) getDate(datePath) else defaultValue
    }
    def getDouble(path: String, defaultValue: Double): Double = {
      if (config.hasPath(path)) config.getDouble(path) else defaultValue
    }
    def getDoubleList(path: String, defaultValue: Seq[Double]): Seq[Double] = {
      if (config.hasPath(path)) config.getDoubleList(path).asScala.map(_.doubleValue()) else defaultValue
    }
    def getInt(path: String, defaultValue: Int): Int = {
      if (config.hasPath(path)) config.getInt(path) else defaultValue
    }
    def getIntList(path: String, defaultValue: Seq[Int]): Seq[Int] = {
      if (config.hasPath(path)) config.getIntList(path).asScala.map(_.intValue()) else defaultValue
    }
    def getLong(path: String, defaultValue: Long): Long = {
      if (config.hasPath(path)) config.getLong(path) else defaultValue
    }
    def getLongList(path: String, defaultValue: Seq[Long]): Seq[Long] = {
      if (config.hasPath(path)) config.getLongList(path).asScala.map(_.longValue()) else defaultValue
    }
    def getString(path: String, defaultValue: String): String = {
      if (config.hasPath(path)) config.getString(path) else defaultValue
    }
    def getStringList(path: String, defaultValue: Seq[String]): Seq[String] = {
      if (config.hasPath(path)) config.getStringList(path).asScala else defaultValue
    }
  }
}