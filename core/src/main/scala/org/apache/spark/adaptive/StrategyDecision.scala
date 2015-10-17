package org.apache.spark.adaptive

import org.apache.spark._
import org.apache.spark.executor.Executor
import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils

/**
 * Created by spark on 15-9-27.
 */
class StrategyDecision(
    env: SparkEnv,
    executor: Executor)
  extends Logging{

  //get env
  private[spark] val conf = env.conf

  //get collect data
  private[spark] var runtimeStatistics: CollectData = null

  def changeSerializer(serializerName: String): Unit ={
    conf.set("spark.serializer",serializerName)
    val defaultClassName = "org.apache.spark.serializer.JavaSerializer"
    executor._resultSer = instantiateClass[Serializer](conf.
      get("spark.serializer", defaultClassName))
    logInfo(s"change serializer to ${serializerName}")
  }

  // Create an instance of the class with the given name, possibly initializing it with our conf
  def instantiateClass[T](className: String): T = {
    val cls = Utils.classForName(className)
    // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
    // SparkConf, then one taking no arguments
    try {
      cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
        .newInstance(conf, new java.lang.Boolean(false))
        .asInstanceOf[T]
    } catch {
      case _: NoSuchMethodException =>
        try {
          cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
        } catch {
          case _: NoSuchMethodException =>
            cls.getConstructor().newInstance().asInstanceOf[T]
        }
    }
  }
}
