package org.apache.spark.adaptive

import org.apache.spark._
import org.apache.spark.SparkEnv._
import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils

/**
 * Created by spark on 15-9-27.
 */
class StrategyDecision
  extends Logging{

  //get collect data
  private[spark] var runtimeStatistics: CollectData = null

  def decision(runtimeStatistics: CollectData,env: SparkEnv): Unit ={
    env.conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
  }
}
