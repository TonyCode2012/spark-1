package org.apache.spark.adaptive

import java.io.{InputStreamReader, BufferedReader}
import java.lang.management.{MemoryUsage, ManagementFactory, MemoryMXBean}

import org.apache.spark._
import org.apache.spark.executor.Executor
import org.apache.spark.Logging

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.sys.process._
import ExecutionContext.Implicits.global

/**
 * Created by spark on 15-9-27.
 */
private[spark] class CollectData(
    env: SparkEnv,
    executor: Executor)
  extends Runnable with Logging{

  //system configuration
  private[spark] val conf = env.conf

  /** collect runtime data.by yaoz*/
  private[spark] val memoryData = HashMap[Long,Double]()
  private[spark] val instanceData = HashMap[Long,(Long,Double)]()
  private[spark] val deserializeTime = HashMap[String,Double]()
  private[spark] val serializeTime = HashMap[String,Double]()
  private[spark] val deAndSerializationTime = HashMap[Long,Long]()

  /** base metrics byte/s.*/
  private[spark] var baseResSerTimeByByte: Double = 0
  private[spark] var baseDirectResSerTimeByByte: Double = 0
  private[spark] var baseDeserTimeByByte: Double = 0

  //set sleep timeout
  private[spark] var sleepTimeout: Long = 3000

  //memory threshold
  private[spark] val memoyUpperThreshold =
      conf.getOption("spark.storage.memoryFraction").map(_.toDouble).getOrElse(0.5) - 0.2
  private[spark] val memoryLowerThreshold =
    conf.getOption("spark.storage.memoryFraction").map(_.toDouble).getOrElse(0.5) - 0.3

  //if jvm heap volume reaches indicated threshold,try to adopt KryoSerializer
  private[spark] var adoptKryoSerializer: Boolean = false
  private[spark] var conKryoSerializer: Int = 0
  private[spark] var proKryoSerializer: Int = 0
  
  //current serializer
  private [spark] var serializerCur = "org.apache.spark.serializer.JavaSerializer"

  //strategy decision
  private[spark] var strategyDecision: StrategyDecision = new StrategyDecision(env,executor)

  override def run(): Unit = {
    while(true) {
      //===============get freeMemory==================//
      /** system JVM HEAP parameters.*/
      val memorymbean: MemoryMXBean = ManagementFactory.getMemoryMXBean
      val memoryUsage: MemoryUsage = memorymbean.getHeapMemoryUsage
      val heapMemMax = memoryUsage.getMax
      val heapMemUsage = memoryUsage.getUsed
      val heapMemFree = memoryUsage.getInit - memoryUsage.getUsed
      logInfo(s"INIT HEAP:${memoryUsage.getInit.toDouble / 1024 / 1024}MB")
      logInfo(s"MAX HEAP:${memoryUsage.getMax.toDouble / 1024 / 1024 / 1024}GB")
      logInfo(s"USE HEAP:${memoryUsage.getUsed.toDouble / 1024 / 1024}MB")
      logInfo(s"HEAP MEMORY USAGE:${heapMemUsage.toDouble / 1024 / 1024}MB")
      logInfo(s"HEAP MEMORY FREE:${heapMemFree.toDouble / 1024 / 1024}MB")
      logInfo(s"NON-HEAP MEMORY USAGE:${memorymbean.getNonHeapMemoryUsage}")

      val currentTime = System.currentTimeMillis
      val memoryUsageFraction = heapMemUsage.toDouble / heapMemMax.toDouble
      memoryData(currentTime) = memoryUsageFraction

      //===============get jvm heap instance number==============//
      try {
        val pid: String = ManagementFactory.getRuntimeMXBean.getName.replaceAll("(\\d+)@.*", "$1")
        //val cmd_result = Runtime.getRuntime.exec("jmap -histo " + pid)
        val cmd = Seq("jmap","-histo",pid)
        val cmd_result: StringBuilder = new StringBuilder(cmd.!!)
        cmd_result(cmd_result.length - 1) = '#'
        val text = cmd_result.toString()
        val lastLine = text.substring(text.lastIndexOf("\n")+1, text.length-1)
        logInfo(s"total num of current instance is:$lastLine")
        //get instance data
        val curInstanceData = lastLine
        var totalInstanceNum = 0
        var totalInstanceBytes = 0
        var totalInstanceNumCur = curInstanceData.indexOf(" ")
        val totalInstanceBytesCur = curInstanceData.lastIndexOf(" ")
        //get all instance number in jvm
        while(curInstanceData(totalInstanceNumCur)==' ') totalInstanceNumCur += 1
        totalInstanceNum = curInstanceData.substring(totalInstanceNumCur,
          curInstanceData.indexOf(" ",totalInstanceNumCur)-1).toInt
        //get all instance volume
        totalInstanceBytes = curInstanceData.substring(totalInstanceBytesCur+1,
          curInstanceData.length-1).toInt
        //store the data
        instanceData(currentTime) = (totalInstanceNum,totalInstanceBytes)

      } catch {
        case t: Throwable =>
          logError(s"invoke jmap is failed,info is ${t.getMessage}")
      }

      //====================get I/O information====================//

      /** when memory usage reaches threshold change storage strategy.*/
      if(memoryUsageFraction > memoyUpperThreshold && !adoptKryoSerializer){
        conKryoSerializer += 1
        if(proKryoSerializer > 0) {
          proKryoSerializer -= 1
        }
        if(conKryoSerializer >= 3) {
          serializerCur = "org.apache.spark.serializer.KryoSerializer"
          strategyDecision.changeSerializer(serializerCur)
          conKryoSerializer = 0
        }
      } else if(memoryUsageFraction < memoryLowerThreshold && adoptKryoSerializer){
        proKryoSerializer += 1
        if(conKryoSerializer > 0) {
          conKryoSerializer -= 1
        }
        if(proKryoSerializer >= 3) {
          serializerCur = "org.apache.spark.serializer.JavaSerializer"
          strategyDecision.changeSerializer(serializerCur)
          proKryoSerializer = 0
        }
      } else {
        if(conKryoSerializer > 0) {
          conKryoSerializer -= 1
        }
        if(proKryoSerializer > 0) {
          proKryoSerializer -= 1
        }
      }

      /** set collection data interval to 1ms. */
      try {
        Thread.sleep(sleepTimeout)
      }catch {
        case t: Throwable=>
          logError(s"invoking sleep occurs problem,info is ${t.getMessage}")
      }
    }
  }

  private[spark] def setSleepTimeout(timeout: Long): Unit ={
    this.sleepTimeout = timeout
  }
}

object CollectData{

  //indicate whether the collecting data DeamonThread is started
  private[spark] var isStarted: Boolean = false

  //whether collector is running
  private[spark] var isRunning: Boolean = false
}